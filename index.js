// Dotenv
const dotenv = require("dotenv");
dotenv.config();

// Required dependencies
const csvtojson = require("csvtojson"); // csvtojson
const mysql = require("mysql2/promise"); // msyql
const AWS = require("aws-sdk"); // aws
const e = require("express");

// AWS config
AWS.config.update({
  accessKeyId: process.env.MY_AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.MY_AWS_SECRET_ACCESS_KEY,
  region: process.env.MY_AWS_REGION,
});

// Get latest report saved from s3 helper function
const getLatestReport = async () => {
  const s3 = new AWS.S3();
  const listParams = {
    Bucket: process.env.S3_BUCKET_NAME,
    Prefix: `${process.env.S3_PREFIX_NAME}/`,
  };
  const s3Objects = await s3.listObjects(listParams).promise();
  const sortedObjects = s3Objects.Contents.sort(
    (a, b) => b.LastModified - a.LastModified
  );
  if (sortedObjects.length > 0) {
    const latestObjectKey = sortedObjects[0].Key;
    const contentParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: latestObjectKey,
    };
    const data = await s3.getObject(contentParams).promise();
    const fileContent = data.Body.toString("utf-8");
    return fileContent;
  }
};

// MySQL credentials
const mysqlHostname = process.env.MYSQL_HOST_NAME;
const mysqlUser = process.env.MYSQL_USERNAME;
const mysqlPassword = process.env.MYSQL_PASSWORD;
const mysqlDatabaseName = process.env.S3_BUCKET_NAME;
const mysqlTableName = process.env.S3_PREFIX_NAME;
// mysql config
const pool = mysql.createPool({
  host: mysqlHostname,
  user: mysqlUser,
  password: mysqlPassword,
});

const createDatabase = async (connection, databaseName) => {
  try {
    // Creates database if it doesn't exist
    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${databaseName}\``);
    console.log(`Database "${databaseName}" created successfully.`);
  } catch (error) {
    console.error(`Error: `, error.message);
    throw new Error(`An error has occurred while creating database.`);
  }
};

const createTable = async (connection, databaseName, tableName, columnName) => {
  try {
    await connection.query(`USE \`${databaseName}\``);
    // Creates table if it does not exist
    await connection.query(
      `CREATE TABLE IF NOT EXISTS \`${tableName}\` (id SERIAL PRIMARY KEY, ${columnName})`
    );
    console.log(`Table "${tableName}" created successfully.`);
  } catch (error) {
    console.error(`Error: `, error.message);
    throw new Error(`An error has occurred while creating table.`);
  }
};

const confirmDbAndTbl = async (connection, databaseName, tableName, columnName) => {
  try {
    const [db] = await connection.query(`SHOW DATABASES LIKE '${databaseName}'`);
    if (db.length > 0) {
      console.log(`Database found. Selecting database...`);
      await connection.query(`USE \`${databaseName}\``);
      const [tbl] = await connection.query(`SHOW TABLES LIKE '${tableName}'`);
      if (tbl.length > 0) {
        console.log(`Table found.`);
        return ((db.length > 0) && (tbl.length > 0));
      } else {
        console.log(`Table does not exist. Creating table...`);
        await createTable(connection, databaseName, tableName, columnName);
        const [isTblExist] = await connection.query(`SHOW TABLES LIKE '${tableName}'`);
        return ((db.length > 0) && (isTblExist.length > 0));
      }
    } else {
      console.log(`Database does not exist. Creating database...`);
      await createDatabase(connection, databaseName);
      await createTable(connection, databaseName, tableName, columnName);
      const [isDbExist] = await connection.query(`SHOW DATABASES LIKE '${databaseName}'`);
      const [isTblExist] = await connection.query(`SHOW TABLES LIKE '${tableName}'`);
      return ((isDbExist.length > 0) && (isTblExist.length > 0));
    }
  } catch (error) {
    console.error(`Error: `, error.message);
    throw new Error(`An error has occurred while checking database existence.`);
  }
};

const insertLatestReport = async (connection, databaseName, tableName, latestReport) => {
  connection.query(`USE \`${databaseName}\``);
  const column = Object.keys(latestReport[0]).map((key) => {
    return `\`${key.toLowerCase().replace(/\s/g, "-")}\``;
  });

  await latestReport.map(data => {
    // Get columns based on the jsonArray object
    const value = `${Object.values(data).map((val) => {
      return (val.length === 0) ? `null` : `'${val.replace(/'/g, '')}'`;
    })}`;
    const insertQuery = `INSERT INTO \`${tableName}\` (${column}) VALUES (${value})`;
    console.log(insertQuery + "\n");
  })
};

// proccess
const handler = async () => {
  try {
    // Get the latest report file from s3
    const latestReportFromS3 = await getLatestReport();
    // Checking if file not found or empty
    if (latestReportFromS3.length === 0) {
      throw new Error("Folder/File is empty.");
    }
    // Read the CSV file and convert to JSON array
    const jsonArray = await csvtojson().fromString(latestReportFromS3);
    console.log("Successfully accessed the latest file.");

    // Get columns based on the jsonArray object
    const mysqlColumn = Object.keys(jsonArray[0]).map((key) => {
      return `\`${key.toLowerCase().replace(/\s/g, "-")}\` VARCHAR(128)`;
    });

    // const values = jsonArray.map(data => `(${Object.values(data).map(val => `\`${val.replace(/'/g, '')}\``).join(', ')})`).join(', ');
    // console.log(`(${Object.values(jsonArray[0]).map(val => `\`${val.replace(/'/g, '')}\``).join(', ')}),`);


    const connection = await pool.getConnection();
    const isConfirmed = await confirmDbAndTbl(connection, mysqlDatabaseName, mysqlTableName, mysqlColumn);

    if (isConfirmed) {
      await insertLatestReport(connection, mysqlDatabaseName, mysqlTableName, jsonArray);
    }
    connection.release();
  } catch (error) {
    console.error("Error: ", error.message);
    throw new Error(`An error has occurred while processing the data.`);
  } finally {
    pool.end(); // End the pool when done
  }
};

handler();
