# Timon File & S3-Compatible Storage API

This API provides a set of functions for managing databases and tables in both local file storage and S3-compatible storage. It supports creating databases and tables, inserting data, querying using SQL, and more.

## Table of Contents

1. [File Storage Functions](#file-storage-functions)
2. [S3-Compatible Storage Functions](#s3-compatible-storage-functions)
3. [Function Descriptions](#function-descriptions)

---

## File Storage Functions

These functions manage databases and tables stored locally on the file system. Data can be inserted, queried, and organized using SQL-like operations.

```kotlin
// Initialize Timon with a local storage path
external fun initTimon(storagePath: String): String

// Create a new database
external fun createDatabase(dbName: String): String

// Create a new table within a specific database
external fun createTable(dbName: String, tableName: String): String

// List all available databases
external fun listDatabases(): String

// List all tables within a specific database
external fun listTables(dbName: String): String

// Delete a specific database
external fun deleteDatabase(dbName: String): String

// Delete a specific table within a database
external fun deleteTable(dbName: String, tableName: String): String

// Insert data into a table in JSON format
external fun insert(dbName: String, tableName: String, jsonData: String): String

// Query a database with a date range and SQL query
external fun query(dbName: String, dateRange: Map<String, String>, sqlQuery: String): String
```

## S3-Compatible Storage Functions

These functions manage data stored in an S3-compatible bucket, allowing for querying and saving daily data as Parquet files.

```kotlin
// Initialize S3-compatible storage with endpoint and credentials
external fun initBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String): String

// Query the bucket with a date range and SQL query
external fun queryBucket(dateRange: Map<String, String>, sqlQuery: String): String

// Sink dayly data to Parquet format in the bucket
external fun sinkDailyParquet(dbName: String, tableName: String): String
```

## Function Descriptions

- **initTimon(storagePath: String)**
Initializes the local file storage at the specified path.

- **createDatabase(dbName: String)**
Creates a new database with the specified name.

- **createTable(dbName: String, tableName: String)**
Creates a new table in the specified database.

- **listDatabases()**
Lists all databases in the local storage.

- **listTables(dbName: String)**
Lists all tables in the specified database.

- **deleteDatabase(dbName: String)**
Deletes the specified database.

- **deleteTable(dbName: String, tableName: String)**
Deletes the specified table from the given database.

- **insert(dbName: String, tableName: String, jsonData: String)**
Inserts JSON-formatted data into the specified table.

- **query(dbName: String, dateRange: Map<String, String>, sqlQuery: String)**
Executes an SQL query on the specified database within the given date range.

- **initBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String)**
Initializes an S3-compatible bucket for data storage.

- **queryBucket(dateRange: Map<String, String>, sqlQuery: String)**
Queries data in the S3 bucket based on the given date range and SQL query.

- **sinkDailyParquet(dbName: String, tableName: String)**
Upload data from the specified database and table as Parquet files, organized by day into S3-compatible bucket.
