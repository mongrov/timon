use super::helpers::{build_rules_tree, get_property_fields, json_to_arrow, record_batches_to_json, rounded_timestamp, row_to_json};
use super::sql_query_parser::extract_table_names_and_ctes;
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::prelude::*;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{fmt, fs};
use tokio::io::Result as TokioResult;
use tokio::sync::Semaphore;

pub enum DataFusionOutput {
  Json(Value),
  DataFrame(DataFrame),
}

impl fmt::Debug for DataFusionOutput {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DataFusionOutput::Json(s) => write!(f, "Json({})", s),
      DataFusionOutput::DataFrame(df) => {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async { df.clone().collect().await.expect("Failed to collect DataFrame results") });
        for batch in result {
          writeln!(f, "{:?}", batch)?;
        }
        Ok(())
      }
    }
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Metadata {
  databases: HashMap<String, Database>, // Maps database names to their corresponding database structure
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Database {
  tables: HashMap<String, Table>, // Maps table names to table schema
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Table {
  path: String,                         // Path to the table
  schema: serde_json::Value,            // Placeholder for your schema structure (optional)
  last_sync_time: Option<String>,       // ISO 8601 timestamp of last successful sync
  last_sync_type: Option<String>,       // "sync" or "sink" to indicate sync type
  last_sync_operation: Option<String>,  // ISO 8601 timestamp of last sync operation
  last_sink_operation: Option<String>,  // ISO 8601 timestamp of last sink operation
  last_fetch_operation: Option<String>, // ISO 8601 timestamp of last fetch operation
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DatabaseInfo {
  names: Vec<String>,
}

#[derive(Clone)]
pub struct DatabaseManager {
  pub storage_path: String,
  pub username: String,
  metadata: Metadata,
  data_path: String,
  metadata_path: String,
  bucket_interval: u32,
  session_context: SessionContext,
  // Metadata cache with TTL
  cached_metadata: Arc<RwLock<Option<Metadata>>>,
  cache_timestamp: Arc<RwLock<Option<Instant>>>,
  cache_ttl: Duration,
}

impl DatabaseManager {
  pub fn new(storage_path: &str, bucket_interval: u32, username: &str) -> Self {
    let data_path = format!("{}/data", storage_path);
    let metadata_path = format!("{}/metadata.json", storage_path);

    // Create the data directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(&data_path) {
      eprintln!("Error creating data directory {}: {}", data_path, e);
    }

    // Check if the metadata file exists
    if !Path::new(&metadata_path).exists() {
      // Create the metadata file if it doesn't exist
      match fs::File::create(&metadata_path) {
        Ok(_) => {
          // Write the initial metadata structure `{"databases":{}}` into the file
          let initial_metadata = Metadata { databases: HashMap::new() };
          if let Err(e) = fs::write(&metadata_path, serde_json::to_string(&initial_metadata).unwrap()) {
            eprintln!("Error writing initial metadata to file: {}", e);
          }
        }
        Err(e) => eprintln!("Error creating metadata file: {}", e),
      }
    }

    // Load existing metadata from metadata.json
    let metadata: Metadata = if Path::new(&metadata_path).exists() {
      let file_content = fs::read_to_string(&metadata_path).expect("Failed to read metadata file");
      serde_json::from_str(&file_content).unwrap_or_else(|_| Metadata { databases: HashMap::new() })
    } else {
      Metadata { databases: HashMap::new() }
    };

    // Create DatabaseManager instance
    let mut db_manager = DatabaseManager {
      storage_path: storage_path.to_string(),
      username: username.to_string(),
      metadata,
      data_path,
      metadata_path,
      bucket_interval,
      session_context: SessionContext::new(),
      // Initialize cache - infinite TTL, only invalidated on writes
      cached_metadata: Arc::new(RwLock::new(None)),
      cache_timestamp: Arc::new(RwLock::new(None)),
      cache_ttl: Duration::MAX, // Infinite cache - only invalidated on metadata changes
    };

    // Update metadata with the provided storage_path
    if let Err(e) = db_manager.update_metadata(storage_path) {
      eprintln!("Error updating metadata: {}", e);
    }

    db_manager
  }

  pub fn create_database(&mut self, db_name: &str) -> Result<(), DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    let db_data_path = format!("{}/{}", self.data_path, db_name);

    // Create a new directory for the database if it doesn't exist
    if let Err(e) = fs::create_dir(&db_data_path) {
      return Err(DataFusionError::Execution(format!("Error creating data directory {}: {}", db_name, e)));
    }

    // Insert the new database into the metadata
    self
      .metadata
      .databases
      .entry(db_name.to_string())
      .or_insert_with(|| Database { tables: HashMap::new() });

    // Save the updated metadata to metadata.json
    self
      .save_metadata()
      .map_err(|e| DataFusionError::Execution(format!("Failed to save metadata: {}", e)))?;

    Ok(())
  }

  pub fn create_table(&mut self, db_name: &str, table_name: &str, schema_json: &str) -> Result<String, Box<dyn Error>> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Parse the schema JSON
    let schema: Value = serde_json::from_str(schema_json)?;
    // First, we take the database path and validate the schema without borrowing `self` mutably.
    let db_path = self.metadata.databases.get_mut(db_name);
    if db_path.is_none() {
      return Err(format!("Database '{}' does not exist.", db_name).into());
    }

    // Validate the schema structure before doing any mutable operations
    self.validate_schema_structure(&schema)?;

    // Now perform mutable borrow only once after the immutable operations are done
    let database = self
      .metadata
      .databases
      .get_mut(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    // Check if the table already exists
    if database.tables.contains_key(table_name) {
      return Err(format!("Table '{}' already exists in database '{}'.", table_name, db_name).into());
    }

    // Create the table directory
    let table_path = format!("{}/{}/{}", self.data_path, db_name, table_name);
    fs::create_dir_all(&table_path)?;

    // Store the schema for future validation during inserts
    let table = Table {
      schema,
      path: table_path,
      last_sync_time: None,
      last_sync_type: None,
      last_sync_operation: None,
      last_sink_operation: None,
      last_fetch_operation: None,
    };
    database.tables.insert(table_name.to_string(), table);

    // Persist the metadata to disk (e.g., in a metadata.json or similar)
    self.save_metadata()?;

    Ok(format!("Table '{}' was successfully created in database '{}'.", table_name, db_name))
  }

  pub fn list_databases(&mut self) -> Result<Vec<String>, DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Attempt to read metadata file and handle potential errors
    let file_content = match fs::read_to_string(&self.metadata_path) {
      Ok(content) => content,
      Err(e) => return Err(DataFusionError::Execution(format!("Failed to read metadata file: {}", e))),
    };

    // Attempt to parse the metadata and handle potential errors
    let metadata: Metadata = match serde_json::from_str(&file_content) {
      Ok(m) => m,
      Err(e) => return Err(DataFusionError::Execution(format!("Failed to parse metadata: {}", e))),
    };

    let databases_list = metadata.databases.keys().cloned().collect::<Vec<String>>();

    Ok(databases_list)
  }

  pub fn list_tables(&mut self, db_name: &str) -> Result<Vec<String>, DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Check if the database exists in the metadata
    if let Some(database) = self.metadata.databases.get(db_name) {
      let tables_list = database.tables.keys().cloned().collect::<Vec<String>>();

      Ok(tables_list)
    } else {
      Err(DataFusionError::Plan(format!("Database '{}' not found", db_name)))
    }
  }

  pub fn delete_database(&mut self, db_name: &str) -> Result<(), DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Remove the database from metadata and save changes
    if self.metadata.databases.remove(db_name).is_some() {
      self.save_metadata().map_err(|e| e.to_string()).unwrap();
    } else {
      return Err(DataFusionError::Plan(format!("Failed to remove database '{}' from metadata", db_name)));
    }

    // Remove database's directory from filesystem
    let db_path = format!("{}/{}", self.data_path, db_name);
    if fs::remove_dir_all(db_path).is_err() {
      return Err(DataFusionError::Plan(format!("Failed to remove database directory '{}'", db_name)));
    }

    Ok(())
  }

  pub fn delete_table(&mut self, db_name: &str, table_name: &str) -> Result<(), DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))
      .unwrap();

    // Check if the database exists
    if let Some(db) = self.metadata.databases.get_mut(db_name) {
      // Check if the table exists and remove it
      if db.tables.remove(table_name).is_some() {
        // Save the updated metadata
        self.save_metadata().map_err(|e| e.to_string()).unwrap();

        // Remove table's directory from filesystem
        let table_path = format!("{}/{}/{}", self.data_path, db_name, table_name);
        if fs::remove_dir_all(table_path).is_err() {
          return Err(DataFusionError::Plan(format!("Failed to remove table directory '{}'", table_name)));
        }

        Ok(())
      } else {
        Err(DataFusionError::Plan(format!(
          "Table '{}' not found in database '{}'",
          table_name, db_name
        )))
      }
    } else {
      Err(DataFusionError::Plan(format!("Database '{}' not found", db_name)))
    }
  }

  pub fn insert(&mut self, db_name: &str, table_name: &str, json_data: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    // Reload metadata
    self.metadata = self.get_metadata_cached()?;

    let mut new_json_values: Vec<Value> = serde_json::from_str(json_data)?;
    let table_path = self
      .get_table_path(db_name, table_name)
      .ok_or_else(|| format!("Database '{}' or Table '{}' does not exist.", db_name, table_name))?;
    let table_schema = self.get_table_schema(db_name, table_name)?;

    let conditions = build_rules_tree(table_schema.clone());
    let mut invalid_json_values = Vec::new();
    if !conditions.is_empty() {
      let tree = json_rules_engine::and(conditions);
      for json_value in &new_json_values {
        let result = tree.check_value(json_value);
        if result.status == json_rules_engine::Status::NotMet {
          println!("record condition mismatch: {}", json_value);
          invalid_json_values.push(json_value.clone());
        }
      }
    }

    let datetime_binding = get_property_fields(&table_schema, "datetime")?;
    let datetime_field = datetime_binding
      .get(0)
      .ok_or_else(|| format!("No 'datetime' field found in the table schema."))?;
    let unique_fields = get_property_fields(&table_schema, "unique")?;

    let build_key = |record: &Value| -> String {
      unique_fields
        .iter()
        .map(|field| record.get(field).map(|v| v.to_string()).unwrap_or_default())
        .collect::<Vec<String>>()
        .join("-")
    };

    // Ensure datetime fields are present and convert them to timestamps
    for json_value in new_json_values.iter_mut() {
      match json_value.get(datetime_field) {
        Some(Value::String(date_str)) => {
          let parsed_timestamp = NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.3fZ"))
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.fZ"))
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S"))
            .map(|naive_dt| Utc.from_utc_datetime(&naive_dt).timestamp());

          match parsed_timestamp {
            Ok(timestamp) => {
              json_value[datetime_field] = json!(timestamp);
            }
            Err(_) => return Err(format!("Invalid datetime format for field '{}'.", datetime_field).into()),
          }
        }
        _ => return Err(format!("Missing required datetime field: '{}'.", datetime_field).into()),
      }
    }

    for json_value in &new_json_values {
      self.validate_data_against_schema(&table_schema, json_value)?;
    }

    // Load existing records from partitioned files
    let file_list = self.build_files_list(db_name, table_name, None)?;
    let mut file_records: HashMap<String, Vec<Value>> = HashMap::new();
    let mut record_index: HashMap<String, (String, usize)> = HashMap::new(); // key -> (file_path, record_index)

    for file in &file_list {
      if let Ok(existing_records) = self.read_parquet_file(file) {
        for (index, record) in existing_records.iter().enumerate() {
          let key = build_key(record);
          record_index.insert(key, (file.clone(), index));
        }
        file_records.insert(file.clone(), existing_records);
      }
    }

    let mut seen_records: HashMap<String, Value> = HashMap::new();
    let mut updated_files = HashSet::new();
    let mut new_records_by_file: HashMap<String, Vec<Value>> = HashMap::new();

    for new_record in new_json_values.into_iter() {
      let key = build_key(&new_record);
      if seen_records.insert(key.clone(), new_record.clone()).is_some() {
        continue;
      }

      let timestamp = new_record.get(datetime_field).and_then(|t| t.as_i64()).unwrap_or(0);
      let partition_value = rounded_timestamp(timestamp.try_into().unwrap(), self.bucket_interval);

      // Use Hive-style partitioning: date=YYYY-MM-DD/data.parquet
      let partition_dir = format!("{}/date={}", table_path, partition_value);
      fs::create_dir_all(&partition_dir).ok(); // Create partition directory if it doesn't exist
      let target_file = format!("{}/data.parquet", partition_dir);

      if let Some((file, index)) = record_index.get(&key) {
        // Update existing record in-place
        if let Some(records) = file_records.get_mut(file) {
          records[*index] = new_record;
          updated_files.insert(file.clone());
        }
      } else {
        // Ensure we create the correct partitioned file instead of writing to an existing one
        new_records_by_file.entry(target_file.clone()).or_insert_with(Vec::new).push(new_record);
        updated_files.insert(target_file.clone());
      }
    }

    // Create new files if needed and insert records into the correct partition
    for (file, new_records) in new_records_by_file {
      file_records.entry(file.clone()).or_insert_with(Vec::new).extend(new_records);
    }

    // Write updated and newly created files
    for (file, records) in file_records {
      if updated_files.contains(&file) {
        let (arrays, schema) = json_to_arrow(&records)?;
        Self::parquet_file_writer(Path::new(&file), schema, arrays)?;
      }
    }

    // Enforce row limits if configured for this table
    if let Err(e) = self.enforce_row_limits(db_name, table_name, datetime_field) {
      eprintln!("Warning: Failed to enforce row limits for table '{}.{}': {}", db_name, table_name, e);
    }

    Ok(invalid_json_values)
  }

  pub async fn query(
    &self,
    db_name: &str,
    sql_query: &str,
    username: Option<&str>,
    is_json_format: bool,
    limit_partitions: Option<usize>,
  ) -> DataFusionResult<DataFusionOutput> {
    // Extract table names and CTE names from the AST
    let (mut table_names, cte_names) =
      extract_table_names_and_ctes(&sql_query).map_err(|e| DataFusionError::Execution(format!("Failed to extract table names: {}", e)))?;
    // Remove CTE names from table names (CTEs are not real tables)
    table_names.retain(|name| !cte_names.contains(name));

    // Load metadata
    let metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to read metadata: {}", e)))?;

    // Validate that all tables exist before attempting registration
    if let Some(database) = metadata.databases.get(db_name) {
      for table_name in &table_names {
        if !database.tables.contains_key(table_name) {
          return Err(DataFusionError::Plan(format!(
            "Table '{}' referenced in query does not exist in database '{}'",
            table_name, db_name
          )));
        }
      }

      // Register tables in parallel with a semaphore to limit concurrency
      // Limit to 10 concurrent registrations to avoid resource exhaustion
      let semaphore = Arc::new(Semaphore::new(10));
      let mut registration_tasks = Vec::new();

      for table_name in &table_names {
        let table_name = table_name.clone();
        let db_name = db_name.to_string();
        let username = username.map(|u| u.to_string());
        let self_clone = self.clone();
        let semaphore_clone = semaphore.clone();

        // Spawn a task for each table registration
        let task = async move {
          // Acquire semaphore permit before registering
          let _permit = semaphore_clone
            .acquire()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to acquire semaphore: {}", e)))?;

          // Register the table
          self_clone.register_single_table(&db_name, &table_name, username.as_deref()).await
        };

        registration_tasks.push(task);
      }

      // Execute all registration tasks in parallel
      let results = futures::future::join_all(registration_tasks).await;

      // Check for any errors during registration
      // We collect all errors but continue processing to register as many tables as possible
      let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();

      if !errors.is_empty() {
        // Return the first error, but log all of them
        for (i, error) in errors.iter().enumerate() {
          if i > 0 {
            eprintln!("Additional table registration error: {}", error);
          }
        }
        return Err(errors.into_iter().next().unwrap());
      }
    }

    // If limit_partitions is set, modify the SQL query to only scan last N partitions
    let effective_sql = if let Some(limit) = limit_partitions {
      // Get all partition directories for all tables in the database
      let mut all_partitions = Vec::new();
      if let Some(database) = metadata.databases.get(db_name) {
        for (table_name, _) in &database.tables {
          let table_dir = self
            .resolve_table_dir(db_name, table_name, username)
            .map_err(|e| DataFusionError::Execution(format!("Failed to resolve table directory: {}", e)))?;

          if let Ok(entries) = std::fs::read_dir(&table_dir) {
            for entry in entries.flatten() {
              if entry.path().is_dir() {
                if let Some(name) = entry.path().file_name().and_then(|n| n.to_str()) {
                  if name.starts_with("date=") {
                    let date_value = name.strip_prefix("date=").unwrap_or("");
                    if !all_partitions.contains(&date_value.to_string()) {
                      all_partitions.push(date_value.to_string());
                    }
                  }
                }
              }
            }
          }
        }
      }

      // Sort and take last N partitions (most recent)
      all_partitions.sort();
      let selected_dates: Vec<_> = all_partitions.iter().rev().take(limit).cloned().collect();
      if !selected_dates.is_empty() {
        // Build IN clause for the selected dates
        let date_list = selected_dates.iter().map(|d| format!("'{}'", d)).collect::<Vec<_>>().join(", ");
        // Inject date filter into the SQL query
        let has_where = sql_query.to_uppercase().contains("WHERE");
        if has_where {
          format!("{} AND date IN ({})", sql_query, date_list)
        } else {
          format!("{} WHERE date IN ({})", sql_query, date_list)
        }
      } else {
        sql_query.to_string()
      }
    } else {
      sql_query.to_string()
    };

    // Execute the query directly without manual UNION/CTEs; ListingTable handles partitions
    let final_df = self.session_context.sql(&effective_sql).await?;
    let final_results = final_df.collect().await?;

    let result = if is_json_format {
      let json_result = record_batches_to_json(&final_results).unwrap();
      DataFusionOutput::Json(json_result)
    } else {
      let final_schema = final_results[0].schema();
      let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
      let final_df = self.session_context.read_table(Arc::new(final_mem_table))?;
      DataFusionOutput::DataFrame(final_df)
    };

    Ok(result)
  }

  /// Register a single table in the DataFusion session context
  /// This is extracted as a separate method to enable parallel table registration
  async fn register_single_table(&self, db_name: &str, table_name: &str, username: Option<&str>) -> DataFusionResult<()> {
    // Check if table already exists in session context
    let needs_register = match self.session_context.table_exist(table_name) {
      Ok(exists) => !exists,
      Err(_) => true,
    };

    if !needs_register {
      return Ok(());
    }

    // Resolve table directory
    let table_dir = self
      .resolve_table_dir(db_name, table_name, username)
      .map_err(|e| DataFusionError::Execution(format!("Failed to resolve table directory for '{}': {}", table_name, e)))?;

    // Create ListingOptions with partition column for Hive-style partitioning
    let file_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
      .with_file_extension(".parquet")
      .with_table_partition_cols(vec![(
        "date".to_string(),
        DataType::Utf8, // Partition values are stored as strings in directory names
      )]);

    // Create the listing table URL
    let table_url = ListingTableUrl::parse(&table_dir).map_err(|e| DataFusionError::Execution(format!("Failed to parse table URL: {}", e)))?;

    // Configure the listing table and infer schema from parquet files
    let config = ListingTableConfig::new(table_url)
      .with_listing_options(listing_options)
      .infer_schema(&self.session_context.state())
      .await?;

    let listing_table = ListingTable::try_new(config)?;

    // Register the table in the session context
    self.session_context.register_table(table_name, Arc::new(listing_table))?;

    Ok(())
  }

  /// Pre-load specific tables into the DataFusion session context
  /// This method allows pre-warming tables at app startup to eliminate first-query latency
  pub async fn preload_tables(&self, db_name: &str, table_names: Vec<String>, username: Option<&str>) -> DataFusionResult<Vec<String>> {
    // Load metadata to validate tables exist
    let metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to read metadata: {}", e)))?;

    // Validate that the database exists
    let database = metadata
      .databases
      .get(db_name)
      .ok_or_else(|| DataFusionError::Plan(format!("Database '{}' does not exist", db_name)))?;

    // Filter tables that exist and are not already registered
    let mut tables_to_register = Vec::new();
    for table_name in &table_names {
      // Check if table exists in metadata
      if !database.tables.contains_key(table_name) {
        eprintln!("Warning: Table '{}' does not exist in database '{}', skipping", table_name, db_name);
        continue;
      }

      // Check if table is already registered
      let already_registered = match self.session_context.table_exist(table_name) {
        Ok(exists) => exists,
        Err(_) => false,
      };

      if !already_registered {
        tables_to_register.push(table_name.clone());
      }
    }

    // Register tables in parallel with a semaphore to limit concurrency
    let semaphore = Arc::new(Semaphore::new(10));
    let mut registration_tasks = Vec::new();

    for table_name in &tables_to_register {
      let table_name = table_name.clone();
      let db_name = db_name.to_string();
      let username = username.map(|u| u.to_string());
      let self_clone = self.clone();
      let semaphore_clone = semaphore.clone();

      // Spawn a task for each table registration
      let task = async move {
        // Acquire semaphore permit before registering
        let _permit = semaphore_clone
          .acquire()
          .await
          .map_err(|e| DataFusionError::Execution(format!("Failed to acquire semaphore: {}", e)))?;

        // Register the table
        self_clone.register_single_table(&db_name, &table_name, username.as_deref()).await?;
        Ok::<String, DataFusionError>(table_name)
      };

      registration_tasks.push(task);
    }

    // Execute all registration tasks in parallel
    let results = futures::future::join_all(registration_tasks).await;

    // Collect successfully registered tables
    let mut successfully_registered = Vec::new();
    let mut errors = Vec::new();

    for result in results {
      match result {
        Ok(table_name) => successfully_registered.push(table_name),
        Err(e) => errors.push(e),
      }
    }

    // Log errors but don't fail the entire operation
    for error in &errors {
      eprintln!("Table registration error: {}", error);
    }

    Ok(successfully_registered)
  }

  // Resolve the effective directory path for a logical table, preferring group/user path when provided
  fn resolve_table_dir(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<String, Box<dyn Error>> {
    // Reload metadata to ensure it's up-to-date
    let metadata = self.get_metadata_cached()?;

    let database = metadata
      .databases
      .get(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    let table = database
      .tables
      .get(table_name)
      .ok_or_else(|| format!("Table '{}' does not exist in database '{}'.", table_name, db_name))?;

    let base_table_path = Path::new(&table.path);

    // Extract base root like "<storage>/data"
    let base_root = base_table_path
      .ancestors()
      .nth(2)
      .ok_or_else(|| format!("Failed to determine base directory from '{}'", base_table_path.display()))?
      .to_path_buf();

    let group_path = username.map(|user| base_root.join("group").join(user).join(db_name).join(table_name));

    if let Some(group_dir) = group_path {
      if group_dir.exists() {
        return Ok(group_dir.to_string_lossy().to_string());
      }
    }

    Ok(base_table_path.to_string_lossy().to_string())
  }

  fn parquet_file_writer(path: &Path, schema: Schema, array: Vec<Arc<dyn Array>>) -> Result<String, Box<dyn Error>> {
    // Create a Parquet writer
    let file = fs::File::create(&path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;
    // Write the combined record batch to the Parquet file
    let combined_batch = RecordBatch::try_new(Arc::new(schema), array)?;
    writer.write(&combined_batch)?;
    // Close the writer to ensure data is written to the file
    writer.close()?;
    Ok(format!("Data was successfully written to '{}'", path.to_string_lossy()))
  }

  pub fn build_files_list(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<Vec<String>, Box<dyn Error>> {
    // Reload metadata to ensure it's up-to-date
    let metadata = self
      .get_metadata_cached()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Validate if the database exists
    let database = metadata
      .databases
      .get(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    // Validate if the table exists within the database
    let table = database
      .tables
      .get(table_name)
      .ok_or_else(|| format!("Table '{}' does not exist in database '{}'.", table_name, db_name))?;

    // Get the base table path
    let base_table_path = Path::new(&table.path);

    // Extract the base directory (root path) from the table path
    let base_root = base_table_path
      .ancestors()
      .nth(2) // Adjust according to depth: "<base_path>/data/zivaring/activitydetails"
      .ok_or_else(|| format!("Failed to determine base directory from '{}'", base_table_path.display()))?
      .to_path_buf();

    // Determine the final path
    let final_table_path = if let Some(user) = username {
      base_root.join("group").join(user).join(db_name).join(table_name) // Correct order
    } else {
      base_table_path.to_path_buf() // Default to the existing path
    };

    // Ensure the directory exists
    if !final_table_path.exists() {
      return Err(format!("Table path '{}' does not exist.", final_table_path.display()).into());
    }

    // Collect all files in the chosen directory
    let mut file_list = Vec::new();
    for entry in fs::read_dir(final_table_path)? {
      let entry = entry?;
      let path = entry.path();

      // Only include files, ignore directories
      if path.is_file() {
        file_list.push(path.to_string_lossy().to_string());
      }
    }

    // Sort files by their name for consistency
    file_list.sort();

    Ok(file_list)
  }

  fn validate_schema_structure(&self, schema: &Value) -> Result<(), Box<dyn Error>> {
    let schema_obj = schema.as_object().ok_or("Schema should be a JSON object")?;

    for (field_name, field_rules) in schema_obj {
      // Skip validation for max_rows as it's a configuration property, not a data field
      if field_name == "max_rows" {
        continue;
      }

      let field_rules_obj = field_rules
        .as_object()
        .ok_or(format!("Invalid validation rules for field '{}'", field_name))?;

      // Ensure that the schema contains the required "type" field
      if !field_rules_obj.contains_key("type") {
        return Err(format!("Field '{}' is missing a 'type' definition.", field_name).into());
      }

      // Check if "required" is a boolean (optional, defaults to false)
      if let Some(required) = field_rules_obj.get("required") {
        if !required.is_boolean() {
          return Err(format!("Field '{}' has an invalid 'required' value. Must be true or false.", field_name).into());
        }
      }
    }

    Ok(())
  }

  pub fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    // Reload metadata to ensure it's up-to-date
    let metadata = self.get_metadata_cached().map_err(|e| format!("Failed to reload metadata: {}", e))?;
    // Look up the schema from the metadata
    let database = metadata.databases.get(db_name).ok_or("Database not found")?;
    let table = database.tables.get(table_name).ok_or("Table not found")?;
    Ok(table.schema.clone())
  }

  fn validate_data_against_schema(&self, schema: &serde_json::Value, json_data: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let schema_obj = schema.as_object().ok_or("Schema should be a JSON object")?;
    let data_obj = json_data.as_object().ok_or("Data should be a JSON object")?;

    // Check for unexpected fields (fields in JSON data that are not in the schema)
    for (key, _value) in data_obj {
      if !schema_obj.contains_key(key) {
        return Err(format!("Unexpected field: '{}' is not defined in the schema!", key).into());
      }
    }

    // Validate each field in the schema
    for (field_name, field_rules) in schema_obj {
      // Skip validation for max_rows as it's a configuration property, not a data field
      if field_name == "max_rows" {
        continue;
      }

      let field_rules_obj = field_rules
        .as_object()
        .ok_or(format!("Invalid validation rules for field '{}'", field_name))?;

      // Check if the field is required and if it's missing from the data
      if field_rules_obj.get("required").and_then(|v| v.as_bool()).unwrap_or(false) {
        if !data_obj.contains_key(field_name) {
          return Err(format!("Missing required field '{}'", field_name).into());
        }
      }

      // Check the field type if the field exists in the data
      if let Some(value) = data_obj.get(field_name) {
        let field_type = field_rules_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        self.validate_field_type(field_name, field_type, value)?;
      }
    }

    Ok(())
  }

  fn validate_field_type(&self, field_name: &str, field_type: &str, value: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    fn get_value_type(value: &Value) -> &str {
      if value.is_f64() {
        "float"
      } else if value.is_i64() || value.is_u64() {
        "int"
      } else if value.is_string() {
        "string"
      } else if value.is_boolean() {
        "bool"
      } else if value.is_array() {
        "array"
      } else {
        "unknown"
      }
    }

    let actual_type = get_value_type(value);
    let expected_types: Vec<&str> = field_type.split('|').collect();
    if !expected_types.contains(&actual_type) {
      return Err(
        format!(
          "Type mismatch for field '{}': expected '{}', but got '{}'.",
          field_name, field_type, actual_type
        )
        .into(),
      );
    }

    Ok(())
  }

  fn read_parquet_file(&self, file_path: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let file = fs::File::open(&Path::new(file_path))?;
    let reader = SerializedFileReader::new(file)?;
    let mut iter = reader.get_row_iter(None)?;

    let mut json_records = Vec::new();

    while let Some(record_result) = iter.next() {
      match record_result {
        Ok(record) => {
          // Convert the record to a JSON-like format
          let json_record = row_to_json(&record);
          json_records.push(json_record);
        }
        Err(_) => {
          return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error reading record")));
        }
      }
    }
    Ok(json_records)
  }

  fn read_metadata(&self) -> Result<Metadata, Box<dyn Error>> {
    let metadata_contents = fs::read_to_string(&self.metadata_path)?;
    if metadata_contents.trim().is_empty() {
      // If the metadata file is empty, return a default Metadata object
      return Ok(Metadata { databases: HashMap::new() });
    }
    let metadata: Metadata = serde_json::from_str(&metadata_contents).map_err(|e| Box::new(e) as Box<dyn Error>)?;
    Ok(metadata)
  }

  /// Get metadata with caching support (infinite TTL, invalidated only on writes)
  fn get_metadata_cached(&self) -> Result<Metadata, Box<dyn Error>> {
    // Check if we have a valid cache
    let cache_timestamp = self.cache_timestamp.read().unwrap();
    let should_refresh = match *cache_timestamp {
      Some(timestamp) => Instant::now().duration_since(timestamp) > self.cache_ttl,
      None => true,
    };
    drop(cache_timestamp);

    if should_refresh {
      // Cache expired or doesn't exist, refresh it
      let fresh_metadata = self.read_metadata()?;

      // Update cache with write lock
      let mut cached_metadata = self.cached_metadata.write().unwrap();
      *cached_metadata = Some(fresh_metadata.clone());
      drop(cached_metadata);

      let mut cache_timestamp = self.cache_timestamp.write().unwrap();
      *cache_timestamp = Some(Instant::now());
      drop(cache_timestamp);

      Ok(fresh_metadata)
    } else {
      // Return cached metadata
      let cached_metadata = self.cached_metadata.read().unwrap();
      match &*cached_metadata {
        Some(metadata) => Ok(metadata.clone()),
        None => {
          // Shouldn't happen, but handle gracefully
          drop(cached_metadata);
          let fresh_metadata = self.read_metadata()?;

          let mut cached_metadata = self.cached_metadata.write().unwrap();
          *cached_metadata = Some(fresh_metadata.clone());
          drop(cached_metadata);

          let mut cache_timestamp = self.cache_timestamp.write().unwrap();
          *cache_timestamp = Some(Instant::now());
          drop(cache_timestamp);

          Ok(fresh_metadata)
        }
      }
    }
  }

  /// Manually invalidate the metadata cache
  /// Should be called after any operation that modifies metadata (create_table, delete_table, etc.)
  fn invalidate_cache(&self) {
    let mut cached_metadata = self.cached_metadata.write().unwrap();
    *cached_metadata = None;
    drop(cached_metadata);

    let mut cache_timestamp = self.cache_timestamp.write().unwrap();
    *cache_timestamp = None;
    drop(cache_timestamp);
  }

  fn save_metadata(&self) -> TokioResult<()> {
    // Serialize the metadata structure and save it to the file
    let json = serde_json::to_string(&self.metadata)?;
    fs::write(&self.metadata_path, json)?;
    // Invalidate cache after saving metadata
    self.invalidate_cache();
    Ok(())
  }

  pub fn update_metadata(&mut self, storage_path: &str) -> TokioResult<()> {
    // Create a lock file path
    let lock_file_path = format!("{}/metadata.lock", self.storage_path);

    // Try to acquire the lock with retries
    let mut retries = 0;
    let max_retries = 5;
    let retry_delay = Duration::from_millis(100);

    let _lock_file = loop {
      match File::create(&lock_file_path) {
        Ok(file) => {
          // Try to acquire an exclusive lock using fs2
          if let Err(_) = file.lock_exclusive() {
            if retries >= max_retries {
              return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to acquire metadata lock after multiple retries",
              ));
            }
            retries += 1;
            std::thread::sleep(retry_delay);
            continue;
          }
          break file;
        }
        Err(_) => {
          if retries >= max_retries {
            return Err(std::io::Error::new(
              std::io::ErrorKind::Other,
              "Failed to create lock file after multiple retries",
            ));
          }
          retries += 1;
          std::thread::sleep(retry_delay);
        }
      }
    };

    // Ensure the lock file is removed when we're done
    struct LockGuard {
      path: String,
    }

    impl Drop for LockGuard {
      fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
      }
    }

    let _lock_guard = LockGuard { path: lock_file_path };

    // Rest of the update_metadata implementation...
    let new_data_path = storage_path.to_string() + "/data";
    let mut metadata = self.get_metadata_cached().unwrap();

    for (db_name, db) in metadata.databases.iter_mut() {
      for (table_name, table) in db.tables.iter_mut() {
        let new_table_path = format!("{}/{}/{}", new_data_path, db_name, table_name);
        table.path = new_table_path.clone();
      }
    }

    self.metadata = metadata;
    self.save_metadata()?;
    Ok(())
  }

  pub fn get_table_path(&self, db_name: &str, table_name: &str) -> Option<String> {
    self.metadata.databases.get(db_name)?.tables.get(table_name)?.path.clone().into()
  }

  /// Update the last sync time and type for a table
  pub fn update_sync_metadata(&mut self, db_name: &str, table_name: &str, sync_type: &str) -> Result<(), Box<dyn Error>> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self.get_metadata_cached()?;

    if let Some(database) = self.metadata.databases.get_mut(db_name) {
      if let Some(table) = database.tables.get_mut(table_name) {
        let now = chrono::Utc::now();
        let timestamp = now.to_rfc3339();

        // Update the legacy fields for backward compatibility
        table.last_sync_time = Some(timestamp.clone());
        table.last_sync_type = Some(sync_type.to_string());

        // Update the specific operation type field
        match sync_type {
          "sync" => table.last_sync_operation = Some(timestamp),
          "sink" => table.last_sink_operation = Some(timestamp),
          "fetch" => table.last_fetch_operation = Some(timestamp),
          _ => {
            // For any other type, just update the legacy fields
            table.last_sync_time = Some(timestamp);
            table.last_sync_type = Some(sync_type.to_string());
          }
        }

        // Save the updated metadata
        self.save_metadata()?;
        Ok(())
      } else {
        Err(format!("Table '{}' not found in database '{}'", table_name, db_name).into())
      }
    } else {
      Err(format!("Database '{}' not found", db_name).into())
    }
  }

  /// Get the last sync information for a table
  pub fn get_sync_metadata(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    // Reload metadata to ensure we have the latest sync information
    let metadata = self.get_metadata_cached()?;

    if let Some(database) = metadata.databases.get(db_name) {
      if let Some(table) = database.tables.get(table_name) {
        let sync_info = json!({
          "table_name": table_name,
          "database_name": db_name,
          "last_sync_time": table.last_sync_time,
          "last_sync_type": table.last_sync_type,
          "last_sync_operation": table.last_sync_operation,
          "last_sink_operation": table.last_sink_operation,
          "last_fetch_operation": table.last_fetch_operation
        });
        Ok(sync_info)
      } else {
        Err(format!("Table '{}' not found in database '{}'", table_name, db_name).into())
      }
    } else {
      Err(format!("Database '{}' not found", db_name).into())
    }
  }

  /// Get sync metadata for all tables in a database
  pub fn get_all_sync_metadata(&self, db_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    // Reload metadata to ensure we have the latest sync information
    let metadata = self.get_metadata_cached()?;

    if let Some(database) = metadata.databases.get(db_name) {
      let mut sync_info = Vec::new();

      for (table_name, table) in &database.tables {
        sync_info.push(json!({
          "table_name": table_name,
          "database_name": db_name,
          "last_sync_time": table.last_sync_time,
          "last_sync_type": table.last_sync_type,
          "last_sync_operation": table.last_sync_operation,
          "last_sink_operation": table.last_sink_operation,
          "last_fetch_operation": table.last_fetch_operation
        }));
      }

      Ok(json!({
        "database_name": db_name,
        "tables": sync_info
      }))
    } else {
      Err(format!("Database '{}' not found", db_name).into())
    }
  }

  fn enforce_row_limits(&self, db_name: &str, table_name: &str, datetime_field: &str) -> Result<(), Box<dyn Error>> {
    // Get table schema to check for max_rows configuration
    let table_schema = self.get_table_schema(db_name, table_name)?;
    let schema_obj = table_schema.as_object().ok_or("Schema should be a JSON object")?;

    // Check if max_rows is configured in the schema
    let max_rows = if let Some(max_rows_value) = schema_obj.get("max_rows") {
      max_rows_value.as_u64().unwrap_or(0) as usize
    } else {
      return Ok(()); // No row limiting configured
    };

    if max_rows == 0 {
      return Ok(()); // No limit specified
    }

    // Load all records from the table
    let file_list = self.build_files_list(db_name, table_name, None)?;
    let mut all_records = Vec::new();

    for file in &file_list {
      if let Ok(records) = self.read_parquet_file(file) {
        all_records.extend(records);
      }
    }

    // If we're under the limit, no cleanup needed
    if all_records.len() <= max_rows {
      return Ok(());
    }

    // Sort records by timestamp (descending) and keep only the latest max_rows
    all_records.sort_by(|a, b| {
      let a_ts = a.get(datetime_field).and_then(|v| v.as_i64()).unwrap_or(0);
      let b_ts = b.get(datetime_field).and_then(|v| v.as_i64()).unwrap_or(0);
      b_ts.cmp(&a_ts) // Descending order (latest first)
    });

    let records_to_keep = all_records.into_iter().take(max_rows).collect::<Vec<_>>();

    // Get table path for file operations
    let table_path = self
      .get_table_path(db_name, table_name)
      .ok_or_else(|| format!("Table '{}.{}' not found", db_name, table_name))?;

    // Group records by partition and rewrite files
    let mut records_by_file: HashMap<String, Vec<Value>> = HashMap::new();

    for record in records_to_keep {
      let timestamp = record.get(datetime_field).and_then(|t| t.as_i64()).unwrap_or(0);
      let partition_name = format!(
        "{}_{}.parquet",
        table_name,
        rounded_timestamp(timestamp.try_into().unwrap(), self.bucket_interval)
      );
      let target_file = format!("{}/{}", table_path, partition_name);

      records_by_file.entry(target_file).or_insert_with(Vec::new).push(record);
    }

    // Rewrite all files with the limited records
    for (file_path, records) in records_by_file {
      if !records.is_empty() {
        let (arrays, schema) = json_to_arrow(&records)?;
        Self::parquet_file_writer(Path::new(&file_path), schema, arrays)?;
      }
    }

    // Remove empty files
    for file in &file_list {
      if let Ok(records) = self.read_parquet_file(file) {
        if records.is_empty() {
          let _ = fs::remove_file(file);
        }
      }
    }

    Ok(())
  }
}
