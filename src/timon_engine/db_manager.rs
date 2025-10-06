use super::helpers::{
  build_rules_tree, extract_all_table_names, extract_partition_time, extract_query_time_range, extract_table_name, filter_actual_table_names,
  get_monthly_partition_overlaps, get_property_fields, get_table_columns, json_to_arrow, record_batches_to_json, rounded_timestamp, row_to_json,
};
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
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
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, fs};
use tokio::io::Result as TokioResult;

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
  registered_tables: Arc<Mutex<HashMap<String, Vec<String>>>>, // Maps table_name -> list of registered file paths
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
      registered_tables: Arc::new(Mutex::new(HashMap::new())),
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
      .read_metadata()
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
      .read_metadata()
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
      .read_metadata()
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
      .read_metadata()
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
      .read_metadata()
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
      .read_metadata()
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
    self.metadata = self.read_metadata()?;

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
      let partition_name = format!(
        "{}_{}.parquet",
        table_name,
        rounded_timestamp(timestamp.try_into().unwrap(), self.bucket_interval)
      );
      let target_file = format!("{}/{}", table_path, partition_name);

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

  pub async fn query(&self, db_name: &str, sql_query: &str, username: Option<&str>, is_json_format: bool) -> DataFusionResult<DataFusionOutput> {
    let table_name = extract_table_name(sql_query);
    let query_time_range = extract_query_time_range(sql_query, self.bucket_interval);

    let files_list_default_path = self
      .build_files_list(db_name, &table_name, username)
      .map_err(|e| DataFusionError::Execution(format!("Error building files list: {}", e)))?;

    let files_list_group_path = username.map(|_| Vec::new()).unwrap_or_else(|| {
      self
        .build_files_list(db_name, &table_name, Some(&self.username))
        .map_err(|e| DataFusionError::Execution(format!("Error building files list: {}", e)))
        .unwrap_or_default()
    });

    let mut unique_files: HashSet<String> = HashSet::new();
    let mut merged_files = Vec::new();

    // Prioritize group path files, then add default path files if not present
    for file in files_list_group_path.iter().chain(files_list_default_path.iter()) {
      let file_name = Path::new(file).file_name().unwrap().to_string_lossy().to_string();
      if unique_files.insert(file_name.clone()) {
        merged_files.push(file.clone());
      }
    }

    let filtered_files = if let Some((start_time, end_time)) = query_time_range {
      merged_files
        .into_iter()
        .filter(|file_path| {
          let partition_time = extract_partition_time(file_path);
          if self.bucket_interval >= 43200 {
            get_monthly_partition_overlaps(partition_time, start_time, end_time)
          } else {
            partition_time >= start_time && partition_time <= end_time
          }
        })
        .collect::<Vec<_>>()
    } else {
      merged_files
    };

    if filtered_files.is_empty() {
      return Err(DataFusionError::Plan("No relevant partitions found for query".to_string()));
    }

    // Check if we need to register new files for this table
    let mut registered_tables_guard = self.registered_tables.lock().unwrap();
    let registered_files = registered_tables_guard.get(&table_name).cloned().unwrap_or_default();
    let mut files_to_register: Vec<String> = Vec::new();

    for file_path in &filtered_files {
      if Path::new(file_path).exists() && !registered_files.contains(file_path) {
        files_to_register.push(file_path.clone());
      }
    }

    // Register new files if any
    let mut table_names = Vec::new();
    if !files_to_register.is_empty() {
      let mut new_registered_files = registered_files.clone();
      for (i, file_path) in files_to_register.iter().enumerate() {
        let temp_table_name = format!("{}_{}", table_name, registered_files.len() + i);
        if let Err(e) = self
          .session_context
          .register_parquet(&temp_table_name, file_path, ParquetReadOptions::default())
          .await
        {
          eprintln!("Failed to register {}: {:?}", file_path, e);
        } else {
          new_registered_files.push(file_path.clone());
          table_names.push(temp_table_name);
        }
      }
      // Update the registered tables map
      registered_tables_guard.insert(table_name.clone(), new_registered_files);
    }

    // Add existing registered table names
    for i in 0..registered_files.len() {
      let temp_table_name = format!("{}_{}", table_name, i);
      if let Ok(exists) = self.session_context.table_exist(&temp_table_name) {
        if exists {
          table_names.push(temp_table_name);
        }
      }
    }

    // If no tables are registered yet, register them now
    if table_names.is_empty() {
      let mut new_registered_files = Vec::new();
      for (i, file_path) in filtered_files.iter().enumerate() {
        if Path::new(file_path).exists() {
          let temp_table_name = format!("{}_{}", table_name, i);
          if let Err(e) = self
            .session_context
            .register_parquet(&temp_table_name, file_path, ParquetReadOptions::default())
            .await
          {
            eprintln!("Failed to register {}: {:?}", file_path, e);
          } else {
            table_names.push(temp_table_name);
            new_registered_files.push(file_path.clone());
          }
        }
      }
      // Update the registered tables map
      registered_tables_guard.insert(table_name.clone(), new_registered_files);
    }

    if table_names.is_empty() {
      return Err(DataFusionError::Plan("No valid tables found to query.".to_string()));
    }

    let column_names = get_table_columns(&self.session_context, &table_names[0]).await?;

    // Check if this is a JOIN query with multiple actual tables
    if sql_query.to_lowercase().contains("join") {
      // Extract all table names from the join query using proper parsing
      let unique_table_names = extract_all_table_names(sql_query);
      // Filter to get only actual table names (not CTEs or aliases)
      let unique_table_names_refs: Vec<&str> = unique_table_names.iter().map(|s| s.as_str()).collect();
      let actual_table_names = filter_actual_table_names(sql_query, &unique_table_names_refs);

      // Only use handle_join_query if there are multiple actual tables
      if actual_table_names.len() > 1 {
        return self
          .handle_join_query(self.session_context.clone(), db_name, sql_query, username, is_json_format)
          .await;
      }
      // If only one actual table, continue with regular query processing
    }

    let union_query = if table_names.len() == 1 {
      // Single partition - no need for UNION
      format!("SELECT {} FROM {}", column_names, table_names[0])
    } else {
      // Multiple partitions - use UNION ALL but avoid intermediate collection
      format!(
        "SELECT {} FROM ({}) AS combined_table",
        column_names,
        table_names
          .iter()
          .map(|name| format!("SELECT {} FROM {}", column_names, name))
          .collect::<Vec<_>>()
          .join(" UNION ALL ")
      )
    };

    let adjusted_sql_query = sql_query.replace(&table_name, "combined_table");

    let final_query = if table_names.len() == 1 {
      adjusted_sql_query.replace("combined_table", &table_names[0])
    } else {
      format!("WITH combined_table AS ({}) {}", union_query, adjusted_sql_query)
    };

    let final_df = self.session_context.sql(&final_query).await?;
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

  async fn handle_join_query(
    &self,
    session_context: SessionContext,
    db_name: &str,
    sql_query: &str,
    username: Option<&str>,
    is_json_format: bool,
  ) -> DataFusionResult<DataFusionOutput> {
    // Generate a unique suffix for this query execution
    let query_suffix = chrono::Utc::now().timestamp_millis();

    // Extract all table names from the join query
    let words: Vec<&str> = sql_query.split_whitespace().collect();
    let mut table_names = Vec::new();

    for i in 0..words.len() {
      if words[i].to_lowercase() == "from" && i + 1 < words.len() {
        table_names.push(words[i + 1]);
      } else if words[i].to_lowercase() == "join" && i + 1 < words.len() {
        table_names.push(words[i + 1]);
      }
    }

    if table_names.len() < 2 {
      return Err(DataFusionError::Execution(
        "Invalid join query: must specify at least two tables".to_string(),
      ));
    }

    // Remove duplicates while preserving order
    let mut unique_table_names = Vec::new();
    for table_name in table_names {
      if !unique_table_names.contains(&table_name) {
        unique_table_names.push(table_name);
      }
    }

    // Filter to get only actual table names (not CTEs or aliases)
    let actual_table_names = filter_actual_table_names(sql_query, &unique_table_names);
    // If only one actual table, this shouldn't be handled as a join query
    if actual_table_names.len() <= 1 {
      return Err(DataFusionError::Execution(
        "Join query contains only one actual table - should be handled as regular query".to_string(),
      ));
    }

    // Store information about each table
    let mut table_info: HashMap<&str, (Vec<String>, Vec<String>, String)> = HashMap::new(); // table_name -> (file_paths, temp_table_names, columns)

    // Process each actual table (filtered from CTEs and aliases)
    for table_name in &actual_table_names {
      // Get files for this table
      let table_files = self
        .build_files_list(db_name, table_name, username)
        .map_err(|e| DataFusionError::Execution(format!("Failed to get files for table {}: {}", table_name, e)))?;

      // Register all files for this table
      let mut temp_table_names = Vec::new();
      for (i, file_path) in table_files.iter().enumerate() {
        let temp_table_name = format!("{}_{}_{}", table_name, i, query_suffix);
        if let Err(e) = session_context
          .register_parquet(&temp_table_name, file_path, ParquetReadOptions::default())
          .await
        {
          println!("Failed to register {}: {:?}", file_path, e);
        } else {
          temp_table_names.push(temp_table_name);
        }
      }

      if temp_table_names.is_empty() {
        return Err(DataFusionError::Plan(format!("No valid files found for table: {}", table_name)));
      }

      // Get column names for this table
      let columns = get_table_columns(&session_context, &temp_table_names[0]).await?;

      table_info.insert(table_name, (table_files, temp_table_names, columns));
    }

    // Create combined tables for each unique table
    let mut combined_table_names: HashMap<&str, String> = HashMap::new();

    for (table_name, (_, temp_table_names, columns)) in &table_info {
      let combined_query = format!(
        "SELECT {} FROM ({}) AS combined_{}_{}",
        columns,
        temp_table_names
          .iter()
          .map(|name| format!("SELECT {} FROM {}", columns, name))
          .collect::<Vec<_>>()
          .join(" UNION ALL "),
        table_name,
        query_suffix
      );

      // Execute the combined query
      let combined_df = session_context.sql(&combined_query).await?;
      let combined_results = combined_df.collect().await?;

      if combined_results.is_empty() {
        return Err(DataFusionError::Plan(format!("No data found for table: {}", table_name)));
      }

      let schema = combined_results[0].schema();
      let mem_table = MemTable::try_new(schema, vec![combined_results])?;

      let combined_name = format!("combined_{}_{}", table_name, query_suffix);
      session_context.register_table(&combined_name, Arc::new(mem_table))?;

      combined_table_names.insert(table_name, combined_name);
    }

    // Replace all table names in the SQL query with their combined equivalents
    let mut adjusted_sql_query = sql_query.to_string();

    for (original_table, combined_table) in &combined_table_names {
      // Replace in FROM clause
      adjusted_sql_query = adjusted_sql_query.replace(&format!("FROM {}", original_table), &format!("FROM {}", combined_table));

      // Replace in JOIN clauses
      adjusted_sql_query = adjusted_sql_query.replace(&format!("JOIN {}", original_table), &format!("JOIN {}", combined_table));

      // Replace table prefixes in column references (e.g., table.column)
      adjusted_sql_query = adjusted_sql_query.replace(&format!("{}.", original_table), &format!("{}.", combined_table));
    }

    let final_df = session_context.sql(&adjusted_sql_query).await?;
    let final_results = final_df.collect().await?;

    if is_json_format {
      let json_result = record_batches_to_json(&final_results).unwrap();
      Ok(DataFusionOutput::Json(json_result))
    } else {
      let final_schema = final_results[0].schema();
      let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
      let final_df = session_context.read_table(Arc::new(final_mem_table))?;
      Ok(DataFusionOutput::DataFrame(final_df))
    }
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
      .read_metadata()
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
      .nth(3) // Adjust according to depth: "<base_path>/data/zivaring/activitydetails"
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
    let metadata = self.read_metadata().map_err(|e| format!("Failed to reload metadata: {}", e))?;
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

  fn save_metadata(&self) -> TokioResult<()> {
    // Serialize the metadata structure and save it to the file
    let json = serde_json::to_string(&self.metadata)?;
    fs::write(&self.metadata_path, json)?;
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
    let mut metadata = self.read_metadata().unwrap();

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
    self.metadata = self.read_metadata()?;

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
    let metadata = self.read_metadata()?;

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
    let metadata = self.read_metadata()?;

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
