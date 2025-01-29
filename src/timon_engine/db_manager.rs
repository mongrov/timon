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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, fs};
use tokio::io::Result as TokioResult;

use super::helpers::{
  extract_hourly_date, extract_monthly_date, extract_table_name, get_unique_fields, json_to_arrow, record_batches_to_json, rounded_timestamp,
  row_to_json,
};

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
  path: String,              // Path to the table
  schema: serde_json::Value, // Placeholder for your schema structure (optional)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DatabaseInfo {
  names: Vec<String>,
}

#[derive(Clone)]
pub struct DatabaseManager {
  metadata: Metadata,
  data_path: String,
  metadata_path: String,
  pub bucket_interval: u32,
}

impl DatabaseManager {
  pub fn new(storage_path: &str, bucket_interval: u32) -> Self {
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
      metadata,
      data_path,
      metadata_path,
      bucket_interval,
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
    let table = Table { schema, path: table_path };
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

  pub fn insert(&mut self, db_name: &str, table_name: &str, json_data: &str) -> Result<String, Box<dyn Error>> {
    // Reload metadata
    self.metadata = self.read_metadata()?;

    let new_json_values: Vec<Value> = serde_json::from_str(json_data)?;

    // Validate database & table existence
    let table_path = self.get_table_path(db_name, table_name);
    if table_path.is_none() {
      return Err(format!("Database '{}' or Table '{}' does not exist.", db_name, table_name).into());
    }

    let table_schema = self.get_table_schema(db_name, table_name)?;
    for json_value in &new_json_values {
      self.validate_data_against_schema(&table_schema, json_value)?;
    }

    // Get all existing files
    let file_list = self.build_files_list(db_name, table_name)?;

    let unique_fields = get_unique_fields(table_schema.clone())?;
    let build_key = |record: &Value| -> String {
      unique_fields
        .iter()
        .map(|field| record.get(field).map(|v| v.to_string()).unwrap_or_default())
        .collect::<Vec<String>>()
        .join("-")
    };

    // Track records per file
    let mut file_records: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut seen: BTreeMap<String, Value> = BTreeMap::new();
    let mut file_for_key: BTreeMap<String, String> = BTreeMap::new();

    // Read existing files and track records
    for file in &file_list {
      let records = self.read_parquet_file(file)?;
      for record in &records {
        let key = build_key(record);
        if !file_for_key.contains_key(&key) {
          seen.insert(key.clone(), record.clone());
          file_for_key.insert(key, file.clone());
        }
      }
      file_records.insert(file.clone(), records);
    }

    let mut updated_files: HashSet<String> = HashSet::new();
    let mut new_records: Vec<Value> = Vec::new();

    // Determine latest file path
    let current_date = rounded_timestamp(self.bucket_interval);
    let latest_file_path = format!("{}/{}_{}.parquet", table_path.unwrap(), table_name, current_date);
    let latest_file = Path::new(&latest_file_path);
    let mut latest_file_records: Vec<Value> = if latest_file.exists() {
      self.read_parquet_file(latest_file.to_str().unwrap())?
    } else {
      Vec::new()
    };

    let mut latest_keys: HashMap<String, usize> = latest_file_records
      .iter()
      .enumerate()
      .map(|(idx, record)| (build_key(record), idx))
      .collect();

    // Process new records
    for new_record in new_json_values {
      let key = build_key(&new_record);
      if let Some(existing_file) = file_for_key.get(&key) {
        // Update the record in its respective file
        seen.insert(key.clone(), new_record.clone());
        updated_files.insert(existing_file.clone());
      } else if let Some(index) = latest_keys.get(&key) {
        // If found in the latest file, update it there
        latest_file_records[*index] = new_record.clone();
      } else {
        // New unique record, add to the latest file
        new_records.push(new_record);
        latest_keys.insert(key, latest_file_records.len() + new_records.len() - 1);
      }
    }

    // Rewrite modified files
    for file in updated_files {
      if let Some(records) = file_records.get_mut(&file) {
        for record in records.iter_mut() {
          let key = build_key(record);
          if let Some(updated_value) = seen.get(&key) {
            *record = updated_value.clone();
          }
        }
        let (arrays, schema) = json_to_arrow(records)?;
        Self::parquet_file_writer(Path::new(&file), schema, arrays)?;
      }
    }

    // Rewrite latest file (only if there are updates)
    if !new_records.is_empty() || !latest_file_records.is_empty() {
      latest_file_records.extend(new_records);
      let (final_arrays, final_schema) = json_to_arrow(&latest_file_records)?;
      Self::parquet_file_writer(latest_file, final_schema, final_arrays)?;
    }

    Ok(latest_file_path)
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

  #[allow(dead_code)] // TODO: Remove this code or make the logic merge files on the cloud
  fn merge_files<F>(&mut self, group_extractor: F) -> Result<(), Box<dyn std::error::Error>>
  where
    F: Fn(&str) -> Option<String>,
  {
    let databases_list = self.list_databases()?;
    for db_name in databases_list {
      let tables_list = self.list_tables(&db_name)?;
      for table_name in tables_list {
        let files = self.build_files_list(&db_name, &table_name)?;
        if files.is_empty() {
          return Err("No files to merge".into());
        }

        // Group files based on the extractor function
        let mut grouped_files: HashMap<String, Vec<String>> = HashMap::new();
        for file in files {
          if self.is_valid_parquet_file(&file) {
            if let Some(group_key) = group_extractor(&file) {
              grouped_files.entry(group_key).or_default().push(file);
            }
          } else {
            eprintln!("Skipping invalid Parquet file: {}", file);
          }
        }

        // Merge files for each group
        for (group, files_in_group) in grouped_files {
          let mut all_records = Vec::new();

          for file in files_in_group.clone() {
            let json_records = self.read_parquet_file(&file)?;
            all_records.extend(json_records);
          }

          let (arrays, schema) = json_to_arrow(&all_records)?;
          let record_batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

          let table_path = self.get_table_path(&db_name, &table_name);
          let output_file = format!("{}/{}_{}.parquet", table_path.unwrap(), &table_name, group);
          let file = fs::File::create(output_file)?;
          let props = WriterProperties::builder().build();
          let mut writer = ArrowWriter::try_new(file, record_batch.schema().clone(), Some(props))?;
          writer.write(&record_batch)?;
          writer.close()?;

          for file in files_in_group {
            fs::remove_file(file)?;
          }
        }
      }
    }
    Ok(())
  }

  #[allow(dead_code)] // TODO: Remove this code or make the logic merge files on the cloud
  pub fn merge_files_by_hour(&mut self) -> Result<(), Box<dyn std::error::Error>> {
    self.merge_files(extract_hourly_date)
  }

  #[allow(dead_code)] // TODO: Remove this code or make the logic merge files on the cloud
  pub fn merge_files_by_day(&mut self) -> Result<(), Box<dyn std::error::Error>> {
    self.merge_files(extract_monthly_date)
  }

  fn is_valid_parquet_file(&self, file_path: &str) -> bool {
    match self.read_parquet_file(file_path) {
      Ok(_) => true,
      Err(err) => {
        eprintln!("Invalid Parquet file {}: {}", file_path, err);
        false
      }
    }
  }

  pub fn build_files_list(&self, db_name: &str, table_name: &str) -> Result<Vec<String>, Box<dyn Error>> {
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

    // Get the table's path
    let table_path = &table.path;

    // Ensure the directory exists
    if !Path::new(table_path).exists() {
      return Err(format!("Table path '{}' does not exist.", table_path).into());
    }

    // Collect all files in the table directory
    let mut file_list = Vec::new();
    for entry in fs::read_dir(table_path)? {
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

  fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    // Look up the schema from the metadata or wherever it is stored
    let database = self.metadata.databases.get(db_name).ok_or("Database not found")?;
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
    // if the current LibraryDirectoryPath in iOS has changed, update the tables path
    let new_data_path = storage_path.to_string() + "/data";
    let mut metadata = self.read_metadata().unwrap();
    // Update paths for all tables
    for (db_name, db) in metadata.databases.iter_mut() {
      for (table_name, table) in db.tables.iter_mut() {
        let new_table_path = format!("{}/{}/{}", new_data_path, db_name, table_name);
        table.path = new_table_path.clone();
        println!("Updated path for table {}.{} To ({})", db_name, table_name, new_table_path);
      }
    }
    self.metadata = metadata;
    self.save_metadata()?;
    Ok(())
  }

  pub fn get_table_path(&self, db_name: &str, table_name: &str) -> Option<String> {
    let metadata = self.read_metadata().unwrap();
    if let Some(db) = metadata.databases.get(db_name) {
      if let Some(table_path) = db.tables.get(table_name) {
        return Some(table_path.path.clone());
      }
    }
    None
  }

  async fn get_table_columns(ctx: &SessionContext, table_name: &str) -> DataFusionResult<String> {
    let df = ctx.sql(&format!("SELECT * FROM {} LIMIT 1", table_name)).await?;
    let column_names: Vec<String> = df.schema().fields().iter().map(|field| format!("\"{}\"", field.name())).collect();
    Ok(column_names.join(", "))
  }

  pub async fn query(&self, db_name: &str, sql_query: &str, is_json_format: bool) -> DataFusionResult<DataFusionOutput> {
    let ctx = SessionContext::new();
    let table_name = &extract_table_name(sql_query);
    let files_list = self
      .build_files_list(db_name, table_name)
      .map_err(|e| DataFusionError::Execution(format!("Error building files list: {}", e)))?;
    if files_list.is_empty() {
      return Err(DataFusionError::Plan("No valid tables found to query".to_string()));
    }

    let mut table_names = Vec::new();
    for (i, file_path) in files_list.iter().enumerate() {
      if Path::new(file_path).exists() {
        let table_name = format!("{}_{}", table_name, i);
        match ctx.register_parquet(&table_name, file_path, ParquetReadOptions::default()).await {
          Ok(_) => table_names.push(table_name),
          Err(e) => eprintln!("Failed to register {}: {:?}", file_path, e),
        }
      }
    }

    if table_names.is_empty() {
      return Err(DataFusionError::Plan("No valid tables found to query.".to_string()));
    }

    let column_names = Self::get_table_columns(&ctx, &table_names[0]).await?;
    // Combine tables using UNION ALL with explicit column selection
    let combined_query = format!(
      "SELECT {} FROM ({}) AS combined_table",
      column_names,
      table_names
        .iter()
        .map(|name| format!("SELECT {} FROM {}", column_names, name))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
    );

    // Execute the combined query
    let combined_df = ctx.sql(&combined_query).await?;
    let combined_results = combined_df.collect().await?;
    // Create an in-memory table from the combined results
    let schema = combined_results[0].schema();
    let mem_table = MemTable::try_new(schema, vec![combined_results])?;
    ctx.register_table("combined_table", Arc::new(mem_table))?;
    // Adjust the user-provided SQL query to run on the combined table
    let adjusted_sql_query = sql_query.replace(table_name, "combined_table");
    // Execute the user-provided SQL query on the combined table
    let final_df = ctx.sql(&adjusted_sql_query).await?;
    let final_results = final_df.collect().await?;

    if is_json_format {
      let json_result = record_batches_to_json(&final_results).unwrap();
      Ok(DataFusionOutput::Json(json_result))
    } else {
      let final_schema = final_results[0].schema();
      let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
      let final_df = ctx.read_table(Arc::new(final_mem_table))?;
      Ok(DataFusionOutput::DataFrame(final_df))
    }
  }
}
