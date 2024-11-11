use arrow::record_batch::RecordBatch;
use chrono::Utc;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, fs};
use tokio::io::Result as TokioResult;

use super::helpers::{extract_table_name, generate_paths, get_unique_fields, json_to_arrow, record_batches_to_json, row_to_json, Granularity};

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
}

impl DatabaseManager {
  pub fn new(storage_path: &str) -> Self {
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
    DatabaseManager {
      metadata,
      data_path,
      metadata_path,
    }
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

  fn save_metadata(&self) -> TokioResult<()> {
    // Serialize the metadata structure and save it to the file
    let json = serde_json::to_string(&self.metadata)?;
    fs::write(&self.metadata_path, json)?;
    Ok(())
  }

  pub fn insert(&mut self, db_name: &str, table_name: &str, json_data: &str) -> Result<String, Box<dyn Error>> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .read_metadata()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))
      .unwrap();

    // Parse the JSON data
    let json_values: Vec<Value> = serde_json::from_str(json_data)?;

    // Check if the database and table exist
    let table_path = self.get_table_path(db_name, table_name);
    if table_path.is_none() {
      return Err(format!("Database '{}' or Table '{}' does not exist.", db_name, table_name).into());
    }

    let table_schema = self.get_table_schema(db_name, table_name)?;
    for json_value in &json_values {
      self.validate_data_against_schema(&table_schema, json_value)?;
    }

    let current_date = Utc::now().format("%Y-%m-%d").to_string();
    let file_path = format!("{}/{}_{}.parquet", table_path.unwrap(), table_name, current_date);

    // Convert JSON data to Arrow arrays
    let (new_arrays, new_schema) = json_to_arrow(&json_values)?;

    let path = Path::new(&file_path);
    if path.exists() {
      let existing_json_values = self.read_parquet_file(&file_path)?;
      let mut combined_json_values = existing_json_values;
      combined_json_values.extend(json_values);

      // Check and update deduplicated field values
      let unique_fields = get_unique_fields(table_schema)?;
      if !unique_fields.is_empty() {
        let mut seen: HashMap<String, serde_json::Value> = HashMap::new();
        for record in combined_json_values.iter() {
          let key = unique_fields
            .iter()
            .map(|field| record.get(field).map(|v| v.to_string()).unwrap_or_default())
            .collect::<Vec<String>>()
            .join("-");
          // Update the record in the map with the latest entry
          seen.insert(key, record.clone());
        }
        // Replace the original vector with updated values
        combined_json_values = seen.into_values().collect();
      }

      // Convert combined data to Arrow arrays
      let (combined_arrays, combined_schema) = json_to_arrow(&combined_json_values)?;

      // Create a Parquet writer
      let file = fs::File::create(&path)?;
      let props = WriterProperties::builder().build();
      let mut writer = ArrowWriter::try_new(file, Arc::new(combined_schema.clone()), Some(props))?;

      // Write the combined record batch to the Parquet file
      let combined_batch = RecordBatch::try_new(Arc::new(combined_schema), combined_arrays)?;
      writer.write(&combined_batch)?;

      // Close the writer to ensure data is written to the file
      writer.close()?;
    } else {
      // Create a new Parquet file with the new data
      let file = fs::File::create(&path)?;
      let props = WriterProperties::builder().build();
      let mut writer = ArrowWriter::try_new(file, Arc::new(new_schema.clone()), Some(props))?;

      // Write the record batch to the Parquet file
      let record_batch = RecordBatch::try_new(Arc::new(new_schema), new_arrays)?;
      writer.write(&record_batch)?;

      // Close the writer to ensure data is written to the file
      writer.close()?;
    }

    Ok(format!("Data was successfully written to '{}'", file_path))
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

  pub fn get_table_path(&self, db_name: &str, table_name: &str) -> Option<String> {
    let metadata = self.read_metadata().unwrap();
    if let Some(db) = metadata.databases.get(db_name) {
      if let Some(table_path) = db.tables.get(table_name) {
        return Some(table_path.path.clone());
      }
    }
    None
  }

  pub async fn query(
    &self,
    db_name: &str,
    date_range: HashMap<&str, &str>,
    sql_query: &str,
    is_json_format: bool,
  ) -> DataFusionResult<DataFusionOutput> {
    let ctx = SessionContext::new();
    let mut table_names = Vec::new();
    let file_name = &extract_table_name(&sql_query);
    let base_dir = format!("{}/{}/{}", &self.data_path, db_name, file_name);

    let file_list = generate_paths(&base_dir, file_name, date_range, Granularity::Day, false).unwrap();

    for (i, file_path) in file_list.iter().enumerate() {
      if Path::new(file_path).exists() {
        let table_name = format!("{}_{}", file_name, i);
        match ctx.register_parquet(&table_name, file_path, ParquetReadOptions::default()).await {
          Ok(_) => table_names.push(table_name),
          Err(e) => eprintln!("Failed to register {}: {:?}", file_path, e),
        }
      } else {
        eprintln!("File does not exist: {}", file_path);
      }
    }

    if table_names.is_empty() {
      return Err(DataFusionError::Plan("No valid tables found to query.".to_string()));
    }

    // Combine all tables into a single SQL query using UNION ALL
    let combined_query = format!(
      "SELECT * FROM ({}) AS combined_table",
      table_names
        .iter()
        .map(|name| format!("SELECT * FROM {}", name))
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
    let adjusted_sql_query = sql_query.replace(file_name, "combined_table");
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
