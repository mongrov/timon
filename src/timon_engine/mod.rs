pub mod cloud_sync;
pub mod db_manager;
pub mod helpers;

use cloud_sync::CloudStorageManager;
use db_manager::DatabaseManager;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::OnceLock;

/* ******************************** File Storage ********************************
* @ init_timon/new(storage_path)
* @ create_database(db_name)
* @ create_table(db_name, table_name)
* @ list_databases() & list_tables(db_name)
* @ delete_database(db_name) & delete_table(db_name, table_name)
* @ insert(db_name, table_name, json_data)
* @ query(db_name, date_range, sql_query)
 */
#[derive(Serialize)]
pub struct TimonResult {
  pub status: u16,
  pub message: String,
  pub json_string: Option<String>,
}

impl TimonResult {
  pub fn to_string(&self) -> Result<String, serde_json::Error> {
    serde_json::to_string(self)
  }
}

fn vec_to_json_string(vec: Vec<String>) -> String {
  serde_json::to_string(&vec).expect("Failed to convert Vec<String> to JSON")
}

static DATABASE_MANAGER: OnceLock<DatabaseManager> = OnceLock::new();

fn get_database_manager() -> &'static DatabaseManager {
  DATABASE_MANAGER.get().expect("DatabaseManager is not initialized")
}

#[allow(dead_code)]
pub fn init_timon(storage_path: &str) -> Result<String, String> {
  let db_manager = DatabaseManager::new(storage_path);
  match DATABASE_MANAGER.set(db_manager) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: "DatabaseManager initialized successfully".to_owned(),
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(_) => Err("DatabaseManager already initialized".to_owned()),
  }
}

#[allow(dead_code)]
pub fn create_database(db_name: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.clone().create_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}' database created successfully", db_name),
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to create database: {}", err)),
  }
}

#[allow(dead_code)]
pub fn create_table(db_name: &str, table_name: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.clone().create_table(db_name, table_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}.{}' table created successfully", db_name, table_name),
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to create table: {}", err)),
  }
}

#[allow(dead_code)]
pub fn list_databases() -> Result<String, String> {
  let mut database_manager = get_database_manager().clone();
  match database_manager.list_databases() {
    Ok(databases_list) => {
      let result = TimonResult {
        status: 200,
        message: format!("success fetching all databases"),
        json_string: Some(vec_to_json_string(databases_list)),
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to list databases: {}", err)),
  }
}

#[allow(dead_code)]
pub fn list_tables(db_name: &str) -> Result<String, String> {
  let mut database_manager = get_database_manager().clone();
  match database_manager.list_tables(db_name) {
    Ok(tables_list) => {
      let result = TimonResult {
        status: 200,
        message: format!("success fetching '{}' tables", db_name),
        json_string: Some(vec_to_json_string(tables_list)),
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to list tables: {}", err)),
  }
}

#[allow(dead_code)]
pub fn delete_database(db_name: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.clone().delete_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Database '{}' was deleted!", db_name),
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to delete database: {}", err)),
  }
}

#[allow(dead_code)]
pub fn delete_table(db_name: &str, table_name: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.clone().delete_table(db_name, table_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Table '{}.{}' was deleted!", db_name, table_name),
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to delete table: {}", err)),
  }
}

#[allow(dead_code)]
pub fn insert(db_name: &str, table_name: &str, json_data: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.insert(db_name, table_name, json_data) {
    Ok(message) => {
      let result = TimonResult {
        status: 200,
        message,
        json_string: None,
      };
      Ok(result.to_string().expect("Failed to convert to JSON"))
    }
    Err(err) => Err(format!("Failed to insert data: {}", err)),
  }
}

#[allow(dead_code)]
pub async fn query(db_name: &str, date_range: HashMap<&str, &str>, sql_query: &str) -> Result<String, String> {
  let database_manager = get_database_manager();
  match database_manager.query(db_name, date_range, sql_query, true).await {
    Ok(cloud_sync::DataFusionOutput::Json(data)) => {
      let res = TimonResult {
        status: 200,
        message: format!("query data with success from '{}' with '{}'", db_name, sql_query),
        json_string: Some(data),
      };
      Ok(res.to_string().expect("Failed to convert to JSON"))
    }
    Ok(cloud_sync::DataFusionOutput::DataFrame(_df)) => Err("DataFrame output is not directly convertible to string".to_owned()),
    Err(error) => Err(format!("Error: {:?}", error)),
  }
}

/* ******************************** S3 Compatible Storage ********************************
* @ init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key)
* @ query_bucket(bucket_name, date_range, sql_query)
* @ sink_monthly_parquet(db_name, table_name)
 */
static CLOUD_STORAGE_MANAGER: OnceLock<CloudStorageManager> = OnceLock::new();

fn get_cloud_storage_manager() -> &'static CloudStorageManager {
  CLOUD_STORAGE_MANAGER.get().expect("CloudStorageManager is not initialized")
}

#[allow(dead_code)]
pub fn init_bucket(bucket_endpoint: &str, bucket_name: &str, access_key_id: &str, secret_access_key: &str) -> Result<String, String> {
  let cloud_storage_manager = cloud_sync::CloudStorageManager::new(
    get_database_manager().clone(),
    Some(bucket_endpoint),
    Some(access_key_id),
    Some(secret_access_key),
    Some(bucket_name),
  );

  match CLOUD_STORAGE_MANAGER.set(cloud_storage_manager) {
    Ok(_) => Ok("CloudStorageManager initialized successfully".to_owned()),
    Err(_) => Err("CloudStorageManager already initialized".to_string()),
  }
}

#[allow(dead_code)]
pub async fn query_bucket(date_range: HashMap<&str, &str>, sql_query: &str) -> Result<String, String> {
  let cloud_storage_manager = get_cloud_storage_manager();
  match cloud_storage_manager.query_bucket(date_range, &sql_query, true).await {
    Ok(cloud_sync::DataFusionOutput::Json(data)) => {
      let res = TimonResult {
        status: 200,
        message: format!(
          "query data with success from '{}' with '{}'",
          cloud_storage_manager.bucket_name, sql_query
        ),
        json_string: Some(data),
      };
      Ok(res.to_string().expect("Failed to convert to JSON"))
    }
    Ok(cloud_sync::DataFusionOutput::DataFrame(_df)) => Err("DataFrame output is not directly convertible to string".to_owned()),
    Err(error) => Err(format!("Error: {:?}", error)),
  }
}

#[allow(dead_code)]
pub async fn sink_monthly_parquet(db_name: &str, table_name: &str) -> Result<String, String> {
  let cloud_storage_manager = get_cloud_storage_manager();
  match cloud_storage_manager.sink_monthly_parquet(db_name, table_name).await {
    Ok(_) => {
      let res = TimonResult {
        status: 200,
        message: format!(
          "successfully uploaded '{}.{}' table data to '{}' bucket",
          db_name, table_name, cloud_storage_manager.bucket_name
        ),
        json_string: None,
      };
      Ok(res.to_string().expect("Failed to convert to JSON"))
    }
    Err(error) => Err(format!("Error {:?}", error)),
  }
}
