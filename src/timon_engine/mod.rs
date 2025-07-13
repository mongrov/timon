pub mod cloud_sync;
pub mod db_manager;
pub mod helpers;

use cloud_sync::CloudStorageManager;
use datafusion::prelude::DataFrame;
use db_manager::DatabaseManager;
use object_store::aws::AmazonS3;
use serde::Serialize;
use serde_json::Value;
use serde_json::{self, json};
use std::collections::HashMap;
use std::sync::OnceLock;

/* ******************************** Local File Storage ********************************
* @ init_timon/new(storage_path, bucket_interval)
* @ create_database(db_name)
* @ create_table(db_name, table_name)
* @ list_databases() & list_tables(db_name)
* @ delete_database(db_name) & delete_table(db_name, table_name)
* @ insert(db_name, table_name, json_data)
* @ query(db_name, sql_query, username?)
* @ query_df(db_name, sql_query, username?)
 */
#[derive(Serialize)]
pub struct TimonResult {
  pub status: u16,
  pub message: String,
  pub json_value: Option<Value>,
}

static DATABASE_MANAGER: OnceLock<DatabaseManager> = OnceLock::new();

fn get_database_manager() -> Result<&'static DatabaseManager, String> {
  DATABASE_MANAGER
    .get()
    .ok_or("DatabaseManager is not initialized. Please call init_timon() first.".to_string())
}

#[allow(dead_code)]
pub fn init_timon(storage_path: &str, bucket_interval: u32, username: &str) -> Result<Value, String> {
  let db_manager = DatabaseManager::new(storage_path, bucket_interval, username);

  match DATABASE_MANAGER.set(db_manager) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: "DatabaseManager initialized successfully".to_owned(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(_) => {
      let result = TimonResult {
        status: 400,
        message: "DatabaseManager already initialized".to_owned(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn create_database(db_name: &str) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.clone().create_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}' database created successfully", db_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn create_table(db_name: &str, table_name: &str, schema: &str) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.clone().create_table(db_name, table_name, schema) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}.{}' table created successfully", db_name, table_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn list_databases() -> Result<Value, String> {
  let mut database_manager = get_database_manager()?.clone();
  match database_manager.list_databases() {
    Ok(databases_list) => {
      let json_value = serde_json::to_value(databases_list).map_err(|e| e.to_string())?;
      let result = TimonResult {
        status: 200,
        message: "success fetching all databases".to_string(),
        json_value: Some(json_value),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn list_tables(db_name: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager()?.clone();
  match database_manager.list_tables(db_name) {
    Ok(tables_list) => {
      let json_value = serde_json::to_value(&tables_list).map_err(|e| e.to_string())?;
      let result = TimonResult {
        status: 200,
        message: format!("success fetching '{}' tables", db_name),
        json_value: Some(json_value),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn delete_database(db_name: &str) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.clone().delete_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Database '{}' was deleted!", db_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn delete_table(db_name: &str, table_name: &str) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.clone().delete_table(db_name, table_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Table '{}.{}' was deleted!", db_name, table_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn insert(db_name: &str, table_name: &str, json_data: &str) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.clone().insert(db_name, table_name, json_data) {
    Ok(value) => {
      let result = TimonResult {
        status: 200,
        message: "Records that violated (min, max) constraints will be logged and returned".to_string(),
        json_value: Some(json!(value)),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub async fn query(db_name: &str, sql_query: &str, username: Option<&str>) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  match database_manager.query(db_name, sql_query, username, true).await {
    Ok(db_manager::DataFusionOutput::Json(data)) => {
      let json_value = serde_json::to_value(&data).map_err(|e| e.to_string())?;
      let result = TimonResult {
        status: 200,
        message: format!("query data with success from '{}' with '{}'", db_name, sql_query),
        json_value: Some(json_value),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Ok(db_manager::DataFusionOutput::DataFrame(_df)) => Err("DataFrame output is not directly convertible to string".to_owned()),
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub async fn query_df(db_name: &str, sql_query: &str, username: Option<&str>) -> Result<DataFrame, String> {
  let database_manager = get_database_manager()?;
  match database_manager.query(db_name, sql_query, username, false).await {
    Ok(db_manager::DataFusionOutput::DataFrame(df)) => Ok(df),
    Ok(db_manager::DataFusionOutput::Json(_)) => Err("Expected DataFrame output, but got JSON".to_string()),
    Err(err) => Err(err.to_string()),
  }
}

/* ******************************** S3 Compatible Storage ********************************
* @ init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key)
* @ cloud_sync_parquet(db_name, table_name, date_range, username?)
* @ cloud_sink_parquet(db_name, table_name, date_range)
* @ cloud_fetch_parquet(username, db_name, table_name, date_range)
 */

static CLOUD_STORAGE_MANAGER: OnceLock<CloudStorageManager<AmazonS3>> = OnceLock::new();

fn get_cloud_storage_manager() -> Result<&'static CloudStorageManager<AmazonS3>, String> {
  CLOUD_STORAGE_MANAGER
    .get()
    .ok_or("CloudStorageManager is not initialized. Please call init_bucket() first.".to_string())
}

pub fn init_bucket(
  bucket_endpoint: &str,
  bucket_name: &str,
  access_key_id: &str,
  secret_access_key: &str,
  bucket_region: &str,
) -> Result<Value, String> {
  let database_manager = get_database_manager()?;
  let cloud_storage_manager = cloud_sync::CloudStorageManager::<AmazonS3>::new(
    database_manager.clone(),
    Some(bucket_endpoint),
    Some(access_key_id),
    Some(secret_access_key),
    Some(bucket_name),
    Some(bucket_region),
  );

  match CLOUD_STORAGE_MANAGER.set(cloud_storage_manager) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: "CloudStorageManager initialized successfully".to_owned(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(_) => {
      let result = TimonResult {
        status: 400,
        message: "CloudStorageManager already initialized".to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

pub async fn cloud_sync_parquet(db_name: &str, table_name: &str, date_range: HashMap<&str, &str>, username: Option<&str>) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager()?;
  match cloud_storage_manager.cloud_sync_parquet(db_name, table_name, &date_range, username).await {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!(
          "successfully synced '{}.{}.{}' data",
          cloud_storage_manager.bucket_name, db_name, table_name
        ),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

pub async fn cloud_sink_parquet(db_name: &str, table_name: &str) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager()?;
  match cloud_storage_manager.cloud_sink_parquet(db_name, table_name).await {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!(
          "successfully uploaded '{}.{}' table data to '{}' bucket for user '{}'",
          db_name, table_name, cloud_storage_manager.bucket_name, cloud_storage_manager.username
        ),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

pub async fn cloud_fetch_parquet(username: &str, db_name: &str, table_name: &str, date_range: HashMap<&str, &str>) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager()?;
  match cloud_storage_manager
    .cloud_fetch_parquet(username, db_name, table_name, &date_range)
    .await
  {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!(
          "successfully fetched user '{}' data from '{}.{}.{}'",
          username, cloud_storage_manager.bucket_name, db_name, table_name
        ),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let result = TimonResult {
        status: 400,
        message: err.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}
