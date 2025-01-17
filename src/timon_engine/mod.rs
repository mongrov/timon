pub mod cloud_sync;
pub mod db_manager;
pub mod helpers;

use chrono::{Timelike, Utc};
use cloud_sync::CloudStorageManager;
use db_manager::DatabaseManager;
use serde::Serialize;
use serde_json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use tokio::time::{self, Duration};

/* ******************************** Local File Storage ********************************
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
  pub json_value: Option<Value>,
}

static DATABASE_MANAGER: OnceLock<DatabaseManager> = OnceLock::new();

fn get_database_manager() -> &'static DatabaseManager {
  DATABASE_MANAGER.get().expect("DatabaseManager is not initialized")
}

fn schedule_files_merge_tasks(db_manager: DatabaseManager) {
  // Schedule the merge_files_by_hour task
  let db_manager_clone = db_manager.clone();
  tokio::spawn(async move {
    loop {
      // Get the current time and calculate the duration until the next hour
      let now = Utc::now();
      let seconds_until_next_hour = 3600 - (now.minute() as u64 * 60 + now.second() as u64);
      let duration_until_next_hour = Duration::from_secs(seconds_until_next_hour);
      println!("Waiting for {:?} until the next hour.", duration_until_next_hour);

      // Wait until the next hour
      time::sleep(duration_until_next_hour).await;

      // Execute the task
      match db_manager_clone.clone().merge_files_by_hour() {
        Ok(_) => {
          println!("SUCCESS: merge_files_by_hour executed at {:?}", Utc::now());
        }
        Err(err) => {
          eprintln!("ERROR: merge_files_by_hour failed: {}", err);
        }
      }
    }
  });

  // Schedule the merge_files_by_day task
  let db_manager_clone = db_manager.clone();
  tokio::spawn(async move {
    loop {
      // Get the current time and calculate the duration until the next midnight
      let now = Utc::now();
      let seconds_until_next_midnight = 86400 - (now.hour() as u64 * 3600 + now.minute() as u64 * 60 + now.second() as u64);
      let duration_until_next_midnight = Duration::from_secs(seconds_until_next_midnight);
      println!("Waiting for {:?} until the next midnight", duration_until_next_midnight);

      // Wait until the next midnight
      time::sleep(duration_until_next_midnight).await;

      // Execute the task
      match db_manager_clone.clone().merge_files_by_day() {
        Ok(_) => {
          println!("SUCCESS: merge_files_by_day executed at {:?}", Utc::now());
        }
        Err(err) => {
          eprintln!("ERROR: merge_files_by_day failed: {}", err);
        }
      }
    }
  });
}

#[allow(dead_code)]
pub fn init_timon(storage_path: &str) -> Result<Value, String> {
  let db_manager = DatabaseManager::new(storage_path);

  schedule_files_merge_tasks(db_manager.clone());

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
  let database_manager = get_database_manager();
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
  let database_manager = get_database_manager();
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
  let mut database_manager = get_database_manager().clone();
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
  let mut database_manager = get_database_manager().clone();
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
  let database_manager = get_database_manager();
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
  let database_manager = get_database_manager();
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
  let database_manager = get_database_manager();
  match database_manager.clone().insert(db_name, table_name, json_data) {
    Ok(message) => {
      let result = TimonResult {
        status: 200,
        message,
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
pub async fn query(db_name: &str, sql_query: &str) -> Result<Value, String> {
  let database_manager = get_database_manager();
  match database_manager.query(db_name, sql_query, true).await {
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

/* ******************************** S3 Compatible Storage ********************************
* @ init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key)
* @ query_bucket(bucket_name, date_range, sql_query)
* @ sink_daily_parquet(db_name, table_name)
 */

static CLOUD_STORAGE_MANAGER: OnceLock<CloudStorageManager> = OnceLock::new();

fn get_cloud_storage_manager() -> &'static CloudStorageManager {
  CLOUD_STORAGE_MANAGER.get().expect("CloudStorageManager is not initialized")
}

pub fn init_bucket(
  bucket_endpoint: &str,
  bucket_name: &str,
  access_key_id: &str,
  secret_access_key: &str,
  bucket_region: &str,
) -> Result<Value, String> {
  let cloud_storage_manager = cloud_sync::CloudStorageManager::new(
    get_database_manager().clone(),
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

pub async fn query_bucket(username: &str, sql_query: &str, date_range: HashMap<&str, &str>) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager();
  match cloud_storage_manager.query_bucket(&username, &sql_query, date_range, true).await {
    Ok(db_manager::DataFusionOutput::Json(data)) => {
      let json_value = serde_json::to_value(&data).map_err(|e| e.to_string())?;
      let result = TimonResult {
        status: 200,
        message: format!(
          "query data with success from '{}' with '{}'",
          cloud_storage_manager.bucket_name, sql_query
        ),
        json_value: Some(json_value),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Ok(db_manager::DataFusionOutput::DataFrame(_df)) => {
      let result = TimonResult {
        status: 400,
        message: "DataFrame output is not directly convertible to string".to_owned(),
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

pub async fn sink_daily_parquet(username: &str, db_name: &str, table_name: &str) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager();
  match cloud_storage_manager.sink_daily_parquet(username, db_name, table_name).await {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!(
          "successfully uploaded '{}.{}' table data to '{}' bucket",
          db_name, table_name, cloud_storage_manager.bucket_name
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
