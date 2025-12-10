pub mod cloud_sync;
pub mod db_manager;
pub mod errors;
pub mod helpers;
pub mod sql_query_parser;

use cloud_sync::CloudStorageManager;
use datafusion::prelude::DataFrame;
use db_manager::DatabaseManager;
use errors::{TimonError, TimonErrorKind, TimonResult as ErrorResult};
use object_store::aws::AmazonS3;
use serde::Serialize;
use serde_json::Value;
use serde_json::{self, json};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

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

// Store separate DatabaseManager instances per username
// This allows each username to have its own isolated SessionContext
static DATABASE_MANAGERS: LazyLock<Arc<Mutex<HashMap<String, DatabaseManager>>>> = LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
// Store initialization parameters to auto-create managers for new usernames
static INIT_PARAMS: LazyLock<Arc<Mutex<Option<(String, u32)>>>> = LazyLock::new(|| Arc::new(Mutex::new(None)));
static CLOUD_STORAGE_MANAGER: LazyLock<Arc<Mutex<Option<Arc<CloudStorageManager<AmazonS3>>>>>> = LazyLock::new(|| Arc::new(Mutex::new(None)));

fn get_database_manager(username: Option<&str>) -> ErrorResult<DatabaseManager> {
  let mut managers_guard = DATABASE_MANAGERS.lock().map_err(|e| {
    TimonError::new(
      TimonErrorKind::LockAcquisitionFailed,
      format!("Failed to acquire database managers lock: {}", e),
    )
  })?;

  // Use the provided username, or get the first available manager, or use "default"
  let key = username.unwrap_or("default");

  // If manager exists for this username, return a clone
  if let Some(manager) = managers_guard.get(key) {
    return Ok(manager.clone());
  }

  // If no username provided and we have any manager, return the first one
  if username.is_none() && !managers_guard.is_empty() {
    if let Some((_, manager)) = managers_guard.iter().next() {
      return Ok(manager.clone());
    }
  }

  // Manager doesn't exist - try to auto-create it using stored init params
  if let Some(username_str) = username {
    let init_params_guard = INIT_PARAMS.lock().map_err(|e| {
      TimonError::new(
        TimonErrorKind::LockAcquisitionFailed,
        format!("Failed to acquire init params lock: {}", e),
      )
    })?;

    if let Some((storage_path, bucket_interval)) = init_params_guard.as_ref() {
      // Auto-create manager for this username
      let new_manager = DatabaseManager::new(storage_path, *bucket_interval, username_str);
      managers_guard.insert(username_str.to_string(), new_manager.clone());
      return Ok(new_manager);
    }
  }

  Err(TimonError::database_manager_not_initialized())
}

fn get_cloud_storage_manager() -> ErrorResult<Arc<CloudStorageManager<AmazonS3>>> {
  let manager_guard = CLOUD_STORAGE_MANAGER.lock().map_err(|e| {
    TimonError::new(
      TimonErrorKind::LockAcquisitionFailed,
      format!("Failed to acquire cloud storage manager lock: {}", e),
    )
  })?;
  manager_guard.as_ref().cloned().ok_or_else(|| TimonError::cloud_storage_not_initialized())
}

#[allow(dead_code)]
pub fn init_timon(storage_path: &str, bucket_interval: u32, username: &str) -> Result<Value, String> {
  let db_manager = DatabaseManager::new(storage_path, bucket_interval, username);

  // Store initialization parameters so we can auto-create managers for other usernames
  {
    let mut init_params_guard = INIT_PARAMS.lock().map_err(|e| format!("Failed to acquire init params lock: {}", e))?;
    *init_params_guard = Some((storage_path.to_string(), bucket_interval));
  }

  let mut managers_guard = DATABASE_MANAGERS
    .lock()
    .map_err(|e| format!("Failed to acquire database managers lock: {}", e))?;

  // Store manager for this username (or create new entry)
  managers_guard.insert(username.to_string(), db_manager);

  let result = TimonResult {
    status: 200,
    message: format!(
      "DatabaseManager initialized successfully for username '{}'. Managers will be auto-created for other usernames when needed.",
      username
    ),
    json_value: None,
  };
  serde_json::to_value(&result).map_err(|e| e.to_string())
}

#[allow(dead_code)]
pub fn create_database(db_name: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.create_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}' database created successfully", db_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn create_table(db_name: &str, table_name: &str, schema: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.create_table(db_name, table_name, schema) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("'{}.{}' table created successfully", db_name, table_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn list_databases() -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
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
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn list_tables(db_name: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
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
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn delete_database(db_name: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.delete_database(db_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Database '{}' was deleted!", db_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn delete_table(db_name: &str, table_name: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.delete_table(db_name, table_name) {
    Ok(_) => {
      let result = TimonResult {
        status: 200,
        message: format!("Table '{}.{}' was deleted!", db_name, table_name),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn insert(db_name: &str, table_name: &str, json_data: &str) -> Result<Value, String> {
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.insert(db_name, table_name, json_data) {
    Ok(value) => {
      let result = TimonResult {
        status: 200,
        message: "Records that violated (min, max) constraints will be logged and returned".to_string(),
        json_value: Some(json!(value)),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub async fn query(db_name: &str, sql_query: &str, username: Option<&str>, limit_partitions: Option<usize>) -> Result<Value, String> {
  let database_manager = get_database_manager(username).map_err(|e| e.to_string())?;
  match database_manager.query(db_name, sql_query, username, true, limit_partitions).await {
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
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub async fn query_df(db_name: &str, sql_query: &str, username: Option<&str>, limit_partitions: Option<usize>) -> Result<DataFrame, String> {
  let database_manager = get_database_manager(username).map_err(|e| e.to_string())?;
  match database_manager.query(db_name, sql_query, username, false, limit_partitions).await {
    Ok(db_manager::DataFusionOutput::DataFrame(df)) => Ok(df),
    Ok(db_manager::DataFusionOutput::Json(_)) => Err("Expected DataFrame output, but got JSON".to_string()),
    Err(err) => {
      let timon_error: TimonError = err.into();
      Err(timon_error.to_string())
    }
  }
}

#[allow(dead_code)]
pub async fn preload_tables(db_name: &str, table_names: Vec<String>, username: Option<&str>) -> Result<Value, String> {
  let database_manager = get_database_manager(username).map_err(|e| e.to_string())?;
  match database_manager.preload_tables(db_name, table_names, username).await {
    Ok(loaded_tables) => {
      let json_value = serde_json::to_value(&loaded_tables).map_err(|e| e.to_string())?;
      let result = TimonResult {
        status: 200,
        message: format!("Successfully preloaded {} table(s) in database '{}'", loaded_tables.len(), db_name),
        json_value: Some(json_value),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

/* ******************************** S3 Compatible Storage ********************************
* @ init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key)
* @ cloud_sync_parquet(db_name, table_name, date_range, username?)
* @ cloud_sink_parquet(db_name, table_name, date_range)
* @ cloud_fetch_parquet(username, db_name, table_name, date_range)
* @ cloud_fetch_parquet_batch(usernames, db_names, table_names, date_range)
* @ get_sync_metadata(db_name, table_name)
* @ get_all_sync_metadata(db_name)
 */

#[allow(dead_code)]
pub fn init_bucket(
  bucket_endpoint: &str,
  bucket_name: &str,
  access_key_id: &str,
  secret_access_key: &str,
  bucket_region: &str,
) -> Result<Value, String> {
  let database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  let username = database_manager.username.clone();

  // Create a new cloud storage manager with the current database manager's username
  let cloud_storage_manager = cloud_sync::CloudStorageManager::<AmazonS3>::new(
    database_manager,
    Some(bucket_endpoint),
    Some(access_key_id),
    Some(secret_access_key),
    Some(bucket_name),
    Some(bucket_region),
  );

  // Set the cloud storage manager (can be reinitialized now)
  let mut cloud_manager_guard = CLOUD_STORAGE_MANAGER
    .lock()
    .map_err(|e| format!("Failed to acquire cloud storage manager lock: {}", e))?;
  *cloud_manager_guard = Some(Arc::new(cloud_storage_manager));

  let result = TimonResult {
    status: 200,
    message: format!("CloudStorageManager initialized successfully with '{}'", username),
    json_value: None,
  };
  serde_json::to_value(&result).map_err(|e| e.to_string())
}

#[allow(dead_code)]
pub async fn cloud_sync_parquet(db_name: &str, table_name: &str, date_range: HashMap<&str, &str>, username: Option<&str>) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager().map_err(|e| e.to_string())?;
  let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;

  match cloud_storage_manager.cloud_sync_parquet(db_name, table_name, &date_range, username).await {
    Ok(_) => {
      // Update sync metadata on successful sync
      if let Err(e) = database_manager.update_sync_metadata(db_name, table_name, "sync") {
        eprintln!("Warning: Failed to update sync metadata: {}", e);
      }

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

#[allow(dead_code)]
pub async fn cloud_sink_parquet(db_name: &str, table_name: &str) -> Result<Value, String> {
  // Check username consistency before performing cloud operations
  let db_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  let cloud_storage_manager = get_cloud_storage_manager().map_err(|e| e.to_string())?;

  if db_manager.username != cloud_storage_manager.username {
    return Err(format!(
      "Username mismatch detected. Database manager: '{}', Cloud storage manager: '{}'. Please reinitialize with the correct username.",
      db_manager.username, cloud_storage_manager.username
    ));
  }

  match cloud_storage_manager.cloud_sink_parquet(db_name, table_name).await {
    Ok(_) => {
      // Update sync metadata on successful sink
      let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
      if let Err(e) = database_manager.update_sync_metadata(db_name, table_name, "sink") {
        eprintln!("Warning: Failed to update sync metadata: {}", e);
      }

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

#[allow(dead_code)]
pub async fn cloud_fetch_parquet(username: &str, db_name: &str, table_name: &str, date_range: HashMap<&str, &str>) -> Result<Value, String> {
  let cloud_storage_manager = get_cloud_storage_manager().map_err(|e| e.to_string())?;
  match cloud_storage_manager
    .cloud_fetch_parquet(username, db_name, table_name, &date_range)
    .await
  {
    Ok(_) => {
      // Update sync metadata on successful fetch
      let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
      if let Err(e) = database_manager.update_sync_metadata(db_name, table_name, "fetch") {
        eprintln!("Warning: Failed to update sync metadata: {}", e);
      }

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

#[allow(dead_code)]
pub async fn cloud_fetch_parquet_batch(
  usernames: &[&str],
  db_names: &[&str],
  table_names: &[&str],
  date_range: HashMap<&str, &str>,
) -> Result<Value, String> {
  use futures::future;
  use std::time::Instant;

  let start_time = Instant::now();
  let cloud_storage_manager = get_cloud_storage_manager().map_err(|e| e.to_string())?;

  // Create futures for all fetch operations (all combinations)
  let fetch_futures: Vec<_> = usernames
    .iter()
    .flat_map(|username| {
      let cloud_storage_manager = Arc::clone(&cloud_storage_manager);
      let date_range = date_range.clone();
      db_names.iter().flat_map(move |db_name| {
        let cloud_storage_manager = Arc::clone(&cloud_storage_manager);
        let date_range = date_range.clone();
        table_names.iter().map(move |table_name| {
          let manager = Arc::clone(&cloud_storage_manager);
          let username = username.to_string();
          let db_name = db_name.to_string();
          let table_name = table_name.to_string();
          let date_range = date_range.clone();
          async move {
            let result = manager.cloud_fetch_parquet(&username, &db_name, &table_name, &date_range).await;
            (username, db_name, table_name, result)
          }
        })
      })
    })
    .collect();

  // Execute all fetches in parallel
  let results = future::join_all(fetch_futures).await;

  // Process results and collect statistics
  let mut success_count = 0;
  let mut error_count = 0;
  let mut errors = Vec::new();

  for (username, db_name, table_name, result) in results {
    match result {
      Ok(_) => {
        success_count += 1;
        // Update sync metadata on successful fetch
        let mut database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
        if let Err(e) = database_manager.update_sync_metadata(&db_name, &table_name, "fetch") {
          eprintln!("Warning: Failed to update sync metadata for {}.{}: {}", db_name, table_name, e);
        }
      }
      Err(e) => {
        error_count += 1;
        errors.push(format!("{}/{}/{}: {}", username, db_name, table_name, e));
      }
    }
  }

  let duration = start_time.elapsed();
  let total_tasks = success_count + error_count;

  let result = TimonResult {
    status: if error_count == 0 { 200 } else { 207 }, // 207 = Multi-Status
    message: format!(
      "Batch fetch completed: {} successful, {} failed out of {} total tasks in {:.2}s",
      success_count,
      error_count,
      total_tasks,
      duration.as_secs_f64()
    ),
    json_value: Some(json!({
      "success_count": success_count,
      "error_count": error_count,
      "total_tasks": total_tasks,
      "duration_seconds": duration.as_secs_f64(),
      "errors": errors
    })),
  };
  serde_json::to_value(&result).map_err(|e| e.to_string())
}

#[allow(dead_code)]
pub fn get_sync_metadata(db_name: &str, table_name: &str) -> Result<Value, String> {
  let database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.get_sync_metadata(db_name, table_name) {
    Ok(sync_info) => {
      let result = TimonResult {
        status: 200,
        message: format!("Successfully retrieved sync metadata for '{}.{}'", db_name, table_name),
        json_value: Some(sync_info),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}

#[allow(dead_code)]
pub fn get_all_sync_metadata(db_name: &str) -> Result<Value, String> {
  let database_manager = get_database_manager(None).map_err(|e| e.to_string())?;
  match database_manager.get_all_sync_metadata(db_name) {
    Ok(sync_info) => {
      let result = TimonResult {
        status: 200,
        message: format!("Successfully retrieved sync metadata for all tables in '{}'", db_name),
        json_value: Some(sync_info),
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
    Err(err) => {
      let timon_error: TimonError = err.into();
      let result = TimonResult {
        status: timon_error.status_code(),
        message: timon_error.to_string(),
        json_value: None,
      };
      serde_json::to_value(&result).map_err(|e| e.to_string())
    }
  }
}
