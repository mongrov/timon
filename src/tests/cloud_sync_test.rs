use crate::timon_engine::cloud_sync::{DatabaseManagerInterface, MockS3Store};
use crate::timon_engine::{cloud_sync::CloudStorageManager, db_manager::DatabaseManager};
use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use tempfile::NamedTempFile;

struct MockDatabaseManager {
  username: String,
  storage_path: String,
  files: Vec<String>,
  schema: serde_json::Value,
}

impl MockDatabaseManager {
  fn new() -> Self {
    MockDatabaseManager {
      username: "testuser".to_string(),
      storage_path: "tmp/timon_test".to_string(),
      files: vec!["tmp/timon_test/data/test_db/test_table/test_table_2023-01_01.parquet".to_string()],
      schema: json!({
          "id": {"type": "int", "unique": true},
          "timestamp": {"type": "int", "datetime": true},
          "value": {"type": "float"}
      }),
    }
  }
}

impl DatabaseManagerInterface for MockDatabaseManager {
  fn build_files_list(&self, _db_name: &str, _table_name: &str, _username: Option<&str>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(self.files.clone())
  }

  fn get_table_schema(&self, _db_name: &str, _table_name: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    Ok(self.schema.clone())
  }

  fn get_username(&self) -> &str {
    &self.username
  }

  fn get_storage_path(&self) -> &str {
    &self.storage_path
  }
}

impl MockS3Store {
  fn new() -> Self {
    let mut cloud_files = HashMap::new();
    cloud_files.insert(
      "testuser/test_db/test_table/2023/01/test_table_2023-01_01.parquet".to_string(),
      vec![1, 2, 3, 4], // dummy data
    );

    let mut modified_times = HashMap::new();
    modified_times.insert(
      "testuser/test_db/test_table/2023/01/test_table_2023-01_01.parquet".to_string(),
      Utc::now(),
    );

    MockS3Store { cloud_files, modified_times }
  }
}

fn setup_test_environment() -> CloudStorageManager<MockS3Store> {
  // Create temp directories needed for testing
  let storage_path = "tmp/timon_test";
  let data_path = format!("{}/data", storage_path);
  let group_path = format!("{}/group", storage_path);
  let merge_path = format!("{}/merge_workspace", storage_path);

  let _ = std::fs::create_dir_all(&data_path);
  let _ = std::fs::create_dir_all(&group_path);
  let _ = std::fs::create_dir_all(&merge_path);

  // Create a test DB directory
  let db_path = format!("{}/test_db", data_path);
  let _ = std::fs::create_dir_all(&db_path);

  // Create a test table directory
  let table_path = format!("{}/test_table", db_path);
  let _ = std::fs::create_dir_all(&table_path);

  // Create a mock DB manager
  let db_manager = MockDatabaseManager::new();

  // Create dummy Parquet file for testing
  let test_file = format!("{}/test_table_2023-01_01.parquet", table_path);
  let _ = std::fs::write(&test_file, vec![1, 2, 3, 4]); // Dummy data

  let mock_s3 = MockS3Store::new();
  CloudStorageManager::<MockS3Store>::new_with_mock(db_manager, mock_s3, Some("test-bucket"))
}

fn cleanup_test_environment() {
  let _ = std::fs::remove_dir_all("tmp/timon_test");
}

#[tokio::test]
async fn test_new_cloud_storage_manager() {
  let db_manager = DatabaseManager::new("tmp/tests", 30, "ahmed_test"); // Assuming a constructor exists
  let mock_s3 = MockS3Store::new();
  let manager = CloudStorageManager::<MockS3Store>::new_with_mock(db_manager, mock_s3, Some("test-bucket"));
  assert_eq!(manager.bucket_name, "test-bucket");
}

#[tokio::test]
async fn test_new() {
  // Test the constructor
  let db_manager = MockDatabaseManager::new();
  let mock_s3 = MockS3Store::new();
  let cloud_mgr = CloudStorageManager::<MockS3Store>::new_with_mock(db_manager, mock_s3, Some("test-bucket"));
  assert_eq!(cloud_mgr.bucket_name, "test-bucket");
}

#[tokio::test]
async fn test_cloud_sync_parquet() {
  let cloud_mgr = setup_test_environment();

  // Set up date range
  let mut date_range = HashMap::new();
  date_range.insert("start_date", "2023-01-01");
  date_range.insert("end_date", "2023-01-31");

  let result = cloud_mgr.cloud_sync_parquet("test_db", "test_table", &date_range, None).await;

  cleanup_test_environment();

  assert!(result.is_ok(), "cloud_sync_parquet failed: {:?}", result.err());
}

#[tokio::test]
async fn test_cloud_sink_parquet() {
  let cloud_mgr = setup_test_environment();

  let result = cloud_mgr.cloud_sink_parquet("test_db", "test_table").await;

  cleanup_test_environment();

  assert!(result.is_ok(), "cloud_sink_parquet failed: {:?}", result.err());
}

#[tokio::test]
async fn test_cloud_fetch_parquet() {
  let cloud_mgr = setup_test_environment();

  // Set up date range
  let mut date_range = HashMap::new();
  date_range.insert("start_date", "2023-01-01");
  date_range.insert("end_date", "2023-01-31");

  let result = cloud_mgr.cloud_fetch_parquet("testuser", "test_db", "test_table", &date_range).await;

  cleanup_test_environment();

  assert!(result.is_ok(), "cloud_fetch_parquet failed: {:?}", result.err());
}

#[tokio::test]
async fn test_upload_to_bucket() {
  let cloud_mgr = setup_test_environment();

  // Create a unique temporary file with test content
  let mut temp_file = NamedTempFile::new().unwrap();
  writeln!(temp_file, "test content").unwrap();

  // Get the file path as string
  let test_file_path = temp_file.path().to_str().unwrap();

  // Upload to S3 bucket (or your backend)
  let result = cloud_mgr.upload_to_bucket(test_file_path, "testuser/test_upload.txt").await;

  cleanup_test_environment();

  assert!(result.is_ok(), "upload_to_bucket failed: {:?}", result.err());
}

#[tokio::test]
async fn test_download_from_bucket() {
  let cloud_mgr = setup_test_environment();

  // Create an empty temporary file path for download target
  let temp_file = NamedTempFile::new().unwrap();
  let download_path = temp_file.path().to_str().unwrap();

  // Run the download logic
  let result = cloud_mgr
    .download_from_bucket("testuser/test_db/test_table/2023/01/test_table_2023-01_01.parquet", download_path)
    .await;

  cleanup_test_environment();

  assert!(result.is_ok(), "download_from_bucket failed: {:?}", result.err());
}

#[tokio::test]
async fn test_list_cloud_files() {
  let cloud_mgr = setup_test_environment();

  let result = cloud_mgr.list_cloud_files("testuser/test_db/test_table").await;

  cleanup_test_environment();

  assert!(result.is_ok(), "list_cloud_files failed: {:?}", result.err());
  let files = result.unwrap();
  assert!(!files.is_empty(), "No files were returned");
}
