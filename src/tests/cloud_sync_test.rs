use crate::timon_engine::{cloud_sync::CloudStorageManager, db_manager::DatabaseManager};
use std::collections::HashMap;

#[tokio::test]
async fn test_new_cloud_storage_manager() {
  let db_manager = DatabaseManager::new("tmp/tests", 30); // Assuming a constructor exists
  let manager = CloudStorageManager::new(db_manager, None, None, None, Some("test-bucket"), None);
  assert_eq!(manager.bucket_name, "test-bucket");
}

#[tokio::test]
async fn test_query_bucket_invalid_table() {
  let db_manager = DatabaseManager::new("tmp/tests", 30);
  let manager = CloudStorageManager::new(db_manager, None, None, None, None, None);
  let mut date_range = HashMap::new();
  date_range.insert("start_date", "2025-01-01");
  date_range.insert("end_date", "2025-01-31");

  let result = manager.query_bucket("user", "db", "SELECT * FROM non_existent", date_range, true).await;
  assert!(result.is_err());
}

// #[tokio::test]
// async fn test_generate_s3_paths_empty_result() {
//   let db_manager = DatabaseManager::new("tmp/tests", 30);
//   let manager = CloudStorageManager::new(db_manager, None, None, None, None, None);
//   let mut date_range = HashMap::new();
//   date_range.insert("start_date", "2025-01-01");
//   date_range.insert("end_date", "2025-01-31");

//   let result = manager.generate_s3_paths("user", "db", "table", date_range).await;
//   println!("result {:?}", result);
//   assert!(result.unwrap().is_empty());
// }
