use super::super::timon_engine::db_manager::{DataFusionOutput, DatabaseManager};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

fn create_temp_dir() -> PathBuf {
  let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let temp_dir = std::env::temp_dir().join(format!("test_db_{}", timestamp));
  fs::create_dir_all(&temp_dir).expect("Failed to create temp directory");
  temp_dir
}

fn cleanup_temp_dir(path: PathBuf) {
  fs::remove_dir_all(path).expect("Failed to clean up temp directory");
}

#[test]
fn test_create_and_list_databases() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Test creating a database
  assert!(db_manager.create_database("test_db").is_ok());

  // Test listing databases
  let databases = db_manager.list_databases().unwrap();
  assert_eq!(databases, vec!["test_db".to_string()]);

  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_create_and_list_tables() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Create a database
  db_manager.create_database("test_db").unwrap();

  // Create a table
  let schema = json!({
      "id": {"type": "int", "required": true},
      "name": {"type": "string", "required": true}
  });
  assert!(db_manager.create_table("test_db", "test_table", &schema.to_string()).is_ok());

  // Test listing tables
  let tables = db_manager.list_tables("test_db").unwrap();
  assert_eq!(tables, vec!["test_table".to_string()]);

  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_insert_and_query_data() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Create a database and table
  db_manager.create_database("test_db").unwrap();
  let schema = json!({
      "date": {"type": "int", "required": true, "unique": true, "datetime": true},
      "id": {"type": "int", "required": true},
      "name": {"type": "string", "required": true}
  });
  db_manager.create_table("test_db", "test_table", &schema.to_string()).unwrap();

  // Insert data
  let data = json!([
      {"date": "2025.02.20 10:15:00", "id": 1, "name": "Alice"},
      {"date": "2025.02.20 10:20:00", "id": 2, "name": "Bob"}
  ]);
  assert!(db_manager.insert("test_db", "test_table", &data.to_string()).is_ok());

  // Query data
  let rt = Runtime::new().unwrap();
  let result = rt.block_on(db_manager.query("test_db", "SELECT * FROM test_table", None, true)).unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      assert_eq!(
        json_result,
        json!([
          {"date": 1740046500, "id": 1, "name": "Alice"},
          {"date": 1740046800, "id": 2, "name": "Bob"}
        ])
      );
    }
    _ => panic!("Expected JSON output"),
  }

  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_schema_validation() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Create a database and table
  db_manager.create_database("test_db").unwrap();
  let schema = json!({
      "date": {"type": "int", "required": true, "unique": true, "datetime": true},
      "id": {"type": "int", "required": true},
      "name": {"type": "string", "required": true}
  });
  db_manager.create_table("test_db", "test_table", &schema.to_string()).unwrap();

  // Test valid data
  let valid_data = json!([{"date": "2025.02.20 10:15:00", "id": 1, "name": "Alice"}]);
  assert!(db_manager.insert("test_db", "test_table", &valid_data.to_string()).is_ok());

  // Test invalid data (missing required field)
  let invalid_data = json!([{"date": "2025.02.20 10:15:00", "id": 1}]);
  assert!(db_manager.insert("test_db", "test_table", &invalid_data.to_string()).is_err());

  // Test invalid data (wrong type)
  let invalid_data = json!([{"date": "2025.02.20 10:15:00", "id": "not_an_int", "name": "Alice"}]);
  assert!(db_manager.insert("test_db", "test_table", &invalid_data.to_string()).is_err());

  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_delete_database_and_table() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Create a database and table
  db_manager.create_database("test_db").unwrap();
  let schema = json!({
      "id": {"type": "int", "required": true},
      "name": {"type": "string", "required": true}
  });
  db_manager.create_table("test_db", "test_table", &schema.to_string()).unwrap();

  // Delete the table
  assert!(db_manager.delete_table("test_db", "test_table").is_ok());

  // Delete the database
  assert!(db_manager.delete_database("test_db").is_ok());

  // Verify that the database is deleted
  let databases = db_manager.list_databases().unwrap();
  assert!(databases.is_empty());

  cleanup_temp_dir(temp_dir);
}
