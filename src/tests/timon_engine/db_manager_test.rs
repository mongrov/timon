use super::super::super::timon_engine::db_manager::{DataFusionOutput, DatabaseManager};
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

#[test]
fn test_simple_join_query_without_alias() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  // Create database and tables
  db_manager.create_database("test_db").unwrap();

  let schema1 = json!({
      "id": {"type": "int", "required": true, "unique": true, "datetime": true},
      "value": {"type": "int", "required": true}
  });
  let schema2 = json!({
      "id": {"type": "int", "required": true, "unique": true, "datetime": true},
      "desc": {"type": "string", "required": true}
  });

  db_manager.create_table("test_db", "table1", &schema1.to_string()).unwrap();
  db_manager.create_table("test_db", "table2", &schema2.to_string()).unwrap();

  // Insert data
  let data1 = json!([
      {"id": "2025.02.20 10:15:00", "value": 10},
      {"id": "2025.02.20 10:20:00", "value": 20}
  ]);
  let data2 = json!([
      {"id": "2025.02.20 10:15:00", "desc": "foo"},
      {"id": "2025.02.20 10:25:00", "desc": "bar"}
  ]);
  db_manager.insert("test_db", "table1", &data1.to_string()).unwrap();
  db_manager.insert("test_db", "table2", &data2.to_string()).unwrap();

  // JOIN query without alias
  let rt = Runtime::new().unwrap();
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT table1.id, table1.value, table2.desc FROM table1 JOIN table2 ON table1.id = table2.id",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      assert_eq!(
        json_result,
        json!([
            {"id": 1740046500, "value": 10, "desc": "foo"}
        ])
      );
    }
    _ => panic!("Expected JSON output"),
  }

  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_join_query_with_alias_and_group_by() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  let mut db_manager = DatabaseManager::new(storage_path, 30, "ahmed_test");

  db_manager.create_database("test_db").unwrap();

  let schema1 = json!({
      "id": {"type": "int", "required": true, "unique": true, "datetime": true},
      "cat": {"type": "string", "required": true},
      "value": {"type": "int", "required": true}
  });
  let schema2 = json!({
      "id": {"type": "int", "required": true, "unique": true, "datetime": true},
      "cat": {"type": "string", "required": true},
      "score": {"type": "int", "required": true}
  });

  db_manager.create_table("test_db", "t1", &schema1.to_string()).unwrap();
  db_manager.create_table("test_db", "t2", &schema2.to_string()).unwrap();

  let data1 = json!([
      {"id": "2025.02.20 10:15:00", "cat": "A", "value": 10},
      {"id": "2025.02.20 10:20:00", "cat": "B", "value": 20}
  ]);
  let data2 = json!([
      {"id": "2025.02.20 10:15:00", "cat": "A", "score": 100},
      {"id": "2025.02.20 10:20:00", "cat": "B", "score": 200}
  ]);
  db_manager.insert("test_db", "t1", &data1.to_string()).unwrap();
  db_manager.insert("test_db", "t2", &data2.to_string()).unwrap();

  // JOIN with alias and GROUP BY
  let rt = Runtime::new().unwrap();
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT a.cat, AVG(a.value) as avg_value, AVG(b.score) as avg_score \
         FROM t1 a JOIN t2 b ON a.id = b.id GROUP BY a.cat",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      // Should have two groups: A and B
      assert!(json_result.as_array().unwrap().iter().any(|row| row["cat"] == "A"));
      assert!(json_result.as_array().unwrap().iter().any(|row| row["cat"] == "B"));
    }
    _ => panic!("Expected JSON output"),
  }

  cleanup_temp_dir(temp_dir);
}

fn insert_activitydetails(db_manager: &mut DatabaseManager) {
  let schema = json!({
      "date": {"type": "int", "required": true, "unique": true, "datetime": true},
      "user": {"type": "string", "required": true}
  });
  db_manager.create_table("test_db", "activitydetails", &schema.to_string()).unwrap();

  // Insert records spanning several days/hours
  let data = json!([
      {"date": "2025.05.07 10:00:00", "user": "A"}, // 1746612000
      {"date": "2025.05.07 12:00:00", "user": "B"}, // 1746619200
      {"date": "2025.05.08 10:00:00", "user": "C"}, // 1746698400
      {"date": "2025.05.09 10:00:00", "user": "D"}  // 1746784800
  ]);
  db_manager.insert("test_db", "activitydetails", &data.to_string()).unwrap();
}

#[test]
fn test_date_range_query_hourly_bucket() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  // Hourly bucket_interval = 60
  let mut db_manager = DatabaseManager::new(storage_path, 60, "ahmed_test");
  db_manager.create_database("test_db").unwrap();
  insert_activitydetails(&mut db_manager);

  let rt = Runtime::new().unwrap();
  // Query for records between 2025-05-07 09:00:00 and 2025-05-08 11:00:00
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746608400 AND 1746702000",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      // Should include 3 records (A, B, C)
      assert_eq!(json_result[0]["total"], 3);
    }
    _ => panic!("Expected JSON output"),
  }
  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_date_range_query_daily_bucket() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  // Daily bucket_interval = 1440
  let mut db_manager = DatabaseManager::new(storage_path, 1440, "ahmed_test");
  db_manager.create_database("test_db").unwrap();
  insert_activitydetails(&mut db_manager);

  let rt = Runtime::new().unwrap();
  // Query for records on 2025-05-07
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746566400 AND 1746652799",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      // Should include 2 records (A, B)
      assert_eq!(json_result[0]["total"], 2);
    }
    _ => panic!("Expected JSON output"),
  }
  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_date_range_query_weekly_bucket() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  // Weekly bucket_interval = 10080
  let mut db_manager = DatabaseManager::new(storage_path, 10080, "ahmed_test");
  db_manager.create_database("test_db").unwrap();
  insert_activitydetails(&mut db_manager);

  let rt = Runtime::new().unwrap();
  // Query for all records in a week
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746566400 AND 1747171199",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      // Should include all 4 records
      assert_eq!(json_result[0]["total"], 4);
    }
    _ => panic!("Expected JSON output"),
  }
  cleanup_temp_dir(temp_dir);
}

#[test]
fn test_date_range_query_monthly_bucket() {
  let temp_dir = create_temp_dir();
  let storage_path = temp_dir.to_str().unwrap();
  // Monthly bucket_interval = 43200
  let mut db_manager = DatabaseManager::new(storage_path, 43200, "ahmed_test");
  db_manager.create_database("test_db").unwrap();
  insert_activitydetails(&mut db_manager);

  let rt = Runtime::new().unwrap();
  // Query for all records in May 2025
  let result = rt
    .block_on(db_manager.query(
      "test_db",
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746038400 AND 1748716799",
      None,
      true,
    ))
    .unwrap();

  match result {
    DataFusionOutput::Json(json_result) => {
      // Should include all 4 records
      assert_eq!(json_result[0]["total"], 4);
    }
    _ => panic!("Expected JSON output"),
  }
  cleanup_temp_dir(temp_dir);
}
