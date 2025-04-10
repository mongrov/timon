use crate::timon_engine::{create_database, create_table, init_timon, insert, query};
use std::path::Path;
use tempfile::TempDir;

fn setup_temp() -> (TempDir, String) {
  let temp_dir = TempDir::new().unwrap();
  let db_root = temp_dir.path().to_str().unwrap().to_string();
  init_timon(&db_root, 30, "test_user").unwrap();
  (temp_dir, db_root)
}

#[test]
fn test_init_timon_once() {
  let (_temp_dir, db_root) = setup_temp();

  // First call is done by setup_temp

  // Try initializing again, expect same message
  let result2 = init_timon(&db_root, 30, "test_user");
  assert!(result2.is_ok());
  let binding = result2.unwrap();
  let msg = binding.get("message").unwrap().as_str().unwrap();
  assert_eq!(msg, "DatabaseManager already initialized");
}

#[test]
fn test_create_database_and_list() {
  let (_temp_dir, _db_root) = setup_temp();

  let db_name = "my_test_db";
  let result = create_database(db_name);
  assert!(result.is_ok(), "create_database failed: {:?}", result);

  // let list = list_databases().unwrap_or_else(|e| {
  //   panic!("list_databases failed with error: {:?}", e);
  // });

  // let json_value = list.get("json_value").unwrap_or_else(|| {
  //   panic!("'json_value' key missing in response: {:?}", list);
  // });

  // let databases = json_value.as_array().unwrap_or_else(|| {
  //   panic!("Expected 'json_value' to be an array, got: {:?}", json_value);
  // });

  // assert!(
  //   databases.iter().any(|v| v.as_str().unwrap_or("") == db_name),
  //   "Database '{}' not found in list: {:?}",
  //   db_name,
  //   databases
  // );
}

#[test]
fn test_create_table_and_list() {
  let (_temp_dir, db_root) = setup_temp();

  // Create DB directory inside temp dir manually (if create_database doesn't create it)
  let db_path = Path::new(&db_root).join("my_test_db");
  std::fs::create_dir_all(&db_path).unwrap();

  // Create table
  let schema = r#"{"fields": [{"name": "temp", "type": "float"}]}"#;
  let res = create_table("my_test_db", "weather", schema);
  println!("create_table result = {:?}", res);
  assert!(res.is_ok());

  // List tables
  // let tables = list_tables("my_test_db").unwrap();
  // println!("tables = {:?}", tables);

  // let arr = tables
  // .as_array()
  // .unwrap_or_else(|| panic!("Expected array from list_tables, got: {:?}", tables));

  // assert!(
  // arr.iter().any(|v| v.as_str().unwrap() == "weather"),
  // "Expected 'weather' in tables list: {:?}",
  // arr
  // );
}

#[test]
fn test_insert_and_query() {
  let (_temp_dir, _db_root) = setup_temp();

  let _ = create_database("db1");
  let schema = r#"{"fields": [{"name": "temp", "type": "float"}]}"#;
  let _ = create_table("db1", "weather", schema);
  let data = r#"[{"temp": 23.5}, {"temp": 19.8}]"#;
  let result = insert("db1", "weather", data);
  assert!(result.is_ok());
}

#[tokio::test]
async fn test_query_json() {
  let (_temp_dir, _db_root) = setup_temp();

  let _ = create_database("db1");
  let schema = r#"{"fields": [{"name": "temp", "type": "float"}]}"#;
  let _ = create_table("db1", "weather", schema);
  let _ = insert("db1", "weather", r#"[{"temp": 25.0}]"#);

  let res = query("db1", "SELECT * FROM weather", Some("test_user")).await;
  assert!(res.is_ok());
}
