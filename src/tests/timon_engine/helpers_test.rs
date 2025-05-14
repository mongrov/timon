use crate::timon_engine::helpers::*;
use chrono::{Duration, Utc};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::json;
use std::env;
use std::fs;
use std::process;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_record_batches_to_json() {
  // Create a simple schema
  let schema = Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8, false)]);

  // Create arrays
  let id_array = Arc::new(Int64Array::from(vec![1, 2, 3]));
  let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));

  // Create record batch
  let batch = RecordBatch::try_new(Arc::new(schema), vec![id_array, name_array]).unwrap();

  // Convert to JSON
  let result = record_batches_to_json(&[batch]).unwrap();

  // Verify the result
  assert_eq!(
    result,
    json!([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ])
  );
}

#[test]
fn test_json_to_arrow() {
  let json_data = vec![
    json!({"id": 1, "name": "Alice", "score": 95.5}),
    json!({"id": 2, "name": "Bob", "score": 88.0}),
  ];

  let (arrays, schema) = json_to_arrow(&json_data).unwrap();

  assert_eq!(schema.fields().len(), 3);
  assert_eq!(arrays.len(), 3);
  assert_eq!(arrays[0].len(), 2);
}

#[test]
fn test_extract_table_name() {
  let queries = vec![
    ("SELECT * FROM users", "users"),
    ("SELECT * FROM `users`", "users"),
    ("SELECT * FROM \"users\"", "users"),
    ("SELECT * FROM users WHERE id = 1", "users"),
    ("SELECT * FROM users JOIN orders ON users.id = orders.user_id", "users"),
    ("SELECT * FROM to_local_time(users)", ""), // Should be empty as it's a function
  ];

  for (query, expected) in queries {
    assert_eq!(extract_table_name(query), expected);
  }
}

#[test]
fn test_rounded_timestamp() {
  let timestamp = 1672531200; // 2023-01-01 00:00:00 UTC

  // Test hour-based intervals
  assert_eq!(rounded_timestamp(timestamp, 60), "2023-01-01_00-00");
  assert_eq!(rounded_timestamp(timestamp, 120), "2023-01-01_00");

  // Test minute-based intervals
  assert_eq!(rounded_timestamp(timestamp, 15), "2023-01-01_00-00");
  assert_eq!(rounded_timestamp(timestamp + 1800, 15), "2023-01-01_00-30");
}

#[test]
fn test_get_property_fields() {
  let schema = json!({
      "id": {"type": "int", "unique": true},
      "name": {"type": "string"},
      "email": {"type": "string", "unique": true},
      "age": {"type": "int"}
  });

  let unique_fields = get_property_fields(&schema, "unique").unwrap();
  assert_eq!(unique_fields, vec!["email", "id"]);
}

#[test]
fn test_filter_files_by_date_range() {
  let files = vec![
    "data_2023-01-01.parquet".to_string(),
    "data_2023-01-15.parquet".to_string(),
    "data_2023-02-01.parquet".to_string(),
  ];

  let filtered = filter_files_by_date_range(files, "2023-01-01", "2023-01-31").unwrap();
  assert_eq!(filtered, vec!["data_2023-01-01.parquet", "data_2023-01-15.parquet"]);
}

#[test]
fn test_extract_query_time_range() {
  let hourly_queries = vec![
    (
      "SELECT * FROM table WHERE date BETWEEN 1672561800 AND 1676443500 LIMIT 10",
      Some((1672561800, 1676443559)),
    ),
    (
      "SELECT * FROM table WHERE timestamp BETWEEN 1672531200 AND 1675123200",
      Some((1672531200, 1675123259)),
    ),
    (
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746641700 AND 1746728099",
      Some((1746641700, 1746728099)),
    ),
    ("SELECT * FROM table", None),
  ];

  for (query, expected) in hourly_queries {
    assert_eq!(extract_query_time_range(query, 60), expected);
  }

  let daily_queries = vec![
    (
      "SELECT * FROM table WHERE date BETWEEN 1672561800 AND 1676443500 LIMIT 10",
      Some((1672531200, 1676505599)),
    ),
    (
      "SELECT * FROM table WHERE timestamp BETWEEN 1672531200 AND 1675123200",
      Some((1672531200, 1675209599)),
    ),
    (
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746641700 AND 1746728099",
      Some((1746576000, 1746748799)),
    ),
    ("SELECT * FROM table", None),
  ];

  for (query, expected) in daily_queries {
    assert_eq!(extract_query_time_range(query, 1440), expected);
  }

  let weekly_queries = vec![
    (
      "SELECT * FROM table WHERE date BETWEEN 1672561800 AND 1676443500 LIMIT 10",
      Some((1672012800, 1676505599)),
    ),
    (
      "SELECT * FROM table WHERE timestamp BETWEEN 1672531200 AND 1675123200",
      Some((1672012800, 1675209599)),
    ),
    (
      "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN 1746641700 AND 1746728099",
      Some((1746403200, 1746748799)),
    ),
    ("SELECT * FROM table", None),
  ];

  for (query, expected) in weekly_queries {
    assert_eq!(extract_query_time_range(query, 10080), expected);
  }
}

#[test]
fn test_extract_partition_time() {
  let file_paths = vec![
    ("data_2023-01-01_08-30.parquet", 1672561800),
    ("data_2023-02-15_06-45.parquet", 1676443500),
    ("data_2023-02-15_06-45-30.parquet", 1676443500),
  ];

  for (path, expected) in file_paths {
    assert_eq!(extract_partition_time(path), expected);
  }
}

#[test]
fn test_combine_unique_batches() {
  // Create schema with unique field
  let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("value", DataType::Int64, false),
  ]);

  // Create first batch
  let id_array1 = Arc::new(Int64Array::from(vec![1, 2]));
  let value_array1 = Arc::new(Int64Array::from(vec![10, 20]));
  let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![id_array1, value_array1]).unwrap();

  // Create second batch with overlapping id
  let id_array2 = Arc::new(Int64Array::from(vec![2, 3]));
  let value_array2 = Arc::new(Int64Array::from(vec![25, 30]));
  let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![id_array2, value_array2]).unwrap();

  // Combine batches with id as unique field
  let result = combine_unique_batches(vec![batch1], vec![batch2], &["id".to_string()]).unwrap();

  assert_eq!(result.len(), 1);
  let combined_batch = &result[0];
  assert_eq!(combined_batch.num_rows(), 3);
}

#[test]
fn test_build_rules_tree() {
  let schema = json!({
      "age": {"type": "int", "min": 18, "max": 100},
      "score": {"type": "float", "min": 0.0, "max": 100.0},
      "name": {"type": "string"}
  });

  let rules = build_rules_tree(schema);
  assert_eq!(rules.len(), 4); // Two rules for age, two for score
}

#[tokio::test]
async fn test_cleanup_old_files() {
  // Create a unique temporary directory name using timestamp and process ID
  let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
  let pid = process::id();
  let temp_dir = env::temp_dir().join(format!("rust_test_{}_{}", timestamp, pid));

  fs::create_dir_all(&temp_dir).unwrap();

  // Create test files: one old and one future-dated
  let past_date = Utc::now().date_naive() - Duration::days(365);
  let old_file = temp_dir.join(format!("data_{}.parquet", past_date));

  let future_date = Utc::now().date_naive() + Duration::days(365);
  let new_file = temp_dir.join(format!("data_{}.parquet", future_date));

  fs::write(&old_file, "test").unwrap();
  fs::write(&new_file, "test").unwrap();

  // Run your cleanup function
  cleanup_old_files(&[old_file.clone(), new_file.clone()]).await;

  assert!(!old_file.exists());
  assert!(new_file.exists());

  // Optionally clean up after test
  let _ = fs::remove_dir_all(temp_dir);
}
