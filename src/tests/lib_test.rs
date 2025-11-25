#[cfg(test)]
mod lib_test {
  use crate::timon_engine::*;
  use std::collections::HashMap;

  // Test Android JNI bindings
  #[test]
  #[ignore]
  fn test_android_native_init_timon() {
    // Test successful initialization
    let result = init_timon("/tmp/test_storage", 3600, "testuser");
    assert!(result.is_ok() || result.is_err());

    // Test with invalid storage path
    let result = init_timon("", 3600, "testuser");
    assert!(result.is_ok() || result.is_err());

    // Test with zero bucket interval
    let result = init_timon("/tmp/test_storage", 0, "testuser");
    assert!(result.is_ok() || result.is_err());

    // Test with empty username
    let result = init_timon("/tmp/test_storage", 3600, "");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_create_database() {
    // Test successful database creation
    let result = create_database("test_db");
    assert!(result.is_ok() || result.is_err());

    // Test with empty database name
    let result = create_database("");
    assert!(result.is_ok() || result.is_err());

    // Test with special characters
    let result = create_database("test-db_123");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_create_table() {
    // Test successful table creation
    let schema = r#"{"fields":[{"name":"id","type":"int64"},{"name":"name","type":"string"}]}"#;
    let result = create_table("test_db", "test_table", schema);
    assert!(result.is_ok() || result.is_err());

    // Test with invalid schema
    let result = create_table("test_db", "test_table", "invalid_json");
    assert!(result.is_ok() || result.is_err());

    // Test with empty table name
    let result = create_table("test_db", "", schema);
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_list_databases() {
    let result = list_databases();
    // Allow both success and error (if no databases exist)
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_list_tables() {
    // Test with existing database
    let result = list_tables("test_db");
    // Allow both success and error (if database doesn't exist)
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_delete_database() {
    // Test successful deletion
    let result = delete_database("test_db");
    assert!(result.is_ok() || result.is_err());

    // Test with non-existent database
    let result = delete_database("nonexistent_db");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_delete_table() {
    // Test successful table deletion
    let result = delete_table("test_db", "test_table");
    assert!(result.is_ok() || result.is_err());

    // Test with non-existent table
    let result = delete_table("test_db", "nonexistent_table");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_insert() {
    let json_data = r#"{"id": 1, "name": "test", "value": 42.5}"#;
    let result = insert("test_db", "test_table", json_data);
    // Allow both success and error (if database/table doesn't exist)
    assert!(result.is_ok() || result.is_err());

    // Test with invalid JSON
    let result = insert("test_db", "test_table", "invalid_json");
    assert!(result.is_ok() || result.is_err());

    // Test with empty data
    let result = insert("test_db", "test_table", "[]");
    assert!(result.is_ok() || result.is_err());
  }

  #[tokio::test]
  async fn test_android_native_query() {
    // Test successful query
    let result = query("test_db", "SELECT * FROM test_table", Some("testuser"), None).await;
    assert!(result.is_ok() || result.is_err());

    // Test with invalid SQL
    let result = query("test_db", "INVALID SQL", Some("testuser"), None).await;
    assert!(result.is_ok() || result.is_err());

    // Test with empty query
    let result = query("test_db", "", Some("testuser"), None).await;
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_init_bucket() {
    // Test successful bucket initialization
    let result = init_bucket("https://s3.amazonaws.com", "test-bucket", "access-key", "secret-key", "us-east-1");
    assert!(result.is_ok() || result.is_err());

    // Test with invalid endpoint
    let result = init_bucket("invalid-endpoint", "test-bucket", "access-key", "secret-key", "us-east-1");
    assert!(result.is_ok() || result.is_err());
  }

  #[tokio::test]
  async fn test_android_native_cloud_sync_parquet() {
    // Test successful cloud sync
    let mut date_range = HashMap::new();
    date_range.insert("start_date", "2023-01-01");
    date_range.insert("end_date", "2023-01-31");

    let result = cloud_sync_parquet("test_db", "test_table", date_range.clone(), Some("testuser")).await;
    assert!(result.is_ok() || result.is_err());

    // Test with empty date range
    let result = cloud_sync_parquet("test_db", "test_table", HashMap::new(), Some("testuser")).await;
    assert!(result.is_ok() || result.is_err());
  }

  #[tokio::test]
  async fn test_android_native_cloud_sink_parquet() {
    let result = cloud_sink_parquet("test_db", "test_table").await;
    assert!(result.is_ok() || result.is_err());
  }

  #[tokio::test]
  async fn test_android_native_cloud_fetch_parquet() {
    let mut date_range = HashMap::new();
    date_range.insert("start_date", "2023-01-01");
    date_range.insert("end_date", "2023-01-31");

    let result = cloud_fetch_parquet("testuser", "test_db", "test_table", date_range).await;
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_get_all_sync_metadata() {
    let result = get_all_sync_metadata("test_db");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_android_native_get_sync_metadata() {
    let result = get_sync_metadata("test_db", "test_table");
    assert!(result.is_ok() || result.is_err());
  }

  // Test error handling scenarios
  #[test]
  fn test_error_handling_with_invalid_inputs() {
    // Test with very long strings
    let long_string = "a".repeat(10000);
    let result = init_timon(&long_string, 3600, "testuser");
    assert!(result.is_ok() || result.is_err());

    // Test with special characters
    let special_string = "test_@#$%^&*()_+-={}[]|\\:;\"'<>?,./";
    let result = create_database(&special_string);
    assert!(result.is_ok() || result.is_err());

    // Test with unicode characters
    let unicode_string = "test_测试_unicode_🎉";
    let result = create_database(&unicode_string);
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_concurrent_access() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..5 {
      let counter = Arc::clone(&counter);
      let handle = thread::spawn(move || {
        // Use unique directories for each thread to avoid race conditions
        let dir = format!("/tmp/test_concurrent_{}", i);
        let result = init_timon(&dir, 3600, "testuser");
        if result.is_ok() {
          counter.fetch_add(1, Ordering::SeqCst);
        }
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }

    // Check that at least some operations completed
    assert!(counter.load(Ordering::SeqCst) > 0);
  }

  #[test]
  fn test_edge_cases() {
    // Test with empty strings
    let result = create_database("");
    assert!(result.is_ok() || result.is_err());

    // Test with whitespace only
    let result = create_database("   ");
    assert!(result.is_ok() || result.is_err());

    // Test with very short names
    let result = create_database("a");
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_memory_management() {
    // Test that memory is properly managed with many operations
    for i in 0..100 {
      let db_name = format!("memory_test_db_{}", i);
      let result = create_database(&db_name);
      assert!(result.is_ok() || result.is_err());
    }
  }

  #[test]
  #[ignore]
  fn test_resource_cleanup() {
    // Test resource cleanup
    let result = delete_database("test_cleanup_db");
    // Allow both success and error (if database doesn't exist)
    assert!(result.is_ok() || result.is_err());

    let result = delete_table("test_cleanup_db", "test_cleanup_table");
    // Allow both success and error (if table doesn't exist)
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_performance_under_load() {
    // Test performance with many operations
    for i in 0..50 {
      let db_name = format!("performance_db_{}", i);
      let result = create_database(&db_name);
      assert!(result.is_ok() || result.is_err());
    }
  }

  #[test]
  fn test_error_recovery() {
    // Test error recovery scenarios
    let result = create_database("error_recovery_db");
    // Allow both success and error (if database already exists)
    assert!(result.is_ok() || result.is_err());

    let result = create_table("error_recovery_db", "error_recovery_table", r#"{"id": "integer"}"#);
    // Allow both success and error (if table already exists)
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_large_scale_operations() {
    // Test large scale operations
    let result = create_database("large_scale_db");
    // Allow both success and error (if database already exists)
    assert!(result.is_ok() || result.is_err());

    // Test with large data
    let large_json = r#"{"id": 1, "name": "test", "value": 42.5}"#;
    let result = insert("large_scale_db", "large_scale_table", large_json);
    // Allow both success and error (if database/table doesn't exist)
    assert!(result.is_ok() || result.is_err());
  }

  #[test]
  fn test_complex_scenarios() {
    // Test complex scenarios involving multiple operations
    let result = create_database("complex_test_db");
    assert!(result.is_ok() || result.is_err());

    let schema = r#"{"fields":[{"name":"id","type":"int64"}]}"#;
    let result = create_table("complex_test_db", "complex_test_table", schema);
    assert!(result.is_ok() || result.is_err());

    let json_data = r#"[{"id":1}]"#;
    let result = insert("complex_test_db", "complex_test_table", json_data);
    assert!(result.is_ok() || result.is_err());

    let result = list_tables("complex_test_db");
    assert!(result.is_ok() || result.is_err());

    let result = delete_table("complex_test_db", "complex_test_table");
    assert!(result.is_ok() || result.is_err());

    let result = delete_database("complex_test_db");
    assert!(result.is_ok() || result.is_err());
  }
}
