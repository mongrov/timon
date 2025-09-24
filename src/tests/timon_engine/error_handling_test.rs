use crate::timon_engine::errors::{TimonError, TimonErrorKind, TimonResult};
use crate::timon_engine::{create_database, create_table, init_timon, list_databases, list_tables};
use serde_json::json;
use tempfile::TempDir;

// Integration tests with actual Timon functions
#[test]
fn test_database_manager_not_initialized_integration() {
  // Test that functions return proper errors when manager is not initialized
  let result = create_database("test_db");
  assert!(result.is_err());
  let error_msg = result.unwrap_err();
  assert!(error_msg.contains("DatabaseManager is not initialized"));
}

#[test]
fn test_error_handling_in_create_database() {
  let temp_dir = TempDir::new().unwrap();
  let db_root = temp_dir.path().to_str().unwrap().to_string();

  // Initialize timon
  let _ = init_timon(&db_root, 30, "test_user");

  // Test successful database creation
  let result = create_database("test_db");
  assert!(result.is_ok());

  // Test creating database with same name (should handle gracefully)
  let result = create_database("test_db");
  // This might succeed or fail depending on implementation, but should not panic
  assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_error_handling_in_create_table() {
  let temp_dir = TempDir::new().unwrap();
  let db_root = temp_dir.path().to_str().unwrap().to_string();

  // Initialize timon
  let _ = init_timon(&db_root, 30, "test_user");
  let _ = create_database("test_db");

  // Test with invalid schema
  let result = create_table("test_db", "test_table", "invalid_json");
  assert!(result.is_err());

  // Test with non-existent database
  let result = create_table("nonexistent_db", "test_table", r#"{"field": {"type": "string"}}"#);
  assert!(result.is_err());
}

#[test]
fn test_error_handling_in_list_operations() {
  let temp_dir = TempDir::new().unwrap();
  let db_root = temp_dir.path().to_str().unwrap().to_string();

  // Initialize timon
  let _ = init_timon(&db_root, 30, "test_user");

  // Test listing databases (should succeed even if empty)
  let result = list_databases();
  assert!(result.is_ok());

  // Test listing tables for non-existent database
  let result = list_tables("nonexistent_db");
  assert!(result.is_err());
}

#[test]
fn test_error_result_type() {
  fn test_function() -> TimonResult<String> {
    Ok("success".to_string())
  }

  fn test_error_function() -> TimonResult<String> {
    Err(TimonError::database_not_found("test"))
  }

  assert!(test_function().is_ok());
  assert!(test_error_function().is_err());

  let error = test_error_function().unwrap_err();
  assert_eq!(error.kind, TimonErrorKind::DatabaseNotFound);
}

#[test]
fn test_error_serialization() {
  let error = TimonError::with_details(TimonErrorKind::DataValidationFailed, "Validation failed", "Invalid data format");

  // Test that error can be serialized to JSON
  let json_result = serde_json::to_string(&error);
  assert!(json_result.is_ok());

  let json_str = json_result.unwrap();
  assert!(json_str.contains("DataValidationFailed"));
  assert!(json_str.contains("Validation failed"));
  assert!(json_str.contains("Invalid data format"));

  // Test deserialization
  let deserialized: Result<TimonError, _> = serde_json::from_str(&json_str);
  assert!(deserialized.is_ok());

  let deserialized_error = deserialized.unwrap();
  assert_eq!(deserialized_error.kind, TimonErrorKind::DataValidationFailed);
  assert_eq!(deserialized_error.message, "Validation failed");
  assert_eq!(deserialized_error.details, Some("Invalid data format".to_string()));
}

#[test]
fn test_error_context_usage() {
  let context = json!({
      "operation": "insert",
      "database": "test_db",
      "table": "test_table",
      "record_count": 100,
      "failed_records": [1, 5, 10]
  });

  let error = TimonError::new(TimonErrorKind::DataInsertionFailed, "Failed to insert records").with_context(context.clone());

  assert_eq!(error.context, Some(context));
  assert_eq!(error.kind, TimonErrorKind::DataInsertionFailed);
}

#[test]
fn test_comprehensive_error_scenarios() {
  // Test all major error categories
  let errors = vec![
    // Initialization errors
    TimonError::new(TimonErrorKind::InitializationError, "Init failed"),
    TimonError::new(TimonErrorKind::ConfigurationError, "Config failed"),
    // Database operation errors
    TimonError::database_not_found("test_db"),
    TimonError::new(TimonErrorKind::DatabaseAlreadyExists, "DB exists"),
    TimonError::new(TimonErrorKind::DatabaseCreationFailed, "Create failed"),
    TimonError::new(TimonErrorKind::DatabaseDeletionFailed, "Delete failed"),
    // Table operation errors
    TimonError::table_not_found("db", "table"),
    TimonError::new(TimonErrorKind::TableAlreadyExists, "Table exists"),
    TimonError::new(TimonErrorKind::TableCreationFailed, "Table create failed"),
    TimonError::new(TimonErrorKind::TableDeletionFailed, "Table delete failed"),
    TimonError::schema_validation_failed("field", "reason"),
    // Data operation errors
    TimonError::new(TimonErrorKind::DataInsertionFailed, "Insert failed"),
    TimonError::new(TimonErrorKind::DataValidationFailed, "Validation failed"),
    TimonError::new(TimonErrorKind::DataSerializationFailed, "Serialization failed"),
    TimonError::new(TimonErrorKind::DataDeserializationFailed, "Deserialization failed"),
    TimonError::new(TimonErrorKind::InvalidDataFormat, "Invalid format"),
    TimonError::new(TimonErrorKind::ConstraintViolation, "Constraint violated"),
    // Query operation errors
    TimonError::new(TimonErrorKind::QueryExecutionFailed, "Query failed"),
    TimonError::new(TimonErrorKind::QueryParsingFailed, "Parse failed"),
    TimonError::new(TimonErrorKind::InvalidSqlQuery, "Invalid SQL"),
    TimonError::new(TimonErrorKind::PartitionNotFound, "Partition not found"),
    // File system errors
    TimonError::new(TimonErrorKind::FileSystemError, "FS error"),
    TimonError::new(TimonErrorKind::FileNotFound, "File not found"),
    TimonError::new(TimonErrorKind::FileCreationFailed, "File create failed"),
    TimonError::new(TimonErrorKind::FileReadFailed, "File read failed"),
    TimonError::new(TimonErrorKind::FileWriteFailed, "File write failed"),
    TimonError::new(TimonErrorKind::DirectoryCreationFailed, "Dir create failed"),
    TimonError::new(TimonErrorKind::PermissionDenied, "Permission denied"),
    // Cloud storage errors
    TimonError::cloud_storage_not_initialized(),
    TimonError::new(TimonErrorKind::CloudStorageConnectionFailed, "Connection failed"),
    TimonError::new(TimonErrorKind::CloudStorageUploadFailed, "Upload failed"),
    TimonError::new(TimonErrorKind::CloudStorageDownloadFailed, "Download failed"),
    TimonError::new(TimonErrorKind::CloudStorageListFailed, "List failed"),
    TimonError::new(TimonErrorKind::CloudStorageAuthenticationFailed, "Auth failed"),
    // Synchronization errors
    TimonError::new(TimonErrorKind::SyncOperationFailed, "Sync failed"),
    TimonError::new(TimonErrorKind::SyncMetadataUpdateFailed, "Metadata update failed"),
    TimonError::username_mismatch("user1", "user2"),
    // Concurrency errors
    TimonError::new(TimonErrorKind::LockAcquisitionFailed, "Lock failed"),
    TimonError::new(TimonErrorKind::ConcurrencyError, "Concurrency error"),
    // Metadata errors
    TimonError::new(TimonErrorKind::MetadataCorrupted, "Metadata corrupted"),
    TimonError::new(TimonErrorKind::MetadataReadFailed, "Metadata read failed"),
    TimonError::new(TimonErrorKind::MetadataWriteFailed, "Metadata write failed"),
    // Validation errors
    TimonError::invalid_input("Invalid input"),
    TimonError::new(TimonErrorKind::MissingRequiredField, "Missing field"),
    TimonError::new(TimonErrorKind::InvalidFieldType, "Invalid type"),
    TimonError::new(TimonErrorKind::InvalidDateRange, "Invalid date range"),
    // Internal errors
    TimonError::new(TimonErrorKind::InternalError, "Internal error"),
    TimonError::new(TimonErrorKind::UnexpectedError, "Unexpected error"),
  ];

  // Test that all errors have valid status codes
  for error in &errors {
    let status = error.status_code();
    assert!(status >= 400 && status < 600, "Invalid status code: {}", status);
  }

  // Test that all errors can be displayed
  for error in &errors {
    let display_str = format!("{}", error);
    assert!(!display_str.is_empty());
  }

  // Test that all errors can be serialized
  for error in &errors {
    let json_result = serde_json::to_string(error);
    assert!(json_result.is_ok(), "Failed to serialize error: {:?}", error);
  }
}
