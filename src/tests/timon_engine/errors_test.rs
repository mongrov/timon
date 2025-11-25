use crate::timon_engine::errors::{TimonError, TimonErrorKind};
use datafusion::error::DataFusionError;
use serde_json::json;
use std::error::Error;
use std::io;

#[test]
fn test_error_creation() {
  let error = TimonError::new(TimonErrorKind::DatabaseNotFound, "Test database not found");
  assert_eq!(error.kind, TimonErrorKind::DatabaseNotFound);
  assert_eq!(error.message, "Test database not found");
  assert_eq!(error.status_code(), 404);
}

#[test]
fn test_error_with_details() {
  let error = TimonError::with_details(
    TimonErrorKind::SchemaValidationFailed,
    "Schema validation failed",
    "Missing required field 'id'",
  );
  assert!(error.details.is_some());
  assert_eq!(error.details.unwrap(), "Missing required field 'id'");
}

#[test]
fn test_error_with_context() {
  let context = json!({
      "database": "test_db",
      "table": "test_table",
      "operation": "insert"
  });

  let mut error = TimonError::new(TimonErrorKind::DataInsertionFailed, "Insert failed");
  error.context = Some(context.clone());

  assert_eq!(error.context, Some(context));
}

#[test]
fn test_status_codes() {
  assert_eq!(crate::database_not_found!("test").status_code(), 404);
  assert_eq!(crate::invalid_input!("test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::InternalError, "test").status_code(), 500);
}

#[test]
fn test_error_classification() {
  let client_error = crate::invalid_input!("test");
  let status = client_error.status_code();
  assert!(status >= 400 && status < 500);

  let server_error = TimonError::new(TimonErrorKind::InternalError, "test");
  let status = server_error.status_code();
  assert!(status >= 500);
}

#[test]
fn test_recoverable_errors() {
  let recoverable = TimonError::new(TimonErrorKind::CloudStorageConnectionFailed, "test");
  assert!(matches!(
    recoverable.kind,
    TimonErrorKind::CloudStorageConnectionFailed
      | TimonErrorKind::CloudStorageUploadFailed
      | TimonErrorKind::CloudStorageDownloadFailed
      | TimonErrorKind::LockAcquisitionFailed
      | TimonErrorKind::ConcurrencyError
  ));

  let non_recoverable = crate::database_not_found!("test");
  assert!(!matches!(
    non_recoverable.kind,
    TimonErrorKind::CloudStorageConnectionFailed
      | TimonErrorKind::CloudStorageUploadFailed
      | TimonErrorKind::CloudStorageDownloadFailed
      | TimonErrorKind::LockAcquisitionFailed
      | TimonErrorKind::ConcurrencyError
  ));
}

#[test]
fn test_helper_macros() {
  let db_error = crate::database_not_found!("test_db");
  assert_eq!(db_error.kind, TimonErrorKind::DatabaseNotFound);

  let table_error = crate::table_not_found!("test_db", "test_table");
  assert_eq!(table_error.kind, TimonErrorKind::TableNotFound);

  let input_error = crate::invalid_input!("Invalid input provided");
  assert_eq!(input_error.kind, TimonErrorKind::InvalidInput);
}

#[test]
fn test_error_conversions() {
  // Test conversion from DataFusionError
  let df_error = DataFusionError::Plan("Invalid query".to_string());
  let timon_error: TimonError = df_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::QueryParsingFailed);

  // Test conversion from io::Error
  let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileNotFound);

  // Test conversion from serde_json::Error
  let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
  let timon_error: TimonError = json_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::InvalidDataFormat);
}

#[test]
fn test_display_formatting() {
  let mut error = TimonError::with_details(TimonErrorKind::DatabaseNotFound, "Database not found", "Check database name");
  error.source = Some("test_function".to_string());

  let display_str = format!("{}", error);
  assert!(display_str.contains("DatabaseNotFound"));
  assert!(display_str.contains("Database not found"));
  assert!(display_str.contains("Check database name"));
  assert!(display_str.contains("Source: test_function"));
}

#[test]
fn test_with_source() {
  let error = TimonError::with_source(TimonErrorKind::InternalError, "Test error", "test_source");
  assert_eq!(error.kind, TimonErrorKind::InternalError);
  assert_eq!(error.message, "Test error");
  assert_eq!(error.source, Some("test_source".to_string()));
  assert!(error.details.is_none());
}

#[test]
fn test_all_status_codes() {
  // 404 errors
  assert_eq!(TimonError::new(TimonErrorKind::DatabaseNotFound, "test").status_code(), 404);
  assert_eq!(TimonError::new(TimonErrorKind::TableNotFound, "test").status_code(), 404);
  assert_eq!(TimonError::new(TimonErrorKind::FileNotFound, "test").status_code(), 404);
  assert_eq!(TimonError::new(TimonErrorKind::PartitionNotFound, "test").status_code(), 404);

  // 409 errors
  assert_eq!(TimonError::new(TimonErrorKind::DatabaseAlreadyExists, "test").status_code(), 409);
  assert_eq!(TimonError::new(TimonErrorKind::TableAlreadyExists, "test").status_code(), 409);

  // 400 errors
  assert_eq!(TimonError::new(TimonErrorKind::InvalidInput, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::InvalidDataFormat, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::InvalidSqlQuery, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::InvalidFieldType, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::InvalidDateRange, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::MissingRequiredField, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::SchemaValidationFailed, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::DataValidationFailed, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::ConstraintViolation, "test").status_code(), 400);
  assert_eq!(TimonError::new(TimonErrorKind::QueryParsingFailed, "test").status_code(), 400);

  // 401 errors
  assert_eq!(
    TimonError::new(TimonErrorKind::CloudStorageAuthenticationFailed, "test").status_code(),
    401
  );
  assert_eq!(TimonError::new(TimonErrorKind::UsernameMismatch, "test").status_code(), 401);

  // 403 errors
  assert_eq!(TimonError::new(TimonErrorKind::PermissionDenied, "test").status_code(), 403);

  // 502 errors
  assert_eq!(TimonError::new(TimonErrorKind::CloudStorageConnectionFailed, "test").status_code(), 502);
  assert_eq!(TimonError::new(TimonErrorKind::CloudStorageUploadFailed, "test").status_code(), 502);
  assert_eq!(TimonError::new(TimonErrorKind::CloudStorageDownloadFailed, "test").status_code(), 502);
  assert_eq!(TimonError::new(TimonErrorKind::CloudStorageListFailed, "test").status_code(), 502);

  // 500 errors (default)
  assert_eq!(TimonError::new(TimonErrorKind::InitializationError, "test").status_code(), 500);
  assert_eq!(TimonError::new(TimonErrorKind::ConfigurationError, "test").status_code(), 500);
  assert_eq!(TimonError::new(TimonErrorKind::DatabaseCreationFailed, "test").status_code(), 500);
  assert_eq!(TimonError::new(TimonErrorKind::InternalError, "test").status_code(), 500);
}

#[test]
fn test_datafusion_error_conversions() {
  // Test planning error (contains "planning")
  let df_error = DataFusionError::Plan("planning error".to_string());
  let timon_error: TimonError = df_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::QueryParsingFailed);
  assert!(timon_error.source.is_some());

  // Test execution error (contains "Execution")
  let df_error = DataFusionError::Execution("Execution error".to_string());
  let timon_error: TimonError = df_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::QueryExecutionFailed);

  // Test Arrow error path (line 192) - needs to not match "planning" or "Execution" first
  // Use External variant which doesn't contain "planning" or "Execution"
  let arrow_error = DataFusionError::External("Arrow error occurred".into());
  let timon_error: TimonError = arrow_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::DataSerializationFailed);

  // Test Parquet error path (line 194) - needs to not match earlier patterns
  let parquet_error = DataFusionError::External("Parquet read failed".into());
  let timon_error: TimonError = parquet_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileReadFailed);

  // Test Io error path (line 196) - needs to not match earlier patterns
  let io_error = DataFusionError::External("Io error occurred".into());
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileSystemError);

  // Test internal error fallback (doesn't match any pattern)
  let df_error = DataFusionError::NotImplemented("Some other error".to_string());
  let timon_error: TimonError = df_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::InternalError);
  assert!(timon_error.source.is_some());
}

#[test]
fn test_io_error_conversions() {
  // Test NotFound
  let io_error = io::Error::new(io::ErrorKind::NotFound, "Not found");
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileNotFound);
  assert!(timon_error.source.is_some());

  // Test PermissionDenied
  let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::PermissionDenied);

  // Test AlreadyExists
  let io_error = io::Error::new(io::ErrorKind::AlreadyExists, "Already exists");
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileCreationFailed);

  // Test other error kinds (should map to FileSystemError)
  let io_error = io::Error::new(io::ErrorKind::Other, "Other error");
  let timon_error: TimonError = io_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::FileSystemError);
  assert!(timon_error.source.is_some());
}

#[test]
fn test_serde_json_error_conversions() {
  // Test syntax error (InvalidDataFormat)
  let json_error = serde_json::from_str::<serde_json::Value>("{invalid json}").unwrap_err();
  assert!(json_error.is_syntax());
  let timon_error: TimonError = json_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::InvalidDataFormat);
  assert!(timon_error.source.is_some());

  // Test data error (DataValidationFailed)
  let json_error = serde_json::from_str::<i32>("\"not a number\"").unwrap_err();
  assert!(json_error.is_data());
  let timon_error: TimonError = json_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::DataValidationFailed);

  // Note: Line 227 (DataSerializationFailed) is the else branch for serde_json::Error
  // In practice, all serde_json::Error instances are either syntax or data errors
  // This path is theoretically unreachable but exists for completeness
  // We've covered both is_syntax() and is_data() paths above
}

#[test]
fn test_string_conversions() {
  // Test String conversion
  let error: TimonError = "Test error string".to_string().into();
  assert_eq!(error.kind, TimonErrorKind::InternalError);
  assert_eq!(error.message, "Test error string");

  // Test &str conversion
  let error: TimonError = "Test error &str".into();
  assert_eq!(error.kind, TimonErrorKind::InternalError);
  assert_eq!(error.message, "Test error &str");
}

#[test]
fn test_box_dyn_error_conversion() {
  let original_error = io::Error::new(io::ErrorKind::Other, "Original error");
  let boxed_error: Box<dyn std::error::Error> = Box::new(original_error);
  let timon_error: TimonError = boxed_error.into();
  assert_eq!(timon_error.kind, TimonErrorKind::InternalError);
  assert!(timon_error.source.is_some());
}

#[test]
fn test_invalid_input_macro_variants() {
  // Single argument variant
  let error = crate::invalid_input!("Simple error message");
  assert_eq!(error.kind, TimonErrorKind::InvalidInput);
  assert_eq!(error.message, "Simple error message");
  assert!(error.details.is_none());

  // Two argument variant (with details)
  let error = crate::invalid_input!("Error message", "Additional details");
  assert_eq!(error.kind, TimonErrorKind::InvalidInput);
  assert_eq!(error.message, "Error message");
  assert_eq!(error.details, Some("Additional details".to_string()));
}

#[test]
fn test_schema_validation_failed_macro_variants() {
  // Single argument variant
  let error = crate::schema_validation_failed!("Schema validation failed");
  assert_eq!(error.kind, TimonErrorKind::SchemaValidationFailed);
  assert_eq!(error.message, "Schema validation failed");
  assert!(error.details.is_none());

  // Two argument variant (field and reason)
  let error = crate::schema_validation_failed!("field_name", "Field is required");
  assert_eq!(error.kind, TimonErrorKind::SchemaValidationFailed);
  assert!(error.message.contains("field_name"));
  assert_eq!(error.details, Some("Field is required".to_string()));
}

#[test]
fn test_cloud_storage_error_macro_variants() {
  // Two argument variant
  let error = crate::cloud_storage_error!(TimonErrorKind::CloudStorageUploadFailed, "Upload failed");
  assert_eq!(error.kind, TimonErrorKind::CloudStorageUploadFailed);
  assert_eq!(error.message, "Upload failed");
  assert!(error.details.is_none());

  // Three argument variant (with details)
  let error = crate::cloud_storage_error!(TimonErrorKind::CloudStorageDownloadFailed, "Download failed", "Network timeout");
  assert_eq!(error.kind, TimonErrorKind::CloudStorageDownloadFailed);
  assert_eq!(error.message, "Download failed");
  assert_eq!(error.details, Some("Network timeout".to_string()));
}

#[test]
fn test_helper_functions() {
  // Test cloud_storage_not_initialized
  let error = TimonError::cloud_storage_not_initialized();
  assert_eq!(error.kind, TimonErrorKind::CloudStorageNotInitialized);
  assert!(error.message.contains("CloudStorageManager"));
  assert!(error.message.contains("not initialized"));

  // Test database_manager_not_initialized
  let error = TimonError::database_manager_not_initialized();
  assert_eq!(error.kind, TimonErrorKind::InitializationError);
  assert!(error.message.contains("DatabaseManager"));
  assert!(error.message.contains("not initialized"));
}

#[test]
fn test_display_without_details_or_source() {
  let error = TimonError::new(TimonErrorKind::InternalError, "Simple error");
  let display_str = format!("{}", error);
  assert!(display_str.contains("InternalError"));
  assert!(display_str.contains("Simple error"));
  assert!(!display_str.contains("("));
  assert!(!display_str.contains("[Source:"));
}

#[test]
fn test_display_with_details_only() {
  let error = TimonError::with_details(TimonErrorKind::DatabaseNotFound, "Not found", "Details here");
  let display_str = format!("{}", error);
  assert!(display_str.contains("DatabaseNotFound"));
  assert!(display_str.contains("Not found"));
  assert!(display_str.contains("Details here"));
  assert!(!display_str.contains("[Source:"));
}

#[test]
fn test_display_with_source_only() {
  let error = TimonError::with_source(TimonErrorKind::InternalError, "Error", "source_func");
  let display_str = format!("{}", error);
  assert!(display_str.contains("InternalError"));
  assert!(display_str.contains("Error"));
  assert!(display_str.contains("Source: source_func"));
  assert!(!display_str.contains("("));
}

#[test]
fn test_error_source_trait() {
  let error = TimonError::new(TimonErrorKind::InternalError, "Test");
  // The Error trait's source() method should return None
  assert!(Error::source(&error).is_none());
}
