use crate::timon_engine::errors::{TimonError, TimonErrorKind};
use datafusion::error::DataFusionError;
use serde_json::json;
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
