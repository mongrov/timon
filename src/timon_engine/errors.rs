use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Standardized error types for the Timon library
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimonErrorKind {
  // Initialization errors
  InitializationError,
  ConfigurationError,

  // Database operation errors
  DatabaseNotFound,
  DatabaseAlreadyExists,
  DatabaseCreationFailed,
  DatabaseDeletionFailed,

  // Table operation errors
  TableNotFound,
  TableAlreadyExists,
  TableCreationFailed,
  TableDeletionFailed,
  SchemaValidationFailed,

  // Data operation errors
  DataInsertionFailed,
  DataValidationFailed,
  DataSerializationFailed,
  DataDeserializationFailed,
  InvalidDataFormat,
  ConstraintViolation,

  // Query operation errors
  QueryExecutionFailed,
  QueryParsingFailed,
  InvalidSqlQuery,
  PartitionNotFound,

  // File system errors
  FileSystemError,
  FileNotFound,
  FileCreationFailed,
  FileReadFailed,
  FileWriteFailed,
  DirectoryCreationFailed,
  PermissionDenied,

  // Cloud storage errors
  CloudStorageNotInitialized,
  CloudStorageConnectionFailed,
  CloudStorageUploadFailed,
  CloudStorageDownloadFailed,
  CloudStorageListFailed,
  CloudStorageAuthenticationFailed,

  // Synchronization errors
  SyncOperationFailed,
  SyncMetadataUpdateFailed,
  UsernameMismatch,

  // Concurrency errors
  LockAcquisitionFailed,
  ConcurrencyError,

  // Metadata errors
  MetadataCorrupted,
  MetadataReadFailed,
  MetadataWriteFailed,

  // Validation errors
  InvalidInput,
  MissingRequiredField,
  InvalidFieldType,
  InvalidDateRange,

  // Internal errors
  InternalError,
  UnexpectedError,
}

/// Standardized error structure for Timon operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimonError {
  pub kind: TimonErrorKind,
  pub message: String,
  pub details: Option<String>,
  pub source: Option<String>,
  pub context: Option<serde_json::Value>,
}

impl TimonError {
  /// Create a new TimonError with the specified kind and message
  pub fn new(kind: TimonErrorKind, message: impl Into<String>) -> Self {
    Self {
      kind,
      message: message.into(),
      details: None,
      source: None,
      context: None,
    }
  }

  /// Create a new TimonError with additional details
  #[allow(dead_code)]
  pub fn with_details(kind: TimonErrorKind, message: impl Into<String>, details: impl Into<String>) -> Self {
    Self {
      kind,
      message: message.into(),
      details: Some(details.into()),
      source: None,
      context: None,
    }
  }

  /// Create a new TimonError with source information
  pub fn with_source(kind: TimonErrorKind, message: impl Into<String>, source: impl Into<String>) -> Self {
    Self {
      kind,
      message: message.into(),
      details: None,
      source: Some(source.into()),
      context: None,
    }
  }

  /// Get the HTTP status code for this error
  pub fn status_code(&self) -> u16 {
    match self.kind {
      TimonErrorKind::DatabaseNotFound | TimonErrorKind::TableNotFound | TimonErrorKind::FileNotFound | TimonErrorKind::PartitionNotFound => 404,

      TimonErrorKind::DatabaseAlreadyExists | TimonErrorKind::TableAlreadyExists => 409,

      TimonErrorKind::InvalidInput
      | TimonErrorKind::InvalidDataFormat
      | TimonErrorKind::InvalidSqlQuery
      | TimonErrorKind::InvalidFieldType
      | TimonErrorKind::InvalidDateRange
      | TimonErrorKind::MissingRequiredField
      | TimonErrorKind::SchemaValidationFailed
      | TimonErrorKind::DataValidationFailed
      | TimonErrorKind::ConstraintViolation
      | TimonErrorKind::QueryParsingFailed => 400,

      TimonErrorKind::CloudStorageAuthenticationFailed | TimonErrorKind::UsernameMismatch => 401,

      TimonErrorKind::PermissionDenied => 403,

      TimonErrorKind::CloudStorageConnectionFailed
      | TimonErrorKind::CloudStorageUploadFailed
      | TimonErrorKind::CloudStorageDownloadFailed
      | TimonErrorKind::CloudStorageListFailed => 502,

      _ => 500,
    }
  }
}

impl fmt::Display for TimonError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}: {}", self.kind, self.message)?;

    if let Some(details) = &self.details {
      write!(f, " ({})", details)?;
    }

    if let Some(source) = &self.source {
      write!(f, " [Source: {}]", source)?;
    }

    Ok(())
  }
}

impl std::error::Error for TimonError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    None
  }
}

/// Result type alias for Timon operations
pub type TimonResult<T> = Result<T, TimonError>;

/// Conversion from DataFusionError to TimonError
impl From<DataFusionError> for TimonError {
  fn from(err: DataFusionError) -> Self {
    let err_str = err.to_string();
    let kind = if err_str.contains("planning") {
      TimonErrorKind::QueryParsingFailed
    } else if err_str.contains("Execution") {
      TimonErrorKind::QueryExecutionFailed
    } else if err_str.contains("Arrow") {
      TimonErrorKind::DataSerializationFailed
    } else if err_str.contains("Parquet") {
      TimonErrorKind::FileReadFailed
    } else if err_str.contains("Io") {
      TimonErrorKind::FileSystemError
    } else {
      TimonErrorKind::InternalError
    };

    TimonError::with_source(kind, "DataFusion operation failed", err_str)
  }
}

/// Conversion from std::io::Error to TimonError
impl From<std::io::Error> for TimonError {
  fn from(err: std::io::Error) -> Self {
    let kind = match err.kind() {
      std::io::ErrorKind::NotFound => TimonErrorKind::FileNotFound,
      std::io::ErrorKind::PermissionDenied => TimonErrorKind::PermissionDenied,
      std::io::ErrorKind::AlreadyExists => TimonErrorKind::FileCreationFailed,
      _ => TimonErrorKind::FileSystemError,
    };

    TimonError::with_source(kind, "File system operation failed", err.to_string())
  }
}

/// Conversion from serde_json::Error to TimonError
impl From<serde_json::Error> for TimonError {
  fn from(err: serde_json::Error) -> Self {
    let kind = if err.is_data() {
      TimonErrorKind::DataValidationFailed
    } else if err.is_syntax() {
      TimonErrorKind::InvalidDataFormat
    } else {
      TimonErrorKind::DataSerializationFailed
    };

    TimonError::with_source(kind, "JSON operation failed", err.to_string())
  }
}

/// Conversion from Box<dyn std::error::Error> to TimonError
impl From<Box<dyn std::error::Error>> for TimonError {
  fn from(err: Box<dyn std::error::Error>) -> Self {
    TimonError::with_source(TimonErrorKind::InternalError, "Operation failed", err.to_string())
  }
}

/// Conversion from String to TimonError (for backward compatibility)
impl From<String> for TimonError {
  fn from(err: String) -> Self {
    TimonError::new(TimonErrorKind::InternalError, err)
  }
}

/// Conversion from &str to TimonError (for backward compatibility)
impl From<&str> for TimonError {
  fn from(err: &str) -> Self {
    TimonError::new(TimonErrorKind::InternalError, err)
  }
}

/// Helper macros for creating specific error types
#[macro_export]
macro_rules! database_not_found {
  ($db_name:expr) => {
    TimonError::new(TimonErrorKind::DatabaseNotFound, format!("Database '{}' does not exist", $db_name))
  };
}

#[macro_export]
macro_rules! table_not_found {
  ($db_name:expr, $table_name:expr) => {
    TimonError::new(
      TimonErrorKind::TableNotFound,
      format!("Table '{}.{}' does not exist", $db_name, $table_name),
    )
  };
}

#[macro_export]
macro_rules! invalid_input {
  ($msg:expr) => {
    TimonError::new(TimonErrorKind::InvalidInput, $msg)
  };
  ($msg:expr, $details:expr) => {
    TimonError::with_details(TimonErrorKind::InvalidInput, $msg, $details)
  };
}

#[macro_export]
macro_rules! schema_validation_failed {
  ($msg:expr) => {
    TimonError::new(TimonErrorKind::SchemaValidationFailed, $msg)
  };
  ($field:expr, $reason:expr) => {
    TimonError::with_details(
      TimonErrorKind::SchemaValidationFailed,
      format!("Schema validation failed for field '{}'", $field),
      $reason,
    )
  };
}

#[macro_export]
macro_rules! cloud_storage_error {
  ($kind:expr, $msg:expr) => {
    TimonError::new($kind, $msg)
  };
  ($kind:expr, $msg:expr, $details:expr) => {
    TimonError::with_details($kind, $msg, $details)
  };
}

/// Helper functions for creating common errors
impl TimonError {
  pub fn cloud_storage_not_initialized() -> Self {
    TimonError::new(
      TimonErrorKind::CloudStorageNotInitialized,
      "CloudStorageManager is not initialized. Please call init_bucket() first.",
    )
  }

  pub fn database_manager_not_initialized() -> Self {
    TimonError::new(
      TimonErrorKind::InitializationError,
      "DatabaseManager is not initialized. Please call init_timon() first.",
    )
  }
}
