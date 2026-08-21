use super::errors::TimonError;
use super::helpers::{
  build_rules_tree, collect_files_recursive, get_property_fields, infer_schema_with_coercion, json_to_arrow, record_batches_to_json,
  rounded_timestamp, row_to_json,
};
use super::sql_query_parser::{extract_table_names_and_ctes, inject_partition_filter};
include!("imports/db_manager.inc");

// ============================================================================
// Configuration Constants
// ============================================================================
// These constants define timing, retry, and cleanup thresholds used throughout
// the database manager. They are extracted from magic numbers to improve
// maintainability and make the codebase easier to tune for different use cases.

/// Interval for cleaning up unused file locks (60 minutes).
/// Locks that haven't been accessed within this interval are removed to prevent
/// the lock HashMap from growing indefinitely.
const LOCK_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);

/// Threshold for removing unused file locks (60 minutes).
/// Locks unused for longer than this duration are considered stale and removed.
const LOCK_CLEANUP_THRESHOLD: Duration = Duration::from_secs(3600);

/// Age threshold for orphaned temporary files (1 hour).
/// Temporary files older than this are considered orphaned (from crashes/interruptions)
/// and are safely removed during startup cleanup.
const ORPHANED_TEMP_FILE_AGE_THRESHOLD: Duration = Duration::from_secs(3600);

/// Maximum number of retries when reading metadata file.
/// Metadata reads may fail transiently if the file is being written concurrently.
/// This value balances reliability with performance (avoiding excessive retries).
const METADATA_READ_MAX_RETRIES: usize = 10;

/// Delay between retries when reading metadata file (50ms).
/// Short delay allows quick recovery from transient file locking issues while
/// avoiding excessive CPU usage during retries.
const METADATA_READ_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Maximum number of retries when saving metadata file.
/// Metadata saves may fail transiently due to file system issues or concurrent access.
/// Fewer retries than reads since writes are less frequent and failures are more critical.
const METADATA_SAVE_MAX_RETRIES: usize = 5;

/// Delay between retries when saving metadata file (100ms).
/// Longer delay than read retries to allow file system operations to complete.
const METADATA_SAVE_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Maximum number of retries when acquiring metadata lock.
/// Used when updating metadata to prevent concurrent modifications.
const METADATA_LOCK_MAX_RETRIES: usize = 5;

/// Delay between retries when acquiring metadata lock (100ms).
/// Allows other processes/threads holding the lock to complete their operations.
const METADATA_LOCK_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Maximum number of metadata backup files to retain.
/// Prevents unlimited backup growth while maintaining a reasonable history for recovery.
const MAX_METADATA_BACKUPS: usize = 5;

/// Type of name being validated (Database or Table)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameType {
  Database,
  Table,
}

impl fmt::Display for NameType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      NameType::Database => write!(f, "Database"),
      NameType::Table => write!(f, "Table"),
    }
  }
}

// Structure to track file locks with their last access time
struct FileLockEntry {
  lock: Arc<Mutex<()>>,
  last_accessed: Instant,
}

// Global mutex map for file-level locking (process-wide, works across threads)
// Tracks last access time to enable cleanup of unused locks
fn get_file_locks() -> &'static Mutex<HashMap<String, FileLockEntry>> {
  static FILE_LOCKS: OnceLock<Mutex<HashMap<String, FileLockEntry>>> = OnceLock::new();
  FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cleanup_unused_locks(locks: &mut HashMap<String, FileLockEntry>) {
  let now = Instant::now();
  // Remove locks that haven't been accessed recently
  locks.retain(|_path, entry| now.duration_since(entry.last_accessed) < LOCK_CLEANUP_THRESHOLD);
}

pub enum DataFusionOutput {
  Json(Value),
  DataFrame(DataFrame),
}

impl fmt::Debug for DataFusionOutput {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DataFusionOutput::Json(s) => write!(f, "Json({})", s),
      DataFusionOutput::DataFrame(df) => {
        // Create runtime with proper error handling
        let runtime = match tokio::runtime::Runtime::new() {
          Ok(rt) => rt,
          Err(e) => {
            // If runtime creation fails, write error message instead of panicking
            return write!(f, "DataFrame(<Failed to create runtime: {}>)", e);
          }
        };

        // Collect DataFrame with proper error handling
        let result = match runtime.block_on(async { df.clone().collect().await }) {
          Ok(batches) => batches,
          Err(e) => {
            // If collection fails, write error message instead of panicking
            return write!(f, "DataFrame(<Failed to collect DataFrame results: {:?}>)", e);
          }
        };

        for batch in result {
          writeln!(f, "{:?}", batch)?;
        }
        Ok(())
      }
    }
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Metadata {
  databases: HashMap<String, Database>, // Maps database names to their corresponding database structure
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Database {
  tables: HashMap<String, Table>, // Maps table names to table schema
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Table {
  path: String,              // Path to the table
  schema: serde_json::Value, // Placeholder for your schema structure (optional)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DatabaseInfo {
  names: Vec<String>,
}

/// Combined metadata cache structure to ensure atomic updates
/// This prevents deadlocks by using a single lock instead of multiple locks
struct MetadataCache {
  metadata: Option<Metadata>,
  timestamp: Option<Instant>,
}

pub struct DatabaseManager {
  pub storage_path: String,
  pub username: String,
  metadata: Metadata,
  data_path: String,
  metadata_path: String,
  bucket_interval: u32,
  session_context: SessionContext,
  // Metadata cache with TTL - using single lock to prevent deadlocks
  cache: Arc<RwLock<MetadataCache>>,
  cache_ttl: Duration,
}

impl Clone for DatabaseManager {
  fn clone(&self) -> Self {
    // Create a new SessionContext for each clone to ensure isolation
    // This is important because SessionContext might not clone properly
    Self {
      storage_path: self.storage_path.clone(),
      username: self.username.clone(),
      metadata: self.metadata.clone(),
      data_path: self.data_path.clone(),
      metadata_path: self.metadata_path.clone(),
      bucket_interval: self.bucket_interval,
      session_context: SessionContext::new(), // Fresh context for each clone
      cache: Arc::clone(&self.cache),         // Share the same cache across clones
      cache_ttl: self.cache_ttl,
    }
  }
}

impl DatabaseManager {
  pub fn new(storage_path: &str, bucket_interval: u32, username: &str) -> Self {
    let data_path = format!("{}/data", storage_path);
    let metadata_path = format!("{}/metadata.json", storage_path);

    // Create the data directory if it doesn't exist
    // Note: This is critical for database operation; failure may cause subsequent operations to fail
    if let Err(e) = fs::create_dir_all(&data_path) {
      eprintln!(
        "Error: Failed to create critical data directory {}: {}. Database operations may fail.",
        data_path, e
      );
    }

    // Check if the metadata file exists
    if !Path::new(&metadata_path).exists() {
      // Create the metadata file if it doesn't exist
      match fs::File::create(&metadata_path) {
        Ok(_) => {
          // Write the initial metadata structure `{"databases":{}}` into the file
          let initial_metadata = Metadata { databases: HashMap::new() };
          match serde_json::to_string(&initial_metadata) {
            Ok(metadata_json) => {
              if let Err(e) = fs::write(&metadata_path, metadata_json) {
                eprintln!("Error writing initial metadata to file: {}", e);
              }
            }
            Err(e) => {
              eprintln!("Error serializing initial metadata: {}", e);
            }
          }
        }
        Err(e) => eprintln!("Error creating metadata file: {}", e),
      }
    }

    // Load existing metadata from metadata.json
    let metadata: Metadata = if Path::new(&metadata_path).exists() {
      match fs::read_to_string(&metadata_path) {
        Ok(file_content) => serde_json::from_str(&file_content).unwrap_or_else(|e| {
          eprintln!("Warning: Failed to parse metadata file, using empty metadata: {}", e);
          Metadata { databases: HashMap::new() }
        }),
        Err(e) => {
          eprintln!("Warning: Failed to read metadata file, using empty metadata: {}", e);
          Metadata { databases: HashMap::new() }
        }
      }
    } else {
      Metadata { databases: HashMap::new() }
    };

    // Create DatabaseManager instance
    let mut db_manager = DatabaseManager {
      storage_path: storage_path.to_string(),
      username: username.to_string(),
      metadata,
      data_path,
      metadata_path,
      bucket_interval,
      session_context: SessionContext::new(),
      // Initialize cache - infinite TTL, only invalidated on writes
      // Using single lock to prevent deadlocks from lock ordering issues
      cache: Arc::new(RwLock::new(MetadataCache {
        metadata: None,
        timestamp: None,
      })),
      cache_ttl: Duration::MAX, // Infinite cache - only invalidated on metadata changes
    };

    // Update metadata with the provided storage_path
    if let Err(e) = db_manager.update_metadata(storage_path) {
      eprintln!("Error updating metadata: {}", e);
    }

    // Clean up orphaned temp files on startup
    db_manager.cleanup_orphaned_temp_files();

    db_manager
  }

  /// Clean up orphaned temporary files that may have been left behind
  /// from previous crashes or interrupted writes. Removes .tmp files older than the threshold.
  fn cleanup_orphaned_temp_files(&self) {
    let cleanup_threshold = ORPHANED_TEMP_FILE_AGE_THRESHOLD;
    let data_path = Path::new(&self.data_path);

    if !data_path.exists() {
      return;
    }

    // Recursively walk through the data directory to find all .tmp files
    fn walk_and_cleanup(dir: &Path, threshold: Duration) -> usize {
      use std::time::SystemTime;
      let mut cleaned_count = 0;

      match fs::read_dir(dir) {
        Ok(entries) => {
          for entry in entries.flatten() {
            let path = entry.path();

            if path.is_dir() {
              // Recursively clean subdirectories
              cleaned_count += walk_and_cleanup(&path, threshold);
            } else if path.is_file() {
              // Check if it's a .tmp file
              if let Some(ext) = path.extension() {
                if ext == "tmp" {
                  // Check file modification time
                  match fs::metadata(&path) {
                    Ok(metadata) => {
                      if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = SystemTime::now().duration_since(modified) {
                          if age > threshold {
                            // File is older than threshold, safe to remove
                            if let Err(e) = fs::remove_file(&path) {
                              eprintln!("Warning: Failed to remove orphaned temp file {:?}: {}", path, e);
                            } else {
                              cleaned_count += 1;
                            }
                          }
                        }
                      }
                    }
                    Err(e) => {
                      eprintln!("Warning: Failed to get metadata for {:?}: {}", path, e);
                    }
                  }
                }
              }
            }
          }
        }
        Err(e) => {
          eprintln!("Warning: Failed to read directory {:?}: {}", dir, e);
        }
      }

      cleaned_count
    }

    let cleaned = walk_and_cleanup(data_path, cleanup_threshold);
    if cleaned > 0 {
      eprintln!("Cleaned up {} orphaned temporary file(s) on startup", cleaned);
    }
  }

  /// Fallback validation without regex (used when the static regex fails to compile).
  #[inline]
  fn name_matches_pattern(name: &str) -> bool {
    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
  }

  /// Validates database or table name to prevent path traversal attacks.
  /// Only allows alphanumeric characters and underscores (A-Z, a-z, 0-9, _).
  fn validate_name(name: &str, name_type: NameType) -> Result<(), DataFusionError> {
    // Check for empty names
    if name.is_empty() {
      return Err(DataFusionError::Plan(format!("{} name cannot be empty", name_type)));
    }

    // Use static regex when available; if it fails to compile (e.g. broken env), fall back to manual check (no panic).
    static VALID_PATTERN: OnceLock<Option<Regex>> = OnceLock::new();
    let valid_pattern = VALID_PATTERN.get_or_init(|| {
      Regex::new(r#"^[a-zA-Z0-9_]+$"#).map_or_else(
        |e| {
          eprintln!("CRITICAL: Failed to compile validation regex, using fallback: {:?}", e);
          None
        },
        Some,
      )
    });

    let ok = match valid_pattern {
      Some(re) => re.is_match(name),
      None => Self::name_matches_pattern(name),
    };
    if !ok {
      return Err(DataFusionError::Plan(format!(
        "{} name '{}' is not valid. Only alphanumeric characters and underscores (A-Z, a-z, 0-9, _) are allowed",
        name_type, name
      )));
    }

    Ok(())
  }

  pub fn create_database(&mut self, db_name: &str) -> Result<(), DataFusionError> {
    // Validate database name to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database)?;

    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    let db_data_path = format!("{}/{}", self.data_path, db_name);

    // Create a new directory for the database if it doesn't exist
    if let Err(e) = fs::create_dir(&db_data_path) {
      return Err(DataFusionError::Execution(format!("Error creating data directory {}: {}", db_name, e)));
    }

    // Insert the new database into the metadata
    self
      .metadata
      .databases
      .entry(db_name.to_string())
      .or_insert_with(|| Database { tables: HashMap::new() });

    // Save the updated metadata to metadata.json
    self
      .save_metadata()
      .map_err(|e| DataFusionError::Execution(format!("Failed to save metadata: {}", e)))?;

    Ok(())
  }

  pub fn create_table(&mut self, db_name: &str, table_name: &str, schema_json: &str) -> Result<String, Box<dyn Error>> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database).map_err(|e| e.to_string())?;
    Self::validate_name(table_name, NameType::Table).map_err(|e| e.to_string())?;

    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Parse the schema JSON
    let schema: Value = serde_json::from_str(schema_json)?;
    // First, we take the database path and validate the schema without borrowing `self` mutably.
    let db_path = self.metadata.databases.get_mut(db_name);
    if db_path.is_none() {
      return Err(format!("Database '{}' does not exist.", db_name).into());
    }

    // Validate the schema structure before doing any mutable operations
    self.validate_schema_structure(&schema)?;

    // Now perform mutable borrow only once after the immutable operations are done
    let database = self
      .metadata
      .databases
      .get_mut(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    // Check if the table already exists
    if database.tables.contains_key(table_name) {
      return Err(format!("Table '{}' already exists in database '{}'.", table_name, db_name).into());
    }

    // Create the table directory
    let table_path = format!("{}/{}/{}", self.data_path, db_name, table_name);
    fs::create_dir_all(&table_path)?;

    // Store the schema for future validation during inserts
    let table = Table { schema, path: table_path };
    database.tables.insert(table_name.to_string(), table);

    // Persist the metadata to disk (e.g., in a metadata.json or similar)
    self.save_metadata()?;

    Ok(format!("Table '{}' was successfully created in database '{}'.", table_name, db_name))
  }

  pub fn list_databases(&mut self) -> Result<Vec<String>, DataFusionError> {
    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Attempt to read metadata file and handle potential errors
    let file_content = match fs::read_to_string(&self.metadata_path) {
      Ok(content) => content,
      Err(e) => return Err(DataFusionError::Execution(format!("Failed to read metadata file: {}", e))),
    };

    // Attempt to parse the metadata and handle potential errors
    let metadata: Metadata = match serde_json::from_str(&file_content) {
      Ok(m) => m,
      Err(e) => return Err(DataFusionError::Execution(format!("Failed to parse metadata: {}", e))),
    };

    let databases_list = metadata.databases.keys().cloned().collect::<Vec<String>>();

    Ok(databases_list)
  }

  pub fn list_tables(&mut self, db_name: &str) -> Result<Vec<String>, DataFusionError> {
    // Validate database name to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database)?;

    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Check if the database exists in the metadata
    if let Some(database) = self.metadata.databases.get(db_name) {
      let tables_list = database.tables.keys().cloned().collect::<Vec<String>>();

      Ok(tables_list)
    } else {
      Err(DataFusionError::Plan(format!("Database '{}' not found", db_name)))
    }
  }

  pub fn delete_database(&mut self, db_name: &str) -> Result<(), DataFusionError> {
    // Validate database name to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database)?;

    // Reload the metadata to ensure it's up to date
    self.metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Remove the database from metadata and save changes
    if self.metadata.databases.remove(db_name).is_some() {
      self.save_metadata().map_err(|e| {
        eprintln!("Error saving metadata after deleting database '{}': {}", db_name, e);
        DataFusionError::Execution(format!("Failed to save metadata after deleting database: {}", e))
      })?;
    } else {
      return Err(DataFusionError::Plan(format!("Failed to remove database '{}' from metadata", db_name)));
    }

    // Remove database's directory from filesystem
    let db_path = format!("{}/{}", self.data_path, db_name);
    if fs::remove_dir_all(db_path).is_err() {
      return Err(DataFusionError::Plan(format!("Failed to remove database directory '{}'", db_name)));
    }

    Ok(())
  }

  pub fn delete_table(&mut self, db_name: &str, table_name: &str) -> Result<(), DataFusionError> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database)?;
    Self::validate_name(table_name, NameType::Table)?;

    // Reload the metadata to ensure it's up to date
    // If metadata file doesn't exist (file not found), that's OK - table might not exist either
    // But if metadata file exists but is corrupted, that's an error
    match self.get_metadata_cached_sync() {
      Ok(metadata) => {
        self.metadata = metadata;
      }
      Err(e) => {
        // Check if it's a "file not found" error - that's OK, we can proceed
        // But if it's a JSON parse error (corrupted file), we should return the error
        let error_msg = e.to_string();
        if error_msg.contains("No such file") || error_msg.contains("not found") {
          // Metadata file doesn't exist, which is fine - table might not exist either
          // We'll proceed with the deletion attempt
        } else {
          // Metadata file exists but is corrupted - return error
          return Err(DataFusionError::Execution(format!("Failed to reload metadata: {}", e)));
        }
      }
    }

    // Check if the database exists
    if let Some(db) = self.metadata.databases.get_mut(db_name) {
      // Check if the table exists and remove it
      if db.tables.remove(table_name).is_some() {
        // Save the updated metadata
        self.save_metadata().map_err(|e| {
          eprintln!("Error saving metadata after deleting table '{}': {}", table_name, e);
          DataFusionError::Execution(format!("Failed to save metadata after deleting table: {}", e))
        })?;

        // Remove table's directory from filesystem
        let table_path = format!("{}/{}/{}", self.data_path, db_name, table_name);
        if fs::remove_dir_all(table_path).is_err() {
          return Err(DataFusionError::Plan(format!("Failed to remove table directory '{}'", table_name)));
        }

        Ok(())
      } else {
        Err(DataFusionError::Plan(format!(
          "Table '{}' not found in database '{}'",
          table_name, db_name
        )))
      }
    } else {
      Err(DataFusionError::Plan(format!("Database '{}' not found", db_name)))
    }
  }

  pub fn insert(&mut self, db_name: &str, table_name: &str, json_data: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database).map_err(|e| e.to_string())?;
    Self::validate_name(table_name, NameType::Table).map_err(|e| e.to_string())?;

    // Reload metadata
    self.metadata = self.get_metadata_cached_sync()?;

    let mut new_json_values: Vec<Value> = serde_json::from_str(json_data)?;
    let table_path = self
      .get_table_path(db_name, table_name)
      .ok_or_else(|| format!("Database '{}' or Table '{}' does not exist.", db_name, table_name))?;
    let table_schema = self.get_table_schema(db_name, table_name)?;

    let conditions = build_rules_tree(table_schema.clone());
    let mut invalid_json_values = Vec::new();
    if !conditions.is_empty() {
      let tree = json_rules_engine::and(conditions);
      for json_value in &new_json_values {
        let result = tree.check_value(json_value);
        if result.status == json_rules_engine::Status::NotMet {
          println!("record condition mismatch: {}", json_value);
          invalid_json_values.push(json_value.clone());
        }
      }
    }

    let datetime_binding = get_property_fields(&table_schema, "datetime")?;
    let datetime_field = datetime_binding.first().ok_or("No 'datetime' field found in the table schema.")?;
    let unique_fields = get_property_fields(&table_schema, "unique")?;

    let build_key = |record: &Value| -> String {
      unique_fields
        .iter()
        .map(|field| record.get(field).map(|v| v.to_string()).unwrap_or_default())
        .collect::<Vec<String>>()
        .join("-")
    };

    // Field order from table schema (metadata) so written Parquet columns match schema order
    let schema_field_order: Vec<String> = table_schema.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();

    // Ensure datetime fields are present and convert them to timestamps and convert them to timestamps
    for json_value in new_json_values.iter_mut() {
      match json_value.get(datetime_field) {
        Some(Value::String(date_str)) => {
          let parsed_timestamp = NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.3fZ"))
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.fZ"))
            .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S"))
            .map(|naive_dt| Utc.from_utc_datetime(&naive_dt).timestamp());

          match parsed_timestamp {
            Ok(timestamp) => {
              json_value[datetime_field] = json!(timestamp);
            }
            Err(_) => return Err(format!("Invalid datetime format for field '{}'.", datetime_field).into()),
          }
        }
        _ => return Err(format!("Missing required datetime field: '{}'.", datetime_field).into()),
      }
    }

    for json_value in new_json_values.iter_mut() {
      self.validate_data_against_schema(&table_schema, json_value)?;
    }

    // Group new records by target file (partition)
    // This allows us to process each file atomically
    let mut seen_records: HashMap<String, Value> = HashMap::new();
    let mut records_by_file: HashMap<String, Vec<Value>> = HashMap::new();

    for new_record in new_json_values.into_iter() {
      let key = build_key(&new_record);
      // Skip duplicates within the same insert batch
      if seen_records.insert(key.clone(), new_record.clone()).is_some() {
        continue;
      }

      let timestamp = new_record.get(datetime_field).and_then(|t| t.as_i64()).unwrap_or(0);
      let partition_value = rounded_timestamp(timestamp, self.bucket_interval);

      // Use Hive-style partitioning: partition_date=YYYY-MM-DD/data.parquet
      let partition_dir = format!("{}/partition_date={}", table_path, partition_value);
      // Create partition directory if it doesn't exist
      if let Err(e) = fs::create_dir_all(&partition_dir) {
        eprintln!("Error creating partition directory '{}': {}", partition_dir, e);
        return Err(format!("Failed to create partition directory '{}': {}", partition_dir, e).into());
      }
      let target_file = format!("{}/data.parquet", partition_dir);

      records_by_file.entry(target_file).or_default().push(new_record);
    }

    // Process each file atomically: lock -> read -> merge -> write -> unlock
    for (file_path, new_records) in records_by_file {
      let file_path_clone = file_path.clone();
      if let Err(e) = Self::atomic_file_insert(
        Path::new(&file_path_clone),
        &new_records,
        &build_key,
        Some(&schema_field_order),
        Some(&table_schema),
      ) {
        // Log the error but don't fail the entire insert operation
        // This allows other files to be processed even if one fails
        eprintln!("Error in atomic_file_insert for '{}': {}", file_path_clone, e);
        return Err(format!("Failed to insert records into file '{}': {}", file_path_clone, e).into());
      }
    }

    Ok(invalid_json_values)
  }

  pub async fn query(
    &self,
    db_name: &str,
    sql_query: &str,
    username: Option<&str>,
    is_json_format: bool,
    limit_partitions: Option<usize>,
  ) -> DataFusionResult<DataFusionOutput> {
    // Validate database name to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database)?;

    // Extract table names and CTE names from the AST
    let (mut table_names, cte_names) =
      extract_table_names_and_ctes(sql_query).map_err(|e| DataFusionError::Execution(format!("Failed to extract table names: {}", e)))?;

    // Validate all table names to prevent path traversal attacks
    for table_name in &table_names {
      Self::validate_name(table_name, NameType::Table)?;
    }
    // Remove CTE names from table names (CTEs are not real tables)
    table_names.retain(|name| !cte_names.contains(name));

    // Load metadata
    let metadata = self
      .get_metadata_cached()
      .await
      .map_err(|e| DataFusionError::Execution(format!("Failed to read metadata: {}", e)))?;

    // Validate that all tables exist before attempting registration
    // For group users, we allow tables that don't exist in metadata (they might exist in group paths)
    // For default user (None), tables must exist in metadata
    if let Some(database) = metadata.databases.get(db_name) {
      for table_name in &table_names {
        if !database.tables.contains_key(table_name) {
          // For group users, allow tables not in metadata (they might exist in group paths)
          if username.is_none() {
            return Err(DataFusionError::Plan(format!(
              "Table '{}' referenced in query does not exist in database '{}'",
              table_name, db_name
            )));
          }
        }
      }

      for table_name in &table_names {
        self.register_single_table(db_name, table_name, username).await?;
      }
    }

    // If limit_partitions is set, modify the SQL query to only scan last N partitions
    let effective_sql = if let Some(limit) = limit_partitions {
      // Get all partition directories for all tables in the database
      let mut all_partitions = Vec::new();
      if let Some(database) = metadata.databases.get(db_name) {
        for table_name in database.tables.keys() {
          let table_dir = self
            .resolve_table_dir(db_name, table_name, username)
            .map_err(|e| DataFusionError::Execution(format!("Failed to resolve table directory: {}", e)))?;

          if let Ok(entries) = std::fs::read_dir(&table_dir) {
            for entry in entries.flatten() {
              if entry.path().is_dir() {
                if let Some(name) = entry.path().file_name().and_then(|n| n.to_str()) {
                  if name.starts_with("partition_date=") {
                    let date_value = name.strip_prefix("partition_date=").unwrap_or("");
                    if !all_partitions.contains(&date_value.to_string()) {
                      all_partitions.push(date_value.to_string());
                    }
                  }
                }
              }
            }
          }
        }
      }

      // Sort and take last N partitions (most recent)
      all_partitions.sort();
      let selected_dates: Vec<_> = all_partitions.iter().rev().take(limit).cloned().collect();
      // Inject the partition_date filter through the AST so that trailing clauses (ORDER BY, LIMIT,
      // GROUP BY, HAVING) keep their position and an existing WHERE clause is combined with AND
      inject_partition_filter(sql_query, "partition_date", &selected_dates)
        .map_err(|e| DataFusionError::Execution(format!("Failed to apply partition limit: {}", e)))?
    } else {
      sql_query.to_string()
    };

    // Execute the query directly without manual UNION/CTEs; ListingTable handles partitions
    let final_df = self.session_context.sql(&effective_sql).await?;
    let final_results = final_df.collect().await?;

    let result = if is_json_format {
      let json_result = record_batches_to_json(&final_results)
        .map_err(|e| DataFusionError::Execution(format!("Failed to convert record batches to JSON: {:?}", e)))?;
      DataFusionOutput::Json(json_result)
    } else {
      let final_schema = final_results[0].schema();
      let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
      let final_df = self.session_context.read_table(Arc::new(final_mem_table))?;
      DataFusionOutput::DataFrame(final_df)
    };

    Ok(result)
  }

  async fn register_single_table(&self, db_name: &str, table_name: &str, username: Option<&str>) -> DataFusionResult<()> {
    // ALWAYS deregister the table FIRST, before resolving the path
    // This is critical: even if the table doesn't exist, try to deregister it
    // This ensures we don't use a stale registration from a previous query with a different username
    // The deregister will fail silently if the table doesn't exist, which is fine
    let _ = self.session_context.deregister_table(table_name);

    // Resolve the table directory to get the correct path for this username
    let table_dir = match self.resolve_table_dir(db_name, table_name, username) {
      Ok(dir) => dir,
      Err(e) => {
        // For group users, if table doesn't exist in their path, skip registration silently
        // This allows multi-table joins where the user doesn't have access to all tables
        if username.is_some() {
          eprintln!(
            "INFO: Table '{}' does not exist for group user '{:?}': {}. Skipping registration - query will return 0 rows for this table.",
            table_name, username, e
          );
          return Ok(());
        } else {
          // For local users, this is an error - the table should exist
          let error = TimonError::table_not_found_for_user(table_name, username);
          return Err(DataFusionError::Plan(error.to_string()));
        }
      }
    };

    // Check if table directory has any parquet files
    let has_parquet_files = std::fs::read_dir(&table_dir)
      .map(|entries| {
        entries.filter_map(|e| e.ok()).any(|e| {
          let path = e.path();
          path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet")
            || (path.is_dir() && {
              // Check subdirectories (partitions) for parquet files
              std::fs::read_dir(&path)
                .map(|sub_entries| {
                  sub_entries
                    .filter_map(|se| se.ok())
                    .any(|se| se.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
                })
                .unwrap_or(false)
            })
        })
      })
      .unwrap_or(false);

    // If no parquet files exist yet, handle based on user type
    if !has_parquet_files {
      // For group users, skip registration silently (they might not have data in this table yet)
      if username.is_some() {
        eprintln!(
          "INFO: Table '{}' exists for group user '{:?}' but contains no parquet files. Skipping registration - query will return 0 rows for this table.",
          table_name, username
        );
        return Ok(());
      } else {
        // For local users, this is an error condition - the table exists but has no data
        // This helps distinguish between "empty result set" and "table not properly initialized"
        let error = TimonError::no_data_available(table_name);
        return Err(DataFusionError::Plan(error.to_string()));
      }
    }

    // - Local users Use Hive-style partitioning, Group users use no partitioning.
    let file_format = ParquetFormat::default();
    let listing_options = if username.is_none() {
      // Local users: Use partition_date=YYYY-MM-DD/data.parquet structure
      ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_table_partition_cols(vec![(
          "partition_date".to_string(),
          DataType::Utf8, // Partition values are stored as strings in directory names
        )])
    } else {
      // Group users: Files are directly in the table directory (e.g., hrv_table_2025-07-28.parquet)
      ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet")
    };

    // Create the listing table URL
    let table_url = ListingTableUrl::parse(&table_dir).map_err(|e| DataFusionError::Execution(format!("Failed to parse table URL: {}", e)))?;

    // Infer schema with type coercion to handle schema mismatches (e.g., Int64 vs Float64)
    let merged_schema = match infer_schema_with_coercion(&table_dir).await {
      Ok(schema) => {
        eprintln!(
          "Successfully merged schema for table '{}' with {} fields",
          table_name,
          schema.fields().len()
        );
        Some(schema)
      }
      Err(e) => {
        eprintln!(
          "Warning: Failed to infer schema with coercion for table '{}': {}. Falling back to DataFusion's default inference.",
          table_name, e
        );
        None
      }
    };

    // Configure the listing table with merged schema or fallback to default inference
    let config = if let Some(schema) = merged_schema {
      ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema)
    } else {
      ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(&self.session_context.state())
        .await?
    };

    let listing_table = ListingTable::try_new(config)?;

    // Register the table in the session context
    self
      .session_context
      .register_table(table_name, Arc::new(listing_table))
      .map_err(|e| DataFusionError::Execution(format!("Failed to register table '{}': {}", table_name, e)))?;

    Ok(())
  }

  // Resolve the effective directory path for a logical table, preferring group/user path when provided
  fn resolve_table_dir(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<String, Box<dyn Error>> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database).map_err(|e| e.to_string())?;
    Self::validate_name(table_name, NameType::Table).map_err(|e| e.to_string())?;

    // Reload metadata to ensure it's up-to-date
    let metadata = self.get_metadata_cached_sync()?;

    let database = metadata
      .databases
      .get(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    // For group users, we need to be more flexible - they might have tables that don't exist in metadata
    // (e.g., `spo2` in group paths vs `spo2_readings` in default path)
    // So we'll try to resolve the path even if the table isn't in metadata
    let base_table_path = if let Some(table) = database.tables.get(table_name) {
      Path::new(&table.path)
    } else {
      // Table doesn't exist in metadata - this is OK for group users
      // We'll construct a path based on the default structure
      // For None user, this will fail later when we try to use the path
      // For group users, we'll check their group path
      if username.is_none() {
        return Err(format!("Table '{}' does not exist in database '{}'.", table_name, db_name).into());
      }
      // For group users, we'll construct a synthetic path to get the base root
      // We need to find any table in the database to get the base structure
      if let Some((_, any_table)) = database.tables.iter().next() {
        Path::new(&any_table.path)
      } else {
        return Err(format!("Database '{}' has no tables to determine base path structure.", db_name).into());
      }
    };

    // Extract base root - go up from "data" to get the storage root (e.g., "tmp")
    // Path structure: tmp/data/zivaring/activitydetails
    // ancestors: activitydetails (0), zivaring (1), data (2), tmp (3)
    // We want tmp, which is the parent of "data"
    let base_root = base_table_path
      .ancestors()
      .nth(2) // This gives us "tmp/data"
      .ok_or(format!("Failed to determine base directory from '{}'", base_table_path.display()))?
      .parent() // Get parent of "data" to get "tmp"
      .ok_or("Failed to get parent of base directory")?
      .to_path_buf();

    // For group users, ONLY check for exact table name match
    if let Some(user) = username {
      let group_base = base_root.join("group").join(user).join(db_name);
      let group_path = group_base.join(table_name);

      if group_path.exists() {
        return Ok(group_path.to_string_lossy().to_string());
      }

      return Err(
        format!(
          "Table '{}' does not exist in group path for user '{}'. Group users should only access tables that exist in their path.",
          table_name, user
        )
        .into(),
      );
    }

    // For default user (None), return the default path
    Ok(base_table_path.to_string_lossy().to_string())
  }

  /// Atomically insert records into a parquet file: lock -> read -> merge -> write -> unlock
  /// This ensures that concurrent inserts don't lose data by always working with the latest file contents
  fn atomic_file_insert(
    file_path: &Path,
    new_records: &[Value],
    build_key: &dyn Fn(&Value) -> String,
    preferred_field_order: Option<&[String]>,
    table_schema: Option<&Value>,
  ) -> Result<(), Box<dyn Error>> {
    // Ensure parent directory exists
    if let Some(parent) = file_path.parent() {
      fs::create_dir_all(parent)?;
    }

    // Use process-wide mutex for this file path (works reliably across threads in same process)
    let file_path_str = file_path.to_string_lossy().to_string();

    // Get or create a mutex for this file path, and update last access time
    let file_mutex = {
      let now = Instant::now();
      let mut locks = get_file_locks()
        .lock()
        .map_err(|e| format!("Failed to acquire file locks mutex (poisoned): {}", e))?;

      // Periodically cleanup unused locks (every LOCK_CLEANUP_INTERVAL)
      static LAST_CLEANUP: OnceLock<Mutex<Instant>> = OnceLock::new();
      let last_cleanup = LAST_CLEANUP.get_or_init(|| Mutex::new(Instant::now()));
      if let Ok(mut last) = last_cleanup.lock() {
        if now.duration_since(*last) >= LOCK_CLEANUP_INTERVAL {
          cleanup_unused_locks(&mut locks);
          *last = now;
        }
      }

      let entry = locks.entry(file_path_str.clone()).or_insert_with(|| FileLockEntry {
        lock: Arc::new(Mutex::new(())),
        last_accessed: now,
      });
      entry.last_accessed = now;
      entry.lock.clone()
    };

    // Acquire the mutex (this will block until available)
    let _guard = file_mutex
      .lock()
      .map_err(|e| format!("Failed to acquire file mutex for {} (poisoned): {}", file_path_str, e))?;

    // Now that we have the lock, read the latest data from the file (if it exists)
    let mut existing_records: Vec<Value> = if file_path.exists() {
      // Re-read the file to get the latest data (another thread may have written)
      Self::read_parquet_file_static(file_path).unwrap_or_default()
    } else {
      Vec::new()
    };

    // Build index of existing records by key for efficient lookups
    let mut existing_keys: HashMap<String, usize> = HashMap::new();
    for (index, record) in existing_records.iter().enumerate() {
      let key = build_key(record);
      existing_keys.insert(key, index);
    }

    // Merge new records with existing ones
    let initial_count = existing_records.len();
    for new_record in new_records {
      let key = build_key(new_record);
      if let Some(&existing_index) = existing_keys.get(&key) {
        // Update existing record
        existing_records[existing_index] = new_record.clone();
      } else {
        // Insert new record
        existing_records.push(new_record.clone());
        existing_keys.insert(key, existing_records.len() - 1);
      }
    }

    // Write the merged records back to the file
    // We should always have records to write (either existing or new)
    if existing_records.is_empty() {
      return Err(
        format!(
          "No records to write to '{}' (this should not happen - had {} existing, {} new)",
          file_path.display(),
          initial_count,
          new_records.len()
        )
        .into(),
      );
    }

    let (arrays, schema) = json_to_arrow(&existing_records, preferred_field_order, table_schema)
      .map_err(|e| format!("Failed to convert records to arrow format for '{}': {}", file_path.display(), e))?;

    // Write while holding the lock
    Self::parquet_file_writer_locked(file_path, schema, arrays)
      .map_err(|e| format!("Failed to write parquet file '{}': {}", file_path.display(), e))?;

    // Lock is released when _guard is dropped here
    Ok(())
  }

  /// Validate a parquet file by attempting to read it and verify its structure
  /// Returns the schema from the file if validation succeeds
  fn validate_parquet_file(file_path: &Path, expected_schema: Option<&Schema>) -> Result<Schema, Box<dyn Error>> {
    // Open and create arrow reader - this will fail if file is corrupted or not a valid parquet file
    let file = fs::File::open(file_path).map_err(|e| format!("Failed to open parquet file '{}' for validation: {}", file_path.display(), e))?;

    // Build the reader - this validates the parquet file structure and footer integrity
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
      format!(
        "Failed to create parquet reader for '{}': file may be corrupted or invalid parquet format: {}",
        file_path.display(),
        e
      )
    })?;

    // Get the schema from the parquet file
    let actual_schema = builder.schema().as_ref().clone();

    // Verify the file has at least one row group
    let metadata = builder.metadata();
    if metadata.num_row_groups() == 0 {
      return Err(format!("Parquet file '{}' has no row groups", file_path.display()).into());
    }

    // Try to read at least one record batch to ensure the file is readable
    let mut reader = builder
      .build()
      .map_err(|e| format!("Failed to build parquet reader for '{}': {}", file_path.display(), e))?;

    // Read the first batch to validate data can be read
    match reader.next() {
      Some(Ok(_batch)) => {
        // File is readable, continue with schema validation
      }
      Some(Err(e)) => {
        return Err(format!("Failed to read data from parquet file '{}': {}", file_path.display(), e).into());
      }
      None => {
        // Empty file is acceptable, but we've validated the structure
      }
    }

    // If expected schema is provided, validate it matches
    if let Some(expected) = expected_schema {
      // Compare field count
      if actual_schema.fields().len() != expected.fields().len() {
        return Err(
          format!(
            "Schema mismatch in parquet file '{}': expected {} fields, got {}",
            file_path.display(),
            expected.fields().len(),
            actual_schema.fields().len()
          )
          .into(),
        );
      }

      // Compare field names and types
      for (expected_field, actual_field) in expected.fields().iter().zip(actual_schema.fields().iter()) {
        if expected_field.name() != actual_field.name() {
          return Err(
            format!(
              "Schema field name mismatch in parquet file '{}': expected '{}', got '{}'",
              file_path.display(),
              expected_field.name(),
              actual_field.name()
            )
            .into(),
          );
        }

        if expected_field.data_type() != actual_field.data_type() {
          return Err(
            format!(
              "Schema field type mismatch in parquet file '{}' for field '{}': expected {:?}, got {:?}",
              file_path.display(),
              expected_field.name(),
              expected_field.data_type(),
              actual_field.data_type()
            )
            .into(),
          );
        }
      }
    }

    Ok(actual_schema)
  }

  /// Read parquet file (static version for use in atomic operations)
  fn read_parquet_file_static(file_path: &Path) -> Result<Vec<Value>, Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;

    let mut json_records: Vec<Value> = Vec::new();
    for record_result in iter {
      match record_result {
        Ok(record) => {
          let json_record = row_to_json(&record);
          json_records.push(json_record);
        }
        Err(_) => {
          return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Error reading record from parquet file",
          )));
        }
      }
    }
    Ok(json_records)
  }

  /// Write parquet file (internal version that assumes lock is already held)
  /// This version doesn't acquire a lock - it should only be called from atomic_file_insert
  fn parquet_file_writer_locked(path: &Path, schema: Schema, array: Vec<Arc<dyn Array>>) -> Result<String, Box<dyn Error>> {
    // Clone schema for validation after write (schema will be moved during write)
    let schema_for_validation = schema.clone();

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    // Strategy: Write to a temporary file first, then atomically rename
    // This prevents corruption if the process crashes mid-write
    // Use a unique temp file name with timestamp to avoid conflicts
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| format!("System time is before UNIX epoch: {:?}", e))?
      .as_nanos();
    let temp_path = path
      .parent()
      .map(|p| {
        p.join(format!(
          "{}.{}.tmp",
          path.file_name().and_then(|n| n.to_str()).unwrap_or("data.parquet"),
          timestamp
        ))
      })
      .unwrap_or_else(|| path.with_file_name(format!("data.parquet.{}.tmp", timestamp)));

    // Helper function to clean up temp file on error
    let cleanup_temp = |temp_path: &Path| {
      if temp_path.exists() {
        if let Err(e) = fs::remove_file(temp_path) {
          eprintln!("Warning: Failed to clean up temp file {:?}: {}", temp_path, e);
        }
      }
    };

    // Write to temporary file (lock is already held by caller)
    // Use a match to ensure cleanup on error
    let write_result = (|| -> Result<(), Box<dyn Error>> {
      let temp_file = fs::File::create(&temp_path)?;
      let props = WriterProperties::builder().build();
      let mut writer = ArrowWriter::try_new(&temp_file, Arc::new(schema.clone()), Some(props))?;
      let combined_batch = RecordBatch::try_new(Arc::new(schema), array)?;
      writer.write(&combined_batch)?;
      writer.close()?;

      // Flush and sync to ensure data is written to disk
      temp_file.sync_all()?;
      drop(temp_file);
      Ok(())
    })();

    // If write failed, clean up temp file and return error
    if let Err(e) = write_result {
      cleanup_temp(&temp_path);
      return Err(e);
    }

    // Atomically rename temporary file to final location
    // This is an atomic operation on most filesystems
    // IMPORTANT: We must hold the lock until AFTER the rename completes
    // to ensure no other thread can overwrite our file
    let rename_result = fs::rename(&temp_path, path);

    // If rename failed, clean up temp file
    if let Err(e) = rename_result {
      cleanup_temp(&temp_path);
      return Err(Box::new(e));
    }

    // Sync the parent directory to ensure the rename is persisted to disk
    if let Some(parent) = path.parent() {
      if let Ok(parent_file) = fs::File::open(parent) {
        if let Err(e) = parent_file.sync_all() {
          eprintln!("Warning: Failed to sync parent directory {:?} after file rename: {}", parent, e);
        }
      }
    }

    // Validate the written parquet file
    // This ensures the file is readable, has valid parquet structure, and schema matches
    Self::validate_parquet_file(path, Some(&schema_for_validation)).map_err(|e| {
      // If validation fails, try to clean up the corrupted file
      cleanup_temp(path);
      format!("Parquet file validation failed after write: {}", e)
    })?;

    // Clean up temp file if it still exists (shouldn't happen after successful rename)
    cleanup_temp(&temp_path);

    Ok(format!("Data was successfully written to '{}'", path.to_string_lossy()))
  }

  pub fn build_files_list(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<Vec<String>, Box<dyn Error>> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database).map_err(|e| e.to_string())?;
    Self::validate_name(table_name, NameType::Table).map_err(|e| e.to_string())?;

    // Reload metadata to ensure it's up-to-date
    let metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| DataFusionError::Execution(format!("Failed to reload metadata: {}", e)))?;

    // Validate if the database exists
    let database = metadata
      .databases
      .get(db_name)
      .ok_or_else(|| format!("Database '{}' does not exist.", db_name))?;

    // Validate if the table exists within the database
    let table = database
      .tables
      .get(table_name)
      .ok_or_else(|| format!("Table '{}' does not exist in database '{}'.", table_name, db_name))?;

    // Get the base table path
    let base_table_path = Path::new(&table.path);

    // Extract the base directory (root path) from the table path
    let base_root = base_table_path
      .ancestors()
      .nth(2) // Adjust according to depth: "<base_path>/data/zivaring/activitydetails"
      .ok_or_else(|| format!("Failed to determine base directory from '{}'", base_table_path.display()))?
      .to_path_buf();

    // Determine the final path
    let final_table_path = if let Some(user) = username {
      base_root.join("group").join(user).join(db_name).join(table_name) // Correct order
    } else {
      base_table_path.to_path_buf() // Default to the existing path
    };

    // Ensure the directory exists
    if !final_table_path.exists() {
      return Err(format!("Table path '{}' does not exist.", final_table_path.display()).into());
    }

    // Collect all files in the chosen directory (recursively to support partitioned tables)
    let mut file_list = Vec::new();
    collect_files_recursive(&final_table_path, &mut file_list)?;

    // Sort files by their name for consistency
    file_list.sort();

    Ok(file_list)
  }

  fn validate_schema_structure(&self, schema: &Value) -> Result<(), Box<dyn Error>> {
    let schema_obj = schema.as_object().ok_or("Schema should be a JSON object")?;

    for (field_name, field_rules) in schema_obj {
      let field_rules_obj = field_rules
        .as_object()
        .ok_or(format!("Invalid validation rules for field '{}'", field_name))?;

      // Ensure that the schema contains the required "type" field
      if !field_rules_obj.contains_key("type") {
        return Err(format!("Field '{}' is missing a 'type' definition.", field_name).into());
      }

      // Check if "required" is a boolean (optional, defaults to false)
      if let Some(required) = field_rules_obj.get("required") {
        if !required.is_boolean() {
          return Err(format!("Field '{}' has an invalid 'required' value. Must be true or false.", field_name).into());
        }
      }
    }

    Ok(())
  }

  pub fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    // Validate database and table names to prevent path traversal attacks
    Self::validate_name(db_name, NameType::Database).map_err(|e| e.to_string())?;
    Self::validate_name(table_name, NameType::Table).map_err(|e| e.to_string())?;

    // Reload metadata to ensure it's up-to-date
    let metadata = self.get_metadata_cached_sync().map_err(|e| format!("Failed to reload metadata: {}", e))?;
    // Look up the schema from the metadata
    let database = metadata.databases.get(db_name).ok_or("Database not found")?;
    let table = database.tables.get(table_name).ok_or("Table not found")?;
    Ok(table.schema.clone())
  }

  fn validate_data_against_schema(&self, schema: &serde_json::Value, json_data: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let schema_obj = schema.as_object().ok_or("Schema should be a JSON object")?;
    let data_obj = json_data.as_object().ok_or("Data should be a JSON object")?;

    // Check for unexpected fields (fields in JSON data that are not in the schema)
    for (key, _value) in data_obj {
      if !schema_obj.contains_key(key) {
        return Err(format!("Unexpected field: '{}' is not defined in the schema!", key).into());
      }
    }

    // Validate each field in the schema
    for (field_name, field_rules) in schema_obj {
      let field_rules_obj = field_rules
        .as_object()
        .ok_or(format!("Invalid validation rules for field '{}'", field_name))?;

      // Check if the field is required and if it's missing from the data
      if field_rules_obj.get("required").and_then(|v| v.as_bool()).unwrap_or(false) && !data_obj.contains_key(field_name) {
        return Err(format!("Missing required field '{}'", field_name).into());
      }

      // Check the field type if the field exists in the data
      if let Some(value) = data_obj.get(field_name) {
        let field_type = field_rules_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        self.validate_field_type(field_name, field_type, value)?;
      }
    }

    Ok(())
  }

  fn validate_field_type(&self, field_name: &str, field_type: &str, value: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    /// Returns (raw_type, type_for_schema_check). Int is treated as valid for float (int is subset of float).
    fn get_value_type(value: &Value, expected_type: &str) -> (&'static str, &'static str) {
      let raw = if value.is_f64() {
        "float"
      } else if value.is_i64() || value.is_u64() {
        "int"
      } else if value.is_string() {
        "string"
      } else if value.is_boolean() {
        "bool"
      } else if value.is_array() {
        "array"
      } else {
        "unknown"
      };
      let for_check = if raw == "int" && expected_type.trim() == "float" { "float" } else { raw };
      (raw, for_check)
    }

    let expected_type = field_type.trim();
    let (actual_type, type_for_check) = get_value_type(value, expected_type);
    if type_for_check != expected_type {
      return Err(
        format!(
          "Type mismatch for field '{}': expected '{}', but got '{}'.",
          field_name, field_type, actual_type
        )
        .into(),
      );
    }

    Ok(())
  }

  async fn read_metadata(&self) -> Result<Metadata, Box<dyn Error>> {
    // Retry logic to handle cases where metadata is being written
    let max_retries = METADATA_READ_MAX_RETRIES;
    let retry_delay = METADATA_READ_RETRY_DELAY;

    for attempt in 0..max_retries {
      match tokio::fs::read_to_string(&self.metadata_path).await {
        Ok(metadata_contents) => {
          if metadata_contents.trim().is_empty() {
            // If the metadata file is empty, return a default Metadata object
            return Ok(Metadata { databases: HashMap::new() });
          }

          // Try to parse the JSON
          match serde_json::from_str::<Metadata>(&metadata_contents) {
            Ok(metadata) => return Ok(metadata),
            Err(e) => {
              // If parsing fails, it might be because the file is being written
              // Check if it's a JSON parse error (not just a corrupted file)
              if attempt < max_retries - 1 {
                // Wait and retry - file might be partially written
                tokio::time::sleep(retry_delay).await;
                continue;
              } else {
                // Last attempt failed - return the error
                return Err(format!("Failed to parse metadata after {} attempts: {}", max_retries, e).into());
              }
            }
          }
        }
        Err(e) => {
          // File doesn't exist or can't be read
          if e.kind() == std::io::ErrorKind::NotFound {
            // Metadata file doesn't exist - return empty metadata
            return Ok(Metadata { databases: HashMap::new() });
          }

          // Other error - retry if we have attempts left
          if attempt < max_retries - 1 {
            tokio::time::sleep(retry_delay).await;
            continue;
          } else {
            return Err(format!("Failed to read metadata after {} attempts: {}", max_retries, e).into());
          }
        }
      }
    }

    // Should never reach here, but return empty metadata as fallback
    Ok(Metadata { databases: HashMap::new() })
  }

  /// Get metadata with caching support (infinite TTL, invalidated only on writes)
  async fn get_metadata_cached(&self) -> Result<Metadata, Box<dyn Error>> {
    // Check if we have a valid cache using a single lock to prevent deadlocks
    let should_refresh = {
      let cache = self
        .cache
        .read()
        .map_err(|e| format!("Failed to acquire read lock on cache (poisoned): {:?}", e))?;
      match cache.timestamp {
        Some(timestamp) => Instant::now().duration_since(timestamp) > self.cache_ttl,
        None => true,
      }
    };

    if should_refresh {
      // Cache expired or doesn't exist, refresh it
      let fresh_metadata = self.read_metadata().await?;

      // Update cache atomically with a single write lock
      let mut cache = self
        .cache
        .write()
        .map_err(|e| format!("Failed to acquire write lock on cache (poisoned): {:?}", e))?;
      cache.metadata = Some(fresh_metadata.clone());
      cache.timestamp = Some(Instant::now());
      drop(cache);

      Ok(fresh_metadata)
    } else {
      // Return cached metadata
      let cache = self
        .cache
        .read()
        .map_err(|e| format!("Failed to acquire read lock on cache (poisoned): {:?}", e))?;
      match &cache.metadata {
        Some(metadata) => Ok(metadata.clone()),
        None => {
          // Shouldn't happen, but handle gracefully
          drop(cache);
          let fresh_metadata = self.read_metadata().await?;

          // Update cache atomically
          let mut cache = self
            .cache
            .write()
            .map_err(|e| format!("Failed to acquire write lock on cache (poisoned): {:?}", e))?;
          cache.metadata = Some(fresh_metadata.clone());
          cache.timestamp = Some(Instant::now());
          drop(cache);

          Ok(fresh_metadata)
        }
      }
    }
  }

  /// Get metadata with caching support (sync version for use in non-async contexts)
  /// This function blocks on the async version, handling both sync and async callers
  fn get_metadata_cached_sync(&self) -> Result<Metadata, Box<dyn Error>> {
    // Check if we're already in an async context
    match tokio::runtime::Handle::try_current() {
      Ok(handle) => {
        // We're in an async context - use spawn_blocking to avoid deadlocks
        // This moves execution to a blocking thread where we can safely create a runtime
        let self_clone = self.clone();
        let join_handle = handle.spawn_blocking(move || {
          // Create a new runtime in the blocking thread
          let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => return Err(format!("Failed to create tokio runtime: {}", e)),
          };
          match rt.block_on(self_clone.get_metadata_cached()) {
            Ok(metadata) => Ok(metadata),
            Err(e) => Err(format!("Failed to get metadata: {}", e)),
          }
        });

        // Use futures::executor::block_on to wait for the join handle without creating another runtime
        // This is safe because we're blocking on a JoinHandle, not creating a new runtime
        match executor::block_on(join_handle) {
          Ok(result) => result.map_err(|e| e.into()),
          Err(e) => Err(format!("Failed to join blocking task: {}", e).into()),
        }
      }
      Err(_) => {
        // We're not in an async context - safe to create a new runtime
        let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create tokio runtime: {}", e))?;
        rt.block_on(self.get_metadata_cached())
      }
    }
  }

  /// Manually invalidate the metadata cache
  /// Should be called after any operation that modifies metadata (create_table, delete_table, etc.)
  fn invalidate_cache(&self) {
    // Use into_inner() to recover from a poisoned lock and still invalidate cache.
    // A poisoned lock means a thread panicked while holding it, but we still want
    // to clear the cache so stale data isn't served. Setting fields to None is safe.
    let mut cache = self.cache.write().unwrap_or_else(|e| {
      eprintln!("Warning: cache RwLock was poisoned; recovering and invalidating anyway");
      e.into_inner()
    });
    cache.metadata = None;
    cache.timestamp = None;
  }

  fn save_metadata(&self) -> TokioResult<()> {
    // Check if we're already in an async context
    match tokio::runtime::Handle::try_current() {
      Ok(handle) => {
        // We're in an async context - use spawn_blocking to avoid deadlocks
        let self_clone = self.clone();
        let join_handle = handle.spawn_blocking(move || {
          // Create a new runtime in the blocking thread
          let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
              return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create tokio runtime: {}", e),
              ))
            }
          };
          rt.block_on(self_clone.save_metadata_async())
        });

        // Use futures::executor::block_on to wait for the join handle without creating another runtime
        match executor::block_on(join_handle) {
          Ok(result) => result,
          Err(e) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to join blocking task: {}", e),
          )),
        }
      }
      Err(_) => {
        // We're not in an async context - safe to create a new runtime
        let rt = tokio::runtime::Runtime::new()
          .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create tokio runtime: {}", e)))?;
        rt.block_on(self.save_metadata_async())
      }
    }
  }

  /// Async version of save_metadata that performs the actual save with retry logic
  async fn save_metadata_async(&self) -> TokioResult<()> {
    // Retry logic to handle transient failures (similar to read_metadata)
    let max_retries = METADATA_SAVE_MAX_RETRIES;
    let retry_delay = METADATA_SAVE_RETRY_DELAY;

    for attempt in 0..max_retries {
      match self.save_metadata_attempt().await {
        Ok(()) => return Ok(()),
        Err(e) => {
          // Check if this is a transient error that might succeed on retry
          let is_transient = matches!(
            e.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted
          );

          if is_transient && attempt < max_retries - 1 {
            eprintln!(
              "Transient error saving metadata (attempt {}/{}): {}. Retrying...",
              attempt + 1,
              max_retries,
              e
            );
            tokio::time::sleep(retry_delay).await;
            continue;
          } else {
            // Last attempt or non-transient error - return the error
            if attempt == max_retries - 1 {
              eprintln!("Failed to save metadata after {} attempts: {}", max_retries, e);
            }
            return Err(e);
          }
        }
      }
    }

    // Should never reach here, but return error as fallback
    Err(std::io::Error::new(
      std::io::ErrorKind::Other,
      "Failed to save metadata after all retry attempts",
    ))
  }

  /// Internal method that performs a single save attempt
  /// This is separated to allow retry logic in the public method
  async fn save_metadata_attempt(&self) -> TokioResult<()> {
    // Create backup of existing metadata file before writing (if it exists)
    if tokio::fs::metadata(&self.metadata_path).await.is_ok() {
      if let Err(e) = self.create_metadata_backup() {
        eprintln!("Warning: Failed to create metadata backup: {}. Continuing with save...", e);
        // Don't fail the save operation if backup fails, but log the warning
      }
    }

    // Use atomic write: write to temp file, then rename
    // This ensures metadata file is never in a partially-written state
    let temp_path = format!("{}.tmp", self.metadata_path);

    // Serialize the metadata structure
    let json = serde_json::to_string(&self.metadata)
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Failed to serialize metadata to JSON: {}", e)))?;

    // Write to temporary file
    tokio::fs::write(&temp_path, json).await?;

    // Sync to ensure data is written to disk
    let temp_file = tokio::fs::File::open(&temp_path).await.map_err(|e| {
      std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("Failed to open temporary metadata file '{}' for syncing: {}", temp_path, e),
      )
    })?;
    temp_file.sync_all().await.map_err(|e| {
      std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("Failed to sync temporary metadata file '{}': {}", temp_path, e),
      )
    })?;

    // Atomically rename temp file to final location
    tokio::fs::rename(&temp_path, &self.metadata_path).await?;

    // Sync the parent directory to ensure rename is persisted
    if let Some(parent) = Path::new(&self.metadata_path).parent() {
      let parent_file = tokio::fs::File::open(parent).await.map_err(|e| {
        std::io::Error::new(
          std::io::ErrorKind::Other,
          format!(
            "Failed to open parent directory for syncing metadata file '{}': {}",
            self.metadata_path, e
          ),
        )
      })?;
      parent_file.sync_all().await.map_err(|e| {
        std::io::Error::new(
          std::io::ErrorKind::Other,
          format!("Failed to sync parent directory for metadata file '{}': {}", self.metadata_path, e),
        )
      })?;
    }

    // Invalidate cache after saving metadata
    self.invalidate_cache();
    Ok(())
  }

  /// Create a backup of the current metadata file
  /// Keeps only the last MAX_METADATA_BACKUPS backups to prevent disk space issues
  fn create_metadata_backup(&self) -> Result<(), Box<dyn Error>> {
    if !Path::new(&self.metadata_path).exists() {
      return Ok(()); // Nothing to backup
    }

    // Generate backup filename with timestamp
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map_err(|e| format!("System time is before UNIX epoch: {:?}", e))?
      .as_secs();

    let backup_dir = format!("{}.backups", self.metadata_path);
    fs::create_dir_all(&backup_dir)?;

    let backup_path = format!("{}/metadata.{}.json", backup_dir, timestamp);

    // Copy current metadata file to backup location
    fs::copy(&self.metadata_path, &backup_path)?;

    // Clean up old backups, keeping only the last MAX_METADATA_BACKUPS
    self.cleanup_old_backups(&backup_dir, MAX_METADATA_BACKUPS)?;

    Ok(())
  }

  /// Clean up old backup files, keeping only the last N backups
  fn cleanup_old_backups(&self, backup_dir: &str, max_backups: usize) -> Result<(), Box<dyn Error>> {
    use std::time::SystemTime;

    let backup_path = Path::new(backup_dir);
    if !backup_path.exists() {
      return Ok(());
    }

    // Collect all backup files with their modification times
    let mut backups: Vec<(SystemTime, String)> = Vec::new();

    for entry in fs::read_dir(backup_path)? {
      let entry = entry?;
      let path = entry.path();

      if path.is_file() {
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
          if file_name.starts_with("metadata.") && file_name.ends_with(".json") {
            if let Ok(metadata) = fs::metadata(&path) {
              if let Ok(modified) = metadata.modified() {
                backups.push((modified, path.to_string_lossy().to_string()));
              }
            }
          }
        }
      }
    }

    // Sort by modification time (oldest first)
    backups.sort_by_key(|(time, _)| *time);

    // Remove oldest backups if we exceed the limit
    if backups.len() > max_backups {
      let to_remove = backups.len() - max_backups;
      for (_, path) in backups.iter().take(to_remove) {
        if let Err(e) = fs::remove_file(path) {
          eprintln!("Warning: Failed to remove old backup file '{}': {}", path, e);
        }
      }
    }

    Ok(())
  }

  pub fn update_metadata(&mut self, storage_path: &str) -> TokioResult<()> {
    // Create a lock file path
    let lock_file_path = format!("{}/metadata.lock", self.storage_path);

    // Try to acquire the lock with retries
    let mut retries = 0;
    let max_retries = METADATA_LOCK_MAX_RETRIES;
    let retry_delay = METADATA_LOCK_RETRY_DELAY;

    let _lock_file = loop {
      match File::create(&lock_file_path) {
        Ok(file) => {
          // Try to acquire an exclusive lock using fs2
          if file.lock_exclusive().is_err() {
            if retries >= max_retries {
              return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to acquire metadata lock after multiple retries",
              ));
            }
            retries += 1;
            std::thread::sleep(retry_delay);
            continue;
          }
          break file;
        }
        Err(_) => {
          if retries >= max_retries {
            return Err(std::io::Error::new(
              std::io::ErrorKind::Other,
              "Failed to create lock file after multiple retries",
            ));
          }
          retries += 1;
          std::thread::sleep(retry_delay);
        }
      }
    };

    // Ensure the lock file is removed when we're done
    struct LockGuard {
      path: String,
    }

    impl Drop for LockGuard {
      fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
      }
    }

    let _lock_guard = LockGuard { path: lock_file_path };

    // Rest of the update_metadata implementation...
    let new_data_path = storage_path.to_string() + "/data";
    let mut metadata = self
      .get_metadata_cached_sync()
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read metadata: {}", e)))?;

    for (db_name, db) in metadata.databases.iter_mut() {
      for (table_name, table) in db.tables.iter_mut() {
        let new_table_path = format!("{}/{}/{}", new_data_path, db_name, table_name);
        table.path = new_table_path.clone();
      }
    }

    self.metadata = metadata;
    self.save_metadata()?;
    Ok(())
  }

  pub fn get_table_path(&self, db_name: &str, table_name: &str) -> Option<String> {
    // Validate database and table names to prevent path traversal attacks
    // Return None if validation fails (matching the function's return type)
    if Self::validate_name(db_name, NameType::Database).is_err() || Self::validate_name(table_name, NameType::Table).is_err() {
      return None;
    }
    self.metadata.databases.get(db_name)?.tables.get(table_name)?.path.clone().into()
  }
}
