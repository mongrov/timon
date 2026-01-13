## 1. Library Purpose & Core Functionality

Timon is a specialized library built on top of Apache DataFusion that provides:

1. Efficient local file storage for time-series data using Parquet format
2. S3-compatible cloud storage synchronization capabilities
3. SQL query capabilities powered by DataFusion
4. Cross-platform support via JNI (Android) and C-compatible interfaces (iOS)

The library serves as a middleware that simplifies working with time-series data by handling:

- Database and table management with schema validation
- Data partitioning by time intervals
- Local storage with efficient querying
- Cloud synchronization capabilities
- Time-based filtering of data

## 2. Core Library Design & API Review

### API Ergonomics

The public API is well-structured and divided into two clear categories:

1. __File Storage Functions__:

   - `init_timon`, `create_database`, `create_table`, `list_databases`, etc.
   - Focused on local data management

2. __S3-Compatible Storage Functions__:

   - `init_bucket`, `cloud_sink_parquet`, `cloud_fetch_parquet`, etc.
   - Focused on cloud synchronization

The API design follows a logical flow, making it intuitive for common operations:

1. Initialize storage
2. Create databases and tables
3. Insert data
4. Query data
5. Synchronize with cloud storage

### Modularity

The codebase is well-organized into modules:

- `timon_engine`: Core functionality

  - `mod.rs`: Public API implementation
  - `db_manager.rs`: Database management
  - `cloud_sync.rs`: S3 synchronization
  - `helpers.rs`: Utility functions

There's clear separation of concerns:

- Database operations are isolated from cloud operations
- Error handling is consistently implemented
- Manager classes have well-defined responsibilities

### Error Handling

Error handling is robust, with:

- Consistent use of `Result` types throughout the codebase
- Proper error propagation
- User-friendly error messages
- Error wrapping to maintain context

The `TimonResult` struct provides a standardized return format for all API functions:

```rust
struct TimonResult {
  pub status: u16,
  pub message: String,
  pub json_value: Option<Value>,
}
```

### Configuration

Configuration is straightforward through the initializers:

- `init_timon` for local storage with configurable bucket interval
- `init_bucket` for S3-compatible storage with endpoint, credentials, etc.

## 3. DataFusion Integration

### Efficient API Usage

The library generally uses DataFusion's APIs effectively:

__Strengths:__

- Uses lazy execution through the DataFrame API
- Creates appropriate DataFusion contexts for operations
- Registers Parquet files properly with DataFusion
- Transforms SQL queries appropriately
- Handles schema extraction and conversion well

### Custom Logic

The library extends DataFusion with:

1. __Partition Management__:

   - Time-based partitioning for efficient querying
   - Merges data from multiple partitions when needed

2. __Query Transformation__:

   - Modifies queries to work with the partitioned structure
   - Handles special cases like JOINs across partitions

3. __Schema Management__:

   - Custom schema validation and enforcement
   - Type validation against defined schemas

### State Management

The `SessionContext` is handled with per-username isolation:

- Each `DatabaseManager` instance maintains its own `SessionContext` (one per username)
- When cloning `DatabaseManager`, a fresh `SessionContext` is created for isolation (see `Clone` implementation in `db_manager.rs:96-113`)
- Tables are registered per-username context, allowing concurrent queries for different users
- This design ensures thread-safety and isolation between different user contexts

### Data Source Interaction

The library creates Parquet files for DataFusion to query:

- Files are properly partitioned for time-based queries
- File management is handled logically
- Predicate pushdown is leveraged through date filtering

### Query Path Resolution

The library supports two data path structures:

- **Default Path**: `{storage_path}/data/{db_name}/{table_name}/` - for local user data
- **Group Path**: `{storage_path}/group/{username}/{db_name}/{table_name}/` - for group/shared user data

**Current Implementation** (`resolve_table_dir` in `db_manager.rs:674-738`):
- For default user (username=None): Only queries default path
- For group users (username provided): Only queries group path
- ⚠️ **Known Limitation**: See [Issue #64](https://github.com/mongrov/timon/issues/64) - queries do NOT merge data from both paths
  - If same file exists in both locations, only one path is queried
  - This can lead to missing data if files exist in both default and group paths
  - Potential solutions: merge both files or query both paths with duplicate removal at query time

## 4. Rust Code Quality & Best Practices

### Ownership and Borrowing

The code demonstrates good understanding of Rust's ownership model:

- Appropriate use of borrowing where possible
- Strategic cloning when necessary
- Proper use of `Arc` for shared ownership

✅ **Fixed: Ownership and borrowing optimizations implemented**

- Removed unnecessary clones in hot paths (create_database, create_table, delete_database, delete_table, insert, list_databases, list_tables)
- Optimized function calls to pass mutable references instead of cloning DatabaseManager instances
- Improved performance by eliminating redundant memory allocations in frequently called functions

### Use of `unsafe`

The library uses `unsafe` code primarily in the JNI and iOS interfaces, which is unavoidable when working with foreign function interfaces. The unsafe code is:

- Well-encapsulated
- Used only where necessary
- Properly documented

### Dependencies

The dependencies are appropriate for the library's needs:

- `datafusion` for query processing
- `object_store` for S3 integration
- `tokio` for async operations
- `serde` for serialization

### Performance Considerations

Several performance optimizations are evident:

- Time-based partitioning for efficient querying
- Proper file management to avoid unnecessary operations
- File locking for concurrent access safety
- Query filtering to minimize data processing

**Performance Benchmarks** (see `INSERT_PERFORMANCE_REPORT.md` and `TIMON_QUERY_REPORT.md`):

- **Insert Operations**: 
  - Fresh inserts: ~15K records/sec (consistent across 1K-1M records)
  - Updates: Performance degrades with existing data size (594-11,713 records/sec)
  - Table loading: 29K-42K records/sec (2-3x faster than inserts)
  
- **Query Operations** (tested with 1M records per table):
  - Simple queries: 17-23ms (excellent)
  - COUNT aggregations: 17.7ms (excellent)
  - JOIN queries: 435-510ms (good, scales linearly)
  - Complex analytics: 87-490ms (good)
  - Partition filtering: 20.7ms (excellent, found 112K records efficiently)
  
- **Realistic Use Case** (20K records/month, typical for ZivaApp-Ring):
  - Insert: ~1.4s
  - Load: ~0.5s
  - Query: <25ms for simple queries

### Concurrency & Race Condition Handling

The library implements robust concurrency controls:

- ✅ **Atomic File Writes**: Implemented using temp file strategy (`parquet_file_writer_locked` in `db_manager.rs:853-904`)
  - Writes to temporary file first, then atomically renames to final location
  - Prevents corruption if process crashes mid-write
  - Uses unique timestamp-based temp file names to avoid conflicts
- ✅ **File-Level Locking**: Process-wide mutex map for file-level locking (`atomic_file_insert` in `db_manager.rs:742-825`)
  - Each file path has its own mutex to prevent concurrent writes
  - Lock is held during read-merge-write cycle to ensure atomicity
  - Prevents race conditions when multiple threads insert to same partition
- ⚠️ **Known Limitation**: See [Issue #135](https://github.com/mongrov/timon/issues/135) - Insert-Query Race Condition
  - **Problem**: Queries may attempt to read parquet files while inserts are writing them, even with atomic writes
  - **Impact**: Can cause ("Corrupt Footer", "Protocol Error" and "Out of Range of File") errors when DataFusion reads incomplete files
  - **Status**: Marked as "wontfix" - atomic write strategy is implemented but insert-query race condition remains

## 5. Testing & Documentation

### Test Coverage

The test suite is comprehensive:

- Tests for normal operations
- Tests for error handling
- Tests for edge cases
- Tests for concurrent operations

### Documentation

Documentation could be improved:

- More in-code documentation would be beneficial
- Function-level documentation is sparse in some areas
- Example usage could be expanded beyond the README

## Recommendations for Improvement

1. __DataFusion Integration:__

   - ✅ Optimize the partition selection logic *(Fixed: Implemented efficient CTE-based partition handling)*
   - ⚠️ **SessionContext Management**: Each `DatabaseManager` clone creates a fresh `SessionContext` for isolation
     - This ensures thread-safety but means table registrations are not shared across clones
     - Current design prioritizes isolation over reuse, which is appropriate for multi-user scenarios
   - Evaluate using DataFusion's more advanced features like caching

2. __Error Handling:__

   - ✅ Standardize error types across the codebase *(Completed: Implemented comprehensive TimonError system with standardized error kinds)*
   - ✅ Consider more granular error types for better error handling *(Completed: Added 30+ granular error types covering all operation categories)*

   **Implementation Details:**
   - Created comprehensive `TimonErrorKind` enum with granular error categories for initialization, database operations, table operations, data operations, query operations, file system operations, cloud storage operations, synchronization, concurrency, metadata, validation, and internal errors
   - Implemented structured `TimonError` with kind, message, details, source, and context fields
   - Created conversion implementations from common error types (DataFusionError, std::io::Error, serde_json::Error)
   - Added helper macros and functions for creating common error types
   - Comprehensive test coverage with 21+ error handling tests all passing

3. __API Ergonomics:__

   - ✅ Add higher-level functions for common operations *(Not needed - server code only, and server code is not used)*
   - ✅ Consider builder patterns for complex configurations *(Not needed - server code only, and server code is not used)*

   **Note:** These improvements are server-specific and not needed for library usage. Higher-level functions and builder patterns are beneficial for HTTP API consumers and complex server configurations, but the core library API is already ergonomic for direct programmatic usage. **Note: Server code (`server/mod.rs`) is not currently used in production.**

4. __Performance:__

   - ✅ Benchmark and optimize the query path *(Not needed - server code only, and server code is not used)*
   - ✅ Look for opportunities to reduce cloning *(Already optimized - see section 4)*
   - ✅ Consider more aggressive caching strategies *(Not needed - server code only, and server code is not used)*

   **Note:** These performance optimizations are server-specific and not needed for library usage. Benchmarking and aggressive caching are beneficial for multi-user server workloads with concurrent requests, but the library's performance is already optimized for embedded usage through efficient ownership patterns and minimal allocations. **Note: Server code (`server/mod.rs`) is not currently used in production.**

5. __Documentation:__

   - Add more comprehensive function-level documentation
   - Include examples for complex operations
   - ✅ **Performance characteristics documented** - See `INSERT_PERFORMANCE_REPORT.md` and `QUERY_PERFORMANCE_REPORT.md`
   
   **Performance Summary:**
   - **Insert Performance**: ~15K records/sec for fresh inserts, consistent across sizes (1K-1M)
   - **Update Performance**: 594-11,713 records/sec depending on existing data size (degrades with larger datasets)
   - **Query Performance**: Excellent for simple queries (<25ms with 1M records), good for JOINs (<520ms)
   - **Table Loading**: 29K-42K records/sec (2-3x faster than inserts)
   - **Realistic Use Case** (20K records/month): Insert ~1.4s, Load ~0.5s

6. __Known Issues & Limitations:__

   - ⚠️ **Issue #64**: Query default user missing group files
     - **Problem**: Queries only check one path (default OR group), not both
     - **Impact**: Can miss data if same file exists in both locations
     - **Status**: Open issue - see [GitHub Issue #64](https://github.com/mongrov/timon/issues/64)
     - **Potential Solutions**: Merge both files or query both paths with duplicate removal
   
   - ⚠️ **Issue #135**: Insert-Query Race Condition
     - **Problem**: Queries may read parquet files while inserts are writing them, causing "Corrupt Footer", "Out of Range of File", and "Protocol Error" errors
     - **Impact**: DataFusion can encounter incomplete or corrupted files during query execution
     - **Status**: Marked as "wontfix" - see [GitHub Issue #135](https://github.com/mongrov/timon/issues/135)
     - **Note**: Atomic temp file strategy IS implemented (`parquet_file_writer_locked`) for write-write safety, but insert-query race condition remains

## 7. Additional Issues & Concerns Identified

### Error Handling & Panic Safety

⚠️ **Issue: Excessive use of `unwrap()` and `expect()` in production code** (VALID ISSUE - medium priority)
- **Location**: Found 1046+ instances across the codebase
- **Risk**: Can cause panics in production, especially in:
  - `lib.rs`: JNI interface functions use `expect()` for string conversions (lines 29-265)
  - `db_manager.rs`: Metadata operations, cache access, and file operations
  - `main.rs`: Test code, but patterns may leak into production
- **Impact**: 
  - Panics in JNI layer can crash Android/iOS applications
  - Panics during file operations can leave system in inconsistent state
  - Panics during metadata operations can corrupt metadata
- **Recommendation**: 
  - Replace `unwrap()`/`expect()` with proper error handling in production paths
  - Use `?` operator for error propagation
  - Add fallback behavior for critical operations
  - Consider using `unwrap_or_else()` with logging for non-critical paths

⚠️ **Issue: Silent error handling with `.ok()`** (VALID ISSUE, but some errors are intentionally suppressed - medium priority)
- **Location**: `db_manager.rs:515` - `fs::create_dir_all(&partition_dir).ok()`
- **Problem**: 
  - Directory creation failures are silently ignored
  - Multiple other locations use `.ok()` to ignore errors (see grep results)
  - `db_manager.rs:1328, 1337` - `sync_all().ok()` ignores sync failures
  - `helpers.rs:516-518` - Optional parsing with `.ok()` may hide validation issues
- **Impact**: 
  - Failed directory creation can lead to file write failures later
  - Empty partition directories can cause DataFusion validation errors
  - Difficult to debug issues when errors are swallowed
  - Sync failures can lead to data loss if system crashes
- **Recommendation**: 
  - Log directory creation failures
  - Return errors instead of silently ignoring them
  - Clean up empty partition directories on failure
  - Log sync failures (even if non-fatal)
  - Consider using `unwrap_or_else()` with logging for non-critical paths

### Resource Management & Memory Leaks

⚠️ **Issue: DatabaseManager instances never cleaned up** (This won't be a problem when we consider the number of usernames(5-10 usernames for family sharing) each app will have - low priority)
- **Location**: `mod.rs:37` - `static DATABASE_MANAGERS: LazyLock<Arc<Mutex<HashMap<String, DatabaseManager>>>>`
- **Problem**: 
  - DatabaseManager instances are stored in a static HashMap
  - No mechanism to remove unused managers
  - Each manager holds a `SessionContext` and metadata cache
  - HashMap grows indefinitely as new usernames are accessed
  - Auto-creation of managers for new usernames (lines 66-79) accelerates growth
  - No TTL or access tracking for managers
- **Impact**: 
  - Memory usage grows over time
  - SessionContext instances accumulate (each holds query execution state)
  - Potential memory leak in long-running applications
  - Can grow unbounded in multi-user scenarios
- **Recommendation**: 
  - Implement LRU cache with TTL for DatabaseManager instances
  - Add cleanup mechanism for unused managers (e.g., remove after 1 hour of inactivity)
  - Track last access time for each manager
  - Consider weak references or periodic cleanup task
  - Add configurable maximum number of managers

⚠️ **Issue: File lock cleanup may not be sufficient** (I considered the number of files acquiring locks and lock size and that won't be an issue)
- **Location**: `db_manager.rs:54-68` - File lock cleanup logic
- **Problem**: 
  - Locks are cleaned up after 60 minutes of inactivity
  - If many unique file paths are accessed, HashMap can grow large
  - Cleanup only happens during lock acquisition, not proactively
- **Impact**: 
  - Memory usage can grow if many unique files are accessed
  - Locks for deleted files may persist until cleanup
- **Recommendation**: 
  - Add periodic background cleanup task
  - Verify file existence before keeping lock entry
  - Consider reducing cleanup threshold or making it configurable

✅ **Issue: Temporary files may not be cleaned up on error** (FIXED)
- **Location**: `db_manager.rs:261-323` - `cleanup_orphaned_temp_files()`
- **Status**: Fixed - Startup cleanup implemented
- **Implementation**: 
  - `cleanup_orphaned_temp_files()` function scans for `.tmp` files older than 1 hour threshold
  - Called automatically on `init_timon` startup (line 256)
  - Recursively walks through data directory to find all `.tmp` files
  - Removes orphaned temp files that may have been left behind from crashes
  - Uses `ORPHANED_TEMP_FILE_AGE_THRESHOLD` constant (1 hour) for age threshold

⚠️ **Issue: Metadata temporary files may not be cleaned up** (Keeping metadata.json.tmp files is fine because:
1) they're small (JSON metadata), so disk usage is negligible;
2) they're useful for debugging crashes between write and rename;
3) the code always reads metadata.json, never metadata.json.tmp, so there's no risk of reading stale data;
4) the atomic rename pattern ensures metadata.json is either complete or missing, so temp files are harmless leftovers. The original concern about cleanup is not a valid issue in this case)
- **Location**: `db_manager.rs:1315-1344` - `save_metadata()`
- **Problem**: 
  - Metadata writes use temp file pattern: `metadata.json.tmp` (line 1318)
  - Temp file is renamed atomically (line 1332)
  - If process crashes between write and rename, `metadata.json.tmp` remains
  - No cleanup mechanism for orphaned metadata temp files
- **Impact**: 
  - Orphaned `metadata.json.tmp` files can accumulate
  - May cause confusion during debugging
  - Could potentially be read instead of actual metadata if error handling is incorrect
- **Recommendation**: 
  - Add startup cleanup of `metadata.json.tmp` files
  - Verify temp file age before using (reject files older than threshold)
  - Add explicit cleanup after successful rename

### Security Concerns

**Note**: Server code (`server/mod.rs`) is not currently used in production. Any security issues related to server code can be ignored.

🔴 **Issue: Hardcoded default credentials in cloud storage** (VALID SECURITY ISSUE - High Priority)
- **Note**: Server code (`server/mod.rs`) is not currently used in production, so server-related issues can be ignored
- **Location**: 
  - `cloud_sync.rs:177-181` - Internal `CloudStorageManager::new()` uses defaults
  - ~~`server/mod.rs:179-182`~~ - Server code uses defaults (NOT USED - can be ignored)
- **Problem**: 
  ```rust
  // cloud_sync.rs:177-181
  let bucket_endpoint = bucket_endpoint.unwrap_or("http://localhost:9000").to_owned();
  let bucket_name = bucket_name.unwrap_or("timon").to_owned();
  let access_key_id = access_key_id.unwrap_or("ahmed").to_owned();
  let secret_access_key = secret_access_key.unwrap_or("ahmed1234").to_owned();
  
  // server/mod.rs:179-182 (NOT USED - can be ignored)
  // let access_key_id = env::var("ACCESS_KEY_ID").unwrap_or_else(|_| "ahmed".to_string());
  // let secret_access_key = env::var("SECRET_ACCESS_KEY").unwrap_or_else(|_| "ahmed1234".to_string());
  ```
- **Security Analysis**:
  - **Public API Protection**: The public `init_bucket()` function requires all parameters as `&str` (non-optional), so defaults are NOT used in normal library usage
  - **Internal API Risk**: `CloudStorageManager::new()` accepts `Option<&str>` and WILL use defaults if `None` is passed
  - ~~**Server Code Risk**: The HTTP server (`server/mod.rs`) uses hardcoded defaults as fallback~~ *(NOT USED - can be ignored)*
  - **Source Code Exposure**: Default credentials are visible in source code (anyone with code access can see them)
  
- **Security Breaches That Can Occur**:
  1. **Unauthorized S3 Access**: If defaults are used, attackers with code access know the credentials
  2. **Data Exfiltration**: Weak default credentials ("ahmed/ahmed1234") can be easily guessed/brute-forced
  3. **Production Misconfiguration**: If environment variables aren't set in production, insecure defaults are used
  4. **Code Repository Exposure**: If code is in public/private repos, credentials are visible to anyone with access
  5. **Supply Chain Attacks**: Malicious actors can exploit known default credentials if they gain code access
  6. **Compliance Violations**: Using default credentials violates security best practices and may violate regulations (GDPR, HIPAA, etc.)
  
- **Real-World Impact Scenarios**:
  - **Scenario 1**: ~~Developer forgets to set environment variables in production → defaults are used~~ *(NOT APPLICABLE - server code not used)*
  - **Scenario 2**: Internal API misuse → `CloudStorageManager::new()` called with `None` → defaults used → security breach
  - **Scenario 3**: Code repository compromised → attacker sees defaults → attempts to use them on any S3 endpoint
  - **Scenario 4**: If someone accidentally calls internal API with `None` values, and the S3 endpoint is publicly accessible, data is exposed
  
- **Current State**:
  - ✅ **Good**: Public API (`init_bucket()`) requires credentials, preventing accidental use of defaults
  - ❌ **Bad**: Internal API allows `None` values, falling back to defaults
  - ~~❌ **Bad**: Server code uses defaults as fallback~~ *(NOT USED - can be ignored)*
  - ❌ **Bad**: Default credentials are weak and predictable
  
- **Recommendation**: 
  - **High Priority**: Remove hardcoded default credentials from `cloud_sync.rs`
  - ~~**High Priority**: Remove hardcoded defaults from `server/mod.rs`~~ *(NOT USED - can be ignored)*
  - Change `CloudStorageManager::new()` to require non-optional parameters OR return error if `None` is provided
  - Add validation to reject weak/default credentials at runtime
  - Never log credentials in error messages or debug output (currently good - no credential logging found)
  - Consider using a configuration struct that validates all required fields are present
  - Add security documentation warning about credential management

⚠️ **Issue: Credentials passed through JNI interface** 
- **Location**: 
  - `lib.rs:406-456` - `nativeInitBucket` (JNI/Android) receives credentials as `JString` parameters
  - `lib.rs:1220-1242` - `nativeInitBucket` (C interface) receives credentials as `*const c_char` parameters
- **Problem**: 
  - Credentials (`access_key_id`, `secret_access_key`) are passed as Java strings through JNI
  - Java strings are immutable and may remain in memory until garbage collected (unpredictable timing)
  - Rust strings created from JNI are heap-allocated and remain in memory until dropped
  - Credentials are stored in `CloudStorageManager` which keeps them in the `AmazonS3` client (likely stored internally)
  - No explicit memory clearing/zeroization of credentials after use
  - Credentials persist in memory for the lifetime of the application or until the S3 client is dropped
- **Impact**: 
  - **Memory dumps**: Credentials may be exposed in memory dumps (core dumps, crash reports, debugging)
  - **Swap files**: If system uses swap, credentials may be written to disk
  - **Process inspection**: Other processes with appropriate permissions could read memory
  - **JNI layer**: JNI may create temporary copies during string conversion
  - **S3 client**: The `object_store::aws::AmazonS3` client likely stores credentials internally for the lifetime of the client
- **Current State Analysis**:
  - ✅ Good: Error messages don't log actual credential values (only conversion errors)
  - ❌ Issue: Credentials stored in `CloudStorageManager` which lives in a static `Arc` (`CLOUD_STORAGE_MANAGER`)
  - ❌ Issue: No explicit zeroization of credential strings after passing to `init_bucket`
  - ❌ Issue: Credentials passed as regular `String` types, not secure types
- **Recommendations** (in order of priority):
  1. **Use `zeroize` crate for credential handling**:
     - Replace `String` with `zeroize::Zeroizing<String>` for credential variables
     - This ensures credentials are zeroed when dropped
     - Example: `let rust_secret_access_key = Zeroizing::new(jstring_to_rust_string(...)?);`
  
  2. **Clear credentials immediately after use**:
     - After passing credentials to `init_bucket`, explicitly drop/clear the local variables
     - Use `std::mem::drop()` or scope the credentials to minimize lifetime
  
  3. **Review S3 client credential storage**:
     - Check if `object_store::aws::AmazonS3` stores credentials in memory
     - Consider if credentials can be passed differently (e.g., environment variables, but this has trade-offs)
     - The S3 client likely needs credentials for each request, so they may need to persist
  
  4. **Java/Kotlin side considerations**:
     - Use `char[]` instead of `String` for credentials in Java/Kotlin (if possible with JNI)
     - Clear the Java string arrays after passing to JNI
     - Note: JNI `JString` is still a Java `String`, so this has limitations
  
  5. **Alternative approaches**:
     - Consider using Android Keystore or secure storage for credentials
     - Use credential tokens that can be rotated instead of long-lived secrets
     - Implement credential encryption at rest if they must be stored
  
  6. **Limitations to be aware of**:
     - Rust's `String` is heap-allocated and the allocator may not immediately clear freed memory
     - The S3 client (`AmazonS3`) needs credentials for API calls, so they must be accessible
     - JNI string conversion creates copies, so credentials exist in both Java and Rust memory
     - Complete protection is difficult without hardware security modules (HSM)
  
- **Implementation Priority**:
  - **High**: Add `zeroize` crate and use `Zeroizing<String>` for credential variables
  - **Medium**: Ensure credentials are dropped as soon as possible after use
  - **Low**: Investigate if S3 client can use alternative credential mechanisms

### Code Quality & Robustness

⚠️ **Issue: Metadata cache with infinite TTL** (NOT SURE HOW "metadata can get modified externally in our use cases")
- **Location**: `db_manager.rs:195` - `cache_ttl: Duration::MAX`
- **Problem**: 
  - Metadata cache never expires automatically
  - Cache is only invalidated on writes
  - If metadata is modified externally, cache becomes stale
- **How Metadata Can Be Modified Externally**:
  - **Manual editing**: Someone could manually edit `metadata.json` file
  - **File system recovery**: After a crash or file system issue, metadata might be restored from backup
  - **External scripts**: While not currently in the codebase, future scripts could modify metadata.json directly
  - **Cloud sync operations**: If cloud sync is extended to sync metadata files, external changes could occur
  - **Multiple instances**: If multiple app instances share the same storage path, one could modify metadata while another has it cached
- **Impact**:
  - Stale metadata can cause incorrect behavior
  - External metadata changes may not be reflected
- **Recommendation**: 
  - Add configurable TTL for metadata cache
  - Implement cache invalidation on file modification time changes
  - Consider using file watchers for metadata changes

✅ **Issue: No validation of written parquet files** (FIXED)
- **Location**: `db_manager.rs:1062-1095` - `validate_parquet_file()` and `db_manager.rs:1255-1261` - validation call
- **Status**: Fixed - Parquet file validation implemented
- **Implementation**: 
  - `validate_parquet_file()` function validates parquet file structure and integrity
  - Called automatically after each parquet file write in `parquet_file_writer_locked()` (line 1257)
  - Validates parquet file structure using `ParquetRecordBatchReaderBuilder`
  - Verifies footer integrity and schema matches expected schema
  - Checks that file has at least one row group
  - If validation fails, corrupted file is cleaned up automatically

✅ **Issue: Error handling in metadata operations** (FIXED)
- **Location**: `db_manager.rs:1629-1705` - `save_metadata()` with retry logic
- **Status**: Fixed - Retry logic and proper error handling implemented
- **Implementation**: 
  - `save_metadata()` now has comprehensive retry logic with 3 attempts
  - Detects transient errors (PermissionDenied, WouldBlock, TimedOut, Interrupted) and retries
  - Uses exponential backoff with configurable retry delay
  - `save_metadata_attempt()` performs atomic writes using temp file + rename pattern
  - Creates metadata backup before writing (if existing file exists)
  - Preserves original error information instead of converting to string
  - Returns proper `Result` types instead of panicking

### Performance & Scalability

⚠️ **Issue: No limits on concurrent operations** (Not a valid issue in our use cases - low priority)
- **Location**: Throughout codebase
- **Problem**: 
  - No rate limiting on inserts
  - No limit on concurrent queries
  - No limit on file operations
- **Impact**: 
  - Resource exhaustion under high load
  - Potential DoS vulnerability
  - Unpredictable performance degradation
- **Recommendation**: 
  - Add configurable limits on concurrent operations
  - Implement backpressure mechanisms
  - Add rate limiting for API calls

⚠️ **Issue: Synchronous file operations in async context** (VALID ISSUE - medium priority)
- **Location**: `db_manager.rs:1203-1253` - `read_metadata()` uses blocking I/O
- **Problem**: 
  - Metadata reads use blocking `fs::read_to_string()`
  - Called from async contexts
  - Can block async runtime
- **Impact**: 
  - Blocks async runtime threads
  - Reduces concurrency
  - Can cause deadlocks in high-load scenarios
- **Recommendation**: 
  - Use `tokio::fs` for async file operations
  - Or use `spawn_blocking` for blocking operations
  - Ensure all I/O in async paths is non-blocking

✅ **Issue: Multiple Runtime instances created in JNI/iOS interfaces** (FIXED)
- **Location**: `lib.rs:21-26` (JNI) and `lib.rs:990-995` (iOS) - Shared static `RUNTIME`
- **Status**: Fixed - Shared runtime instances implemented
- **Implementation**: 
  - Shared static `LazyLock<Runtime>` for JNI interface (line 21-26)
  - Shared static `LazyLock<Runtime>` for iOS interface (line 990-995)
  - Single runtime instance reused across all JNI/iOS calls
  - Runtime creation errors handled gracefully with `unwrap_or_else()` and process abort (critical system error)
  - Eliminates performance overhead from repeated runtime creation
  - Prevents resource exhaustion from multiple thread pools

### Documentation & Maintainability

⚠️ **Issue: Missing function documentation**
- **Location**: Many functions lack `///` documentation comments
- **Problem**: 
  - Public API functions lack documentation
  - Internal functions lack explanations
  - Complex logic lacks inline comments
- **Impact**: 
  - Difficult for new developers to understand
  - Hard to maintain and modify
  - API usage unclear without reading source
- **Recommendation**: 
  - Add comprehensive doc comments to all public functions
  - Document error conditions and return values
  - Add examples for complex operations
  - Document thread-safety guarantees

⚠️ **Issue: Magic numbers and constants**
- **Location**: Various locations (e.g., `db_manager.rs:61-62` - lock cleanup intervals)
- **Problem**: 
  - Hardcoded values without explanation
  - Magic numbers in calculations
  - No configuration options
- **Impact**: 
  - Difficult to tune for different use cases
  - Unclear why specific values were chosen
- **Recommendation**: 
  - Extract constants with descriptive names
  - Add comments explaining value choices
  - Make configurable where appropriate

## 8. Priority Recommendations

### High Priority (Security & Stability)
1. 🔴 **Remove hardcoded credentials** - **CRITICAL Security risk** (see section 7 for detailed analysis)
2. **Replace `unwrap()`/`expect()` in production paths** - Stability risk
3. **Fix silent error handling** - Data integrity risk
4. ✅ **Add parquet file validation** - Data integrity risk *(FIXED - see section 7)*

### Medium Priority (Performance & Resource Management)
1. **Implement DatabaseManager cleanup** - Memory leak prevention
2. ✅ **Reuse Runtime instances in JNI/iOS** - Performance improvement, resource efficiency *(FIXED - see section 7)*
3. **Add limits on concurrent operations** - Resource protection
4. **Use async file operations** - Performance improvement
5. ✅ **Improve temp file cleanup** - Disk space management (both parquet and metadata temp files) *(FIXED - see section 7)*

### Low Priority (Code Quality & Documentation)
1. **Add comprehensive documentation** - Maintainability
2. **Extract magic numbers** - Code clarity
3. **Add metadata cache TTL** - Correctness
4. **Improve error messages** - Debugging

## 9. Additional Issues Identified (2024 Review Update)

### Resource Management & Efficiency

✅ **Issue: Inefficient Runtime creation in foreign interfaces** (FIXED)
- **Location**: `lib.rs:21-26` (JNI) and `lib.rs:990-995` (iOS) - Shared static `RUNTIME`
- **Status**: Fixed - See "Multiple Runtime instances created in JNI/iOS interfaces" above
- **Note**: Test code still creates individual runtimes, which is acceptable for test isolation

### File System & Cleanup

✅ **Issue: Orphaned temporary files from metadata operations** (FIXED)
- **Location**: `db_manager.rs:261-323` - `cleanup_orphaned_temp_files()`
- **Status**: Fixed - Startup cleanup implemented for all temp files (including metadata)
- **Implementation**: 
  - `cleanup_orphaned_temp_files()` cleans up all `.tmp` files, including `metadata.json.tmp`
  - Called automatically on `init_timon` startup
  - Removes files older than 1 hour threshold
  - See "Temporary files may not be cleaned up on error" above for details

### Error Handling Improvements

⚠️ **Issue: Inconsistent error handling patterns**
- **Location**: Throughout codebase
- **Details**: 
  - Mix of `unwrap()`, `expect()`, `.ok()`, and proper error handling
  - Some critical paths use `unwrap()` (e.g., `db_manager.rs:635` - `record_batches_to_json().unwrap()`)
  - Error information sometimes lost when converting to strings
- **Impact**: 
  - Inconsistent error reporting
  - Some errors cause panics, others are silently ignored
  - Difficult to debug production issues
- **Recommendation**: 
  - Standardize error handling approach
  - Replace all `unwrap()`/`expect()` in production paths
  - Preserve error context when converting errors

### Concurrency & Thread Safety

⚠️ **Issue: Potential deadlock in metadata cache access**
- **Location**: `db_manager.rs:1256-1301` - `get_metadata_cached()`
- **Details**: 
  - Uses `RwLock` for cache with read/write locks
  - Multiple lock acquisitions in same function
  - Lock ordering could cause issues if called from multiple threads
- **Impact**: 
  - Potential deadlocks if locks are acquired in different order
  - Reduced concurrency due to lock contention
- **Recommendation**: 
  - Review lock ordering
  - Consider using `Arc<Mutex<>>` for simpler locking model
  - Add deadlock detection in tests

Overall, Timon is a well-designed library that effectively leverages DataFusion for time-series data management, with a clean API and good error handling. The library implements robust concurrency controls with atomic file writes and file-level locking. Several critical issues have been addressed:

✅ **Fixed Issues:**
- Runtime reuse in JNI/iOS interfaces (shared static runtime)
- Temporary file cleanup on startup (orphaned temp file removal)
- Parquet file validation after writes
- Metadata save retry logic with transient error handling
- Atomic file writes with temp file + rename pattern

The main areas for improvement are in query path resolution (merging default and group paths), documentation, handling edge cases around data synchronization between paths, addressing remaining security and stability concerns (hardcoded credentials, unwrap/expect usage), and DatabaseManager lifecycle management.
