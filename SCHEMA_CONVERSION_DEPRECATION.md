# Schema Conversion Deprecation Plan

## Overview
This document outlines the planned deprecation of automatic schema conversion functionality in the Timon engine, specifically the `convert_batch_schema` function and related compatibility checks.

## Current Status: Phase 2 Complete - Warnings Exposed to API + Schema Inference Warnings

### What Was Changed

#### 1. **helpers.rs - `convert_batch_schema` Function**
- **Location**: Lines 743-776
- **Status**: Marked for deprecation with comprehensive TODO documentation
- **Changes**:
  - Added extensive documentation explaining why this should be removed
  - Listed 5 key reasons for removal (data integrity, hidden bugs, performance, maintainability, predictability)
  - Provided recommended approach and migration path

#### 2. **helpers.rs - `validate_schema_compatibility` Function**
- **Location**: Lines 614-677
- **Changes**:
  - Added TODO comment explaining deprecation plan for `is_compatible_conversion` check
  - Added `eprintln!` warning when schema auto-conversion is applied
  - Warning message informs users that this feature will be removed in future versions

#### 3. **helpers.rs - `combine_unique_batches` Function**
- **Location**: Lines 679-761
- **Changes**:
  - Modified return type from `Result<Vec<RecordBatch>, Error>` to `Result<(Vec<RecordBatch>, Vec<String>), Error>`
  - Created new type alias: `CombineBatchesResult`
  - Now collects and returns deprecation warnings alongside results
  - Added TODO comment explaining why `convert_batch_schema` call is deprecated
  - Detects schema mismatches and generates user-facing warning messages

#### 4. **cloud_sync.rs - Warning Propagation**
- **Location**: Lines 27-36 (documentation), Lines 433-451 (implementation)
- **Changes**:
  - Added comprehensive TODO comment about future API warning propagation
  - Updated `combine_unique_batches` call site to handle new tuple return type
  - Warnings are now logged to stderr via `eprintln!` for each schema conversion
  - Provided roadmap for exposing warnings in API responses

#### 5. **Test Updates**
- **File**: `tests/timon_engine/helpers_test.rs`
- **Changes**:
  - Updated all test cases calling `combine_unique_batches` to destructure tuple return
  - Added assertions to verify warnings are generated when expected
  - Tests now validate both the data results and warning behavior

#### 6. **helpers.rs - `merge_schemas` Function** (NEW)
- **Location**: Lines 961-1023
- **Status**: Marked for deprecation with comprehensive TODO documentation
- **Changes**:
  - Modified return type from `Result<Arc<Schema>, Error>` to `Result<(Arc<Schema>, Vec<String>), Error>`
  - Now detects type coercion during schema merging and generates warnings
  - Warnings include specific field names and type conversions (e.g., Int64 → Float64)
  - Added deprecation warning to function documentation
  - Each type coercion is logged to stderr with detailed information

#### 7. **helpers.rs - `infer_schema_with_coercion` Function** (NEW)
- **Location**: Lines 1047-1074
- **Status**: Marked for deprecation with comprehensive TODO documentation
- **Changes**:
  - Modified return type from `Result<Arc<Schema>, Error>` to `Result<(Arc<Schema>, Vec<String>), Error>`
  - Propagates warnings from `merge_schemas` to caller
  - Added deprecation warning to function documentation
  - This function is the entry point for schema inference during query registration

#### 8. **db_manager.rs - Query Registration with Schema Warnings**
- **Location**: Lines 901-925 in `register_single_table()`
- **Changes**:
  - Updated call to `infer_schema_with_coercion` to handle new tuple return type
  - Logs all schema coercion warnings to stderr with table context
  - Warnings are displayed during table registration in query operations
  - Provides visibility into schema mismatches at query time

## Why Remove Schema Conversion?

### 1. **Data Integrity Issues**
Automatic schema conversion can silently transform data in unexpected ways. For example, converting `Int64` to `List<Int64>` fundamentally changes the data semantics, potentially causing data loss or corruption.

### 2. **Hidden Bugs**
Schema mismatches often indicate upstream data quality issues that should be caught and fixed at the source, not masked by automatic conversion. Silent conversions hide these problems.

### 3. **Performance Overhead**
Runtime schema conversion adds computational overhead and complexity to the merge process, slowing down operations unnecessarily.

### 4. **Maintainability**
Supporting multiple conversion paths increases code complexity and the test surface area, making the codebase harder to maintain and reason about.

### 5. **Predictability**
Users should know exactly what schema their data has without implicit transformations. Explicit is better than implicit.

## Recommended Approach

### Instead of Auto-Conversion:
1. **Enforce strict schema validation** at data ingestion time
2. **Fail fast** with clear error messages when schemas don't match
3. **Require explicit schema migration/evolution** steps when schema changes are needed
4. **Use schema versioning** to track changes over time
5. **Validate data at the source** before it enters the system

## Migration Path

### Phase 1: ✅ COMPLETED - Add Warnings
- [x] Add deprecation warnings to code
- [x] Log warnings when schema conversion is used
- [x] Update return types to include warnings
- [x] Document the deprecation plan

### Phase 2: ✅ COMPLETED - Expose Warnings to Users
- [x] Modify CloudStorageManager to collect warnings from operations
- [x] Update cloud_sync_parquet to aggregate and return warnings
- [x] Update cloud_sink_parquet to collect and return warnings
- [x] Update cloud_fetch_parquet to return warnings (empty for now)
- [x] Add "warnings" field to TimonResponse struct
- [x] Update API handlers to include warnings in responses
- [x] Warnings are now returned in JSON API responses

### Phase 3: 📋 PLANNED - Prepare for Removal
- [ ] Audit all data ingestion pipelines
- [ ] Ensure schema consistency across all data sources
- [ ] Add schema validation at ingestion points
- [ ] Create schema migration tools/scripts
- [ ] Update documentation with new schema requirements
- [ ] Monitor warning frequency in production to assess impact

### Phase 4: 🔮 FUTURE - Remove Conversion Code
- [ ] Remove `convert_batch_schema` function
- [ ] Remove `is_compatible_conversion` check from `validate_schema_compatibility`
- [ ] Remove `merge_data_types` function from `merge_schemas`
- [ ] Remove `infer_schema_with_coercion` function (or make it strict validation only)
- [ ] Update `merge_schemas` to fail on type mismatches instead of coercing
- [ ] Update error messages to be more explicit
- [ ] Remove warning collection code (no longer needed)
- [ ] Update all tests
- [ ] Release as breaking change with major version bump

## Current Warning Behavior

### 1. Cloud Sync Operations (combine_unique_batches)
When schema conversion is triggered during cloud sync, users will see:

```
⚠️ DEPRECATION WARNING: Schema auto-conversion applied for field 'field_name': Int64 <-> List(Field { name: "item", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }). This feature will be removed in a future version. Please ensure schemas match exactly.
```

### 2. Query Operations (infer_schema_with_coercion)
When schema coercion is triggered during query table registration, users will see:

```
⚠️ Schema coercion warnings for table 'table_name':
  ⚠️ DEPRECATION WARNING: Schema auto-coercion applied for field 'field_name': Int64 <-> Float64 merged to Float64. This feature will be removed in a future version. Please ensure schemas match exactly across all parquet files.
```

## Future API Response Format

Once Phase 2 is complete, API responses will include warnings:

```json
{
  "status": "success",
  "data": { ... },
  "warnings": [
    "DEPRECATION WARNING: Schema auto-conversion applied for field 'field_name': Int64 <-> List<Int64>. This feature will be removed in a future version. Please ensure schemas match exactly."
  ]
}
```

## Impact Assessment

### Low Risk Areas:
- New data ingestion (can enforce strict schemas from the start)
- Single-source tables (no merging needed)

### High Risk Areas:
- Cloud sync operations merging local and S3 data
- Tables with historical schema evolution
- Multi-user environments with different client versions

## Testing Strategy

### Current Tests:
- All existing tests updated to handle new return type
- Tests verify warnings are generated appropriately
- Tests validate both success and error paths

### Future Tests Needed:
- Integration tests for strict schema validation
- Tests for schema migration tools
- Tests for improved error messages
- Performance benchmarks without conversion overhead

## Timeline

- **Phase 1**: ✅ Completed (2026-02-01)
- **Phase 2**: ✅ Completed (2026-02-03) - Extended to include schema inference warnings
- **Phase 3**: Target Q3 2026
- **Phase 4**: Target Q4 2026 (Breaking change)

## Impact Areas

### Cloud Sync Operations
- Warnings are returned in API responses through `warnings` field
- Users can monitor and address schema inconsistencies proactively
- Affects: `cloud_sync_parquet`, `cloud_sink_parquet`

### Query Operations
- Warnings are returned in API response during table registration
- Happens when DataFusion loads parquet files with mixed schemas
- Affects: All `query()` operations that use `infer_schema_with_coercion`
- **Note**: `query()` returns warnings in API response, `query_df()` only logs to stderr

## Questions or Concerns?

If you encounter issues with schema mismatches or have questions about this deprecation plan, please:
1. Check your data ingestion pipelines for schema consistency
2. Review the error/warning messages for specific field mismatches
3. Use parquet-tools or similar to inspect schema of existing parquet files
4. Consider re-generating parquet files with consistent schema
5. Contact the development team for assistance with schema migrations

---

**Last Updated**: 2026-02-03
**Status**: Phase 2 Complete - Warnings Now Exposed in API Responses + Schema Inference Warnings Added
