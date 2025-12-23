# Insert Performance Benchmark Report

**Storage Path:** tmp | **Username:** ahmed_test | **Iterations:** 3 | **Generated:** 2025-12-23

## Executive Summary

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| **Fresh Inserts** | ~15K records/sec | Consistent across all sizes (1K - 1M) |
| **Updates** | 594-11,713 records/sec | Depends on existing data size (1K-50K tested) |
| **Table Loading** | 29K-42K records/sec | 2-3x faster than inserts, improves with size |

**Realistic Use Case (20K records/month) with ZivaApp-Ring:**
- Insert: ~1.4s
- Load: ~0.5s

---

## Test 1: Fresh Inserts (New Records)

| Records | Avg Time (s) | Min (s) | Max (s) | Throughput (rec/s) |
|---------|--------------|---------|---------|-------------------|
| 1.0K | 0.069 | 0.066 | 0.073 | 14,496 |
| 10.0K | 0.659 | 0.632 | 0.685 | 15,202 |
| 100.0K | 6.014 | 5.875 | 6.246 | 16,641 |
| 1.0M | 61.103 | 59.544 | 63.166 | 16,376 |

**Performance Breakdown (per 1K records):**
- JSON parsing: ~8ms
- Datetime conversion: ~5ms
- Schema validation: ~9ms
- Record processing: ~15ms
- File writing: ~22ms

---

## Test 2: Updates (Re-inserting Same Records)

**Note:** Updates require loading all existing records into memory. Performance depends on the size of existing data.

### Update Performance by Existing Data Size

Testing updates of **1,000 records** against different amounts of existing data:

| Existing Records | Avg Update Time (s) | Min (s) | Max (s) | Throughput (rec/s) |
|-----------------|---------------------|---------|---------|-------------------|
| 1.0K | 0.087 | 0.072 | 0.104 | 11,713 |
| 10.0K | 0.390 | 0.367 | 0.425 | 2,575 |
| 20.0K | 0.653 | 0.646 | 0.663 | 1,531 |
| 50.0K | 1.692 | 1.572 | 1.859 | 594 |

**Key Insight:** Update performance degrades as existing dataset grows:
- **1K existing:** ~11.7K records/sec (comparable to fresh inserts)
- **10K existing:** ~2.6K records/sec (4.5x slower)
- **20K existing:** ~1.5K records/sec (10x slower)
- **50K existing:** ~594 records/sec (25x slower)

**Bottleneck Breakdown (for 20K existing records):**
- File loading: ~250ms (loading 20K existing records)
- File writing: ~370ms (rewriting updated file)
- Record processing: ~13ms (actual update logic)

---

## Test 3: Table Loading Performance

**Note:** Test sizes based on realistic monthly data volumes. Max expected: ~20K records/file/month (zivaapp ring usage).

| Records | Insert (s) | Load (s) | Load Speedup | Throughput (rec/s) |
|---------|-----------|----------|--------------|-------------------|
| 1.0K | 0.071 | 0.034 | 2.09x faster | 29,256 |
| 10.0K | 0.797 | 0.284 | 2.81x faster | 35,229 |
| 20.0K | 1.415 | 0.482 | 2.94x faster | 41,454 |
| 50.0K | 3.306 | 1.174 | 2.82x faster | 42,606 |

**Key Insight:** Loading throughput improves with table size (29K → 42K records/sec)

---

## Performance Comparison

### Fresh Inserts vs Updates (Updating 1K Records)

| Existing Data | Fresh Insert 1K (s) | Update 1K (s) | Update Penalty |
|---------------|---------------------|----------------|----------------|
| 1K existing | 0.069 | 0.087 | 1.26x slower |
| 10K existing | 0.069 | 0.390 | 5.65x slower |
| 20K existing | 0.069 | 0.653 | 9.46x slower |
| 50K existing | 0.069 | 1.692 | 24.5x slower |
| 1M+ existing | 0.069 | 31.048 | 450x slower |

**Note:** The 1M+ existing records case shows extreme degradation and is not typical for zivaapp ring usage (max ~20K/month).

### Fresh Inserts vs Updates (Large Batch Updates)

| Records | Fresh Insert (s) | Update with 1M+ Existing (s) | Update Penalty |
|---------|------------------|------------------------------|----------------|
| 1.0K | 0.069 | 31.048 | 450x slower |
| 10.0K | 0.659 | 33.075 | 50x slower |
| 100.0K | 6.014 | 38.249 | 6x slower |

### Insert vs Load

| Records | Insert (s) | Load (s) | Load Advantage |
|---------|-----------|----------|----------------|
| 1.0K | 0.071 | 0.034 | 2.09x faster |
| 10.0K | 0.797 | 0.284 | 2.81x faster |
| 20.0K | 1.415 | 0.482 | 2.94x faster |
| 50.0K | 3.306 | 1.174 | 2.82x faster |

---

## Key Findings

### ✅ Strengths
- **Consistent insert performance:** ~15K records/sec regardless of size (1K - 1M)
- **Fast loading:** 2-3x faster than inserts, excellent for queries
- **Linear scaling:** Time increases proportionally with record count
- **Realistic use case optimized:** 20K records/month = ~1.4s insert, ~0.5s load

### ⚠️ Limitations
- **Updates slow down with existing data:** 
  - 1K existing: ~11.7K rec/s (comparable to inserts)
  - 20K existing: ~1.5K rec/s (10x slower)
  - 50K existing: ~594 rec/s (25x slower)
- **Update bottleneck:** File loading + file writing (must load all existing records)
- **For realistic use case (20K/month):** Updates take ~0.65s for 1K records (acceptable)
- **Recommendation:** For very large datasets (100K+), consider batch updates or alternative strategies

---

## Test Schema

The benchmarks used the following table(**activitydetails**) schema, which can influence performance:

```json
{
  "date": {
    "type": "int",
    "required": true,
    "unique": true,
    "datetime": true
  },
  "distance": {
    "type": "int|float"
  },
  "step": {
    "type": "int"
  },
  "calories": {
    "type": "int|float"
  },
  "arraySteps": {
    "type": "array"
  },
  "is_sync": {
    "type": "bool"
  }
}
```

**Schema Characteristics:**
- **6 fields total** (date, distance, step, calories, arraySteps, is_sync)
- **1 unique field** (date) - used for update detection
- **1 datetime field** (date) - requires timestamp conversion
- **1 array field** (arraySteps) - contains 10 integer elements
- **Mixed types:** int, float, bool, array
- **No validation constraints** (min/max removed for these benchmarks)

**Note:** Performance may vary with different schemas:
- More fields = longer validation/processing time
- More complex validation rules = longer validation time
- Larger arrays = more memory/processing overhead

---

## System Configuration

- **Storage:** Local filesystem (tmp directory)
- **Partitioning:** Hive-style (partition_date=YYYY-MM-DD)
- **File Format:** Parquet
- **Bucket Interval:** 43200 (monthly)
- **Test Methodology:** Each test run 3 times, statistics calculated across iterations
