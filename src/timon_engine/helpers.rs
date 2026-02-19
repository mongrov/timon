use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Datelike, Days, Local, NaiveDate, TimeZone, Timelike, Utc};
use datafusion::arrow::array::{
  new_null_array, Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Float64Array, Float64Builder, Int32Array, Int64Array, Int64Builder,
  ListArray, ListBuilder, StringArray, StringBuilder, StringViewArray, StructArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use datafusion::parquet::data_type::{AsBytes, Decimal};
use datafusion::parquet::record::{Field as ParquetField, Row};
use datafusion::scalar::ScalarValue;
use json_rules_engine::{float_greater_than, float_less_than, int_greater_than, int_less_than, Condition};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, metadata, File};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Value, serde_json::Error> {
  fn array_value_to_json(array: &ArrayRef, row_index: usize) -> Result<serde_json::Value, String> {
    match array.data_type() {
      DataType::Int64 => array
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|arr| json!(arr.value(row_index)))
        .ok_or_else(|| format!("Failed to downcast array to Int64Array for row {}", row_index)),
      DataType::Int32 => array
        .as_any()
        .downcast_ref::<Int32Array>()
        .map(|arr| json!(arr.value(row_index)))
        .ok_or_else(|| format!("Failed to downcast array to Int32Array for row {}", row_index)),
      DataType::Float64 => array
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|arr| json!(arr.value(row_index)))
        .ok_or_else(|| format!("Failed to downcast array to Float64Array for row {}", row_index)),
      DataType::Utf8 => array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|string_array| {
          if string_array.is_null(row_index) {
            json!(null)
          } else {
            json!(string_array.value(row_index))
          }
        })
        .ok_or_else(|| format!("Failed to downcast array to StringArray for row {}", row_index)),
      DataType::Utf8View => array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .map(|string_view_array| {
          if string_view_array.is_null(row_index) {
            json!(null)
          } else {
            json!(string_view_array.value(row_index).to_string())
          }
        })
        .ok_or_else(|| format!("Failed to downcast array to StringViewArray for row {}", row_index)),
      DataType::Boolean => array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|arr| json!(arr.value(row_index)))
        .ok_or_else(|| format!("Failed to downcast array to BooleanArray for row {}", row_index)),
      DataType::Timestamp(TimeUnit::Millisecond, None) => array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .map(|arr| json!(arr.value(row_index)))
        .ok_or_else(|| format!("Failed to downcast array to TimestampMillisecondArray for row {}", row_index)),
      DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
        let timestamp_ms = array
          .as_any()
          .downcast_ref::<TimestampMillisecondArray>()
          .ok_or_else(|| format!("Failed to downcast array to TimestampMillisecondArray for row {}", row_index))?
          .value(row_index);
        let naive_datetime = DateTime::from_timestamp(
          timestamp_ms / 1_000,                      // Seconds
          (timestamp_ms % 1_000 * 1_000_000) as u32, // Nanoseconds
        )
        .ok_or_else(|| format!("Invalid timestamp value {} for row {}", timestamp_ms, row_index))?;
        let local_time = naive_datetime.with_timezone(&Local);
        Ok(json!(local_time.format("%Y-%m-%d %H:%M:%S").to_string()))
      }
      DataType::Timestamp(TimeUnit::Nanosecond, None) => {
        let timestamp_ns = array
          .as_any()
          .downcast_ref::<TimestampNanosecondArray>()
          .ok_or_else(|| format!("Failed to downcast array to TimestampNanosecondArray for row {}", row_index))?
          .value(row_index);
        let naive_datetime = DateTime::from_timestamp(
          timestamp_ns / 1_000_000_000,          // Seconds
          (timestamp_ns % 1_000_000_000) as u32, // Nanoseconds
        )
        .ok_or_else(|| format!("Invalid timestamp value {} for row {}", timestamp_ns, row_index))?;
        let local_time = naive_datetime.with_timezone(&Local);
        Ok(json!(local_time.format("%Y-%m-%d %H:%M:%S").to_string()))
      }
      DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
        let timestamp_ns = array
          .as_any()
          .downcast_ref::<TimestampNanosecondArray>()
          .ok_or_else(|| format!("Failed to downcast array to TimestampNanosecondArray for row {}", row_index))?
          .value(row_index);
        let naive_datetime = DateTime::from_timestamp(
          timestamp_ns / 1_000_000_000,          // Seconds
          (timestamp_ns % 1_000_000_000) as u32, // Nanoseconds
        )
        .ok_or_else(|| format!("Invalid timestamp value {} for row {}", timestamp_ns, row_index))?;
        let local_time = naive_datetime.with_timezone(&Local);
        Ok(json!(local_time.format("%Y-%m-%d %H:%M:%S").to_string()))
      }
      DataType::Date32 => {
        let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| "Failed to create base date (1970-01-01)".to_string())?;
        Ok(
          array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|date_array| date_array.value(row_index))
            .and_then(|days_since_epoch| base_date.checked_add_days(Days::new(days_since_epoch as u64)))
            .map_or(json!(null), |naive_date| json!(naive_date)),
        )
      }
      DataType::List(_inner_field) => {
        let list_array = array
          .as_any()
          .downcast_ref::<ListArray>()
          .ok_or_else(|| format!("Failed to downcast array to ListArray for row {}", row_index))?;
        let offsets = list_array.value_offsets();
        let start_idx = offsets[row_index] as usize;
        let end_idx = offsets[row_index + 1] as usize;
        let values_array = list_array.values();

        // Recursive function to handle nested lists
        fn extract_list_values(array: &dyn Array, start_idx: usize, end_idx: usize) -> Result<Vec<serde_json::Value>, String> {
          match array.data_type() {
            DataType::Utf8 => array
              .as_any()
              .downcast_ref::<StringArray>()
              .map(|string_array| (start_idx..end_idx).map(|i| json!(string_array.value(i))).collect())
              .ok_or_else(|| "Failed to downcast list values to StringArray".to_string()),
            DataType::Int64 => array
              .as_any()
              .downcast_ref::<Int64Array>()
              .map(|int_array| (start_idx..end_idx).map(|i| json!(int_array.value(i))).collect())
              .ok_or_else(|| "Failed to downcast list values to Int64Array".to_string()),
            DataType::Float64 => array
              .as_any()
              .downcast_ref::<Float64Array>()
              .map(|float_array| (start_idx..end_idx).map(|i| json!(float_array.value(i))).collect())
              .ok_or_else(|| "Failed to downcast list values to Float64Array".to_string()),
            DataType::Boolean => array
              .as_any()
              .downcast_ref::<BooleanArray>()
              .map(|bool_array| (start_idx..end_idx).map(|i| json!(bool_array.value(i))).collect())
              .ok_or_else(|| "Failed to downcast list values to BooleanArray".to_string()),
            _ => Ok(Vec::new()),
          }
        }

        extract_list_values(values_array.as_ref(), start_idx, end_idx).map(|values| json!(values))
      }
      DataType::Struct(fields) => {
        let struct_array = array
          .as_any()
          .downcast_ref::<StructArray>()
          .ok_or_else(|| format!("Failed to downcast array to StructArray for row {}", row_index))?;

        // Check if the struct value is null
        if struct_array.is_null(row_index) {
          return Ok(json!(null));
        }

        // Build a JSON object from the struct fields
        let mut obj = serde_json::Map::new();
        for (i, field) in fields.iter().enumerate() {
          let column = struct_array.column(i);
          let field_value = array_value_to_json(column, row_index)?;
          obj.insert(field.name().clone(), field_value);
        }
        Ok(json!(obj))
      }
      datatype => {
        eprintln!("Warning: unsupported Datatype {} for row {}", datatype, row_index);
        Ok(json!(null))
      }
    }
  }

  // Convert each row of the record batches into a JSON object
  let mut rows = Vec::new();
  for batch in batches {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    for row_index in 0..num_rows {
      let mut row = HashMap::with_capacity(schema.fields().len());
      for (col_index, field) in schema.fields().iter().enumerate() {
        let column = batch.column(col_index);
        match array_value_to_json(column, row_index) {
          Ok(value) => {
            row.insert(field.name().clone(), value);
          }
          Err(e) => {
            return Err(serde_json::Error::io(std::io::Error::new(
              ErrorKind::InvalidData,
              format!("Failed to convert field '{}' at row {}: {}", field.name(), row_index, e),
            )));
          }
        }
      }
      rows.push(row);
    }
  }

  serde_json::to_value(&rows)
}

pub fn row_to_json(row: &Row) -> serde_json::Value {
  fn parquet_value_to_json(value: &ParquetField) -> serde_json::Value {
    fn decimal_to_string(decimal: &Decimal) -> String {
      let value = decimal.as_bytes();
      let precision = decimal.precision();
      let scale = decimal.scale();
      let int_part = &value[..precision as usize - scale as usize];
      let frac_part = &value[precision as usize - scale as usize..];
      format!("{}.{:?}", hex::encode(int_part), frac_part)
    }

    match value {
      ParquetField::Bool(b) => json!(*b),
      ParquetField::Byte(b) => json!(*b),
      ParquetField::Short(s) => json!(*s),
      ParquetField::Int(i) => json!(*i),
      ParquetField::Long(l) => json!(*l),
      ParquetField::Float(f) => json!(*f),
      ParquetField::Double(d) => json!(*d),
      ParquetField::Str(s) => json!(s),
      ParquetField::Bytes(b) => json!(general_purpose::STANDARD.encode(b)),
      ParquetField::TimestampMicros(t) => json!(t),
      ParquetField::TimestampMillis(t) => json!(t),
      ParquetField::Decimal(d) => json!(decimal_to_string(d)),
      ParquetField::ListInternal(list) => {
        let json_array: Vec<serde_json::Value> = list.elements().iter().map(|element| parquet_value_to_json(element)).collect();
        serde_json::Value::Array(json_array)
      }
      ParquetField::Group(g) => {
        let json_object: serde_json::Map<_, _> = g
          .get_column_iter()
          .map(|(name, field)| (name.clone(), parquet_value_to_json(field)))
          .collect();
        serde_json::Value::Object(json_object)
      }
      _ => serde_json::Value::Null,
    }
  }

  let json_map: serde_json::Map<_, _> = row
    .get_column_iter()
    .map(|(name, value)| (name.clone(), parquet_value_to_json(value)))
    .collect();

  serde_json::Value::Object(json_map)
}

/// Convert JSON records to Arrow arrays and schema. When `table_schema` is provided (from table creation),
/// each field's nullability is taken from `"required"`: `required: false` or omitted => nullable; `required: true` => not nullable.
/// That schema is written into Parquet so DESCRIBE and readers see the correct is_nullable.
pub fn json_to_arrow(
  json_values: &[Value],
  preferred_field_order: Option<&[String]>,
  table_schema: Option<&Value>,
) -> Result<(Vec<ArrayRef>, Schema), Box<dyn std::error::Error>> {
  fn resolve_data_type_conflict(current: Option<DataType>, new_type: DataType) -> DataType {
    match (current, new_type) {
      (None, new) => new,
      (Some(DataType::Int64), DataType::Float64) => DataType::Float64, // Promote Int64 to Float64
      (Some(DataType::Float64), DataType::Int64) => DataType::Float64, // Promote Int64 to Float64
      (Some(current), new) if current == new => current,               // Same type
      (_, new) => new,                                                 // Prefer the new type
    }
  }

  if json_values.is_empty() {
    return Err("No data to write".into());
  }

  // Determine the schema dynamically
  let mut field_types: std::collections::HashMap<String, DataType> = std::collections::HashMap::new();

  // Iterate through each JSON object to detect data types
  for obj in json_values.iter().filter_map(Value::as_object) {
    for (key, value) in obj.iter() {
      let current_type = field_types.get(key).cloned();
      let new_type = match value {
        Value::Number(num) if num.is_f64() => DataType::Float64,
        Value::Number(_) => DataType::Int64,
        Value::String(_) => DataType::Utf8,
        Value::Bool(_) => DataType::Boolean,
        Value::Array(arr) => {
          if let Some(first_val) = arr.first() {
            match first_val {
              Value::Number(n) if n.is_f64() => DataType::List(Box::new(ArrowField::new("item", DataType::Float64, true)).into()),
              Value::Number(_) => DataType::List(Box::new(ArrowField::new("item", DataType::Int64, true)).into()),
              Value::String(_) => DataType::List(Box::new(ArrowField::new("item", DataType::Utf8, true)).into()),
              Value::Bool(_) => DataType::List(Box::new(ArrowField::new("item", DataType::Boolean, true)).into()),
              _ => DataType::List(Box::new(ArrowField::new("item", DataType::Null, true)).into()),
            }
          } else {
            DataType::List(Box::new(ArrowField::new("item", DataType::Null, true)).into())
          }
        }
        datatype => {
          println!("json_to_arrow: unsupported datatype {}", datatype);
          DataType::Null
        }
      };

      // Resolve potential conflicts by promoting types
      field_types.insert(key.clone(), resolve_data_type_conflict(current_type, new_type));
    }
  }

  // Define schema fields in deterministic order: use preferred_field_order when provided,
  // then append any fields present in data but not in that order (sorted by name).
  let field_names: Vec<String> = if let Some(order) = preferred_field_order {
    let order_set: HashSet<_> = order.iter().map(String::as_str).collect();
    let mut ordered: Vec<String> = order.iter().filter(|n| field_types.contains_key(*n)).cloned().collect();
    let mut rest: Vec<String> = field_types.keys().filter(|k| !order_set.contains(k.as_str())).cloned().collect();
    rest.sort();
    ordered.extend(rest);
    ordered
  } else {
    let mut names: Vec<String> = field_types.keys().cloned().collect();
    names.sort();
    names
  };

  let table_obj = table_schema.and_then(Value::as_object);
  let fields: Vec<ArrowField> = field_names
    .iter()
    .map(|key| {
      let nullable = table_obj
        .and_then(|o| o.get(key))
        .and_then(|f| f.get("required"))
        .and_then(Value::as_bool)
        .map(|required| !required)
        .unwrap_or(true); // default: not required => nullable
      ArrowField::new(key, field_types[key].clone(), nullable)
    })
    .collect();
  let schema = Schema::new(fields);

  // Create Arrow arrays based on the detected schema
  let arrays: Vec<ArrayRef> = schema
    .fields()
    .iter()
    .map(|field| {
      Ok(match field.data_type() {
        DataType::Int64 => {
          let values: Vec<i64> = json_values
            .iter()
            .map(|v| v.get(&field.name()).and_then(Value::as_i64).unwrap_or_default())
            .collect();
          Arc::new(Int64Array::from(values)) as ArrayRef
        }
        DataType::Float64 => {
          let values: Vec<f64> = json_values
            .iter()
            .map(|v| v.get(&field.name()).and_then(Value::as_f64).unwrap_or_default())
            .collect();
          Arc::new(Float64Array::from(values)) as ArrayRef
        }
        DataType::Utf8 => {
          let values: Vec<String> = json_values
            .iter()
            .map(|v| v.get(&field.name()).and_then(Value::as_str).unwrap_or_default().to_string())
            .collect();
          Arc::new(StringArray::from(values)) as ArrayRef
        }
        DataType::Boolean => {
          let values: Vec<bool> = json_values
            .iter()
            .map(|v| v.get(&field.name()).and_then(Value::as_bool).unwrap_or_default())
            .collect();
          Arc::new(BooleanArray::from(values)) as ArrayRef
        }
        DataType::List(inner_field) => {
          let element_type = inner_field.data_type();

          match element_type {
            DataType::Utf8 => {
              let string_builder = StringBuilder::new();
              let mut list_builder = ListBuilder::new(string_builder);

              for value in json_values.iter().map(|v| v.get(&field.name())) {
                if let Some(Value::Array(arr)) = value {
                  let string_builder = list_builder.values();
                  for item in arr {
                    let str_val = item.as_str().unwrap_or_default();
                    string_builder.append_value(str_val);
                  }
                  list_builder.append(true);
                } else {
                  list_builder.append(false); // Handle missing or non-array values
                }
              }

              let list_array = list_builder.finish();
              Arc::new(list_array) as ArrayRef
            }
            DataType::Int64 => {
              let int_builder = Int64Builder::new();
              let mut list_builder = ListBuilder::new(int_builder);

              for value in json_values.iter().map(|v| v.get(&field.name())) {
                if let Some(Value::Array(arr)) = value {
                  let int_builder = list_builder.values();
                  for item in arr {
                    let int_val = item.as_i64().unwrap_or_default();
                    int_builder.append_value(int_val);
                  }
                  list_builder.append(true);
                } else {
                  list_builder.append(false);
                }
              }

              let list_array = list_builder.finish();
              Arc::new(list_array) as ArrayRef
            }
            DataType::Float64 => {
              let float_builder = Float64Builder::new();
              let mut list_builder = ListBuilder::new(float_builder);

              for value in json_values.iter().map(|v| v.get(&field.name())) {
                if let Some(Value::Array(arr)) = value {
                  let float_builder = list_builder.values();
                  for item in arr {
                    let float_val = item.as_f64().unwrap_or_default();
                    float_builder.append_value(float_val);
                  }
                  list_builder.append(true);
                } else {
                  list_builder.append(false);
                }
              }

              let list_array = list_builder.finish();
              Arc::new(list_array) as ArrayRef
            }
            DataType::Boolean => {
              let bool_builder = BooleanBuilder::new();
              let mut list_builder = ListBuilder::new(bool_builder);

              for value in json_values.iter().map(|v| v.get(&field.name())) {
                if let Some(Value::Array(arr)) = value {
                  let bool_builder = list_builder.values();
                  for item in arr {
                    let bool_val = item.as_bool().unwrap_or(false);
                    bool_builder.append_value(bool_val);
                  }
                  list_builder.append(true);
                } else {
                  list_builder.append(false);
                }
              }

              let list_array = list_builder.finish();
              Arc::new(list_array) as ArrayRef
            }
            _ => {
              return Err(format!("Unsupported inner data type for ListArray: '{:?}'", element_type).into());
            }
          }
        }
        _ => return Err(format!("Unsupported data type for field '{}'", field.name()).into()),
      })
    })
    .collect::<Result<_, Box<dyn std::error::Error>>>()?;

  Ok((arrays, schema))
}

pub fn rounded_timestamp(timestamp: i64, interval: u32) -> String {
  let dt = match Utc.timestamp_opt(timestamp, 0).single() {
    Some(dt) => dt,
    None => {
      eprintln!("Warning: Invalid timestamp {}, using current time", timestamp);
      Utc::now()
    }
  };

  let rounded_time = if interval >= 43200 {
    // 30 days * 24 hours * 60 minutes = 43200
    // For monthly intervals
    dt.with_day(1)
      .and_then(|d| d.with_hour(0))
      .and_then(|d| d.with_minute(0))
      .and_then(|d| d.with_second(0))
      .and_then(|d| d.with_nanosecond(0))
      .unwrap_or_else(|| {
        eprintln!("Warning: Failed to round timestamp for monthly interval, using original");
        dt
      })
  } else if interval >= 10080 {
    // 7 days * 24 hours * 60 minutes = 10080
    // For weekly intervals
    let days_since_monday = dt.weekday().num_days_from_monday();
    (dt - chrono::Duration::days(days_since_monday as i64))
      .with_hour(0)
      .and_then(|d| d.with_minute(0))
      .and_then(|d| d.with_second(0))
      .and_then(|d| d.with_nanosecond(0))
      .unwrap_or_else(|| {
        eprintln!("Warning: Failed to round timestamp for weekly interval, using original");
        dt
      })
  } else if interval >= 1440 {
    // 1 day * 24 hours * 60 minutes = 1440
    // For daily intervals
    dt.with_hour(0)
      .and_then(|d| d.with_minute(0))
      .and_then(|d| d.with_second(0))
      .and_then(|d| d.with_nanosecond(0))
      .unwrap_or_else(|| {
        eprintln!("Warning: Failed to round timestamp for daily interval, using original");
        dt
      })
  } else if interval > 60 {
    // For intervals greater than 60 minutes
    let total_minutes = dt.hour() * 60 + dt.minute();
    let rounded_total_minutes = (total_minutes / interval) * interval;
    let rounded_hour = rounded_total_minutes / 60;
    let rounded_minute = rounded_total_minutes % 60;

    dt.with_hour(rounded_hour as u32)
      .and_then(|d| d.with_minute(rounded_minute as u32))
      .and_then(|d| d.with_second(0))
      .and_then(|d| d.with_nanosecond(0))
      .unwrap_or_else(|| {
        eprintln!("Warning: Failed to round timestamp for {} minute interval, using original", interval);
        dt
      })
  } else {
    // For intervals within 60 minutes
    let rounded_minute = (dt.minute() / interval) * interval;
    dt.with_minute(rounded_minute)
      .and_then(|d| d.with_second(0))
      .and_then(|d| d.with_nanosecond(0))
      .unwrap_or_else(|| {
        eprintln!("Warning: Failed to round timestamp for {} minute interval, using original", interval);
        dt
      })
  };

  // Output format based on interval
  if interval >= 43200 {
    // Monthly format: YYYY-MM
    rounded_time.format("%Y-%m").to_string()
  } else if interval >= 10080 {
    // Weekly format: YYYY-MM-DD
    rounded_time.format("%Y-%m-%d").to_string()
  } else if interval >= 1440 {
    // Daily format: YYYY-MM-DD
    rounded_time.format("%Y-%m-%d").to_string()
  } else if interval > 60 && interval % 60 == 0 {
    // Hourly format: YYYY-MM-DD_HH
    rounded_time.format("%Y-%m-%d_%H").to_string()
  } else {
    // Minute format: YYYY-MM-DD_HH-MM
    rounded_time.format("%Y-%m-%d_%H-%M").to_string()
  }
}

pub fn get_property_fields(schema: &Value, property: &str) -> Result<Vec<String>, Box<dyn Error>> {
  let mut fields = Vec::new();
  if let Some(properties) = schema.as_object() {
    for (field_name, field_properties) in properties {
      if let Some(prop_value) = field_properties.get(property) {
        if prop_value.as_bool() == Some(true) {
          fields.push(field_name.clone());
        }
      }
    }
  }
  Ok(fields)
}

pub fn filter_files_by_date_range(files: Vec<String>, start_date: &str, end_date: &str) -> Result<Vec<String>, Box<dyn Error>> {
  let start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")?;
  let end_date = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")?;
  // Regex to match different file formats: YYYY-MM-DD, YYYY-MM, YYYY
  let regx =
    Regex::new(r"(?P<year>\d{4})(?:-(?P<month>\d{2})(?:-(?P<day>\d{2}))?)?").map_err(|e| format!("Failed to compile regex pattern: {}", e))?;

  let filtered_files: Vec<String> = files
    .iter()
    .filter(|file| {
      if let Some(date_str) = file.split('/').last() {
        if let Some(caps) = regx.captures(date_str) {
          // Parse date components - .ok() is intentional here: invalid date formats
          // should be skipped (return false) rather than causing the filter to fail
          let year = caps["year"].parse::<i32>().ok();
          let month = caps.name("month").map(|m| m.as_str().parse::<u32>().ok()).flatten();
          let day = caps.name("day").map(|d| d.as_str().parse::<u32>().ok()).flatten();

          if let Some(year) = year {
            let file_date = match (month, day) {
              (Some(m), Some(d)) => NaiveDate::from_ymd_opt(year, m, d),
              (Some(m), None) => NaiveDate::from_ymd_opt(year, m, 1),
              (None, None) => NaiveDate::from_ymd_opt(year, 1, 1),
              // Invalid pattern: day without month should not occur with current regex, handle gracefully by excluding the file
              (None, Some(_)) => None,
            };

            if let Some(file_date) = file_date {
              return file_date >= start_date && file_date <= end_date;
            }
          }
        }
      }
      false
    })
    .cloned()
    .collect();

  Ok(filtered_files)
}

pub fn get_local_file_modified_time(local_path: &str) -> Option<DateTime<Utc>> {
  if let Ok(metadata) = metadata(local_path) {
    if let Ok(modified) = metadata.modified() {
      let duration_since_epoch = modified.duration_since(UNIX_EPOCH).unwrap_or_default();
      return Some(DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(duration_since_epoch.as_secs())));
    }
  }
  None
}

pub fn combine_unique_batches(
  local_batches: Vec<RecordBatch>,
  s3_batches: Vec<RecordBatch>,
  unique_fields: &[String],
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
  let schema = local_batches
    .first()
    .map(|b| b.schema())
    .or_else(|| s3_batches.first().map(|b| b.schema()))
    .ok_or("No batches provided")?;

  let unique_indices: Vec<usize> = unique_fields
    .iter()
    .map(|field| schema.index_of(field).map_err(|e| format!("Field '{}' not found: {:?}", field, e)))
    .collect::<Result<Vec<_>, _>>()?;

  let mut unique_map: HashMap<Vec<ScalarValue>, Vec<ScalarValue>> = HashMap::new();

  for batch in s3_batches.into_iter().chain(local_batches) {
    let unified_batch = convert_batch_schema(&batch, &schema)?; // Fixed type mismatch

    for row_index in 0..unified_batch.num_rows() {
      let unique_key: Vec<ScalarValue> = unique_indices
        .iter()
        .map(|&index| {
          ScalarValue::try_from_array(unified_batch.column(index), row_index)
            .map_err(|e| format!("Failed to convert unique key field at index {} for row {}: {:?}", index, row_index, e))
        })
        .collect::<Result<Vec<_>, _>>()?;

      let row_values: Vec<ScalarValue> = (0..unified_batch.num_columns())
        .map(|col_index| {
          ScalarValue::try_from_array(unified_batch.column(col_index), row_index)
            .map_err(|e| format!("Failed to convert column {} for row {}: {:?}", col_index, row_index, e))
        })
        .collect::<Result<Vec<_>, _>>()?;

      unique_map.insert(unique_key, row_values);
    }
  }

  let mut column_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); schema.fields().len()];
  for row_values in unique_map.values() {
    for (col_idx, value) in row_values.iter().enumerate() {
      column_values[col_idx].push(value.clone());
    }
  }

  let mut final_columns = Vec::new();
  for (col_idx, _field) in schema.fields().iter().enumerate() {
    let column_data = &column_values[col_idx];
    let array = ScalarValue::iter_to_array(column_data.iter().cloned())?;
    final_columns.push(array);
  }

  let combined_unique_batches = RecordBatch::try_new(schema.clone(), final_columns)?;
  Ok(vec![combined_unique_batches])
}

fn convert_batch_schema(batch: &RecordBatch, target_schema: &Schema) -> Result<RecordBatch, Box<dyn std::error::Error>> {
  let mut new_columns = Vec::new();
  for field in target_schema.fields() {
    let column = if let Some(existing_column) = batch.column_by_name(field.name()) {
      if existing_column.data_type() != field.data_type() {
        eprintln!("Warning: Cannot auto-convert {} to {}", existing_column.data_type(), field.data_type());
        existing_column.clone()
      } else {
        existing_column.clone()
      }
    } else {
      new_null_array(field.data_type(), batch.num_rows())
    };
    new_columns.push(column);
  }
  Ok(RecordBatch::try_new(Arc::new(target_schema.clone()), new_columns)?)
}

pub fn read_parquet_batches(file_path: &Path, batches: &mut Vec<RecordBatch>) -> Result<bool, Box<dyn std::error::Error>> {
  let mut buffer = Vec::new();
  File::open(file_path)?.read_to_end(&mut buffer)?;
  let reader = ParquetRecordBatchReader::try_new(bytes::Bytes::from(buffer), 1024)?;
  batches.extend(reader.collect::<Result<Vec<_>, _>>()?);
  Ok(true)
}

pub async fn cleanup_old_files(processed_files: &[PathBuf]) {
  let regx = match Regex::new(r"(\d{4})-(\d{2})-(\d{2})") {
    Ok(regex) => regex,
    Err(e) => {
      eprintln!("Warning: Failed to compile regex pattern for cleanup: {}", e);
      return;
    }
  };
  let current_date = chrono::Utc::now().naive_utc().date();
  for file_path in processed_files {
    if let Some(filename) = file_path.file_name().and_then(|n| n.to_str()) {
      if let Some(caps) = regx.captures(filename) {
        let file_date_str = format!("{}-{}-{}", &caps[1], &caps[2], &caps[3]);
        if NaiveDate::parse_from_str(&file_date_str, "%Y-%m-%d").map_or(false, |file_date| file_date < current_date) {
          if let Err(e) = fs::remove_file(file_path) {
            eprintln!("Warning: Failed to delete file {}: {:?}", file_path.display(), e);
          }
        }
      }
    }
  }
}

pub fn build_rules_tree(table_schema: Value) -> Vec<Condition> {
  let mut conditions = Vec::new();

  if let Some(schema_map) = table_schema.as_object() {
    for (field, properties) in schema_map {
      if let Some(field_type) = properties.get("type").and_then(|v| v.as_str()) {
        let min = properties.get("min").and_then(|v| v.as_f64());
        let max = properties.get("max").and_then(|v| v.as_f64());

        match field_type {
          "int" => {
            if let Some(min_val) = min {
              conditions.push(int_greater_than(field, min_val as i64));
            }
            if let Some(max_val) = max {
              conditions.push(int_less_than(field, max_val as i64));
            }
          }
          "float" => {
            if let Some(min_val) = min {
              conditions.push(float_greater_than(field, min_val));
            }
            if let Some(max_val) = max {
              conditions.push(float_less_than(field, max_val));
            }
          }
          _ => {}
        }
      }
    }
  }

  conditions
}

/// Merge two data types, promoting compatible types (e.g., Int64 -> Float64)
fn merge_data_types(dt1: &DataType, dt2: &DataType) -> DataType {
  use DataType::*;
  match (dt1, dt2) {
    // If types are the same, return as-is
    (a, b) if a == b => a.clone(),
    // Promote Int64 to Float64 when mixing with Float64
    (Int64, Float64) | (Float64, Int64) => Float64,
    // Promote Int32 to Int64 when mixing with Int64
    (Int32, Int64) | (Int64, Int32) => Int64,
    // Promote Int32 to Float64 when mixing with Float64
    (Int32, Float64) | (Float64, Int32) => Float64,
    // Promote smaller integers to larger ones
    (Int8, Int16) | (Int16, Int8) => Int16,
    (Int8, Int32) | (Int32, Int8) => Int32,
    (Int16, Int32) | (Int32, Int16) => Int32,
    (Int16, Int64) | (Int64, Int16) => Int64,
    // Promote unsigned to signed when mixing
    (UInt8, Int16) | (Int16, UInt8) => Int16,
    (UInt16, Int32) | (Int32, UInt16) => Int32,
    (UInt32, Int64) | (Int64, UInt32) => Int64,
    // For other cases, prefer the more general type or return the first
    _ => {
      eprintln!("Warning: Cannot merge incompatible types {:?} and {:?}, using {:?}", dt1, dt2, dt1);
      dt1.clone()
    }
  }
}

/// Collect all parquet file paths recursively from a directory
fn collect_parquet_files(dir: &Path) -> Vec<std::path::PathBuf> {
  let mut files = Vec::new();
  if let Ok(entries) = std::fs::read_dir(dir) {
    for entry in entries.flatten() {
      let path = entry.path();
      if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
        files.push(path);
      } else if path.is_dir() {
        // Recursively collect from subdirectories (partitions)
        files.extend(collect_parquet_files(&path));
      }
    }
  }
  files
}

fn read_parquet_schema(file_path: &Path) -> Result<Arc<Schema>, Box<dyn Error>> {
  let file = fs::File::open(file_path)?;
  let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
  Ok(builder.schema().clone())
}

/// Merge multiple schemas, handling type coercion for compatible types
fn merge_schemas(schemas: Vec<Arc<Schema>>) -> Result<Arc<Schema>, Box<dyn Error>> {
  if schemas.is_empty() {
    return Err("No schemas to merge".into());
  }

  if schemas.len() == 1 {
    return Ok(schemas[0].clone());
  }

  // Start with the first schema
  let mut merged_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> = schemas[0].fields().to_vec();

  // Merge each subsequent schema
  for schema in schemas.iter().skip(1) {
    let mut updated_fields = Vec::new();
    let schema_fields = schema.fields();

    // For each field in the merged schema, try to find and merge with fields from the new schema
    for merged_field in &merged_fields {
      if let Some(new_field) = schema_fields.iter().find(|f| f.name() == merged_field.name()) {
        // Field exists in both schemas, merge the data types
        let merged_type = merge_data_types(merged_field.data_type(), new_field.data_type());
        let merged_field = merged_field.clone().as_ref().clone().with_data_type(merged_type);
        updated_fields.push(Arc::new(merged_field));
      } else {
        // Field only exists in merged schema, keep it
        updated_fields.push(merged_field.clone());
      }
    }

    // Add fields that only exist in the new schema
    for new_field in schema_fields {
      if !merged_fields.iter().any(|f| f.name() == new_field.name()) {
        updated_fields.push(new_field.clone());
      }
    }

    merged_fields = updated_fields;
  }

  Ok(Arc::new(Schema::new(merged_fields)))
}

/// Infer schema from parquet files with type coercion support
pub async fn infer_schema_with_coercion(table_dir: &str) -> Result<Arc<Schema>, Box<dyn Error>> {
  let dir_path = Path::new(table_dir);
  let mut parquet_files = collect_parquet_files(dir_path);
  parquet_files.sort();

  if parquet_files.is_empty() {
    return Err("No parquet files found".into());
  }

  // Read schemas from all parquet files
  let mut schemas = Vec::new();
  for file_path in &parquet_files {
    match read_parquet_schema(file_path) {
      Ok(schema) => schemas.push(schema),
      Err(e) => {
        eprintln!("Warning: Failed to read schema from {:?}: {}", file_path, e);
        // Continue with other files
      }
    }
  }

  if schemas.is_empty() {
    return Err("No valid schemas found in parquet files".into());
  }

  // Merge all schemas with type coercion
  merge_schemas(schemas)
}
