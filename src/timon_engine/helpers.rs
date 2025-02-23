use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Days, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use datafusion::arrow::array::{
  new_null_array, Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Float64Array, Float64Builder, Int32Array, Int64Array, Int64Builder,
  ListArray, ListBuilder, StringArray, StringBuilder, StringViewArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Field as ArrowField, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use datafusion::parquet::data_type::{AsBytes, Decimal};
use datafusion::parquet::record::{Field as ParquetField, Row};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, metadata, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Value, serde_json::Error> {
  fn array_value_to_json(array: &ArrayRef, row_index: usize) -> serde_json::Value {
    match array.data_type() {
      DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_index)),
      DataType::Int32 => json!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_index)),
      DataType::Float64 => json!(array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_index)),
      DataType::Utf8 => json!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row_index)),
      DataType::Utf8View => {
        // Downcast the array to StringViewArray
        let string_view_array = array
          .as_any()
          .downcast_ref::<StringViewArray>()
          .expect("Failed to downcast to StringViewArray");
        // Extract the string values
        let values: Vec<String> = (0..string_view_array.len())
          .map(|i| {
            if string_view_array.is_null(i) {
              "null".to_string()
            } else {
              string_view_array.value(i).to_string()
            }
          })
          .collect();
        json!(values.get(row_index))
      }
      DataType::Boolean => json!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_index)),
      DataType::Timestamp(TimeUnit::Millisecond, None) => json!(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row_index)),
      DataType::Timestamp(TimeUnit::Nanosecond, None) => {
        let timestamp_ns = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row_index);
        // Convert nanoseconds to seconds and nanoseconds part
        let naive_datetime = DateTime::from_timestamp(
          timestamp_ns / 1_000_000_000,          // Seconds
          (timestamp_ns % 1_000_000_000) as u32, // Nanoseconds
        )
        .unwrap();
        let local_time = naive_datetime.with_timezone(&Local);
        // Format as ISO 8601 datetime string
        json!(local_time.format("%Y-%m-%d %H:%M:%S").to_string())
      }
      DataType::Date32 => {
        let base_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        array
          .as_any()
          .downcast_ref::<Date32Array>()
          .map(|date_array| date_array.value(row_index))
          .and_then(|days_since_epoch| base_date.checked_add_days(Days::new(days_since_epoch as u64)))
          .map_or(json!(null), |naive_date| json!(naive_date))
      }
      DataType::List(_inner_field) => {
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let offsets = list_array.value_offsets();
        let start_idx = offsets[row_index] as usize;
        let end_idx = offsets[row_index + 1] as usize;
        let values_array = list_array.values();

        // Recursive function to handle nested lists
        fn extract_list_values(array: &dyn Array, start_idx: usize, end_idx: usize) -> Vec<serde_json::Value> {
          match array.data_type() {
            DataType::Utf8 => {
              let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
              (start_idx..end_idx).map(|i| json!(string_array.value(i))).collect()
            }
            DataType::Int64 => {
              let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
              (start_idx..end_idx).map(|i| json!(int_array.value(i))).collect()
            }
            DataType::Float64 => {
              let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
              (start_idx..end_idx).map(|i| json!(float_array.value(i))).collect()
            }
            DataType::Boolean => {
              let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
              (start_idx..end_idx).map(|i| json!(bool_array.value(i))).collect()
            }
            _ => Vec::new(),
          }
        }

        let values = extract_list_values(values_array.as_ref(), start_idx, end_idx);
        json!(values)
      }
      datatype => {
        println!("Warning: unsupported Datatype {}", datatype);
        json!(null)
      }
    }
  }

  // Convert each row of the record batches into a JSON object
  let rows: Vec<_> = batches
    .iter()
    .flat_map(|batch| {
      let schema = batch.schema();
      let num_rows = batch.num_rows();
      (0..num_rows).map(move |row_index| {
        schema.fields().iter().enumerate().fold(HashMap::new(), |mut row, (col_index, field)| {
          let column = batch.column(col_index);
          row.insert(field.name().clone(), array_value_to_json(column, row_index));
          row
        })
      })
    })
    .collect();

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

pub fn json_to_arrow(json_values: &[Value]) -> Result<(Vec<ArrayRef>, Schema), Box<dyn std::error::Error>> {
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

  // Define schema fields
  let fields: Vec<ArrowField> = field_types
    .into_iter()
    .map(|(key, data_type)| ArrowField::new(&key, data_type, false))
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

pub fn extract_table_name(sql_query: &str) -> String {
  Regex::new(r#"(?i)(?:FROM|JOIN)\s+[`\"]?(\w+)['\"]?\s*(?:,|\b)"#)
    .unwrap()
    .captures_iter(sql_query)
    .filter_map(|cap| {
      let table_name = cap.get(1)?.as_str();
      // Exclude function-like entries (e.g., to_local_time) by checking if followed by '('
      if sql_query.contains(&format!("{table_name}(")) {
        None
      } else {
        Some(table_name.to_string())
      }
    })
    .nth(0)
    .unwrap_or_else(|| {
      eprintln!("No table name found in the SQL query.");
      String::new()
    })
}

pub fn rounded_timestamp(timestamp: i64, interval: u32) -> String {
  let dt = Utc.timestamp_opt(timestamp, 0).single().expect("Invalid timestamp");
  let rounded_time = if interval > 60 {
    // For intervals greater than 60, calculate hour buckets
    let total_minutes = dt.hour() * 60 + dt.minute();
    let rounded_total_minutes = (total_minutes / interval) * interval;
    let rounded_hour = rounded_total_minutes / 60;
    let rounded_minute = rounded_total_minutes % 60;

    dt.with_hour(rounded_hour as u32)
      .unwrap()
      .with_minute(rounded_minute as u32)
      .unwrap()
      .with_second(0)
      .unwrap()
      .with_nanosecond(0)
      .unwrap()
  } else {
    // For intervals within 60 minutes, calculate minute buckets
    let rounded_minute = (dt.minute() / interval) * interval;
    dt.with_minute(rounded_minute)
      .unwrap()
      .with_second(0)
      .unwrap()
      .with_nanosecond(0)
      .unwrap()
  };

  // Output format: exclude minutes for hour-based intervals
  if interval > 60 && interval % 60 == 0 {
    rounded_time.format("%Y-%m-%d_%H").to_string()
  } else {
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

pub async fn get_table_columns(session_context: &SessionContext, table_name: &str) -> DataFusionResult<String> {
  let df = session_context.sql(&format!("SELECT * FROM {} LIMIT 1", table_name)).await?;
  let column_names: Vec<String> = df.schema().fields().iter().map(|field| format!("\"{}\"", field.name())).collect();
  Ok(column_names.join(", "))
}

pub fn filter_files_by_date_range(files: Vec<String>, start_date: &str, end_date: &str) -> Result<Vec<String>, Box<dyn Error>> {
  let start_date = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")?;
  let end_date = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")?;

  let filtered_files = files
    .into_iter()
    .filter(|file| {
      // Extract the date part from the file path
      if let Some(date_str) = file.split('/').last() {
        if let Some(date_part) = date_str.split('_').nth(1) {
          if let Ok(file_date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            return file_date >= start_date && file_date <= end_date;
          }
        }
      }
      false
    })
    .collect();

  Ok(filtered_files)
}

pub fn extract_query_time_range(sql_query: &str) -> Option<(i64, i64)> {
  let re =
    Regex::new(r#"WHERE\s+.*?\b(date|timestamp)\b\s+BETWEEN\s+['\"]?(\d+|[\d-]+\s+[\d:]+)['\"]?\s+AND\s+['\"]?(\d+|[\d-]+\s+[\d:]+)['\"]?"#).ok()?;
  if let Some(captures) = re.captures(sql_query) {
    let start_time_str = &captures[2];
    let end_time_str = &captures[3];
    let start_timestamp = parse_timestamp(start_time_str)?;
    let end_timestamp = parse_timestamp(end_time_str)?;
    Some((start_timestamp, end_timestamp))
  } else {
    None
  }
}

pub fn extract_partition_time(file_path: &str) -> i64 {
  let re = Regex::new(r"(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2})").unwrap(); // Ensure regex compiles
  if let Some(captures) = re.captures(file_path) {
    let date_part = &captures[1]; // e.g., "2025-02-10"
    let time_part = &captures[2]; // e.g., "01-30"
    let datetime_str = format!("{} {}", date_part, time_part.replace("-", ":"));
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M") {
      return Utc.from_utc_datetime(&naive_dt).timestamp();
    }
  }
  eprintln!("Failed to extract partition time from: {}", file_path);
  i64::MIN // Indicate failure
}

fn parse_timestamp(datetime_str: &str) -> Option<i64> {
  // Parses either epoch timestamps or datetime strings
  if let Ok(epoch) = datetime_str.parse::<i64>() {
    Some(epoch)
  } else {
    NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")
      .ok()
      .map(|dt| Utc.from_utc_datetime(&dt).timestamp())
  }
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
        .map(|&index| ScalarValue::try_from_array(unified_batch.column(index), row_index).unwrap())
        .collect();

      let row_values: Vec<ScalarValue> = (0..unified_batch.num_columns())
        .map(|col_index| ScalarValue::try_from_array(unified_batch.column(col_index), row_index).unwrap())
        .collect();

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
        match field.data_type() {
          DataType::List(inner_field) if *inner_field.data_type() == DataType::Int64 => {
            let int_column = existing_column
              .as_any()
              .downcast_ref::<Int64Array>()
              .ok_or("Failed to downcast column to Int64Array")?;

            // Convert Int64Array into a ListArray
            let values = Arc::new(int_column.clone()) as ArrayRef;
            let offsets: Vec<i32> = (0..=int_column.len() as i32).collect();
            let offset_buffer = OffsetBuffer::new(offsets.into());
            let list_array = ListArray::new(Arc::new(Field::new("item", DataType::Int64, true)), offset_buffer, values, None);
            Arc::new(list_array) as ArrayRef
          }
          _ => {
            eprintln!("Warning: Cannot auto-convert {} to {}", existing_column.data_type(), field.data_type());
            existing_column.clone()
          }
        }
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

pub async fn cleanup_old_files(processed_files: &[PathBuf], regx: &Regex) {
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
