use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use base64::{engine::general_purpose, Engine as _};
use chrono::{Datelike, NaiveDate, ParseError};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::data_type::{AsBytes, ByteArray, Decimal};
use parquet::record::{Field as ParquetField, Row};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Value, serde_json::Error> {
  let mut rows = Vec::new();

  for batch in batches {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    for row_index in 0..num_rows {
      let mut row = HashMap::new();

      for (col_index, field) in schema.fields().iter().enumerate() {
        let column = batch.column(col_index);
        let value = array_value_to_json(column, row_index);
        row.insert(field.name().clone(), value);
      }

      rows.push(row);
    }
  }

  serde_json::to_value(&rows)
}

fn array_value_to_json(array: &ArrayRef, row_index: usize) -> serde_json::Value {
  use datafusion::arrow::array::*;
  use datafusion::arrow::datatypes::DataType;

  match array.data_type() {
    DataType::Int32 => {
      let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
      json!(array.value(row_index))
    }
    DataType::Int64 => {
      let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
      json!(array.value(row_index))
    }
    DataType::Float32 => {
      let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
      json!(array.value(row_index))
    }
    DataType::Float64 => {
      let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
      json!(array.value(row_index))
    }
    DataType::Utf8 => {
      let array = array.as_any().downcast_ref::<StringArray>().unwrap();
      json!(array.value(row_index))
    }
    DataType::Timestamp(TimeUnit::Millisecond, None) => {
      let ts_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
      let value = ts_array.value(row_index);
      Value::Number(value.into())
    }
    _ => json!(null),
  }
}

pub fn row_to_json(row: &Row) -> serde_json::Value {
  let mut json_map = serde_json::Map::new();

  for (name, value) in row.get_column_iter() {
    let json_value = match value {
      ParquetField::Null => serde_json::Value::Null,
      ParquetField::Bool(b) => json!(*b),
      ParquetField::Byte(b) => json!(*b),
      ParquetField::Short(s) => json!(*s),
      ParquetField::Int(i) => json!(*i),
      ParquetField::Long(l) => json!(*l),
      ParquetField::UByte(b) => json!(*b),
      ParquetField::UShort(s) => json!(*s),
      ParquetField::UInt(i) => json!(*i),
      ParquetField::ULong(l) => json!(*l),
      ParquetField::Float(f) => json!(*f),
      ParquetField::Double(d) => json!(*d),
      ParquetField::Str(s) => json!(s),
      ParquetField::Bytes(b) => json!(byte_array_to_base64(b)),
      ParquetField::TimestampMicros(t) => json!(t),
      ParquetField::TimestampMillis(t) => json!(t),
      ParquetField::Decimal(d) => json!(decimal_to_string(d)),
      ParquetField::Group(g) => row_to_json(g),
      _ => serde_json::Value::Null,
    };

    json_map.insert(name.clone(), json_value);
  }

  serde_json::Value::Object(json_map)
}

fn byte_array_to_base64(byte_array: &ByteArray) -> String {
  general_purpose::STANDARD.encode(&byte_array)
}

fn decimal_to_string(decimal: &Decimal) -> String {
  let value = decimal.as_bytes();
  // Assuming the decimal's precision and scale are known
  // You'll need to adapt this to your specific use case
  let precision = decimal.precision();
  let scale = decimal.scale();
  let mut result = String::new();

  // Convert the bytes to a string representation
  if value[0] & 0x80 != 0 {
    result.push('-');
  }

  let int_part = &value[..precision as usize - scale as usize];
  let frac_part = &value[precision as usize - scale as usize..];

  result.push_str(&format!("{:?}", int_part));
  if scale > 0 {
    result.push('.');
    result.push_str(&format!("{:?}", frac_part));
  }

  result
}

pub fn json_to_arrow(json_values: &[Value]) -> Result<(Vec<ArrayRef>, Schema), Box<dyn std::error::Error>> {
  // Extract schema from the first JSON object
  let first_obj = json_values.first().ok_or("No data to write")?.as_object().ok_or("Expected JSON object")?;

  let mut fields = Vec::new();
  let mut arrays: Vec<ArrayRef> = Vec::new();

  for (key, value) in first_obj {
    let field = match value {
      Value::Number(_) => ArrowField::new(key, DataType::Int64, false),
      Value::String(_) => ArrowField::new(key, DataType::Utf8, false),
      _ => return Err(format!("Unsupported JSON value type for key {}", key).into()),
    };
    fields.push(field);
  }

  let schema = Schema::new(fields);

  for field in schema.fields() {
    let array = match field.data_type() {
      DataType::Int64 => {
        let values: Vec<i64> = json_values
          .iter()
          .map(|v| v.get(&field.name()).and_then(Value::as_i64).unwrap_or_default())
          .collect();
        Arc::new(Int64Array::from(values)) as ArrayRef
      }
      DataType::Utf8 => {
        let values: Vec<String> = json_values
          .iter()
          .map(|v| v.get(&field.name()).and_then(Value::as_str).unwrap_or_default().to_string())
          .collect();
        Arc::new(StringArray::from(values)) as ArrayRef
      }
      _ => return Err(format!("Unsupported data type for field {}", field.name()).into()),
    };
    arrays.push(array);
  }

  Ok((arrays, schema))
}

#[allow(dead_code)]
pub enum Granularity {
  Month,
  Day,
}

pub fn generate_paths(
  base_dir: &str,
  file_name: &str,
  date_range: HashMap<&str, &str>,
  granularity: Granularity,
  is_s3: bool,
) -> Result<Vec<String>, ParseError> {
  let start_date = NaiveDate::parse_from_str(date_range.get("start_date").unwrap(), "%Y-%m-%d")?;
  let end_date = NaiveDate::parse_from_str(date_range.get("end_date").unwrap(), "%Y-%m-%d")?;

  let mut file_list = Vec::new();
  let mut current_date = start_date;

  while current_date <= end_date {
    let file_path = match granularity {
      Granularity::Month => format!(
        "{}{}/{}_{}.parquet",
        if is_s3 { "s3://" } else { "" },
        base_dir,
        file_name,
        current_date.format("%Y-%m")
      ),
      Granularity::Day => format!("{}/{}_{}.parquet", base_dir, file_name, current_date.format("%Y-%m-%d")),
    };

    file_list.push(file_path);

    // Increment based on granularity
    current_date = match granularity {
      Granularity::Month => current_date
        .with_month(current_date.month() % 12 + 1)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(current_date.year() + 1, 1, 1).unwrap()),
      Granularity::Day => current_date.succ_opt().unwrap(),
    };
  }

  Ok(file_list)
}

pub fn extract_table_name(sql_query: &str) -> String {
  let regex = Regex::new(r##"(?:FROM|JOIN)\s+[`\"]?(\w+)[`\"]?"##).unwrap();
  let matches: Vec<String> = regex
    .captures_iter(sql_query)
    .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
    .collect();

  match matches.len() {
    1 => matches[0].clone(),
    n if n > 1 => {
      eprintln!("Multiple table names found in the SQL query.");
      String::new()
    }
    _ => {
      eprintln!("No table name found in the SQL query.");
      String::new()
    }
  }
}
