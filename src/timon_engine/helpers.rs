use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use base64::{engine::general_purpose, Engine as _};
use chrono::{Datelike, NaiveDate, ParseError};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::data_type::{AsBytes, Decimal};
use parquet::record::{Field as ParquetField, Row};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Value, serde_json::Error> {
  fn array_value_to_json(array: &ArrayRef, row_index: usize) -> serde_json::Value {
    match array.data_type() {
      DataType::Int32 => json!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_index)),
      DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_index)),
      DataType::Float32 => json!(array.as_any().downcast_ref::<Float32Array>().unwrap().value(row_index)),
      DataType::Float64 => json!(array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_index)),
      DataType::Utf8 => json!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row_index)),
      DataType::Timestamp(TimeUnit::Millisecond, None) => {
        json!(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row_index))
      }
      _ => json!(null),
    }
  }

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
      ParquetField::Null => serde_json::Value::Null,
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
      ParquetField::Group(g) => row_to_json(g),
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
  // Extract schema from the first JSON object
  let first_obj = json_values
    .first()
    .ok_or("No data to write".to_string())? // Convert the error to String
    .as_object()
    .ok_or("Expected JSON object".to_string())?; // Convert the error to String

  // Define fields and handle errors separately
  let mut fields = Vec::new();

  for (key, value) in first_obj {
    let field = match value {
      Value::Number(num) if num.is_f64() => ArrowField::new(key, DataType::Float64, false),
      Value::Number(_) => ArrowField::new(key, DataType::Int64, false),
      Value::String(_) => ArrowField::new(key, DataType::Utf8, false),
      _ => return Err(format!("Unsupported JSON value type for key {}", key).into()), // Explicit conversion
    };
    fields.push(field);
  }

  let schema = Schema::new(fields);

  // Convert JSON values into corresponding Arrow arrays
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
        _ => return Err(format!("Unsupported data type for field {}", field.name()).into()), // Explicit error type
      })
    })
    .collect::<Result<_, Box<dyn std::error::Error>>>()?; // Specify error type explicitly

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
  let mut current_date = start_date;

  let mut file_list = Vec::new();
  while current_date <= end_date {
    let path = match granularity {
      Granularity::Month => format!(
        "{}{}/{}_{}.parquet",
        if is_s3 { "s3://" } else { "" },
        base_dir,
        file_name,
        current_date.format("%Y-%m")
      ),
      Granularity::Day => format!("{}/{}_{}.parquet", base_dir, file_name, current_date.format("%Y-%m-%d")),
    };
    file_list.push(path);
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
  Regex::new(r##"(?:FROM|JOIN)\s+[`\"]?(\w+)[`\"]?"##)
    .unwrap()
    .captures_iter(sql_query)
    .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
    .nth(0)
    .unwrap_or_else(|| {
      eprintln!("No table name found in the SQL query.");
      String::new()
    })
}
