use arrow::datatypes::{TimeUnit, DataType, Schema, Field as ArrowField};
use serde_json::{json, Value};
use datafusion::arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use std::collections::HashMap;
use parquet::record::{Field as ParquetField, Row};
use parquet::data_type::{AsBytes, ByteArray, Decimal};
use base64::{engine::general_purpose, Engine as _};
use std::sync::Arc;
use regex::Regex;

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<String, serde_json::Error> {
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

    serde_json::to_string(&rows)
}

fn array_value_to_json(array: &ArrayRef, row_index: usize) -> serde_json::Value {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::array::*;
  
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
              let values: Vec<i64> = json_values.iter().map(|v| v.get(&field.name()).and_then(Value::as_i64).unwrap_or_default()).collect();
              Arc::new(Int64Array::from(values)) as ArrayRef
          }
          DataType::Utf8 => {
              let values: Vec<String> = json_values.iter().map(|v| v.get(&field.name()).and_then(Value::as_str).unwrap_or_default().to_string()).collect();
              Arc::new(StringArray::from(values)) as ArrayRef
          }
          _ => return Err(format!("Unsupported data type for field {}", field.name()).into()),
      };
      arrays.push(array);
  }

  Ok((arrays, schema))
}

pub fn extract_year_month(file_path: &str) -> Option<String> {
    // Regex to capture the YYYY-MM part of the filename
    let re = Regex::new(r"(\d{4}-\d{2})").unwrap();
    if let Some(caps) = re.captures(file_path) {
        return Some(caps[1].to_string());
    }
    None
}
