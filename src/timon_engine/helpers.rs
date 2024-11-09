use arrow::array::{
  Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder, ListArray, ListBuilder, StringArray,
  StringBuilder, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use base64::{engine::general_purpose, Engine as _};
use chrono::{Datelike, NaiveDate, ParseError};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::data_type::{AsBytes, Decimal};
use parquet::record::{Field as ParquetField, Row};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Value, serde_json::Error> {
  fn array_value_to_json(array: &ArrayRef, row_index: usize) -> serde_json::Value {
    match array.data_type() {
      DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_index)),
      DataType::Float64 => json!(array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_index)),
      DataType::Utf8 => json!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row_index)),
      DataType::Boolean => json!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_index)),
      DataType::Timestamp(TimeUnit::Millisecond, None) => json!(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row_index)),
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
      _ => json!(null),
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
  // Extract schema from the first JSON object
  let first_obj = json_values
    .first()
    .ok_or("No data to write".to_string())?
    .as_object()
    .ok_or("Expected JSON object".to_string())?;

  // Define fields and handle errors separately
  let mut fields = Vec::new();

  for (key, value) in first_obj {
    let field = match value {
      Value::Number(num) if num.is_f64() => ArrowField::new(key, DataType::Float64, false),
      Value::Number(_) => ArrowField::new(key, DataType::Int64, false),
      Value::String(_) => ArrowField::new(key, DataType::Utf8, false),
      Value::Bool(_) => ArrowField::new(key, DataType::Boolean, false),
      Value::Array(values) => {
        let first_value = values.first().ok_or(format!("Array for key '{}' is empty", key))?;
        let element_data_type = match first_value {
          Value::Number(num) if num.is_f64() => DataType::Float64,
          Value::Number(_) => DataType::Int64,
          Value::String(_) => DataType::Utf8,
          Value::Bool(_) => DataType::Boolean,
          _ => DataType::Null,
        };

        ArrowField::new(
          key,
          DataType::List(Box::new(ArrowField::new("item", element_data_type, true)).into()),
          false,
        )
      }
      _ => return Err(format!("Unsupported JSON value type for key '{}' with value '{}'", key, value).into()),
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

pub fn get_unique_fields(schema: Value) -> Result<Vec<String>, Box<dyn Error>> {
  let mut unique_fields = Vec::new();

  if let Some(properties) = schema.as_object() {
    for (field_name, field_properties) in properties {
      if let Some(unique) = field_properties.get("unique") {
        if unique.as_bool() == Some(true) {
          unique_fields.push(field_name.clone());
        }
      }
    }
  }

  Ok(unique_fields)
}
