use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

pub fn write_json_to_parquet(file_path: &str, json_data: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the JSON data
    let json_values: Vec<Value> = serde_json::from_str(json_data)?;

    // Convert JSON data to Arrow arrays
    let (arrays, schema) = json_to_arrow(&json_values)?;

    // Create a Parquet writer
    let file = File::create(&Path::new(file_path))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;

    // Write the record batch to the Parquet file
    let record_batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
    writer.write(&record_batch)?;

    // Close the writer to ensure data is written to the file
    writer.close()?;
    Ok(())
}

fn json_to_arrow(json_values: &[Value]) -> Result<(Vec<ArrayRef>, Schema), Box<dyn std::error::Error>> {
    // Extract schema from the first JSON object
    let first_obj = json_values.first().ok_or("No data to write")?.as_object().ok_or("Expected JSON object")?;

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (key, value) in first_obj {
        let field = match value {
            Value::Number(_) => Field::new(key, DataType::Int64, false),
            Value::String(_) => Field::new(key, DataType::Utf8, false),
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
