mod helpers;
use datafusion::prelude::*;
use helpers::{record_batches_to_json, row_to_json, json_to_arrow};
use std::fs::File;
use std::path::Path;
use std::ffi::{CStr, CString};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[allow(dead_code)]
pub async fn datafusion_querier(parquet_path: &str, table_name: &str, sql_query: &str) -> datafusion::error::Result<String> {
    let ctx = SessionContext::new();    
    ctx.register_parquet(table_name, parquet_path, ParquetReadOptions::default()).await?;

    let df = ctx.sql(sql_query).await?;
    let results = df.collect().await?;
    let json_result = record_batches_to_json(&results).unwrap();

    Ok(json_result)
}

#[no_mangle]
pub extern "C" fn readParquetFile(file_path: *const libc::c_char) -> *mut libc::c_char { // TODO: Fix this and remove it
  let c_str = unsafe { CStr::from_ptr(file_path) };
  let file_path = c_str.to_str().unwrap();
  let result = read_parquet_file(file_path);
  
  match result {
    Ok(json_records) => {
      let json_str = serde_json::to_string(&json_records).unwrap();
      let c_string = CString::new(json_str).unwrap();
      c_string.into_raw()
    },
    Err(_) => CString::new("Error reading Parquet file").unwrap().into_raw(),
  }
}

pub fn read_parquet_file(file_path: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
  let file = File::open(&Path::new(file_path))?;
  let reader = SerializedFileReader::new(file)?;
  let mut iter = reader.get_row_iter(None)?;
  
  let mut json_records = Vec::new();

  while let Some(record_result) = iter.next() {
    match record_result {
      Ok(record) => {
        // Convert the record to a JSON-like format
        let json_record = row_to_json(&record);
        json_records.push(json_record);
      },
      Err(_) => {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error reading record")));
      }
    }
  }
  Ok(json_records)
}

#[allow(dead_code)]
pub fn write_json_to_parquet(file_path: &str, json_data: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Parse the JSON data
    let json_values: Vec<Value> = serde_json::from_str(json_data)?;

    // Convert JSON data to Arrow arrays
    let (new_arrays, new_schema) = json_to_arrow(&json_values)?;

    let path = Path::new(file_path);
    if path.exists() {
        let existing_json_values = read_parquet_file(file_path)?;
        let mut combined_json_values = existing_json_values;
        combined_json_values.extend(json_values);
        // Convert combined data to Arrow arrays
        let (combined_arrays, combined_schema) = json_to_arrow(&combined_json_values)?;

        // Create a Parquet writer
        let file = File::create(&path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(combined_schema.clone()), Some(props))?;

        // Write the combined record batch to the Parquet file
        let combined_batch = RecordBatch::try_new(Arc::new(combined_schema), combined_arrays)?;
        writer.write(&combined_batch)?;

        // Close the writer to ensure data is written to the file
        writer.close()?;
    } else {
        // Create a new Parquet file with the new data
        let file = File::create(&path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(new_schema.clone()), Some(props))?;

        // Write the record batch to the Parquet file
        let record_batch = RecordBatch::try_new(Arc::new(new_schema), new_arrays)?;
        writer.write(&record_batch)?;

        // Close the writer to ensure data is written to the file
        writer.close()?;
    }

    Ok(())
}
