pub mod helpers;
pub mod cloud_sync;
use cloud_sync::DataFusionOutput;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use helpers::{record_batches_to_json, row_to_json, json_to_arrow, extract_year_month};
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use datafusion::error::Result as DataFusionResult;
use std::fs;
use std::collections::HashMap;
use regex::Regex;
use chrono::Utc;

#[allow(dead_code)]
pub async fn datafusion_querier(parquet_paths: Vec<&str>, file_name: &str, sql_query: &str, is_json_format: bool) -> DataFusionResult<DataFusionOutput> {
  let ctx = SessionContext::new();
  let mut table_names = Vec::new();
  for (i, parquet_path) in parquet_paths.iter().enumerate() {
    if Path::new(parquet_path).exists() {
      let table_name = format!("{}_{}", file_name, i);
      match ctx.register_parquet(&table_name, parquet_path, ParquetReadOptions::default()).await {
        Ok(_) => table_names.push(table_name),
        Err(e) => eprintln!("Failed to register {}: {:?}", parquet_path, e),
      }
    } else {
      eprintln!("File does not exist: {}", parquet_path);
    }
  }

  if table_names.is_empty() {
    return Err(datafusion::error::DataFusionError::Plan("No valid tables found to query.".to_string()));
  }

  // Combine all tables into a single SQL query using UNION ALL
  let combined_query = format!(
    "SELECT * FROM ({}) AS combined_table",
    table_names
      .iter()
      .map(|name| format!("SELECT * FROM {}", name))
      .collect::<Vec<_>>()
      .join(" UNION ALL ")
  );

  // Execute the combined query
  let combined_df = ctx.sql(&combined_query).await?;
  let combined_results = combined_df.collect().await?;
  // Create an in-memory table from the combined results
  let schema = combined_results[0].schema();
  let mem_table = MemTable::try_new(schema, vec![combined_results])?;
  ctx.register_table("combined_table", Arc::new(mem_table))?;
  // Adjust the user-provided SQL query to run on the combined table
  let adjusted_sql_query = sql_query.replace(file_name, "combined_table");
  // Execute the user-provided SQL query on the combined table
  let final_df = ctx.sql(&adjusted_sql_query).await?;
  let final_results = final_df.collect().await?;
  
  if is_json_format {
    let json_result = record_batches_to_json(&final_results).unwrap();
    Ok(DataFusionOutput::Json(json_result))
  } else {
    let final_schema = final_results[0].schema();
    let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
    let final_df = ctx.read_table(Arc::new(final_mem_table))?;
    Ok(DataFusionOutput::DataFrame(final_df))
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

#[allow(dead_code)]
pub async fn aggregate_monthly_parquet(dir_path: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
  let files = fs::read_dir(dir_path)?
    .filter_map(|entry| entry.ok())
    .filter(|entry| {
      entry.path().is_file() && entry.file_name().to_string_lossy().starts_with(format!("{}_", table_name).as_str())
    })
    .map(|entry| entry.path().to_string_lossy().to_string())
    .collect::<Vec<_>>();

  let mut files_by_month: HashMap<String, Vec<String>> = HashMap::new();
  let regx = Regex::new(r"(\d{4}-\d{2})-\d{2}\.parquet$")?; // capture YYYY-MM part of the filename

  for file in files {
    if let Some(filename) = Path::new(&file).file_name().and_then(|n| n.to_str()) {
      if let Some(caps) = regx.captures(filename) {
        if let Some(month) = caps.get(1) {
          files_by_month
            .entry(month.as_str().to_string())
            .or_insert_with(Vec::new)
            .push(file);
        }
      }
    }
  }

  for (month, files) in files_by_month {
    let merged_file_path = format!("{}/{}_{}.parquet", dir_path, table_name, month);
    
    if files.len() == 1 {
      // If there's only one file for the month, simply copy it to the merged file name
      fs::copy(&files[0], &merged_file_path)?;
    } else {
      // Merge multiple files into one Parquet file
      let ctx = SessionContext::new();
      let mut table_names = Vec::new();
      
      for (i, file_path) in files.iter().enumerate() {
        let table_name = format!("table_{}", i);
        
        match SerializedFileReader::try_from(File::open(file_path)?) {
          Ok(_) => {
            ctx.register_parquet(&table_name, file_path, ParquetReadOptions::default()).await?;
            table_names.push(table_name);
          }
          Err(e) => {
            eprintln!("Skipping file due to error: {} - Error: {:?}", file_path, e);
            continue;
          }
        };
      }

      if table_names.is_empty() {
        eprintln!("No valid tables found to merge for month: {}", month);
        continue;
      }
      
      let combined_query = format!(
        "SELECT * FROM ({}) AS combined_table",
        table_names
          .iter()
          .map(|name| format!("SELECT * FROM {}", name))
          .collect::<Vec<_>>()
          .join(" UNION ALL ")
      );
      
      let combined_df = ctx.sql(&combined_query).await?;
      let combined_results = combined_df.collect().await?;
      let schema = combined_results[0].schema();
      let mem_table = MemTable::try_new(schema.clone(), vec![combined_results])?;
      
      let combined_table = ctx.read_table(Arc::new(mem_table))?.collect().await?;
      let output_file = File::create(&merged_file_path)?;
      let props = WriterProperties::builder().build();
      let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;
      
      for batch in combined_table {
        writer.write(&batch)?;
      }
      writer.close()?;
    }

    // upload the merged monthly parquet files to S3 storage
    let target_path = format!("{}_{}.parquet", table_name, month);
    if let Err(e) = cloud_sync::CloudQuerier::sink_data_to_bucket(&merged_file_path, &target_path).await {
      eprintln!("Failed to upload to S3: {:?}", e);
    }

    // clean old files(less than a month)
    let current_year_month = Utc::now().format("%Y-%m").to_string();
    for file in files {
      if let Some(path_year_month) = extract_year_month(&file) {
        if path_year_month < current_year_month {
          // Remove the old file
          fs::remove_file(&file)?;
          // Remove the corresponding merged file for the current month if it exists
          let merged_file_path = format!("{}/{}_{}.parquet", dir_path, table_name, path_year_month);
          if Path::new(&merged_file_path).exists() {
            fs::remove_file(merged_file_path)?;
          }
        }
      }
    }
  }

  Ok(())
}
