mod helpers;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use helpers::{record_batches_to_json, row_to_json, json_to_arrow};
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use datafusion::error::Result as DataFusionResult;
use datafusion::dataframe::DataFrame;
use std::fmt;

pub enum DataFusionOutput {
  Json(String),
  DataFrame(DataFrame),
}

impl fmt::Debug for DataFusionOutput {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DataFusionOutput::Json(s) => write!(f, "Json({})", s),
      DataFusionOutput::DataFrame(df) => {
        let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
          df.clone().collect().await.expect("Failed to collect DataFrame results")
        });
        for batch in result {
          writeln!(f, "{:?}", batch)?;
        }
        Ok(())
      }
    }
  }
}

#[allow(dead_code)]
pub async fn datafusion_querier(parquet_paths: Vec<&str>, table_prefix: &str, sql_query: &str, is_json_format: bool) -> DataFusionResult<DataFusionOutput> {
  let ctx = SessionContext::new();
  let mut table_names = Vec::new();
  for (i, parquet_path) in parquet_paths.iter().enumerate() {
    if Path::new(parquet_path).exists() {
      let table_name = format!("{}_{}", table_prefix, i);
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
  let adjusted_sql_query = sql_query.replace(table_prefix, "combined_table");
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
