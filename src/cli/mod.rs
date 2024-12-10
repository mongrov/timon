mod utils;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::{Parser, Subcommand};
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;
use utils::json_to_arrow;

/// CLI Tool for Converting JSON to Parquet and Executing SQL Queries
#[derive(Parser)]
#[command(name = "Parquet CLI")]
#[command(author = "Ahmed Boutaraa")]
#[command(version = "1.0.0")]
#[command(about = "Convert JSON to Parquet and execute SQL queries", long_about = None)]
pub struct CLI {
  #[command(subcommand)]
  pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
  /// Convert JSON to Parquet
  Convert {
    /// Input JSON file path
    input: String,
    output: String,
  },
  /// Execute SQL Query
  Query {
    /// Parquet file path
    file: String,
    /// SQL query to execute
    query: String,
  },
}

pub fn convert_json_to_parquet(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
  // Read JSON file
  let file = File::open(input)?;
  let json_values: Vec<Value> = serde_json::from_reader(file)?;

  // Convert JSON to Arrow format
  let (arrays, schema) = json_to_arrow(&json_values)?;

  // Create a record batch
  let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

  // Write to Parquet
  let output_file = File::create(output)?;
  let props = WriterProperties::builder().build();
  let mut writer = ArrowWriter::try_new(output_file, batch.schema(), Some(props))?;
  writer.write(&batch)?;
  writer.close()?;

  Ok(())
}

pub async fn execute_query(file: &str, query: &str) -> Result<(), Box<dyn std::error::Error>> {
  let ctx = SessionContext::new();
  ctx.register_parquet("timon", file, ParquetReadOptions::default()).await?;
  let df = ctx.sql(query).await?;
  let results = df.collect().await?;
  let _ = print_batches(&results);
  Ok(())
}
