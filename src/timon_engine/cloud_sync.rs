use crate::timon_engine::helpers;
use chrono::Utc;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use helpers::extract_year_month;
use helpers::{generate_paths, record_batches_to_json, Granularity};
use object_store::{
  aws::{AmazonS3, AmazonS3Builder},
  path::Path as StorePath,
  ObjectStore,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::SerializedFileReader;
use regex::Regex;
use std::fmt;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;
use url::Url;

use super::db_manager::DatabaseManager;
use super::helpers::extract_table_name;

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
        let result = runtime.block_on(async { df.clone().collect().await.expect("Failed to collect DataFrame results") });
        for batch in result {
          writeln!(f, "{:?}", batch)?;
        }
        Ok(())
      }
    }
  }
}

pub struct CloudStorageManager {
  s3_store: Arc<AmazonS3>,
  db_manager: DatabaseManager,
  pub bucket_name: String,
}

impl CloudStorageManager {
  #[allow(dead_code)]
  pub fn new(
    db_manager: DatabaseManager,
    bucket_endpoint: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    bucket_name: Option<&str>,
  ) -> Self {
    let bucket_endpoint = bucket_endpoint.unwrap_or("http://localhost:9000").to_owned();
    let bucket_name = bucket_name.unwrap_or("timon").to_owned();
    let access_key_id = access_key_id.unwrap_or("ahmed").to_owned();
    let secret_access_key = secret_access_key.unwrap_or("ahmed1234").to_owned();

    let s3_store = AmazonS3Builder::new()
      .with_endpoint(&bucket_endpoint)
      .with_bucket_name(&bucket_name)
      .with_access_key_id(&access_key_id)
      .with_secret_access_key(&secret_access_key)
      .with_allow_http(true)
      .build()
      .unwrap();

    CloudStorageManager {
      s3_store: Arc::new(s3_store),
      db_manager,
      bucket_name,
    }
  }

  #[allow(dead_code)]
  pub async fn query_bucket(&self, date_range: HashMap<&str, &str>, sql_query: &str, is_json_format: bool) -> DataFusionResult<DataFusionOutput> {
    let session_context = SessionContext::new();
    let file_name = &extract_table_name(sql_query);

    // Parse the date_range and generate Parquet file paths
    let file_list = generate_paths(&self.bucket_name, file_name, date_range, Granularity::Month, true).unwrap();
    // Register the object store with the session context
    let store_url = Url::parse(&format!("s3://{}", &self.bucket_name)).unwrap();
    session_context.runtime_env().register_object_store(&store_url, self.s3_store.clone());

    // Create a list of table names and register Parquet files
    let mut table_names = Vec::new();
    for (i, file_url) in file_list.iter().enumerate() {
      let table_name = format!("{}_{}", file_name, i);
      let file_url_parsed = ListingTableUrl::parse(file_url)?;

      let mut config = ListingTableConfig::new(file_url_parsed);
      config = config.infer(&session_context.state()).await?;

      let table = ListingTable::try_new(config)?;
      session_context.register_table(&table_name, Arc::new(table))?;
      table_names.push(table_name);
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
    let combined_df = session_context.sql(&combined_query).await?;
    let combined_results = combined_df.collect().await?;
    // Create an in-memory table from the combined results
    let schema = combined_results[0].schema();
    let mem_table = MemTable::try_new(schema, vec![combined_results])?;
    session_context.register_table("combined_table", Arc::new(mem_table))?;
    // Adjust the user-provided SQL query to run on the combined table
    let adjusted_sql_query = sql_query.replace(file_name, "combined_table");
    // Execute the user-provided SQL query on the combined table
    let final_df = session_context.sql(&adjusted_sql_query).await?;
    let final_results = final_df.collect().await?;

    if is_json_format {
      let json_result = record_batches_to_json(&final_results).unwrap();
      Ok(DataFusionOutput::Json(json_result))
    } else {
      let final_schema = final_results[0].schema();
      let final_mem_table = MemTable::try_new(final_schema, vec![final_results])?;
      let final_df = session_context.read_table(Arc::new(final_mem_table))?;
      Ok(DataFusionOutput::DataFrame(final_df))
    }
  }

  async fn upload_to_bucket(&self, source_path: &str, target_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let s3_store = &self.s3_store;
    let object_store = Arc::new(s3_store);

    // Prepare the file for upload
    let mut file = tokio::fs::File::open(source_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
    object_store.put(&StorePath::from(target_path), data.into()).await?;

    Ok(())
  }

  #[allow(dead_code)]
  pub async fn sink_monthly_parquet(&self, db_name: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dir_path = &self.db_manager.get_table_path(db_name, table_name).unwrap();
    let files = fs::read_dir(dir_path)?
      .filter_map(|entry| entry.ok())
      .filter(|entry| entry.path().is_file() && entry.file_name().to_string_lossy().starts_with(format!("{}_", table_name).as_str()))
      .map(|entry| entry.path().to_string_lossy().to_string())
      .collect::<Vec<_>>();

    let mut files_by_month: HashMap<String, Vec<String>> = HashMap::new();
    let regx = Regex::new(r"(\d{4}-\d{2})-\d{2}\.parquet$")?; // capture YYYY-MM part of the filename

    for file in files {
      if let Some(filename) = Path::new(&file).file_name().and_then(|n| n.to_str()) {
        if let Some(caps) = regx.captures(filename) {
          if let Some(month) = caps.get(1) {
            files_by_month.entry(month.as_str().to_string()).or_insert_with(Vec::new).push(file);
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
      if let Err(e) = self.upload_to_bucket(&merged_file_path, &target_path).await {
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
}
