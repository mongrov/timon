use super::db_manager::{DataFusionOutput, DatabaseManager};
use super::helpers::{extract_table_name, generate_s3_paths, record_batches_to_json};
use chrono::NaiveDate;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use object_store::ClientOptions;
use object_store::{
  aws::{AmazonS3, AmazonS3Builder},
  path::Path as StorePath,
  ObjectStore,
};
use regex::Regex;
use std::fs;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;
use url::Url;

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
    bucket_region: Option<&str>,
  ) -> Self {
    let bucket_endpoint = bucket_endpoint.unwrap_or("http://localhost:9000").to_owned();
    let bucket_name = bucket_name.unwrap_or("timon").to_owned();
    let access_key_id = access_key_id.unwrap_or("ahmed").to_owned();
    let secret_access_key = secret_access_key.unwrap_or("ahmed1234").to_owned();
    let bucket_region = bucket_region.unwrap_or("us-west-1").to_owned();

    let client_options = ClientOptions::new()
      .with_allow_http(true)
      .with_allow_http2()
      // .with_root_certificate(certificate)
      .with_allow_invalid_certificates(true);

    let s3_store = AmazonS3Builder::new()
      .with_endpoint(&bucket_endpoint)
      .with_bucket_name(&bucket_name)
      .with_access_key_id(&access_key_id)
      .with_secret_access_key(&secret_access_key)
      .with_region(&bucket_region)
      .with_allow_http(true)
      .with_client_options(client_options)
      .build()
      .unwrap();

    CloudStorageManager {
      s3_store: Arc::new(s3_store),
      db_manager,
      bucket_name,
    }
  }

  #[allow(dead_code)]
  pub async fn query_bucket(
    &self,
    username: &str,
    db_name: &str,
    sql_query: &str,
    date_range: HashMap<&str, &str>,
    is_json_format: bool,
  ) -> DataFusionResult<DataFusionOutput> {
    let bucket_interval = self.db_manager.bucket_interval;
    let session_context = SessionContext::new();
    let table_name = &extract_table_name(sql_query);

    // Parse the date_range and generate Parquet file paths
    let file_list = generate_s3_paths(&self.bucket_name, username, db_name, table_name, bucket_interval, date_range).unwrap();
    // Register the object store with the session context
    let store_url = Url::parse(&format!("s3://{}", &self.bucket_name)).unwrap();
    session_context.runtime_env().register_object_store(&store_url, self.s3_store.clone());

    // Create a list of table names and register Parquet files
    let mut table_names = Vec::new();
    for (i, file_url) in file_list.iter().enumerate() {
      let table_name = format!("{}_{}", table_name, i);
      let file_url_parsed = match ListingTableUrl::parse(file_url) {
        Ok(url) => url,
        Err(e) => {
          eprintln!("Warning: Failed to parse file URL {}: {:?}", file_url, e);
          continue;
        }
      };

      let config = match ListingTableConfig::new(file_url_parsed).infer(&session_context.state()).await {
        Ok(cfg) => cfg,
        Err(_) => {
          eprintln!("Warning: Failed to infer schema for {}", file_url);
          continue;
        }
      };

      match ListingTable::try_new(config) {
        Ok(table) => match session_context.register_table(&table_name, Arc::new(table)) {
          Ok(_) => {
            table_names.push(table_name);
          }
          Err(e) => {
            eprintln!("Warning: Failed to register table {}: {:?}", table_name, e);
          }
        },
        Err(e) => {
          eprintln!("Warning: Failed to create ListingTable for {}: {:?}", file_url, e);
        }
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
    let combined_df = session_context.sql(&combined_query).await?;
    let combined_results = combined_df.collect().await?;
    // Create an in-memory table from the combined results
    let schema = combined_results[0].schema();
    let mem_table = MemTable::try_new(schema, vec![combined_results])?;
    session_context.register_table("combined_table", Arc::new(mem_table))?;
    // Adjust the user-provided SQL query to run on the combined table
    let adjusted_sql_query = sql_query.replace(table_name, "combined_table");
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
  pub async fn cloud_sync_parquet(&self, username: &str, db_name: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let files = self.db_manager.build_files_list(db_name, table_name)?;
    if files.is_empty() {
      return Err(format!("No data files found for Table '{}' in Database '{}'.", table_name, db_name).into());
    }

    // Regex to match YYYY-MM-dd filenames
    let regx = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();

    for file in files {
      if let Some(filename) = Path::new(&file).file_name().and_then(|n| n.to_str()) {
        let caps = regx
          .captures(filename)
          .ok_or_else(|| format!("Filename '{}' does not match the expected pattern", filename))?;
        let year = caps.get(1).map_or("", |m| m.as_str());
        let month = caps.get(2).map_or("", |m| m.as_str());
        let day = caps.get(3).map_or("", |m| m.as_str());
        let file_date_extension = caps.get(0).map_or("", |m| m.as_str());

        let source_path = file.clone();
        let target_path = format!("{}/{}/{}/{}/{}/{}/{}", username, db_name, table_name, year, month, day, filename);

        self
          .upload_to_bucket(&source_path, &target_path)
          .await
          .map_err(|e| format!("Failed to upload file {} to S3 path {}: {:?}", source_path, target_path, e))?;

        // Remove the local file after successful upload
        let current_date = chrono::Utc::now().naive_utc().date();
        let date_part = file_date_extension.split('.').next().unwrap_or("");
        match NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
          Ok(file_date) => {
            if file_date < current_date {
              fs::remove_file(&source_path).map_err(|e| format!("Failed to delete local file {}: {:?}", source_path, e))?;
            }
          }
          Err(e) => {
            eprintln!("Warning: Failed to parse file date '{}': {:?}", file_date_extension, e);
          }
        }
      }
    }

    Ok(())
  }
}
