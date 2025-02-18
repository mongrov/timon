use super::db_manager::{DataFusionOutput, DatabaseManager};
use super::helpers::{
  cleanup_old_files, combine_unique_batches, extract_table_name, filter_files_by_date_range, get_property_fields, get_table_columns,
  read_parquet_batches, record_batches_to_json,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as StorePath;
use object_store::{ClientOptions, ObjectMeta, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;
use url::Url;

pub struct CloudStorageManager {
  s3_store: Arc<AmazonS3>,
  db_manager: DatabaseManager,
  pub bucket_name: String,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
  files: Vec<String>,
}

impl CloudStorageManager {
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

  pub async fn query_bucket(
    &self,
    username: &str,
    db_name: &str,
    sql_query: &str,
    date_range: HashMap<&str, &str>,
    is_json_format: bool,
  ) -> DataFusionResult<DataFusionOutput> {
    let session_context = SessionContext::new();
    let table_name = &extract_table_name(sql_query);

    // Parse the date_range and generate Parquet file paths
    let file_list = match self.generate_s3_paths(username, db_name, table_name, date_range).await {
      Ok(files) => files.iter().map(|file| format!("s3://{}/{}", self.bucket_name, file)).collect::<Vec<_>>(),
      Err(e) => {
        eprintln!("Error generating S3 paths: {:?}", e);
        return Err(datafusion::error::DataFusionError::Execution(format!(
          "Failed to generate S3 paths: {:?}",
          e
        )));
      }
    };

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

    let column_names = get_table_columns(&session_context, &table_names[0]).await?;
    // Combine tables using UNION ALL with explicit column selection
    let combined_query = format!(
      "SELECT {} FROM ({}) AS combined_table",
      column_names,
      table_names
        .iter()
        .map(|name| format!("SELECT {} FROM {}", column_names, name))
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

  pub async fn cloud_sink_parquet(&self, username: &str, db_name: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let files = self.db_manager.build_files_list(db_name, table_name, None)?;
    if files.is_empty() {
      return Err(format!("No data files found for Table '{}' in Database '{}'.", table_name, db_name).into());
    }

    let regx = Regex::new(r"(\d{4})-(\d{2})-(\d{2})")?;
    let table_schema = self.db_manager.get_table_schema(db_name, table_name)?;
    let unique_fields = get_property_fields(&table_schema, "unique")?;
    let mut batches = Vec::new();
    let mut processed_files = Vec::new();
    let mut merge_target_paths = Vec::new();

    for file in &files {
      if let Some(target_path) = self
        .process_file(
          file,
          username,
          db_name,
          table_name,
          &regx,
          &unique_fields,
          &mut batches,
          &mut processed_files,
        )
        .await?
      {
        merge_target_paths.push(target_path);
      }
    }

    if !batches.is_empty() {
      self.upload_merged_batches(&batches, &merge_target_paths, username).await?;
    }

    cleanup_old_files(&processed_files, &regx).await;

    Ok(())
  }

  async fn process_file(
    &self,
    file: &str,
    username: &str,
    db_name: &str,
    table_name: &str,
    regx: &Regex,
    unique_fields: &[String],
    batches: &mut Vec<RecordBatch>,
    processed_files: &mut Vec<PathBuf>,
  ) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let file_path = PathBuf::from(file);
    let filename = file_path.file_name().and_then(|n| n.to_str());

    if let Some(name) = filename {
      if let Some(caps) = regx.captures(name) {
        let (year, month, day) = (&caps[1], &caps[2], &caps[3]);
        let target_path = format!("{}/{}/{}/{}/{}/{}/{}", username, db_name, table_name, year, month, day, name);

        let s3_temp_path = format!("{}/merge_workspace/{}/{}", self.db_manager.storage_path, username, name);
        let mut s3_batches = Vec::new();

        let s3_available = self
          .download_from_bucket(&target_path, &s3_temp_path)
          .await
          .map(|_| read_parquet_batches(Path::new(&s3_temp_path), &mut s3_batches).is_ok())
          .unwrap_or(false);

        let mut local_batches = Vec::new();
        read_parquet_batches(&file_path, &mut local_batches)?;

        if s3_available {
          let merged_batches = combine_unique_batches(local_batches, s3_batches, unique_fields)?;
          if !merged_batches.is_empty() {
            batches.extend(merged_batches);
            processed_files.push(file_path);
            processed_files.push(PathBuf::from(&s3_temp_path));
            return Ok(Some(target_path));
          }
        } else {
          processed_files.push(PathBuf::from(&file_path));
          self.upload_to_bucket(&file_path.to_string_lossy(), &target_path).await?;
          println!("Successfully uploaded new: '{}'", file_path.to_string_lossy());
        }
      }
    }
    Ok(None)
  }

  async fn upload_merged_batches(
    &self,
    batches: &[RecordBatch],
    merge_target_paths: &[String],
    username: &str,
  ) -> Result<(), Box<dyn std::error::Error>> {
    for (index, batch) in batches.iter().enumerate() {
      let merge_target_path = &merge_target_paths[index];
      let file_path = PathBuf::from(merge_target_path);
      let filename = file_path.file_name().and_then(|n| n.to_str()).unwrap();
      let merged_file_path = format!("{}/merge_workspace/{}/merged_{}", self.db_manager.storage_path, username, filename);

      let merge_file = File::create(&merged_file_path)?;
      let mut writer = ArrowWriter::try_new(merge_file, batch.schema(), None)?;
      writer.write(batch)?;
      writer.close()?;

      self.upload_to_bucket(&merged_file_path, merge_target_path).await?;
      fs::remove_file(&merged_file_path)?;
      println!("Successfully uploaded merged: '{}'", merged_file_path);
    }
    Ok(())
  }

  pub async fn cloud_fetch_parquet(
    &self,
    username: &str,
    db_name: &str,
    table_name: &str,
    date_range: HashMap<&str, &str>,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let prefix_path = format!("{}/{}/{}", username, db_name, table_name);
    let cloud_files = &self.list_cloud_files(&prefix_path).await?;
    let start_date = date_range.get("start_date").ok_or("Missing start_date")?;
    let end_date = date_range.get("end_date").ok_or("Missing end_date")?;
    let filtered_cloud_files = filter_files_by_date_range(cloud_files.to_vec(), start_date, end_date)?;

    for cloud_file in filtered_cloud_files {
      if let Some(filename) = Path::new(&cloud_file).file_name().and_then(|n| n.to_str()) {
        let local_path = format!(
          "{}/group/{}/{}/{}/{}",
          &self.db_manager.storage_path, username, db_name, table_name, filename
        );
        self.download_from_bucket(&cloud_file, &local_path).await?;
      }
    }
    Ok(())
  }

  async fn generate_s3_paths(
    &self,
    username: &str,
    db_name: &str,
    table_name: &str,
    date_range: HashMap<&str, &str>,
  ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Construct the prefix (path) to list files from
    let prefix_path = format!("{}/{}/{}", username, db_name, table_name);

    // Extract file paths from the collected ObjectMeta
    let files: Vec<String> = self.list_cloud_files(&prefix_path).await?;
    // object_metas.into_iter().map(|object_meta| object_meta.location.to_string()).collect(); // Filter files by date range
    let start_date = date_range.get("start_date").ok_or("Missing start_date")?;
    let end_date = date_range.get("end_date").ok_or("Missing end_date")?;
    let filtered_files = filter_files_by_date_range(files, start_date, end_date)?;

    Ok(filtered_files)
  }

  async fn list_cloud_files(&self, prefix_path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // List all objects under the prefix
    let objects = self.s3_store.list(Some(&StorePath::from(prefix_path)));
    // Collect the stream of ObjectMeta into a Vec<ObjectMeta>
    let object_metas: Vec<ObjectMeta> = objects
      .map(|result| result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
      .try_collect()
      .await?;
    // Extract file paths from the collected ObjectMeta
    let files: Vec<String> = object_metas.into_iter().map(|object_meta| object_meta.location.to_string()).collect();
    Ok(files)
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

  async fn download_from_bucket(&self, target_path: &str, local_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let object_store = &self.s3_store;
    let path = StorePath::from(target_path);

    // Ensure the parent directory exists
    if let Some(parent) = std::path::Path::new(local_path).parent() {
      fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory '{}': {}", parent.display(), e))?;
    }

    // Stream the bytes from object storage
    let mut stream = match object_store.get(&path).await {
      Ok(s) => s.into_stream(),
      Err(e) => {
        if e.to_string().contains("NotFound") {
          eprintln!("Warning: File '{}' not found in S3, skipping fetch.", target_path);
          return Ok(()); // Skip processing
        }
        return Err(format!("Failed to stream object '{}': {}", target_path, e).into());
      }
    };

    // Create a local file to write the Parquet data
    let file = fs::File::create(local_path).map_err(|e| format!("Failed to create local file '{}': {}", local_path, e))?;
    let mut writer = BufWriter::new(file);
    let mut total_bytes_written = 0;

    // Process the stream and write chunks directly to the file
    while let Some(chunk) = stream.next().await {
      let bytes = chunk.map_err(|e| format!("Error reading stream for '{}': {}", target_path, e))?;
      writer
        .write_all(&bytes)
        .map_err(|e| format!("Failed to write to file '{}': {}", local_path, e))?;
      total_bytes_written += bytes.len();
    }

    writer.flush().map_err(|e| format!("Failed to flush file '{}': {}", local_path, e))?;

    println!("Successfully downloaded '{}' from S3 ({} bytes)", target_path, total_bytes_written);

    Ok(())
  }
}
