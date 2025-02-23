use super::db_manager::DatabaseManager;
use super::helpers::{
  cleanup_old_files, combine_unique_batches, filter_files_by_date_range, get_local_file_modified_time, get_property_fields, read_parquet_batches,
};
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use object_store::{ClientOptions, ObjectMeta};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::fs::{self};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;

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

  pub async fn cloud_sink_parquet(&self, db_name: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let files = self.db_manager.build_files_list(db_name, table_name, None)?;
    if files.is_empty() {
      return Err(format!("No data files found for Table '{}' in Database '{}'.", table_name, db_name).into());
    }

    let username = &self.db_manager.username;
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
    let s3_store = &self.s3_store;
    let file_path = PathBuf::from(file);
    let filename = file_path.file_name().and_then(|n| n.to_str());

    if let Some(name) = filename {
      if let Some(caps) = regx.captures(name) {
        let (year, month, day) = (&caps[1], &caps[2], &caps[3]);
        let target_path = format!("{}/{}/{}/{}/{}/{}/{}", username, db_name, table_name, year, month, day, name);

        let s3_temp_path = format!("{}/merge_workspace/{}/{}", self.db_manager.storage_path, username, name);
        let mut s3_batches = Vec::new();

        let local_modified_datetime = get_local_file_modified_time(&file_path.to_string_lossy()).unwrap_or_default();

        // Use `head()` to check if file exists and get metadata
        let s3_modified_datetime = match s3_store.head(&StorePath::from(target_path.clone())).await {
          Ok(meta) => meta.last_modified,
          Err(_) => {
            println!("S3 file does not exist, uploading local file...");
            self.upload_to_bucket(&file_path.to_string_lossy(), &target_path).await?;
            println!("Successfully uploaded new: '{}'", file_path.to_string_lossy());
            return Ok(None);
          }
        };

        // Compare timestamps before downloading
        if local_modified_datetime > s3_modified_datetime {
          println!("Local file is newer than S3, downloading S3 version for merge...");
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
              processed_files.push(PathBuf::from(&s3_temp_path));
              return Ok(Some(target_path));
            }
          }
        } else {
          println!("Local file is older or identical to S3, '{}' skipping download", name);
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
    let cloud_files = self.list_cloud_files(&prefix_path).await?;
    let start_date = date_range.get("start_date").ok_or("Missing start_date")?;
    let end_date = date_range.get("end_date").ok_or("Missing end_date")?;
    let filtered_files: HashSet<_> = filter_files_by_date_range(cloud_files.iter().map(|(path, _)| path.clone()).collect(), start_date, end_date)?
      .into_iter()
      .collect();

    for (cloud_file, cloud_modified_time) in cloud_files {
      if !filtered_files.contains(&cloud_file) {
        continue;
      }

      if let Some(filename) = Path::new(&cloud_file).file_name().and_then(|n| n.to_str()) {
        let local_path = format!(
          "{}/group/{}/{}/{}/{}",
          self.db_manager.storage_path, username, db_name, table_name, filename
        );

        match get_local_file_modified_time(&local_path) {
          Some(local_modified_time) if local_modified_time >= cloud_modified_time => {
            println!("Skipping {} (Up to date)", filename);
            continue;
          }
          _ => {
            println!("Downloading {}", filename);
            self.download_from_bucket(&cloud_file, &local_path).await?;
          }
        }
      }
    }

    Ok(())
  }

  async fn list_cloud_files(&self, prefix_path: &str) -> Result<Vec<(String, DateTime<Utc>)>, Box<dyn std::error::Error>> {
    // List all objects under the prefix
    let objects = self.s3_store.list(Some(&StorePath::from(prefix_path)));
    // Collect the stream of ObjectMeta into a Vec<ObjectMeta>
    let object_metas: Vec<ObjectMeta> = objects
      .map(|result| result.map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
      .try_collect()
      .await?;

    // Extract file paths and last modified timestamps
    let files: Vec<(String, DateTime<Utc>)> = object_metas
      .into_iter()
      .map(|object_meta| {
        let path = object_meta.location.to_string();
        let modified = object_meta.last_modified;
        (path, modified)
      })
      .collect();

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
