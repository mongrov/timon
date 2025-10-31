use super::db_manager::DatabaseManager;
use super::helpers::{
  cleanup_old_files, combine_unique_batches, filter_files_by_date_range, get_local_file_modified_time, get_property_fields, read_parquet_batches,
};
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use futures::{stream, StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as StorePath;
use object_store::{Attributes, ClientOptions, GetResultPayload, ObjectMeta};
use object_store::{GetResult, ObjectStore};
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

pub trait DatabaseManagerInterface: Send + Sync {
  fn build_files_list(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<Vec<String>, Box<dyn std::error::Error>>;
  fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>>;
  fn get_username(&self) -> &str;
  fn get_storage_path(&self) -> &str;
}

impl DatabaseManagerInterface for DatabaseManager {
  fn build_files_list(&self, db_name: &str, table_name: &str, username: Option<&str>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    self.build_files_list(db_name, table_name, username)
  }

  fn get_table_schema(&self, db_name: &str, table_name: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    self.get_table_schema(db_name, table_name)
  }

  fn get_username(&self) -> &str {
    &self.username
  }

  fn get_storage_path(&self) -> &str {
    &self.storage_path
  }
}

pub trait S3StoreInterface: Send + Sync {
  fn store_list(&self, prefix: Option<&StorePath>) -> futures::stream::BoxStream<'_, Result<ObjectMeta, object_store::Error>>;
  fn store_head(&self, path: &StorePath) -> impl std::future::Future<Output = Result<ObjectMeta, Box<dyn std::error::Error>>> + Send;
  fn store_get(&self, path: &StorePath) -> impl std::future::Future<Output = Result<GetResult, Box<dyn std::error::Error>>> + Send;
  fn store_put(&self, path: &StorePath, data: bytes::Bytes) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;
}

impl S3StoreInterface for AmazonS3 {
  fn store_list(&self, prefix: Option<&StorePath>) -> futures::stream::BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
    self.list(prefix)
  }

  async fn store_head(&self, path: &StorePath) -> Result<ObjectMeta, Box<dyn std::error::Error>> {
    let result = self.head(path).await;
    match result {
      Ok(meta) => Ok(meta),
      Err(e) => Err(Box::new(e)),
    }
  }

  async fn store_get(&self, path: &StorePath) -> Result<GetResult, Box<dyn std::error::Error>> {
    let result = self.get(path).await;
    match result {
      Ok(get_result) => Ok(get_result),
      Err(e) => Err(Box::new(e)),
    }
  }

  async fn store_put(&self, path: &StorePath, data: bytes::Bytes) -> Result<(), Box<dyn std::error::Error>> {
    let result = self.put(path, data.into()).await;
    match result {
      Ok(_) => Ok(()),
      Err(e) => Err(Box::new(e)),
    }
  }
}

pub struct MockS3Store {
  pub cloud_files: HashMap<String, Vec<u8>>,
  pub modified_times: HashMap<String, DateTime<Utc>>,
}

impl S3StoreInterface for MockS3Store {
  fn store_list(&self, prefix: Option<&StorePath>) -> futures::stream::BoxStream<'_, Result<ObjectMeta, object_store::Error>> {
    let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
    let files = self
      .cloud_files
      .keys()
      .filter(|k| k.starts_with(&prefix_str))
      .map(|k| {
        let meta = ObjectMeta {
          location: StorePath::from(k.clone()),
          last_modified: self.modified_times.get(k).unwrap_or(&Utc::now()).clone(),
          size: self.cloud_files.get(k).unwrap().len(),
          e_tag: None,
          version: None,
        };
        Ok(meta)
      })
      .collect::<Vec<_>>();

    stream::iter(files).boxed()
  }

  async fn store_head(&self, path: &StorePath) -> Result<ObjectMeta, Box<dyn std::error::Error>> {
    let path_str = path.to_string();
    if self.cloud_files.contains_key(&path_str) {
      let last_modified = self.modified_times.get(&path_str).unwrap_or(&Utc::now()).clone();
      Ok(ObjectMeta {
        location: path.clone(),
        last_modified,
        size: self.cloud_files.get(&path_str).unwrap().len(),
        e_tag: None,
        version: None,
      })
    } else {
      Err("NotFound".into())
    }
  }

  async fn store_get(&self, path: &StorePath) -> Result<GetResult, Box<dyn std::error::Error>> {
    let path_str = path.to_string();
    if let Some(data) = self.cloud_files.get(&path_str) {
      let data_clone = data.clone();
      Ok(GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes::Bytes::from(data_clone)) }))),
        meta: ObjectMeta {
          location: path.clone(),
          last_modified: self.modified_times.get(&path_str).unwrap_or(&Utc::now()).clone(),
          size: data.len(),
          e_tag: None,
          version: None,
        },
        range: 0..data.len(),
        attributes: Attributes::default(),
      })
    } else {
      Err("NotFound".into())
    }
  }

  async fn store_put(&self, _path: &StorePath, _data: bytes::Bytes) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
  }
}

pub struct CloudStorageManager<S: S3StoreInterface> {
  pub(crate) s3_store: Arc<S>,
  db_manager: Arc<dyn DatabaseManagerInterface>,
  pub username: String,
  pub bucket_name: String,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
  files: Vec<String>,
}

impl<S: S3StoreInterface> CloudStorageManager<S> {
  pub fn new(
    db_manager: impl DatabaseManagerInterface + 'static,
    bucket_endpoint: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
    bucket_name: Option<&str>,
    bucket_region: Option<&str>,
  ) -> CloudStorageManager<AmazonS3> {
    let username = db_manager.get_username().to_string();
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
      db_manager: Arc::new(db_manager),
      username,
      bucket_name,
    }
  }

  #[allow(dead_code)]
  pub fn new_with_mock(
    db_manager: impl DatabaseManagerInterface + 'static,
    mock_store: MockS3Store,
    bucket_name: Option<&str>,
  ) -> CloudStorageManager<MockS3Store> {
    CloudStorageManager {
      s3_store: Arc::new(mock_store),
      db_manager: Arc::new(db_manager),
      username: "mock_user".to_string(),
      bucket_name: bucket_name.unwrap_or("timon").to_owned(),
    }
  }

  pub async fn cloud_sync_parquet(
    &self,
    db_name: &str,
    table_name: &str,
    date_range: &HashMap<&str, &str>,
    username: Option<&str>,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let default_username = &self.db_manager.get_username();

    self.cloud_sink_parquet(db_name, table_name).await?;
    self.cloud_fetch_parquet(default_username, db_name, table_name, date_range).await?;

    if let Some(group_username) = username.filter(|u| *u != self.db_manager.get_username()) {
      self.cloud_fetch_parquet(group_username, db_name, table_name, date_range).await?;
    }

    Ok(())
  }

  pub async fn cloud_sink_parquet(&self, db_name: &str, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let files = self.db_manager.build_files_list(db_name, table_name, None)?;
    if files.is_empty() {
      return Err(format!("No data files found for Table '{}' in Database '{}'.", table_name, db_name).into());
    }

    let username = &self.db_manager.get_username();
    let table_schema = self.db_manager.get_table_schema(db_name, table_name)?;
    let unique_fields = get_property_fields(&table_schema, "unique")?;
    let mut batches = Vec::new();
    let mut processed_files = Vec::new();
    let mut merge_target_paths = Vec::new();

    for file in &files {
      if let Some(target_path) = self
        .process_sink_parquet_file(file, username, db_name, table_name, &unique_fields, &mut batches, &mut processed_files)
        .await?
      {
        merge_target_paths.push(target_path);
      }
    }

    if !batches.is_empty() {
      self.upload_merged_batches(&batches, &merge_target_paths, username).await?;
    }

    cleanup_old_files(&processed_files).await;

    Ok(())
  }

  pub async fn cloud_fetch_parquet(
    &self,
    username: &str,
    db_name: &str,
    table_name: &str,
    date_range: &HashMap<&str, &str>,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let prefix_path = format!("{}/{}/{}", username, db_name, table_name);
    let cloud_files = self.list_cloud_files(&prefix_path).await?;
    let start_date = date_range.get("start_date").ok_or("Missing start_date")?;
    let end_date = date_range.get("end_date").ok_or("Missing end_date")?;
    let filtered_cloud_files: HashSet<_> =
      filter_files_by_date_range(cloud_files.iter().map(|(path, _)| path.clone()).collect(), start_date, end_date)?
        .into_iter()
        .collect();

    let local_dir = format!("{}/group/{}/{}/{}", self.db_manager.get_storage_path(), username, db_name, table_name);
    if fs::metadata(&local_dir).is_err() {
      println!("Directory does not exist, skipping deletion.");
    } else {
      let local_files: HashSet<String> = fs::read_dir(&local_dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.path().file_name()?.to_str().map(String::from))
        .collect();

      let cloud_filenames: HashSet<String> = filtered_cloud_files
        .iter()
        .filter_map(|cloud_path| Path::new(cloud_path).file_name()?.to_str().map(String::from))
        .collect();

      // Delete local files that are NOT present in the cloud
      for local_file in &local_files {
        if !cloud_filenames.contains(local_file) {
          let local_file_path = format!("{}/{}", local_dir, local_file);
          println!("Deleting out of sync file: {}", local_file);
          fs::remove_file(&local_file_path)?;
        }
      }
    }

    // Download missing or outdated files
    for (cloud_file, cloud_modified_time) in cloud_files {
      if !filtered_cloud_files.contains(&cloud_file) {
        continue;
      }

      if let Some(filename) = Path::new(&cloud_file).file_name().and_then(|n| n.to_str()) {
        let local_path = format!("{}/{}", local_dir, filename);

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

  async fn process_sink_parquet_file(
    &self,
    file: &str,
    username: &str,
    db_name: &str,
    table_name: &str,
    unique_fields: &[String],
    batches: &mut Vec<RecordBatch>,
    processed_files: &mut Vec<PathBuf>,
  ) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let s3_store = &self.s3_store;
    let file_path = PathBuf::from(file);

    // Regex for partitioned format: supports multiple formats
    // Monthly: partition_date=YYYY-MM
    // Daily/Weekly: partition_date=YYYY-MM-DD
    // Hourly: partition_date=YYYY-MM-DD_HH
    // Minute: partition_date=YYYY-MM-DD_HH-MM
    let partition_regx =
      Regex::new(r"partition_date=(?P<year>\d{4})-(?P<month>\d{2})(?:-(?P<day>\d{2}))?(?:_(?P<hour>\d{2}))?(?:-(?P<minute>\d{2}))?")
        .expect("Invalid partition regex");

    // Extract date from parent directory path
    let parent_path = file_path.parent().and_then(|p| p.to_str()).unwrap_or("");

    if let Some(caps) = partition_regx.captures(parent_path) {
      let year = caps.name("year").map(|m| m.as_str()).unwrap_or("0000");
      let month = caps.name("month").map(|m| m.as_str()).unwrap_or("00");
      let day = caps.name("day").map(|m| m.as_str()).unwrap_or("01"); // Default to day 01 for monthly partitions

      // Generate S3 filename that includes the date
      let s3_filename = format!("{}_{}-{}-{}.parquet", table_name, year, month, day);
      let target_path = format!("{}/{}/{}/{}/{}/{}", username, db_name, table_name, year, month, s3_filename);

      let s3_temp_path = format!("{}/merge_workspace/{}/{}", self.db_manager.get_storage_path(), username, s3_filename);
      let mut s3_batches = Vec::new();

      let local_modified_datetime = get_local_file_modified_time(&file_path.to_string_lossy()).unwrap_or_default();

      // Use `head()` to check if file exists and get metadata
      let s3_modified_datetime = match s3_store.store_head(&StorePath::from(target_path.clone())).await {
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
        println!("Local file is older or identical to S3, '{}' skipping download", s3_filename);
      }
    } else {
      println!("No partition found in file path: {:?}", file_path);
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
      let merged_file_path = format!("{}/merge_workspace/{}/merged_{}", self.db_manager.get_storage_path(), username, filename);

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

  pub async fn upload_to_bucket(&self, source_path: &str, target_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let s3_store = &self.s3_store;
    let object_store = Arc::new(s3_store);

    // Prepare the file for upload
    let mut file = tokio::fs::File::open(source_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
    object_store.store_put(&StorePath::from(target_path), data.into()).await?;

    Ok(())
  }

  pub async fn list_cloud_files(&self, prefix_path: &str) -> Result<Vec<(String, DateTime<Utc>)>, Box<dyn std::error::Error>> {
    // List all objects under the prefix
    let objects = self.s3_store.store_list(Some(&StorePath::from(prefix_path)));
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

  pub async fn download_from_bucket(&self, target_path: &str, local_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let object_store = &self.s3_store;
    let path = StorePath::from(target_path);

    // Ensure the parent directory exists
    if let Some(parent) = std::path::Path::new(local_path).parent() {
      fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory '{}': {}", parent.display(), e))?;
    }

    // Stream the bytes from object storage
    let mut stream = match object_store.store_get(&path).await {
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
