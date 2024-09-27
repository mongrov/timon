use crate::datafusion_query::helpers;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use helpers::{generate_paths, record_batches_to_json, Granularity};
use object_store::{
  aws::{AmazonS3, AmazonS3Builder},
  path::Path,
  ObjectStore,
};
use std::fmt;
use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;
use url::Url;

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

pub struct CloudStorageManager;

impl CloudStorageManager {
  fn get_s3_store(minio_endpoint: Option<&str>, access_key_id: Option<&str>, secret_access_key: Option<&str>, bucket_name: Option<&str>) -> AmazonS3 {
    let minio_endpoint = minio_endpoint.unwrap_or("http://localhost:9000");
    let bucket_name = bucket_name.unwrap_or("timon");
    let access_key_id = access_key_id.unwrap_or("ahmed");
    let secret_access_key = secret_access_key.unwrap_or("ahmed1234");

    let s3_store = AmazonS3Builder::new()
      .with_endpoint(minio_endpoint)
      .with_bucket_name(bucket_name)
      .with_access_key_id(access_key_id)
      .with_secret_access_key(secret_access_key)
      .with_allow_http(true)
      .build()
      .unwrap();

    s3_store
  }

  #[allow(dead_code)]
  pub async fn query_bucket(
    bucket_name: &str,
    date_range: HashMap<&str, &str>,
    sql_query: &str,
    is_json_format: bool,
  ) -> DataFusionResult<DataFusionOutput> {
    let s3_store = CloudStorageManager::get_s3_store(None, None, None, None);
    let session_context = SessionContext::new();
    let object_store = Arc::new(s3_store);
    let file_name = &extract_table_name(sql_query);

    // Parse the date_range and generate Parquet file paths
    let file_list = generate_paths(bucket_name, file_name, date_range, Granularity::Month, true).unwrap();
    // Register the object store with the session context
    let store_url = Url::parse(&format!("s3://{}", bucket_name)).unwrap();
    session_context.runtime_env().register_object_store(&store_url, object_store);

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

  pub async fn sink_data_to_bucket(source_path: &str, target_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize S3 store (assumed MinIO in your case)
    let s3_store = CloudStorageManager::get_s3_store(None, None, None, None);
    let object_store = Arc::new(s3_store);

    // Prepare the file for upload
    let mut file = tokio::fs::File::open(source_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
    object_store.put(&Path::from(target_path), data.into()).await?;

    Ok(())
  }
}
