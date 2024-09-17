use object_store::{aws::{AmazonS3, AmazonS3Builder}, path::Path, ObjectStore};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

pub struct CloudQuerier;

impl CloudQuerier {
  fn get_s3_store(minio_endpoint: Option<&str>, access_key_id: Option<&str>, secret_access_key: Option<&str>, bucket_name: Option<&str>) ->AmazonS3 {
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
  pub async fn sink_data_to_bucket(source_path: &str, target_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize S3 store (assumed MinIO in your case)
    let s3_store = CloudQuerier::get_s3_store(None, None, None, None);
    let object_store = Arc::new(s3_store);

    // Prepare the file for upload
    let mut file = tokio::fs::File::open(source_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;
    object_store.put(&Path::from(target_path), data.into()).await?;
    
    Ok(())
  }
}
