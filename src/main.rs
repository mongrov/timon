mod timon_engine;
pub use timon_engine::{
  cloud_sync_parquet, create_database, create_table, delete_database, delete_table, init_bucket, init_timon, insert, list_databases, list_tables,
  query, query_bucket,
};
#[cfg(feature = "dev_cli")]
mod cli;
#[cfg(feature = "cloud_server")]
mod server;

#[cfg(feature = "cloud_server")]
mod cloud_server {
  use crate::server::timon_server;
  use std::io;

  #[actix_web::main]
  pub async fn main() -> io::Result<()> {
    timon_server().await
  }
}

#[cfg(feature = "dev_cli")]
mod dev_cli {
  use crate::cli::{convert_json_to_parquet, execute_query, Commands, CLI};
  use clap::Parser;

  #[tokio::main]
  pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CLI::parse();

    match &cli.command {
      Commands::Convert { input, output } => {
        convert_json_to_parquet(input.as_str(), output.as_str())?;
        println!("JSON converted to Parquet successfully.");
      }
      Commands::Query { file, query } => {
        execute_query(file.as_str(), query.as_str()).await?;
      }
    }
    Ok(())
  }
}

#[cfg(feature = "cloud_server")]
fn main() {
  if let Err(e) = cloud_server::main() {
    eprintln!("Failed to start the cloud server: {}", e);
  }
}

#[cfg(feature = "dev_cli")]
fn main() {
  if let Err(e) = dev_cli::main() {
    eprintln!("Failed to build the CLI tool: {}", e);
  }
}

#[allow(dead_code)]
async fn test_local_storage() {
  const STORAGE_PATH: &str = "/tmp/timon";
  let timon_result = init_timon(STORAGE_PATH, 30).unwrap();
  println!("init_timon -> {}", timon_result);

  const DATABASE_NAME: &str = "test";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  let table_schema = r#"
    {
      "date": { "type": "string", "required": true, "unique": true },
      "temperature": { "type": "int|float", "required": true },
      "humidity": { "type": "int|float", "required": true },
      "full_counter": { "type": "int", "required": true },
      "is_cool": { "type": "bool", "required": true },
      "ring_details": { "type": "array", "required": true }
    }
  "#;
  let table_result = create_table(DATABASE_NAME, "temperature", &table_schema);
  println!("create_table -> {}", table_result.unwrap());

  let databases_list = list_databases().unwrap();
  let tables_list = list_tables(DATABASE_NAME).unwrap();
  println!("databases_list -> {:?}", databases_list);
  println!("tables_list -> {:?}", tables_list);

  let json_data: String = r#"
    [
      {
        "date": "2024.08.18 20:58:32",
        "humidity": 12,
        "temperature": 22,
        "full_counter": 7,
        "is_cool": true,
        "ring_details": ["Ahmed", "Eyal", "Olive"]
      },
      {
        "date": "2024.08.18 20:58:35",
        "humidity": 88.5,
        "temperature": 44.0,
        "full_counter": 77,
        "is_cool": true,
        "ring_details": ["Moin", "Jeel"]
      }
    ]
  "#
  .to_string();
  let insertion_result = insert(DATABASE_NAME, "temperature", &json_data);
  println!("insertion_result: {}", insertion_result.unwrap());

  // let range: std::collections::HashMap<&str, &str> = std::collections::HashMap::from([("start_date", "2024-12-12"), ("end_date", "2025-01-12")]);
  let sql_query = format!("SELECT * FROM temperature ORDER BY date ASC LIMIT 25");
  let query_result = query(DATABASE_NAME, &sql_query).await;
  println!("query_result: {}", query_result.unwrap());

  let delete_table_result = delete_table(DATABASE_NAME, "iot").unwrap();
  println!("delete_table_result -> {}", delete_table_result);
  let delete_database_result = delete_database(DATABASE_NAME).unwrap();
  println!("delete_database_result -> {}", delete_database_result);
}

#[allow(dead_code)]
async fn test_s3_sync() {
  init_timon("/tmp/timon", 30).unwrap();

  let bucket_endpoint = "http://localhost:9000";
  let bucket_name = "timon";
  let access_key_id = "ahmed";
  let secret_access_key = "ahmed1234";
  let bucket_region = "us-east-1";
  let init_bucket_result = init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region).unwrap();
  println!("init_bucket_result: {}", init_bucket_result);

  let range = std::collections::HashMap::from([("start_date", "2024-07-01"), ("end_date", "2024-08-01")]);
  let sql_query = "SELECT * FROM temperature LIMIT 25";
  let df_result = query_bucket("user6172", &sql_query, range).await.unwrap();
  println!("query_bucket {:?}", df_result);

  let cloud_sync_parquet_result = cloud_sync_parquet("user6172", "test", "temperature").await;
  println!("{}", cloud_sync_parquet_result.unwrap());
}

// This block is executed for local development testing(run async tests for local_storage and S3 cloud_sync).
#[cfg(all(not(feature = "dev_cli"), not(feature = "cloud_server")))]
fn main() {
  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    test_local_storage().await;
    test_s3_sync().await;
  });
}
