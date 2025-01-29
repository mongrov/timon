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
  const STORAGE_PATH: &str = "tmp/timon";
  let timon_result = init_timon(STORAGE_PATH, 5).unwrap();
  println!("init_timon -> {}", timon_result);

  const DATABASE_NAME: &str = "zivaring";
  const TABLE_NANE: &str = "activitydetails";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  let table_schema = r#"
    {
      "date": {
        "type": "string",
        "required": true,
        "unique": true
      },
      "distance": {
        "type": "int|float"
      },
      "step": {
        "type": "int"
      },
      "calories": {
        "type": "int|float"
      },
      "arraySteps": {
        "type": "array"
      },
      "is_sync": {
        "type": "bool"
      }
    }
  "#;
  let table_result = create_table(DATABASE_NAME, TABLE_NANE, &table_schema);
  println!("create_table -> {}", table_result.unwrap());

  let databases_list = list_databases().unwrap();
  let tables_list = list_tables(DATABASE_NAME).unwrap();
  println!("databases_list -> {:?}", databases_list);
  println!("tables_list -> {:?}", tables_list);

  let json_data: String = r#"
    [
      {"arraySteps":[43,39,0,0,0,0,0,0,0,0],"calories":2.56,"date":"2025.01.01 08:32:45","distance":0.05,"step":82},
      {"arraySteps":[20,0,0,0,0,0,0,0,0,0],"calories":0.61,"date":"2025.01.01 09:24:19","distance":0.01,"step":20},
      {"arraySteps":[19,0,0,0,0,0,0,0,0,0],"calories":0.65,"date":"2025.01.01 10:13:45","distance":0.01,"step":19},
      {"arraySteps":[54,33,2,0,0,0,0,0,0,0],"calories":2.83,"date":"2025.01.01 10:29:56","distance":0.06,"step":89},
      {"arraySteps":[38,0,0,15,0,0,0,0,0,0],"calories":1.53,"date":"2025.01.01 11:58:16","distance":0.03,"step":53},
      {"arraySteps":[50,16,0,55,23,0,0,18,46,0],"calories":6.19,"date":"2025.01.01 12:15:38","distance":0.14,"step":208},
      {"arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":1.05,"date":"2025.01.01 13:16:51","distance":0.01,"step":38}
    ]
  "#
  .to_string();
  let insertion_result = insert(DATABASE_NAME, TABLE_NANE, &json_data);
  println!("insertion_result: {}", insertion_result.unwrap());

  let sql_query = format!("SELECT * FROM {} ORDER BY date DESC LIMIT 25", TABLE_NANE);
  let query_result = query(DATABASE_NAME, &sql_query).await;
  println!("query_result: {}", query_result.unwrap());

  // let delete_table_result = delete_table(DATABASE_NAME, "iot").unwrap();
  // println!("delete_table_result -> {}", delete_table_result);
  // let delete_database_result = delete_database(DATABASE_NAME).unwrap();
  // println!("delete_database_result -> {}", delete_database_result);
}

#[allow(dead_code)]
async fn test_s3_sync() {
  init_timon("tmp/timon", 5).unwrap();

  let bucket_endpoint = "https://amazonaws.com";
  let bucket_name = "zivaone_app";
  let access_key_id = "xxx-xxx";
  let secret_access_key = "xxx-xxx-xxx-xxx";
  let bucket_region = "us-west-2";
  let init_bucket_result = init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region).unwrap();
  println!("init_bucket_result: {}", init_bucket_result);

  const USERNAME: &str = "wRE3w2vJcZLaabPQs";
  const DATABASE_NAME: &str = "zivaring";
  const TABLE_NAME: &str = "activitydetails";

  let cloud_sync_parquet_result = cloud_sync_parquet(USERNAME, DATABASE_NAME, TABLE_NAME).await;
  println!("{}", cloud_sync_parquet_result.unwrap());

  let range = std::collections::HashMap::from([("start_date", "2025-01-29"), ("end_date", "2025-01-29")]);
  let sql_query = format!("SELECT * FROM {} ORDER BY date DESC LIMIT 25", TABLE_NAME);
  let df_result = query_bucket(USERNAME, "zivaring", &sql_query, range).await.unwrap();
  println!("query_bucket {:?}", df_result);
}

// This block is executed for local development testing(run async tests for local_storage and S3 cloud_sync).
#[cfg(all(not(feature = "dev_cli"), not(feature = "cloud_server")))]
fn main() {
  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    test_local_storage().await;
    test_s3_sync().await;
  });
}
