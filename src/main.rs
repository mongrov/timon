mod timon_engine;
use chrono::{Duration, Local};
use serde_json::json;
use std::time::Instant;
pub use timon_engine::{
  cloud_fetch_parquet, cloud_sink_parquet, create_database, create_table, delete_database, delete_table, init_bucket, init_timon, insert,
  list_databases, list_tables, query,
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
  const USERNAME: &str = "ahmed_test";
  let timon_result = init_timon(STORAGE_PATH, 5, USERNAME).unwrap();
  println!("init_timon -> {}", timon_result);

  const DATABASE_NAME: &str = "zivaring";
  const TABLE_NAME: &str = "activitydetails";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  let table_schema = r#"
    {
      "date": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
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
  let table_result = create_table(DATABASE_NAME, TABLE_NAME, &table_schema);
  println!("create_table -> {}", table_result.unwrap());

  let databases_list: serde_json::Value = list_databases().unwrap();
  let tables_list = list_tables(DATABASE_NAME).unwrap();
  println!("databases_list -> {:?}", databases_list);
  println!("tables_list -> {:?}", tables_list);

  struct DataPoint {
    date: String,
    array_steps: Vec<i32>,
    calories: i32,
    distance: f64,
    step: i32,
  }

  fn generate_data(n: usize) -> String {
    let start_time = Local::now().naive_local() - Duration::hours(12); // Set start time to now - 12hours
    let mut data = Vec::new();
    let mut time_counter = 0;
    for i in 0..n {
      time_counter += 1000;
      let date = start_time + Duration::milliseconds(time_counter);
      let array_steps: Vec<i32> = (0..10).map(|x| (i as i32 + x) % 50).collect();
      let calories = (i % 50) + 1;
      let distance = (i as f64 * 0.01) % 5.0;
      let step = array_steps.iter().sum::<i32>();
      data.push(json!({
          "date": date.format("%Y.%m.%d %H:%M:%S").to_string(),
          "arraySteps": array_steps,
          "calories": calories,
          "distance": distance,
          "step": step
      }));
    }
    serde_json::to_string_pretty(&data).unwrap()
  }

  // let json_data = generate_data(1_000_000);
  let json_data: String = r#"
    [
      {"date":"2025.02.10 10:00:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":1.05,"distance":0.01,"step":1000},
      {"date":"2025.02.10 10:01:00","arraySteps":[43,39,0,0,0,0,0,0,0,0],"calories":2.56,"distance":0.05,"step":1001},
      {"date":"2025.02.10 10:02:00","arraySteps":[20,0,0,0,0,0,0,0,0,0],"calories":0.61,"distance":0.01,"step":1002},
      {"date":"2025.02.10 10:03:00","arraySteps":[19,0,0,0,0,0,0,0,0,0],"calories":0.65,"distance":0.01,"step":1003},
      {"date":"2025.02.10 10:21:00","arraySteps":[54,33,2,0,0,0,0,0,0,0],"calories":2.83,"distance":0.06,"step":1021},
      {"date":"2025.02.10 10:25:00","arraySteps":[38,0,0,15,0,0,0,0,0,0],"calories":1.53,"distance":0.03,"step":1025},
      {"date":"2025.02.10 10:30:00","arraySteps":[50,16,0,55,23,0,0,18,46,0],"calories":6.19,"distance":0.14,"step":1030},
      {"date":"2025.02.10 10:31:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":1.05,"distance":0.01,"step":1031}
    ]
  "#
  .to_string();

  let start_time = Instant::now(); // Start timing
  let insertion_result = insert(DATABASE_NAME, TABLE_NAME, &json_data);
  let duration = start_time.elapsed(); // Measure elapsed time
  println!("Insertion result: {}", insertion_result.unwrap());
  println!("Time taken for insertion: {:.3} seconds", duration.as_secs_f64());

  let sql_query = format!(r#"SELECT * FROM activitydetails ORDER BY date ASC LIMIT 5"#);
  let query_result = query(DATABASE_NAME, &sql_query, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  let start_time = Instant::now(); // Start timing
  let sql_query2 = format!(r#"SELECT * FROM activitydetails"#); // WHERE date BETWEEN '1730016996' AND '1739209996'
  let query_result2 = query(DATABASE_NAME, &sql_query2, None).await;
  let duration = start_time.elapsed(); // Measure elapsed time
  println!("query_result: {}", query_result2.unwrap()["json_value"]);
  println!("Time taken for query: {:.3} seconds", duration.as_secs_f64());

  // let delete_table_result = delete_table(DATABASE_NAME, "iot").unwrap();
  // println!("delete_table_result -> {}", delete_table_result);
  // let delete_database_result = delete_database(DATABASE_NAME).unwrap();
  // println!("delete_database_result -> {}", delete_database_result);
}

#[allow(dead_code)]
async fn test_s3_sync() {
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  const TABLE_NAME: &str = "activitydetails";
  init_timon("tmp/timon", 5, USERNAME).unwrap();

  let bucket_endpoint = "https://s3.us-west-2.amazonaws.com";
  let bucket_name = "zivaoneapp";
  let access_key_id = "AKIASXLNFKSVBDW4IAMJ";
  let secret_access_key = "REMOVED_SECRET";
  let bucket_region = "us-west-2";
  let init_bucket_result = init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region).unwrap();
  println!("init_bucket_result: {}", init_bucket_result);

  let fetch_range = std::collections::HashMap::from([("start_date", "2025-01-01"), ("end_date", "2025-12-30")]);
  let cloud_fetch_parquet_result = cloud_fetch_parquet(USERNAME, DATABASE_NAME, TABLE_NAME, fetch_range).await;
  println!("{}", cloud_fetch_parquet_result.unwrap());

  let cloud_sink_parquet_result: Result<serde_json::Value, String> = cloud_sink_parquet(DATABASE_NAME, TABLE_NAME).await;
  println!("{}", cloud_sink_parquet_result.unwrap());
}

// This block is executed for local development testing(run async tests for local_storage and S3 cloud_sync).
#[cfg(all(not(feature = "dev_cli"), not(feature = "cloud_server")))]
fn main() {
  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    test_local_storage().await;
    test_s3_sync().await;
  });
}
