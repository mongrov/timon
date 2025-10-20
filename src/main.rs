mod timon_engine;
use chrono::{DateTime, Duration, Local, Utc};
use serde_json::json;
use std::time::Instant;
pub use timon_engine::{
  cloud_fetch_parquet, cloud_sink_parquet, cloud_sync_parquet, create_database, create_table, delete_database, delete_table, init_bucket, init_timon,
  insert, list_databases, list_tables, query, query_df,
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
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let timon_result = init_timon(STORAGE_PATH, 5, USERNAME).unwrap();
  println!("init_timon -> {}", timon_result);

  const DATABASE_NAME: &str = "zivaring";
  const TABLE_NAME: &str = "activitydetails";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  let table_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "distance": {
        "type": "int|float",
        "max": 2500
      },
      "step": {
        "type": "int",
        "min": 10,
        "max": 100
      },
      "calories": {
        "type": "int|float",
        "min": 50,
        "max": 1200
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
      {"date":"2025.02.10 10:00:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":111,"distance":0.01,"step":10},
      {"date":"2025.02.10 10:01:00","arraySteps":[43,39,0,0,0,0,0,0,0,0],"calories":200,"distance":0.05,"step":11},
      {"date":"2025.02.10 10:02:00","arraySteps":[20,0,0,0,0,0,0,0,0,0],"calories":160,"distance":0.01,"step":12},
      {"date":"2025.02.10 10:03:00","arraySteps":[19,0,0,0,0,0,0,0,0,0],"calories":111,"distance":0.01,"step":1013},
      {"date":"2025.02.10 10:21:00","arraySteps":[54,33,2,0,0,0,0,0,0,0],"calories":180,"distance":0.06,"step":21},
      {"date":"2025.02.10 10:25:00","arraySteps":[38,0,0,15,0,0,0,0,0,0],"calories":120,"distance":0.03,"step":25},
      {"date":"2025.02.10 10:30:00","arraySteps":[50,16,0,55,23,0,0,18,46,0],"calories":6,"distance":140,"step":30},
      {"date":"2025.02.10 10:31:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":170,"distance":2530.5,"step":1031}
    ]
  "#
  .to_string();

  let start_time = Instant::now(); // Start timing
  let insertion_result = insert(DATABASE_NAME, TABLE_NAME, &json_data);
  let duration = start_time.elapsed(); // Measure elapsed time
  println!("Insertion result: {}", insertion_result.unwrap());
  println!("Time taken for insertion: {:.3} seconds", duration.as_secs_f64());

  let sql_query = format!(r#"SELECT * FROM activitydetails ORDER BY date ASC"#);
  let query_result = query(DATABASE_NAME, &sql_query, None, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  // let start_time = Instant::now(); // Start timing
  // let sql_query2 = format!(r#"SELECT * FROM activitydetails LIMIT 10"#); // WHERE date BETWEEN '1730016996' AND '1739209996'
  // let query_result2 = query(DATABASE_NAME, &sql_query2, None).await;
  // let duration = start_time.elapsed(); // Measure elapsed time
  // println!("query_result: {}", query_result2.unwrap()["json_value"]);
  // println!("Time taken for query: {:.3} seconds", duration.as_secs_f64());

  // let sql_query3 = format!(r#"SELECT * FROM activitydetails"#);
  // let query_df_result = query_df(DATABASE_NAME, &sql_query3, None).await;
  // println!("query_df_result: {:?}", query_df_result.unwrap());

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
  let access_key_id = "xxx";
  let secret_access_key = "xxx";
  let bucket_region = "us-west-2";
  let init_bucket_result = init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region).unwrap();
  println!("init_bucket_result: {}", init_bucket_result);

  let fetch_range = std::collections::HashMap::from([("start_date", "2025-01-01"), ("end_date", "2025-12-30")]);
  let cloud_fetch_parquet_result = cloud_fetch_parquet(USERNAME, DATABASE_NAME, TABLE_NAME, fetch_range.clone()).await;
  println!("{}", cloud_fetch_parquet_result.unwrap());

  let cloud_sink_parquet_result: Result<serde_json::Value, String> = cloud_sink_parquet(DATABASE_NAME, TABLE_NAME).await;
  println!("{}", cloud_sink_parquet_result.unwrap());

  let cloud_sync_parquet_result = cloud_sync_parquet(DATABASE_NAME, TABLE_NAME, fetch_range.clone(), None);
  println!("{}", cloud_sync_parquet_result.await.unwrap());
}

// This block is executed for local development testing(run async tests for local_storage and S3 cloud_sync).
#[cfg(all(not(feature = "dev_cli"), not(feature = "cloud_server")))]
fn main() {
  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    test_local_storage().await;
    test_s3_sync().await;
    let _ = test_ziva_ring_insert().await;
    let _ = test_ziva_ring_query().await;
    let _ = test_ziva_join_query().await;
    let _ = insert_ziva_data_six_months().await;
    let _ = test_ziva_range_selction_query().await;
    let _ = test_max_rows().await;
    let _ = test_partition_limit().await;
    let _ = test_sleep_queries().await;
    let _ = test_hrv_queries().await;
    let _ = test_rhr_queries().await;
    let _ = test_vitality_queries().await;
    let _ = ziva_app_queries().await;
  });
}

/*
****** bucket_interval ******
Hourly = 60
Daily = 1440
Weekly = 10080
Monthly = 43200
*/

async fn test_ziva_ring_insert() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let timon_result = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();
  println!("init_timon -> {}", timon_result);

  const DATABASE_NAME: &str = "zivaring";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  // Activity Details Table Schema
  let activity_details_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "step": {
        "type": "int"
      },
      "arraySteps": {
        "type": "array"
      },
      "calories": {
        "type": "int|float"
      },
      "distance": {
        "type": "int|float"
      }
    }
  "#;

  // SPO2 Table Schema
  let spo2_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "automaticSpo2Data": {
        "type": "int"
      }
    }
    "#;

  // Heart Rate Table Schema
  let heartrate_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "singleHR": {
        "type": "int"
      }
    }
    "#;

  // HRV Table Schema
  let hrv_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "hrv": {
        "type": "int"
      },
      "heartRate": {
        "type": "int"
      },
      "stress": {
        "type": "int"
      },
      "diastolicBP": {
        "type": "int"
      },
      "systolicBP": {
        "type": "int"
      },
      "vascularAging": {
        "type": "int"
      },
      "is_sync": {
        "type": "bool"
      }
    }
    "#;

  // Sleep Table Schema
  let sleep_schema = r#"
    {
      "timestamp": {
        "type":"int",
        "required":true,
        "unique":true,
        "datetime":true
      },
      "unitLength":{
        "type":"int|float"
      },
      "quality":{
        "type":"int|float"
      },
      "start":{
        "type":"string"
      }
    }
    "#;

  // Temperature Table Schema
  let temperature_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "temperature": {
        "type": "int|float"
      }
    }
    "#;

  // Create tables
  let activity_details_result = create_table(DATABASE_NAME, "activitydetails", &activity_details_schema);
  println!("Create activitydetails table -> {}", activity_details_result.unwrap());

  let sleep_result = create_table(DATABASE_NAME, "sleep_table", &sleep_schema);
  println!("Create sleep table -> {}", sleep_result.unwrap());

  let spo2_result = create_table(DATABASE_NAME, "spo2_readings", &spo2_schema);
  println!("Create SPO2 table -> {}", spo2_result.unwrap());

  let hr_result = create_table(DATABASE_NAME, "heartrate", &heartrate_schema);
  println!("Create heart rate table -> {}", hr_result.unwrap());

  let hrv_result = create_table(DATABASE_NAME, "hrv_table", &hrv_schema);
  println!("Create HRV table -> {}", hrv_result.unwrap());

  let temp_result = create_table(DATABASE_NAME, "temperature_readings", &temperature_schema);
  println!("Create temperature table -> {}", temp_result.unwrap());

  // Read JSON file
  let file_content =
    std::fs::read_to_string("/home/ahmed/Documents/ziva_data_android.json").map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
  let json_data: serde_json::Value = serde_json::from_str(&file_content).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
  let start_time = Instant::now();

  // Insert activity details
  if let Some(activity_details) = json_data["activitydetails"].as_array() {
    let formatted_activity_details: Vec<serde_json::Value> = activity_details
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "step": reading["step"],
          "arraySteps": reading["arraySteps"],
          "calories": reading["calories"],
          "distance": reading["distance"]
        })
      })
      .collect();
    let activity_details_json = serde_json::to_string(&formatted_activity_details)?;
    let insertion_result = insert(DATABASE_NAME, "activitydetails", &activity_details_json)?;
    println!("Activity details insertion result: {}", insertion_result);
  }

  // Insert SPO2 readings
  if let Some(spo2) = json_data["spo2"].as_array() {
    let formatted_spo2: Vec<serde_json::Value> = spo2
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "automaticSpo2Data": reading["automaticSpo2Data"]
        })
      })
      .collect();
    let spo2_json = serde_json::to_string(&formatted_spo2)?;
    let insertion_result = insert(DATABASE_NAME, "spo2_readings", &spo2_json)?;
    println!("SPO2 insertion result: {}", insertion_result);
  }

  // Insert heart rate readings
  if let Some(heartrate) = json_data["heartrate"].as_array() {
    let formatted_hr: Vec<serde_json::Value> = heartrate
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "singleHR": reading["singleHR"]
        })
      })
      .collect();
    let heartrate_json = serde_json::to_string(&formatted_hr)?;
    let insertion_result = insert(DATABASE_NAME, "heartrate", &heartrate_json)?;
    println!("Heart rate insertion result: {}", insertion_result);
  }

  // Insert HRV readings
  if let Some(hrv) = json_data["hrv_table"].as_array() {
    let formatted_hrv: Vec<serde_json::Value> = hrv
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "heartRate": reading["heartRate"],
          "hrv": reading["hrv"],
          "stress": reading["stress"],
          "vascularAging": reading["vascularAging"],
          "diastolicBP": reading["diastolicBP"],
          "systolicBP": reading["systolicBP"],
        })
      })
      .collect();
    let hrv_json = serde_json::to_string(&formatted_hrv)?;
    let insertion_result = insert(DATABASE_NAME, "hrv_table", &hrv_json)?;
    println!("HRV insertion result: {}", insertion_result);
  }

  // Insert sleep readings
  if let Some(sleep) = json_data["sleep"].as_array() {
    let formatted_sleep: Vec<serde_json::Value> = sleep
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "unitLength": reading["unitLength"],
          "quality": reading["quality"],
          "start": reading["start"]
        })
      })
      .collect();
    let sleep_json = serde_json::to_string(&formatted_sleep)?;
    let insertion_result = insert(DATABASE_NAME, "sleep_table", &sleep_json)?;
    println!("Sleep insertion result: {}", insertion_result);
  }

  // Insert temperature readings
  if let Some(temperature) = json_data["temperature_table"].as_array() {
    let formatted_temp: Vec<serde_json::Value> = temperature
      .iter()
      .map(|reading| {
        let date_str = reading["timestamp"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "timestamp": timestamp,
          "temperature": reading["temperature"]
        })
      })
      .collect();
    let temperature_json = serde_json::to_string(&formatted_temp)?;
    let insertion_result = insert(DATABASE_NAME, "temperature_readings", &temperature_json)?;
    println!("Temperature insertion result: {}", insertion_result);
  }

  let duration = start_time.elapsed();
  println!("Total time taken for all insertions: {:.3} seconds", duration.as_secs_f64());

  Ok(())
}

async fn test_ziva_ring_query() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  let _ = init_timon(STORAGE_PATH, 60, USERNAME).unwrap();

  // Query activity details
  let start_time = Instant::now();
  let activity_details_query = format!(r#"SELECT * FROM activitydetails"#);
  let activity_details_result = query(DATABASE_NAME, &activity_details_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Activity details {} (Time taken: {:.3} seconds)",
    activity_details_result["status"],
    duration.as_secs_f64()
  );

  // Query SPO2 readings
  let start_time = Instant::now();
  let spo2_query = format!(r#"SELECT * FROM spo2_readings"#);
  let spo2_result = query(DATABASE_NAME, &spo2_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "SPO2 readings {} (Time taken: {:.3} seconds)",
    spo2_result["status"],
    duration.as_secs_f64()
  );

  // Query heart rate readings
  let start_time = Instant::now();
  let hr_query = format!(r#"SELECT * FROM heartrate"#);
  let hr_result = query(DATABASE_NAME, &hr_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Heart rate readings {} (Time taken: {:.3} seconds)",
    hr_result["status"],
    duration.as_secs_f64()
  );

  // Query HRV readings
  let start_time = Instant::now();
  let hrv_query = format!(r#"SELECT * FROM hrv_table"#);
  let hrv_result = query(DATABASE_NAME, &hrv_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "HRV readings {} (Time taken: {:.3} seconds)",
    hrv_result["status"],
    duration.as_secs_f64()
  );

  // Query temperature readings
  let start_time = Instant::now();
  let temp_query = format!(r#"SELECT * FROM temperature_readings"#);
  let temp_result = query(DATABASE_NAME, &temp_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Temperature readings {} (Time taken: {:.3} seconds)",
    temp_result["status"],
    duration.as_secs_f64()
  );

  // Test some specific queries
  println!("\nTesting specific queries:");

  // Query for average heart rate
  let start_time = Instant::now();
  let avg_hr_query = format!(r#"SELECT * FROM heartrate"#);
  let avg_hr_result = query(DATABASE_NAME, &avg_hr_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Average heart rate {} (Time taken: {:.3} seconds)",
    avg_hr_result["status"],
    duration.as_secs_f64()
  );

  // Query for max SPO2
  let start_time = Instant::now();
  let max_spo2_query = format!(r#"SELECT * FROM spo2_readings"#);
  let max_spo2_result = query(DATABASE_NAME, &max_spo2_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Max SPO2: {} (Time taken: {:.3} seconds)",
    max_spo2_result["status"],
    duration.as_secs_f64()
  );

  // Query for stress levels over time
  let start_time = Instant::now();
  let stress_query = format!(r#"SELECT * FROM hrv_table"#);
  let stress_result = query(DATABASE_NAME, &stress_query, None, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Stress levels over time: {} (Time taken: {:.3} seconds)",
    stress_result["status"],
    duration.as_secs_f64()
  );

  Ok(())
}

async fn test_ziva_join_query() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  let start_time = Instant::now();
  let sql_query = "SELECT * FROM activitydetails JOIN spo2_readings ON to_char(to_timestamp(activitydetails.timestamp), 'YYYY-MM-DD') = to_char(to_timestamp(spo2_readings.timestamp), 'YYYY-MM-DD') LIMIT 100";
  let result = query(DATABASE_NAME, sql_query, None, None).await?;
  let duration = start_time.elapsed();
  println!("Query time: {:.3} seconds", duration.as_secs_f64());
  println!("JOIN Query Result: {} status: {}", result["json_value"], result["status"]);

  Ok(())
}

fn generate_spo2_data(start: &str, end: &str) -> Result<String, Box<dyn std::error::Error>> {
  use chrono::{Duration, NaiveDateTime};
  use serde_json::json;
  use std::time::{SystemTime, UNIX_EPOCH};
  // Simple random number generator using system time
  fn get_random_number(max: u32) -> u32 {
    let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u32;
    seed % max
  }
  let start_time = NaiveDateTime::parse_from_str(start, "%Y-%m-%d %H:%M:%S")?;
  let end_time = NaiveDateTime::parse_from_str(end, "%Y-%m-%d %H:%M:%S")?;
  let mut data = Vec::new();
  let mut current = start_time;
  while current < end_time {
    data.push(json!({
        "date": current.format("%Y-%m-%d %H:%M:%S").to_string(),
        "automaticSpo2Data": get_random_number(100)
    }));
    current = current + Duration::minutes(5);
  }
  // Convert the data to JSON string
  Ok(serde_json::to_string(&data)?)
}

async fn insert_ziva_data_six_months() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();
  let _ = create_database(DATABASE_NAME);

  // SPO2 Table Schema
  let spo2_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "automaticSpo2Data": {
        "type": "int"
      }
    }
    "#;
  let spo2_result = create_table(DATABASE_NAME, "spo2_readings", &spo2_schema);
  println!("Create SPO2 table -> {}", spo2_result.unwrap());

  // // SPO2 table Count: 53k rows
  let start: &'static str = "2024-11-01 00:00:00";
  let end = "2025-05-01 00:00:00";
  let _json_data = generate_spo2_data(start, end)?;
  // let insertion_result = insert(DATABASE_NAME, "spo2_readings", &_json_data)?;
  // println!("SPO2 data insertion result: {}", insertion_result);

  const QUERY: &str = "SELECT * FROM spo2_readings ORDER BY date ASC";
  let start_time = std::time::Instant::now();
  let result = query(DATABASE_NAME, QUERY, None, None).await?;
  let duration = start_time.elapsed();
  println!("Query execution time: {:.3} seconds", duration.as_secs_f64());
  println!("Result: {:?}", result.get("status").unwrap());

  Ok(())
}

async fn test_ziva_range_selction_query() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  const QUERY_2: &str = "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN '2025-08-27' AND '2025-09-20'";
  let result = query(DATABASE_NAME, QUERY_2, None, None).await?;
  println!("Range Selction Result: {} status: {}", result["json_value"], result["status"]);

  Ok(())
}

async fn test_max_rows() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  const DATABASE_NAME: &str = "test_maxrows";
  let database_result = create_database(DATABASE_NAME);
  println!("create_database -> {}", database_result.unwrap());

  let activity_details_schema = r#"
    {
      "timestamp": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "battery_level": {
        "type": "int"
      },
      "max_rows": 100
    }
  "#;

  let activity_details_result = create_table(DATABASE_NAME, "battery_readings", &activity_details_schema);
  println!("Create battery_readings table -> {}", activity_details_result.unwrap());

  let battery_readings_json = r#"
    [
      {"date": "2025-01-01 03:04:00", "battery_level": 75},
      {"date": "2025-01-01 03:05:00", "battery_level": 76},
      {"date": "2025-01-01 03:06:00", "battery_level": 77},
      {"date": "2025-01-01 03:07:00", "battery_level": 79},
      {"date": "2025-01-01 03:08:00", "battery_level": 80},
      {"date": "2025-01-01 03:09:00", "battery_level": 80},
      {"date": "2025-01-01 04:00:00", "battery_level": 100}
    ]
  "#;

  let battery_readings_result = insert(DATABASE_NAME, "battery_readings", &battery_readings_json);
  println!("Insert battery_readings -> {}", battery_readings_result.unwrap());

  let sql_query = format!(r#"SELECT battery_level FROM battery_readings ORDER BY date ASC"#);
  let query_result = query(DATABASE_NAME, &sql_query, None, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  let sql_query = format!(r#"SELECT COUNT(*) as total FROM battery_readings"#);
  let query_result = query(DATABASE_NAME, &sql_query, None, None).await;
  println!("query_result count: {}", query_result.unwrap()["json_value"]);

  let sql_query = format!(r#"SELECT MIN(battery_level) as min_battery, MAX(battery_level) as max_battery FROM battery_readings"#);
  let query_result = query(DATABASE_NAME, &sql_query, None, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  Ok(())
}

async fn test_partition_limit() -> Result<(), Box<dyn std::error::Error>> {
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  const DATABASE_NAME: &str = "zivaring";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  println!("\n=== Testing Partition Limit Functionality ===");

  // Test querying last 3 partitions
  let sql_query = "SELECT COUNT(*) AS total FROM activitydetails";
  let result = query(DATABASE_NAME, sql_query, None, Some(2)).await?;
  println!("Last 2 partitions result: {} status: {}", result["json_value"], result["status"]);

  // Test querying last 7 partitions
  let result2 = query(DATABASE_NAME, sql_query, None, Some(3)).await?;
  println!("Last 3 partitions result: {} status: {}", result2["json_value"], result2["status"]);

  // Test without partition limit (all partitions)
  let result3 = query(DATABASE_NAME, sql_query, None, None).await?;
  println!("All partitions result: {} status: {}", result3["json_value"], result3["status"]);

  Ok(())
}

async fn test_sleep_queries() -> Result<(), Box<dyn std::error::Error>> {
  println!("Testing Sleep Queries for Daily Vitality Score");
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  // Test 1: Get last night's sleep data (minute-by-minute objects)
  println!("\n=== LAST NIGHT'S SLEEP DATA ===");

  let sleep_dates = vec![("2025-09-22", 1758499200, 1758585599)];
  for (date_label, start_ts, end_ts) in &sleep_dates {
    println!("\n--- Sleep data for {} ---", date_label);

    let last_night_query = format!(
      r#"
      SELECT
        COUNT(DISTINCT start) as sleep_sessions_count,
        COUNT(*) / 60.0 AS sleep_total_hours,
        (
          SELECT MAX(session_minutes) / 60.0
          FROM (
            SELECT start, COUNT(*) as session_minutes
            FROM sleep_table
            WHERE timestamp BETWEEN {} AND {}
            GROUP BY start
          ) as session_counts
        ) AS sleep_longest_session,
        COUNT(CASE WHEN quality = 1 THEN 1 END) / 60.0 AS sleep_light_hours,
        COUNT(CASE WHEN quality = 2 THEN 1 END) / 60.0 AS sleep_deep_hours,
        COUNT(CASE WHEN quality = 3 THEN 1 END) / 60.0 AS sleep_rem_hours
      FROM sleep_table
      WHERE timestamp BETWEEN {} AND {}
      "#,
      start_ts, end_ts, start_ts, end_ts
    );

    let last_night_result = query("zivaring", &last_night_query, None, None).await?;
    // println!("Last night's sleep data: {}", last_night_result["json_value"]);
    let last_night_value = last_night_result["json_value"][0].clone();
    println!(
      "{:.1} hours total sleep, but broken into {} separate sessions - longest only {:.1} hours",
      last_night_value["sleep_total_hours"].as_f64().unwrap_or(0.0),
      last_night_value["sleep_sessions_count"],
      last_night_value["sleep_longest_session"].as_f64().unwrap_or(0.0)
    );
    println!(
      "Deep: {:.1}h * Light: {:.1}h * REM: {:.1}h",
      last_night_value["sleep_deep_hours"].as_f64().unwrap_or(0.0),
      last_night_value["sleep_light_hours"].as_f64().unwrap_or(0.0),
      last_night_value["sleep_rem_hours"].as_f64().unwrap_or(0.0)
    );
  }

  // Test 2: Get sleep consistency data (previous 6 nights)
  println!("\n=== SLEEP CONSISTENCY DATA (Previous 6 nights) ===");

  // For sleep consistency, we need to analyze sleep sessions from each of the previous 6 days
  // Let's query each day separately to get sleep sessions per day
  let consistency_dates = vec![
    ("2025-09-22", 1758499200, 1758585599), // Sep 22: 00:00 to 23:59
    ("2025-09-21", 1758412800, 1758499199), // Sep 21: 00:00 to 23:59
    ("2025-09-20", 1758326400, 1758412799), // Sep 20: 00:00 to 23:59
    ("2025-09-19", 1758240000, 1758326399), // Sep 19: 00:00 to 23:59
    ("2025-09-18", 1758153600, 1758239999), // Sep 18: 00:00 to 23:59
    ("2025-09-17", 1758067200, 1758153599), // Sep 17: 00:00 to 23:59
  ];

  // Collect all consistency data first
  let mut consistency_data = Vec::new();
  for (date_label, start_ts, end_ts) in &consistency_dates {
    let day_consistency_query = format!(
      r#"
      SELECT
        '{}' as date,
        COUNT(DISTINCT start) as sessions_count,
        COALESCE(SUM(session_duration), 0) as total_sleep_minutes
      FROM (
        SELECT start, COUNT(*) as session_duration
        FROM sleep_table
        WHERE timestamp BETWEEN {} AND {}
        GROUP BY start
      ) as daily_sessions
    "#,
      date_label, start_ts, end_ts
    );

    let day_result = query("zivaring", &day_consistency_query, None, None).await?;
    println!("day_result {} status: {} \n", day_result["json_value"], day_result["status"]);
    if let Some(day_data) = day_result["json_value"].as_array().and_then(|arr| arr.get(0)) {
      if let (Some(sessions), Some(minutes)) = (day_data["sessions_count"].as_i64(), day_data["total_sleep_minutes"].as_i64()) {
        consistency_data.push((sessions, minutes));
      }
    }
  }

  // Calculate sleep consistency score based on variance
  if consistency_data.len() >= 3 {
    let durations: Vec<f64> = consistency_data.iter().map(|(_, minutes)| *minutes as f64).collect();

    let mean = durations.iter().sum::<f64>() / durations.len() as f64;
    let variance = durations.iter().map(|duration| (duration - mean).powi(2)).sum::<f64>() / durations.len() as f64;
    let stddev = variance.sqrt();

    // Calculate consistency score based on standard deviation thresholds
    let consistency_score = if stddev <= 30.0 {
      100 // Excellent consistency (±30 min)
    } else if stddev <= 60.0 {
      75 // Good consistency (±60 min)
    } else if stddev <= 90.0 {
      50 // Fair consistency (±90 min)
    } else {
      25 // Poor consistency (>90 min)
    };

    // Determine consistency message
    let consistency_message = if consistency_score >= 90 {
      "Excellent sleep consistency this week"
    } else if consistency_score >= 70 {
      "Good sleep routine maintained"
    } else if consistency_score >= 50 {
      "Sleep schedule somewhat variable"
    } else {
      "Irregular sleep pattern - try consistent bedtime"
    };

    println!("\n=== SLEEP CONSISTENCY SCORE ===");
    println!("Duration Standard Deviation: {:.1} minutes", stddev);
    println!("Consistency Score: {}/100 ({})", consistency_score, consistency_message);

    // Display individual night data
    println!("\n=== INDIVIDUAL NIGHT DATA ===");
    for (i, (date_label, _, _)) in consistency_dates.iter().enumerate() {
      if i < consistency_data.len() {
        let (sessions, minutes) = consistency_data[i];
        println!(
          "{}: {} sessions, {} minutes ({:.1}h)",
          date_label,
          sessions,
          minutes,
          minutes as f64 / 60.0
        );
      }
    }
  } else {
    println!("\n=== SLEEP CONSISTENCY SCORE ===");
    println!("Insufficient data for consistency scoring (need at least 3 nights)");
    println!("Available data points: {}", consistency_data.len());
  }

  Ok(())
}

async fn test_hrv_queries() -> Result<(), Box<dyn std::error::Error>> {
  println!("\n=== HRV QUERIES FOR RECOVERY COMPONENT ===");
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  let rhr_hrv_query = r#"
  WITH date_params AS (
  SELECT 
      '2025-10-01'::DATE as target_date_local,
      'America/Los_Angeles' as user_timezone,
      
      ('2025-10-01'::DATE + INTERVAL '7 hours') as day_start_utc,
      ('2025-10-01'::DATE + INTERVAL '1 day' + INTERVAL '7 hours') as day_end_utc,
      
      ('2025-10-01'::DATE - INTERVAL '30 days' + INTERVAL '7 hours') as baseline_start_utc,
      ('2025-10-01'::DATE + INTERVAL '7 hours') as baseline_end_utc
  ),

  today_hrv_data AS (
      SELECT 
          TO_TIMESTAMP(h.timestamp) as hrv_timestamp,
          h.hrv,
          h.stress,
          h."heartRate"
      FROM hrv_table h
      CROSS JOIN date_params dp
      WHERE TO_TIMESTAMP(h.timestamp) 
          BETWEEN dp.day_start_utc AND dp.day_end_utc
      AND h.hrv > 0
      AND h.hrv < 200
  ),

  today_hrv_summary AS (
      SELECT 
          COUNT(*) as reading_count,
          AVG(hrv) as avg_hrv,
          STDDEV(hrv) as hrv_stddev,
          MIN(hrv) as min_hrv,
          MAX(hrv) as max_hrv,
          AVG(stress) as avg_stress,
          AVG("heartRate") as avg_heart_rate
      FROM today_hrv_data
  ),

  baseline_hrv_data AS (
      SELECT 
          DATE_TRUNC('day', TO_TIMESTAMP(h.timestamp)) as day_utc,
          h.hrv
      FROM hrv_table h
      CROSS JOIN date_params dp
      WHERE TO_TIMESTAMP(h.timestamp) 
          BETWEEN dp.baseline_start_utc AND dp.day_start_utc
      AND h.hrv > 0
      AND h.hrv < 200
  ),

  daily_baseline_hrv AS (
      SELECT 
          day_utc,
          AVG(hrv) as daily_avg_hrv,
          COUNT(*) as daily_reading_count
      FROM baseline_hrv_data
      GROUP BY day_utc
      HAVING COUNT(*) >= 3
  ),

  baseline_stats AS (
      SELECT 
          AVG(daily_avg_hrv) as baseline_hrv,
          STDDEV(daily_avg_hrv) as baseline_stddev,
          MIN(daily_avg_hrv) as baseline_min,
          MAX(daily_avg_hrv) as baseline_max,
          COUNT(*) as baseline_days_count
      FROM daily_baseline_hrv
  ),

  hrv_recovery_calculation AS (
      SELECT 
          th.avg_hrv as today_hrv,
          th.reading_count as today_readings,
          th.hrv_stddev as today_stddev,
          th.min_hrv as today_min,
          th.max_hrv as today_max,
          th.avg_stress as today_stress,
          th.avg_heart_rate as today_heart_rate,
          
          bs.baseline_hrv,
          bs.baseline_stddev,
          bs.baseline_min,
          bs.baseline_max,
          bs.baseline_days_count,
          
          CASE 
              WHEN bs.baseline_stddev > 0 AND th.avg_hrv IS NOT NULL THEN
                  (th.avg_hrv - bs.baseline_hrv) / bs.baseline_stddev
              ELSE NULL
          END as z_score,
          
          CASE 
              WHEN th.avg_hrv IS NOT NULL AND bs.baseline_hrv IS NOT NULL THEN
                  th.avg_hrv - bs.baseline_hrv
              ELSE NULL
          END as hrv_deviation,
          
          CASE 
              WHEN th.avg_hrv IS NOT NULL AND bs.baseline_hrv IS NOT NULL AND bs.baseline_hrv > 0 THEN
                  ((th.avg_hrv - bs.baseline_hrv) / bs.baseline_hrv) * 100
              ELSE NULL
          END as hrv_percent_change
          
      FROM today_hrv_summary th
      CROSS JOIN baseline_stats bs
  ),

  hrv_recovery_score AS (
      SELECT 
          *,
          CASE 
              WHEN z_score IS NULL THEN NULL
              WHEN baseline_days_count < 7 THEN 75
              WHEN z_score >= 1.0 THEN 100
              WHEN z_score >= 0.5 THEN 85 + ((z_score - 0.5) * 30)
              WHEN z_score >= 0 THEN 70 + (z_score * 30)
              WHEN z_score >= -0.5 THEN 50 + ((z_score + 0.5) * 40)
              WHEN z_score >= -1.0 THEN 25 + ((z_score + 1.0) * 50)
              ELSE 25
          END as recovery_score,
          
          CASE 
              WHEN z_score IS NULL THEN 'No Data'
              WHEN baseline_days_count < 7 THEN 'Building Baseline'
              WHEN z_score >= 0.5 THEN 'High Recovery'
              WHEN z_score >= -0.5 THEN 'Normal Recovery'
              ELSE 'Low Recovery'
          END as recovery_status,
          
          CASE 
              WHEN z_score IS NULL THEN 'No HRV data available for today'
              WHEN baseline_days_count < 7 THEN 
                  'Building your baseline (' || baseline_days_count || ' of 30 days)'
              WHEN z_score >= 1.0 THEN 
                  'Outstanding recovery! Your HRV is significantly above baseline (+' || 
                  ROUND(hrv_percent_change, 1) || '%)'
              WHEN z_score >= 0.5 THEN 
                  'Excellent recovery. Your body is well-rested (+' || 
                  ROUND(hrv_percent_change, 1) || '%)'
              WHEN z_score >= 0 THEN 
                  'Good recovery. You are ready for normal activities'
              WHEN z_score >= -0.5 THEN 
                  'Normal recovery. Your body is functioning well'
              WHEN z_score >= -1.0 THEN 
                  'Below average recovery. Consider lighter activities today'
              ELSE 
                  'Low recovery. Your body needs rest. Focus on recovery today'
          END as recovery_message,
          
          CASE 
              WHEN z_score IS NULL THEN 'Sync your ring for activity guidance'
              WHEN baseline_days_count < 7 THEN 'Normal activities are fine'
              WHEN z_score >= 0.5 THEN 'Perfect day for intense training or challenging work'
              WHEN z_score >= 0 THEN 'Good day for moderate exercise and productive work'
              WHEN z_score >= -0.5 THEN 'Stick to light to moderate activities'
              ELSE 'Prioritize rest, recovery, and light movement only'
          END as activity_recommendation
          
      FROM hrv_recovery_calculation
  )

  SELECT 
      dp.target_date_local as date,
      
      ROUND(hrs.today_hrv, 1) as hrv_ms,
      hrs.today_readings as hrv_reading_count,
      ROUND(hrs.today_min, 1) as hrv_min,
      ROUND(hrs.today_max, 1) as hrv_max,
      ROUND(hrs.today_stddev, 1) as hrv_stddev,
      
      ROUND(hrs.today_stress, 1) as stress_level,
      ROUND(hrs.today_heart_rate, 1) as avg_heart_rate,
      
      ROUND(hrs.baseline_hrv, 1) as baseline_hrv_30day,
      ROUND(hrs.baseline_stddev, 1) as baseline_stddev,
      ROUND(hrs.baseline_min, 1) as baseline_min,
      ROUND(hrs.baseline_max, 1) as baseline_max,
      hrs.baseline_days_count as baseline_days,
      
      ROUND(hrs.z_score, 2) as z_score,
      ROUND(hrs.hrv_deviation, 1) as hrv_change_ms,
      ROUND(hrs.hrv_percent_change, 1) as hrv_change_percent,
      
      hrs.recovery_status as status,
      ROUND(hrs.recovery_score, 0) as recovery_component_score,
      hrs.recovery_message as message,
      hrs.activity_recommendation as recommendation,
      
      CASE 
          WHEN hrs.today_readings >= 12 THEN 'High'
          WHEN hrs.today_readings >= 6 THEN 'Medium'
          WHEN hrs.today_readings >= 3 THEN 'Low'
          ELSE 'Very Low'
      END as data_quality,
      
      now() as calculated_at_utc

  FROM date_params dp
  CROSS JOIN hrv_recovery_score hrs;
  "#;

  let hrv_score_result = query("zivaring", rhr_hrv_query, None, None).await?;
  println!(
    "Resting Heart Rate Result: {} status: {}",
    hrv_score_result["json_value"], hrv_score_result["status"]
  );

  Ok(())
}

async fn test_rhr_queries() -> Result<(), Box<dyn std::error::Error>> {
  println!("\n=== RHR QUERIES FOR HEART HEALTH COMPONENT ===");
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  let rhr_sql_query = r#"
  WITH date_params AS (
    SELECT
      to_timestamp('2025-09-01T00:00:00') AS target_date_utc,
      'America/Los_Angeles' AS user_timezone,

      to_timestamp('2025-09-01T01:00:00') AS sleep_window_start_utc,
      to_timestamp('2025-09-01T13:00:00') AS sleep_window_end_utc,

      to_timestamp('2025-09-01T07:00:00') AS day_start_utc,
      to_timestamp('2025-10-01T07:00:00') AS day_end_utc
  ),

  -- sleep timestamps (epoch seconds as BIGINT + timestamp)
  sleep_timestamps AS (
    SELECT
      CAST(s.timestamp AS BIGINT) AS sleep_epoch,
      to_timestamp_seconds(CAST(s.timestamp AS BIGINT)) AS sleep_timestamp_utc
    FROM sleep_table s
    CROSS JOIN date_params dp
    WHERE to_timestamp_seconds(CAST(s.timestamp AS BIGINT))
      BETWEEN dp.sleep_window_start_utc AND dp.sleep_window_end_utc
  ),

  -- heart rate rows that align to a sleep timestamp within +/-30s
  hr_during_sleep AS (
    SELECT
      hr."singleHR" AS heart_rate,
      CAST(hr.timestamp AS BIGINT) AS hr_epoch,
      to_timestamp_seconds(CAST(hr.timestamp AS BIGINT)) AS hr_timestamp_utc
    FROM heartrate hr
    WHERE hr."singleHR" BETWEEN 40 AND 120
      AND EXISTS (
        SELECT 1
        FROM sleep_timestamps st
        WHERE ABS(CAST(hr.timestamp AS BIGINT) - st.sleep_epoch) <= 30
      )
  ),

  -- SAFE pattern: only compute aggregates if count > 0
  sleep_based_rhr AS (
    SELECT
      COUNT(*) AS reading_count,
      CASE WHEN COUNT(*) > 0 THEN APPROX_PERCENTILE_CONT(heart_rate, 0.2) ELSE NULL END AS rhr_value,
      CASE WHEN COUNT(*) > 0 THEN AVG(heart_rate) ELSE NULL END AS avg_hr,
      CASE WHEN COUNT(*) > 0 THEN MIN(heart_rate) ELSE NULL END AS min_hr,
      CASE WHEN COUNT(*) > 0 THEN MAX(heart_rate) ELSE NULL END AS max_hr,
      'sleep_based' AS rhr_source
    FROM hr_during_sleep
  ),

  -- all-day hr rows (filtered)
  hr_all_day AS (
    SELECT
      hr."singleHR" AS heart_rate
    FROM heartrate hr
    WHERE hr."singleHR" BETWEEN 40 AND 120
      AND to_timestamp_seconds(CAST(hr.timestamp AS BIGINT))
        BETWEEN (SELECT day_start_utc FROM date_params)
        AND (SELECT day_end_utc FROM date_params)
  ),

  all_day_rhr AS (
    SELECT
      COUNT(*) AS reading_count,
      CASE WHEN COUNT(*) > 0 THEN APPROX_PERCENTILE_CONT(heart_rate, 0.2) ELSE NULL END AS rhr_value,
      CASE WHEN COUNT(*) > 0 THEN AVG(heart_rate) ELSE NULL END AS avg_hr,
      CASE WHEN COUNT(*) > 0 THEN MIN(heart_rate) ELSE NULL END AS min_hr,
      CASE WHEN COUNT(*) > 0 THEN MAX(heart_rate) ELSE NULL END AS max_hr,
      'all_day_fallback' AS rhr_source
    FROM hr_all_day
  ),

  today_rhr AS (
    SELECT
      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.rhr_value END,
        CASE WHEN ad.reading_count >= 10 THEN ad.rhr_value END
      ) AS rhr_value,

      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.reading_count END,
        CASE WHEN ad.reading_count >= 10 THEN ad.reading_count END,
        0
      ) AS reading_count,

      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.rhr_source END,
        CASE WHEN ad.reading_count >= 10 THEN ad.rhr_source END,
        'no_data'
      ) AS rhr_source,

      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.avg_hr END,
        CASE WHEN ad.reading_count >= 10 THEN ad.avg_hr END
      ) AS avg_hr,

      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.min_hr END,
        CASE WHEN ad.reading_count >= 10 THEN ad.min_hr END
      ) AS min_hr,

      COALESCE(
        CASE WHEN sb.reading_count >= 10 THEN sb.max_hr END,
        CASE WHEN ad.reading_count >= 10 THEN ad.max_hr END
      ) AS max_hr,

      sb.reading_count AS sleep_reading_count,
      sb.rhr_value AS sleep_rhr_value,
      ad.reading_count AS all_day_reading_count,
      ad.rhr_value AS all_day_rhr_value
    FROM sleep_based_rhr sb
    CROSS JOIN all_day_rhr ad
  ),

  -- baseline per day: compute daily counts + percentile safely per day
  baseline_rhr_data AS (
    SELECT
      DATE_TRUNC('day', to_timestamp_seconds(CAST(hr.timestamp AS BIGINT))) AS day_utc,
      COUNT(hr."singleHR") AS cnt,
      CASE WHEN COUNT(hr."singleHR") > 0
          THEN APPROX_PERCENTILE_CONT(hr."singleHR", 0.2)
          ELSE NULL END AS daily_rhr
    FROM heartrate hr
    WHERE to_timestamp_seconds(CAST(hr.timestamp AS BIGINT))
      BETWEEN to_timestamp('2025-09-23T07:00:00') AND to_timestamp('2025-09-01T06:59:59')
      AND hr."singleHR" BETWEEN 40 AND 120
    GROUP BY DATE_TRUNC('day', to_timestamp_seconds(CAST(hr.timestamp AS BIGINT)))
  ),

  baseline_rhr AS (
    SELECT
      AVG(daily_rhr) AS baseline_rhr_value,
      STDDEV(daily_rhr) AS baseline_rhr_stddev,
      COUNT(*) AS baseline_days_count
    FROM baseline_rhr_data
  ),

  rhr_analysis AS (
    SELECT
      tr.rhr_value AS today_rhr,
      tr.reading_count,
      tr.rhr_source,
      tr.avg_hr,
      tr.min_hr,
      tr.max_hr,
      tr.sleep_reading_count,
      tr.sleep_rhr_value,
      tr.all_day_reading_count,
      tr.all_day_rhr_value,
      br.baseline_rhr_value,
      br.baseline_rhr_stddev,
      br.baseline_days_count,
      ROUND(tr.rhr_value - br.baseline_rhr_value, 1) AS rhr_change_bpm,

      CASE
        WHEN tr.rhr_value IS NULL THEN 'No Data'
        WHEN br.baseline_days_count < 3 THEN 'Building Baseline'
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= -3 THEN 'Improving'
        WHEN ABS(tr.rhr_value - br.baseline_rhr_value) <= 3 THEN 'Stable'
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= 5 THEN 'Slightly Elevated'
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= 10 THEN 'Elevated'
        ELSE 'High'
      END AS rhr_status,

      CASE
        WHEN tr.rhr_value IS NULL THEN NULL
        WHEN br.baseline_days_count < 3 THEN 75
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= -3 THEN 100
        WHEN ABS(tr.rhr_value - br.baseline_rhr_value) <= 3 THEN 85
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= 5 THEN 70
        WHEN (tr.rhr_value - br.baseline_rhr_value) <= 10 THEN 50
        ELSE 30
      END AS rhr_score
    FROM today_rhr tr
    CROSS JOIN baseline_rhr br
  )

  SELECT
    dp.target_date_utc AS date,
    ROUND(ra.today_rhr, 1) AS resting_heart_rate_bpm,
    ra.rhr_source AS calculation_method,
    ra.reading_count AS hr_readings_used,
    ROUND(ra.avg_hr, 1) AS average_heart_rate,
    ROUND(ra.min_hr, 1) AS minimum_heart_rate,
    ROUND(ra.max_hr, 1) AS maximum_heart_rate,
    ROUND(ra.sleep_rhr_value, 1) AS sleep_based_rhr,
    ra.sleep_reading_count AS sleep_hr_count,
    ROUND(ra.all_day_rhr_value, 1) AS all_day_rhr,
    ra.all_day_reading_count AS all_day_hr_count,
    ROUND(ra.baseline_rhr_value, 1) AS baseline_rhr_7day,
    ROUND(ra.baseline_rhr_stddev, 1) AS baseline_stddev,
    ra.baseline_days_count AS baseline_days,
    ra.rhr_change_bpm AS rhr_change,
    ra.rhr_status AS status,
    ra.rhr_score AS rhr_component_score,
    dp.sleep_window_start_utc,
    dp.sleep_window_end_utc,
    now() AS calculated_at_utc
  FROM date_params dp
  CROSS JOIN rhr_analysis ra;
  "#;
  let rhr_score_result = query("zivaring", rhr_sql_query, None, None).await?;
  println!(
    "Resting Heart Rate Result: {} status: {}",
    rhr_score_result["json_value"], rhr_score_result["status"]
  );

  Ok(())
}

async fn test_vitality_queries() -> Result<(), Box<dyn std::error::Error>> {
  println!("\n=== VITALITY QUERIES COMPONENT ===");
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  let vitality_sql_query = r#"
  WITH date_params AS (
      SELECT 
          CURRENT_TIMESTAMP AT TIME ZONE 'UTC' as now_utc,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') as today_utc,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '1 day' as yesterday_utc,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '7 days' as week_ago,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '14 days' as two_weeks_ago,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '30 days' as month_ago,
          -- Sleep window: yesterday 6pm to today 6pm
          (DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '1 day') + INTERVAL '18 hours' as sleep_window_start,
          DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '18 hours' as sleep_window_end,
          65 as user_age
  ),

  hrv_component AS (
      WITH hrv_data_availability AS (
          SELECT
              COUNT(DISTINCT DATE_TRUNC('day', TO_TIMESTAMP(timestamp))) as days_available
          FROM hrv_table
          WHERE TO_TIMESTAMP(timestamp) >= (SELECT month_ago FROM date_params)
              AND hrv BETWEEN 1 AND 200
      ),

      progressive_baseline AS (
          SELECT
              da.days_available,
              CASE
                  WHEN da.days_available >= 30 THEN 30
                  WHEN da.days_available >= 14 THEN da.days_available
                  WHEN da.days_available >= 7 THEN da.days_available
                  WHEN da.days_available >= 3 THEN da.days_available
                  WHEN da.days_available >= 1 THEN da.days_available
                  ELSE 0
              END as baseline_days,

              CASE
                  WHEN da.days_available >= 30 THEN 1.0
                  WHEN da.days_available >= 14 THEN 0.9
                  WHEN da.days_available >= 7 THEN 0.75
                  WHEN da.days_available >= 3 THEN 0.5
                  WHEN da.days_available >= 1 THEN 0.3
                  ELSE 0.0
              END as confidence_factor,

              CASE
                  WHEN da.days_available >= 30 THEN 'Your recovery'
                  WHEN da.days_available >= 14 THEN 'Your recovery (personalizing)'
                  WHEN da.days_available >= 7 THEN 'Recovery (learning your pattern)'
                  WHEN da.days_available >= 3 THEN 'Early recovery data'
                  WHEN da.days_available >= 1 THEN 'Recovery: Calculating...'
                  ELSE 'No recovery data'
              END as confidence_message
          FROM hrv_data_availability da
      ),

      baseline_stats AS (
          SELECT
              pb.baseline_days,
              pb.confidence_factor,
              pb.confidence_message,
              AVG(h.hrv) as baseline_mean,
              STDDEV(h.hrv) as baseline_stddev
          FROM hrv_table h
          CROSS JOIN progressive_baseline pb
          WHERE TO_TIMESTAMP(h.timestamp) >=
                (SELECT today_utc FROM date_params) - INTERVAL '30' day
              AND h.hrv BETWEEN 1 AND 200
          GROUP BY pb.baseline_days, pb.confidence_factor, pb.confidence_message
      ),

      today_hrv AS (
          SELECT
              AVG(hrv) as today_mean,
              AVG(stress) as today_stress,
              COUNT(*) as readings
          FROM hrv_table
          WHERE DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) =
                (SELECT today_utc FROM date_params)
              AND hrv BETWEEN 1 AND 200
      )
      
      SELECT 
          t.today_mean as current_hrv,
          t.readings as hrv_readings,
          b.baseline_mean,
          b.baseline_stddev,
          b.confidence_factor,
          b.confidence_message,
          t.today_stress,
          
          CASE 
              WHEN b.baseline_stddev > 0 AND t.today_mean IS NOT NULL THEN
                  ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor
              ELSE 0.0
          END as adjusted_z_score,
          
          CASE
              WHEN b.baseline_stddev = 0 OR t.today_mean IS NULL THEN NULL
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 1.0 
                  THEN 100.0
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.5 
                  THEN 85.0 + (((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor - 0.5) * 30.0
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.0 
                  THEN 70.0 + ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor * 30.0
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -0.5 
                  THEN 50.0 + (((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor + 0.5) * 40.0
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -1.0 
                  THEN 25.0 + (((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor + 1.0) * 50.0
              ELSE 25.0
          END as recovery_score_100,
          
          CASE
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 1.0 
                  THEN 'Excellent'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.5 
                  THEN 'Great'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.0 
                  THEN 'Good'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -0.5 
                  THEN 'Fair'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -1.0 
                  THEN 'Low'
              ELSE 'Poor'
          END as recovery_status,
          
          CASE
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 1.0 
                  THEN 'Your body is fully recovered'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.5 
                  THEN 'Strong recovery, ready for activity'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= 0.0 
                  THEN 'Normal recovery level'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -0.5 
                  THEN 'Adequate recovery'
              WHEN ((t.today_mean - b.baseline_mean) / b.baseline_stddev) * b.confidence_factor >= -1.0 
                  THEN 'Below normal recovery'
              ELSE 'Prioritize rest and recovery'
          END as recovery_message
          
      FROM today_hrv t
      CROSS JOIN baseline_stats b
  ),

  sleep_quality_component AS (
      WITH last_night_sleep AS (
          SELECT
              COUNT(*) as total_minutes,
              COUNT(DISTINCT start) as session_count,
              SUM(CASE WHEN quality = 3 THEN 1 ELSE 0 END) as deep_minutes,
              SUM(CASE WHEN quality = 5 THEN 1 ELSE 0 END) as rem_minutes,
              SUM(CASE WHEN quality = 2 THEN 1 ELSE 0 END) as light_minutes,
              SUM(CASE WHEN quality = 1 THEN 1 ELSE 0 END) as awake_minutes
          FROM sleep_table
          WHERE TO_TIMESTAMP(timestamp) >=
                (SELECT sleep_window_start FROM date_params)
              AND TO_TIMESTAMP(timestamp) <
                (SELECT sleep_window_end FROM date_params)
      ),
      
      sleep_scoring AS (
          SELECT 
              ls.*,
              dp.user_age,
              CASE 
                  WHEN dp.user_age >= 65 THEN 420.0
                  WHEN dp.user_age >= 50 THEN 390.0
                  ELSE 420.0
              END as min_optimal,
              CASE 
                  WHEN dp.user_age >= 65 THEN 540.0
                  WHEN dp.user_age >= 50 THEN 480.0
                  ELSE 510.0
              END as max_optimal
          FROM last_night_sleep ls
          CROSS JOIN date_params dp
      ),
      
      sleep_calculation AS (
          SELECT 
              total_minutes,
              session_count,
              deep_minutes,
              rem_minutes,
              light_minutes,
              awake_minutes,
              
              -- Duration scoring
              CASE 
                  WHEN total_minutes BETWEEN min_optimal AND max_optimal THEN 100.0
                  WHEN total_minutes < min_optimal THEN
                      GREATEST(25.0, 100.0 - ((min_optimal - total_minutes) * 0.5))
                  WHEN total_minutes <= 600 THEN 90.0
                  ELSE 75.0
              END as duration_score,
              
              -- Continuity scoring
              CASE 
                  WHEN session_count = 1 THEN 100.0
                  WHEN session_count = 2 THEN 85.0
                  WHEN session_count <= 4 THEN 70.0
                  WHEN session_count <= 6 THEN 55.0
                  ELSE 40.0
              END as continuity_score,
              
              -- Quality based on sleep architecture
              CASE 
                  WHEN total_minutes = 0 THEN 0.0
                  WHEN (deep_minutes + rem_minutes) * 1.0 / total_minutes >= 0.25 THEN 100.0
                  WHEN (deep_minutes + rem_minutes) * 1.0 / total_minutes >= 0.20 THEN 85.0
                  WHEN (deep_minutes + rem_minutes) * 1.0 / total_minutes >= 0.15 THEN 70.0
                  WHEN (deep_minutes + rem_minutes) * 1.0 / total_minutes >= 0.10 THEN 55.0
                  ELSE 40.0
              END as architecture_score,
              
              CASE 
                  WHEN total_minutes BETWEEN min_optimal AND max_optimal THEN 'Perfect sleep duration'
                  WHEN total_minutes < min_optimal - 60 THEN 'Significantly under-slept'
                  WHEN total_minutes < min_optimal THEN 'Less sleep than ideal'
                  WHEN total_minutes > 600 THEN 'Very long sleep - check energy levels'
                  WHEN total_minutes > max_optimal THEN 'Extended sleep detected'
                  ELSE 'Good sleep duration'
              END as duration_message,
              
              CASE 
                  WHEN session_count = 1 THEN 'Excellent sleep continuity'
                  WHEN session_count = 2 THEN 'Minimal interruptions'
                  WHEN session_count <= 4 THEN 'Some sleep fragmentation'
                  WHEN session_count <= 6 THEN 'Fragmented sleep'
                  ELSE 'Highly fragmented sleep'
              END as continuity_message,
              
              CASE 
                  WHEN total_minutes = 0 THEN 'No sleep data'
                  WHEN (deep_minutes + rem_minutes) * 1.0 / total_minutes >= 0.20 
                      THEN CONCAT('Good restorative sleep (', 
                          CAST(ROUND((deep_minutes + rem_minutes) * 100.0 / total_minutes, 0) AS VARCHAR),
                          '% deep+REM)')
                  ELSE CONCAT('Limited restorative sleep (', 
                      CAST(ROUND((deep_minutes + rem_minutes) * 100.0 / total_minutes, 0) AS VARCHAR),
                      '% deep+REM)')
              END as architecture_message,
              
              CASE 
                  WHEN total_minutes < min_optimal - 120 THEN 'very_short_sleep'
                  WHEN total_minutes < min_optimal - 60 THEN 'short_sleep'
                  WHEN total_minutes > 720 THEN 'excessive_sleep'
                  WHEN total_minutes > 600 THEN 'oversleep'
                  ELSE NULL
              END as sleep_alert
              
          FROM sleep_scoring
      )
      
      SELECT 
          total_minutes,
          session_count,
          deep_minutes,
          rem_minutes,
          light_minutes,
          awake_minutes,
          duration_score,
          continuity_score,
          architecture_score,
          duration_message,
          continuity_message,
          architecture_message,
          sleep_alert,
          (duration_score * 0.4 + continuity_score * 0.3 + architecture_score * 0.3) as sleep_quality_score_100
      FROM sleep_calculation
  ),

  sleep_consistency_component AS (
      WITH previous_nights AS (
          SELECT
              DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) as sleep_date,
              COUNT(*) as night_minutes,
              COUNT(DISTINCT start) as sessions
          FROM sleep_table
          WHERE TO_TIMESTAMP(timestamp) >=
                (SELECT week_ago FROM date_params)
              AND TO_TIMESTAMP(timestamp) <
                (SELECT sleep_window_start FROM date_params)
          GROUP BY DATE_TRUNC('day', TO_TIMESTAMP(timestamp))
      ),
      
      consistency_stats AS (
          SELECT 
              COUNT(*) as nights_count,
              AVG(night_minutes) as avg_duration,
              STDDEV(night_minutes) as duration_variance
          FROM previous_nights
      ),
      
      consistency_scoring AS (
          SELECT 
              *,
              CASE 
                  WHEN avg_duration BETWEEN 420 AND 480 THEN 100.0
                  WHEN avg_duration BETWEEN 360 AND 540 THEN 80.0
                  ELSE 60.0
              END as duration_score,
              
              CASE 
                  WHEN duration_variance <= 30 THEN 100.0
                  WHEN duration_variance <= 60 THEN 75.0
                  ELSE 50.0
              END as variance_score,
              
              CASE 
                  WHEN duration_variance <= 30 
                      THEN 'Excellent sleep consistency this week'
                  WHEN duration_variance <= 60 
                      THEN 'Good sleep routine maintained'
                  WHEN duration_variance <= 90 
                      THEN 'Sleep schedule somewhat variable'
                  ELSE 'Irregular sleep pattern - try consistent bedtime'
              END as consistency_message
              
          FROM consistency_stats
      )
      
      SELECT 
          nights_count,
          avg_duration,
          duration_variance,
          consistency_message,
          CASE 
              WHEN nights_count >= 3 THEN 
                  (duration_score * 0.5 + variance_score * 0.5)
              ELSE NULL
          END as consistency_score_100,
          CASE 
              WHEN nights_count < 3 THEN 'building_sleep_patterns'
              ELSE NULL
          END as consistency_alert
      FROM consistency_scoring
  ),

  rhr_component AS (
      WITH daily_hr_readings AS (
          SELECT
              DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) as hr_date,
              "singleHR"
          FROM heartrate
          WHERE TO_TIMESTAMP(timestamp) >=
                (SELECT week_ago FROM date_params)
              AND "singleHR" BETWEEN 40 AND 120
      ),

      daily_rhr AS (
          SELECT
              hr_date,
              APPROX_PERCENTILE_CONT("singleHR", 0.2) as rhr_20th
          FROM daily_hr_readings
          GROUP BY hr_date
      ),

      rhr_analysis AS (
          SELECT
              AVG(CASE WHEN hr_date < (SELECT today_utc FROM date_params)
                  THEN rhr_20th END) as baseline_rhr,
              MAX(CASE WHEN hr_date = (SELECT today_utc FROM date_params)
                  THEN rhr_20th END) as today_rhr
          FROM daily_rhr
      ),

      rhr_scoring AS (
          SELECT
              baseline_rhr,
              today_rhr,
              today_rhr - baseline_rhr as rhr_change,

              CASE
                  WHEN today_rhr - baseline_rhr <= -3 THEN 100.0
                  WHEN today_rhr - baseline_rhr <= 3 THEN 85.0
                  WHEN today_rhr - baseline_rhr <= 5 THEN 70.0
                  WHEN today_rhr - baseline_rhr <= 10 THEN 50.0
                  ELSE 30.0
              END as rhr_score_100,

              CASE
                  WHEN today_rhr - baseline_rhr <= -3 THEN 'Improving'
                  WHEN today_rhr - baseline_rhr <= 3 THEN 'Stable'
                  WHEN today_rhr - baseline_rhr <= 5 THEN 'Slightly Elevated'
                  WHEN today_rhr - baseline_rhr <= 10 THEN 'Elevated'
                  ELSE 'High'
              END as rhr_status,

              CASE
                  WHEN today_rhr - baseline_rhr <= -3 THEN 'Heart rate improving'
                  WHEN today_rhr - baseline_rhr <= 3 THEN 'Heart rate stable'
                  WHEN today_rhr - baseline_rhr <= 5 THEN 'Slightly elevated heart rate'
                  WHEN today_rhr - baseline_rhr <= 10 THEN 'Heart rate elevated - monitor'
                  ELSE 'Significant heart rate elevation'
              END as rhr_message,

              CASE
                  WHEN today_rhr - baseline_rhr > 10 THEN 'rhr_high_alert'
                  WHEN today_rhr - baseline_rhr > 5 THEN 'rhr_elevated'
                  ELSE NULL
              END as rhr_alert

          FROM rhr_analysis
      )

      SELECT * FROM rhr_scoring
  ),

  safety_modifiers AS (
      WITH temp_analysis AS (
          SELECT
              AVG(CASE WHEN DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) =
                  (SELECT today_utc FROM date_params)
                  THEN temperature END) as today_temp,
              AVG(CASE WHEN DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) <
                  (SELECT today_utc FROM date_params)
                  THEN temperature END) as baseline_temp
          FROM temperature_readings
          WHERE TO_TIMESTAMP(timestamp) >=
                (SELECT two_weeks_ago FROM date_params)
              AND temperature BETWEEN 30 AND 40
      ),

      temp_scoring AS (
          SELECT
              today_temp - baseline_temp as temp_elevation,

              CASE
                  WHEN today_temp - baseline_temp > 1.5 THEN 50.0
                  WHEN today_temp - baseline_temp > 1.0 THEN 70.0
                  WHEN today_temp - baseline_temp > 0.5 THEN 85.0
                  ELSE 100.0
              END as temp_cap,

              CASE
                  WHEN today_temp - baseline_temp > 1.5 THEN 'critical'
                  WHEN today_temp - baseline_temp > 1.0 THEN 'high'
                  WHEN today_temp - baseline_temp > 0.5 THEN 'medium'
                  ELSE NULL
              END as temp_alert_level,

              CASE
                  WHEN today_temp - baseline_temp > 1.5
                      THEN 'Temperature elevated - possible illness'
                  WHEN today_temp - baseline_temp > 1.0
                      THEN 'Slight temperature elevation'
                  WHEN today_temp - baseline_temp > 0.5
                      THEN 'Minor temperature elevation'
                  ELSE NULL
              END as temp_message

          FROM temp_analysis
      ),

      spo2_analysis AS (
          SELECT
              AVG("automaticSpo2Data") as overnight_avg,
              COUNT(CASE WHEN "automaticSpo2Data" < 90 THEN 1 END) as dips_below_90
          FROM spo2_readings
          WHERE DATE_TRUNC('day', TO_TIMESTAMP(timestamp)) =
                (SELECT today_utc FROM date_params)
              AND "automaticSpo2Data" BETWEEN 70 AND 100
      ),
      
      spo2_scoring AS (
          SELECT 
              overnight_avg,
              dips_below_90,
              
              CASE 
                  WHEN overnight_avg < 88 THEN 40.0
                  WHEN overnight_avg < 92 THEN 60.0
                  WHEN dips_below_90 > 10 THEN 70.0
                  ELSE 100.0
              END as spo2_cap,
              
              CASE 
                  WHEN overnight_avg < 88 THEN 'critical'
                  WHEN overnight_avg < 92 THEN 'high'
                  WHEN dips_below_90 > 10 THEN 'medium'
                  ELSE NULL
              END as spo2_alert_level,
              
              CASE 
                  WHEN overnight_avg < 88 
                      THEN 'Low oxygen levels detected'
                  WHEN overnight_avg < 92 
                      THEN 'Oxygen levels below normal'
                  WHEN dips_below_90 > 10 
                      THEN 'Multiple oxygen dips during sleep'
                  ELSE NULL
              END as spo2_message
              
          FROM spo2_analysis
      )
      
      SELECT 
          t.temp_elevation,
          t.temp_cap,
          t.temp_alert_level,
          t.temp_message,
          s.overnight_avg as spo2_avg,
          s.dips_below_90,
          s.spo2_cap,
          s.spo2_alert_level,
          s.spo2_message
      FROM temp_scoring t
      CROSS JOIN spo2_scoring s
  ),

  vitality_calculation AS (
      SELECT 
          COALESCE(h.recovery_score_100, 0.0) as recovery_score,
          COALESCE(sq.sleep_quality_score_100, 0.0) as sleep_score,
          COALESCE(sc.consistency_score_100, 0.0) as consistency_score,
          COALESCE(r.rhr_score_100, 0.0) as rhr_score,
          
          CASE WHEN h.recovery_score_100 IS NOT NULL THEN 0.35 ELSE 0.0 END +
          CASE WHEN sq.sleep_quality_score_100 IS NOT NULL THEN 0.30 ELSE 0.0 END +
          CASE WHEN sc.consistency_score_100 IS NOT NULL THEN 0.20 ELSE 0.0 END +
          CASE WHEN r.rhr_score_100 IS NOT NULL THEN 0.15 ELSE 0.0 END as total_weight,
          
          -- Sleep details
          sq.total_minutes as sleep_minutes,
          sq.session_count as sleep_sessions,
          sq.deep_minutes,
          sq.rem_minutes,
          sq.light_minutes,
          sq.awake_minutes,
          
          -- HRV details
          h.hrv_readings,
          h.current_hrv,
          h.baseline_mean as baseline_hrv,
          h.adjusted_z_score as hrv_z_score,
          
          -- RHR details
          r.today_rhr,
          r.baseline_rhr,
          r.rhr_change,
          
          -- Component messages
          h.recovery_status,
          h.recovery_message,
          h.confidence_message,
          sq.duration_message,
          sq.continuity_message,
          sq.architecture_message,
          sq.sleep_alert,
          sc.consistency_message,
          sc.consistency_alert,
          r.rhr_status,
          r.rhr_message,
          r.rhr_alert,
          
          -- Safety
          sm.temp_cap,
          sm.temp_alert_level,
          sm.temp_message,
          sm.spo2_cap,
          sm.spo2_alert_level,
          sm.spo2_message,
          
          75.0 as yesterday_score
          
      FROM hrv_component h
      CROSS JOIN sleep_quality_component sq
      LEFT JOIN sleep_consistency_component sc ON true
      LEFT JOIN rhr_component r ON true
      CROSS JOIN safety_modifiers sm
  )

  SELECT 
      -- Vitality Score
      CAST(LEAST(
          CASE 
              WHEN total_weight > 0 THEN
                  ROUND((
                      recovery_score * 0.35 +
                      sleep_score * 0.30 +
                      consistency_score * 0.20 +
                      rhr_score * 0.15
                  ) / total_weight * 100.0)
              ELSE NULL
          END,
          temp_cap,
          spo2_cap
      ) AS INTEGER) as vitality_score,
      
      CASE 
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 85 THEN 'Excellent'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 70 THEN 'Good'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 55 THEN 'Fair'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 40 THEN 'Low'
          ELSE 'Poor'
      END as vitality_category,
      
      CASE 
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 85 THEN 'Excellent vitality!'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 70 THEN 'Good energy today'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 55 THEN 'Moderate vitality'
          WHEN LEAST(
              ROUND((recovery_score * 0.35 + sleep_score * 0.30 + 
                    consistency_score * 0.20 + rhr_score * 0.15) / total_weight * 100.0),
              temp_cap, spo2_cap) >= 40 THEN 'Low energy today'
          ELSE 'Focus on recovery'
      END as primary_message,
      
      -- Component Breakdown
      CAST(ROUND(recovery_score, 1) AS DOUBLE) as recovery_score,
      CAST(ROUND(recovery_score * 0.35, 1) AS DOUBLE) as recovery_points,
      CAST(ROUND(sleep_score, 1) AS DOUBLE) as sleep_score,
      CAST(ROUND(sleep_score * 0.30, 1) AS DOUBLE) as sleep_points,
      CAST(ROUND(consistency_score, 1) AS DOUBLE) as consistency_score,
      CAST(ROUND(consistency_score * 0.20, 1) AS DOUBLE) as consistency_points,
      CAST(ROUND(rhr_score, 1) AS DOUBLE) as rhr_score,
      CAST(ROUND(rhr_score * 0.15, 1) AS DOUBLE) as rhr_points,
      
      -- Sleep Details
      sleep_minutes,
      CAST(ROUND(sleep_minutes / 60.0, 1) AS DOUBLE) as sleep_hours,
      sleep_sessions,
      deep_minutes,
      rem_minutes,
      light_minutes,
      awake_minutes,
      CASE WHEN sleep_minutes > 0 
          THEN CAST(ROUND((deep_minutes + rem_minutes) * 100.0 / sleep_minutes, 1) AS DOUBLE)
          ELSE 0.0 
      END as restorative_pct,
      
      -- HRV Details
      hrv_readings,
      CAST(ROUND(current_hrv, 1) AS DOUBLE) as current_hrv,
      CAST(ROUND(baseline_hrv, 1) AS DOUBLE) as baseline_hrv,
      CAST(ROUND(hrv_z_score, 2) AS DOUBLE) as hrv_z_score,
      
      -- RHR Details
      CAST(ROUND(today_rhr, 1) AS DOUBLE) as today_rhr,
      CAST(ROUND(baseline_rhr, 1) AS DOUBLE) as baseline_rhr,
      CAST(ROUND(rhr_change, 1) AS DOUBLE) as rhr_change,
      
      -- Messages
      recovery_status,
      recovery_message,
      confidence_message,
      duration_message as sleep_duration_message,
      continuity_message as sleep_continuity_message,
      architecture_message as sleep_architecture_message,
      consistency_message,
      rhr_status,
      rhr_message,
      
      -- Safety
      temp_message,
      spo2_message,
      
      -- Metadata
      CAST(ROUND(total_weight * 100, 0) AS INTEGER) as data_completeness_pct,
      CURRENT_TIMESTAMP AT TIME ZONE 'UTC' as calculated_at_utc
      
  FROM vitality_calculation;
  "#;

  let vitality_result = query("zivaring", vitality_sql_query, None, None).await?;
  println!("vitality Result: {} status: {}", vitality_result["json_value"], vitality_result["status"]);

  Ok(())
}

async fn ziva_app_queries() -> Result<(), Box<dyn std::error::Error>> {
  println!("\n=== ZIVA APP QUERIES COMPONENT ===");
  const STORAGE_PATH: &str = "tmp";
  const USERNAME: &str = "ahmed_test";
  let _ = init_timon(STORAGE_PATH, 10080, USERNAME).unwrap();

  let query_x = r#"
  WITH transformed AS (
      SELECT 
        step,
        calories,
        TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles', '%Y.%m.%d %H:%M:%S') AS date,
        TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles', '%Y.%m.%d') AS day,
        TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles', '%H') AS hour
      FROM activitydetails 
      WHERE timestamp BETWEEN '1758499200' AND '1758585599'
    )
    SELECT 
      day, 
      hour, 
      MAX(date) AS date,
      SUM(calories) AS calories,
      SUM(step) AS value
    FROM transformed
    GROUP BY day, hour
    ORDER BY day, hour;
  "#;

  let result_x = query("zivaring", &query_x, None, None).await?;
  println!("result_x: {} status: {}", result_x["json_value"], result_x["status"]);

  Ok(())
}
