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
      "date": {
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
  let query_result = query(DATABASE_NAME, &sql_query, None).await;
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
      "date": {
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
      "date": {
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
      "date": {
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
      "date": {
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
      "date":{
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
      "date": {
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
        let date_str = reading["date"].as_str().unwrap_or("2025.01.01 00:00:00");
        let naive_datetime = chrono::NaiveDateTime::parse_from_str(date_str, "%Y.%m.%d %H:%M:%S").unwrap_or_default();
        let date = DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc);
        json!({
          "date": date,
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
  let activity_details_result = query(DATABASE_NAME, &activity_details_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Activity details {} (Time taken: {:.3} seconds)",
    activity_details_result["status"],
    duration.as_secs_f64()
  );

  // Query SPO2 readings
  let start_time = Instant::now();
  let spo2_query = format!(r#"SELECT * FROM spo2_readings"#);
  let spo2_result = query(DATABASE_NAME, &spo2_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "SPO2 readings {} (Time taken: {:.3} seconds)",
    spo2_result["status"],
    duration.as_secs_f64()
  );

  // Query heart rate readings
  let start_time = Instant::now();
  let hr_query = format!(r#"SELECT * FROM heartrate"#);
  let hr_result = query(DATABASE_NAME, &hr_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Heart rate readings {} (Time taken: {:.3} seconds)",
    hr_result["status"],
    duration.as_secs_f64()
  );

  // Query HRV readings
  let start_time = Instant::now();
  let hrv_query = format!(r#"SELECT * FROM hrv_table"#);
  let hrv_result = query(DATABASE_NAME, &hrv_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "HRV readings {} (Time taken: {:.3} seconds)",
    hrv_result["status"],
    duration.as_secs_f64()
  );

  // Query temperature readings
  let start_time = Instant::now();
  let temp_query = format!(r#"SELECT * FROM temperature_readings"#);
  let temp_result = query(DATABASE_NAME, &temp_query, None).await?;
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
  let avg_hr_result = query(DATABASE_NAME, &avg_hr_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Average heart rate {} (Time taken: {:.3} seconds)",
    avg_hr_result["status"],
    duration.as_secs_f64()
  );

  // Query for max SPO2
  let start_time = Instant::now();
  let max_spo2_query = format!(r#"SELECT * FROM spo2_readings"#);
  let max_spo2_result = query(DATABASE_NAME, &max_spo2_query, None).await?;
  let duration = start_time.elapsed();
  println!(
    "Max SPO2: {} (Time taken: {:.3} seconds)",
    max_spo2_result["status"],
    duration.as_secs_f64()
  );

  // Query for stress levels over time
  let start_time = Instant::now();
  let stress_query = format!(r#"SELECT * FROM hrv_table"#);
  let stress_result = query(DATABASE_NAME, &stress_query, None).await?;
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
  let _ = init_timon(STORAGE_PATH, 1440, USERNAME).unwrap();

  let start_time = Instant::now();
  let sql_query = "SELECT * FROM activitydetails JOIN spo2_readings ON to_char(to_timestamp(activitydetails.date), 'YYYY-MM-DD') = to_char(to_timestamp(spo2_readings.date), 'YYYY-MM-DD') LIMIT 100";
  let result = query(DATABASE_NAME, sql_query, None).await?;
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
      "date": {
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
  let result = query(DATABASE_NAME, QUERY, None).await?;
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

  const QUERY_2: &str = "SELECT COUNT(*) AS total FROM activitydetails WHERE date BETWEEN '1758499200' AND '1758585599'";
  let result = query(DATABASE_NAME, QUERY_2, None).await?;
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
      "date": {
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
  let query_result = query(DATABASE_NAME, &sql_query, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  let sql_query = format!(r#"SELECT COUNT(*) as total FROM battery_readings"#);
  let query_result = query(DATABASE_NAME, &sql_query, None).await;
  println!("query_result count: {}", query_result.unwrap()["json_value"]);

  let sql_query = format!(r#"SELECT MIN(battery_level) as min_battery, MAX(battery_level) as max_battery FROM battery_readings"#);
  let query_result = query(DATABASE_NAME, &sql_query, None).await;
  println!("query_result: {}", query_result.unwrap()["json_value"]);

  Ok(())
}
