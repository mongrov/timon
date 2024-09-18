mod datafusion_query;
use std::collections::HashMap;
pub use datafusion_query::{
  datafusion_querier,
  read_parquet_file,
  write_json_to_parquet,
  aggregate_monthly_parquet,
  cloud_sync,
};

fn main() {
  // const TABLE_NAME: &str = "temperature";
  // let file_path = format!("/tmp/timon/{TABLE_NAME}_2024-07-11.parquet").to_string();

  // **************** write_json_to_parquet **************** //
  // let json_data = r#"
  // [
  //     {"name": "Muzan", "age": 45},
  //     {"name": "Akaza", "age": 29},
  //     {"name": "Tanjiro", "age": 27}
  // ]
  // "#;

  // match write_json_to_parquet(&file_path, json_data) {
  //   Ok(_) => println!("Successfully wrote JSON data to Parquet file {:?}", json_data),
  //   Err(e) => eprintln!("Failed to write JSON data to Parquet file: {:?}", e),
  // }

  // **************** read_parquet_file **************** //    
  // match read_parquet_file(&file_path) {
  //   Ok(result) => {
  //     for record in result {
  //       println!("{}", record);
  //     }
  //   }
  //   Err(e) => eprintln!("Failed to read the Parquet file: {:?}", e),
  // }

  // **************** datafusion_query **************** //
  // "/tmp/greptimedb/data/greptime/public/1025/1025_0000000000/*.parquet"
  // "/tmp/greptimedb/data/greptime/public/1024/1024_0000000000/*.parquet"
  // let sql_query = format!("select * from {TABLE_NAME} LIMIT 100").to_string();
  // let sql_query2 = format!("SELECT * FROM {TABLE_NAME} ORDER BY timestamp ASC LIMIT 10");

  // let parquet_paths: Vec<String> = vec![
  //   format!("/tmp/timon/android/{}_2024-07-15.parquet", TABLE_NAME),
  //   format!("/tmp/timon/android/{}_2024-07-12.parquet", TABLE_NAME),
  //   format!("/tmp/timon/android/{}_2024-07-26.parquet", TABLE_NAME),
  // ];
  // let files_paths: Vec<&str> = parquet_paths.iter().map(|s| s.as_str()).collect();

  // let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
  // runtime.block_on(async {
  //   let df_result = datafusion_querier(files_paths, TABLE_NAME, &sql_query2, false).await;
  //   match df_result {
  //     Ok(cloud_sync::DataFusionOutput::Json(s)) => println!("Json result: {}", s),
  //     Ok(cloud_sync::DataFusionOutput::DataFrame(df)) => {
  //       let df_batches = df.collect().await;
  //       for batch in df_batches.unwrap() {
  //         println!("{:?}", batch);
  //       }
  //     },
  //     Err(e) => eprintln!("Error: {:?}", e),
  //   }
  // });

  // tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
  //   aggregate_monthly_parquet("/tmp/timon", "temperature").await.unwrap();
  //   println!("aggregate_monthly_parquet() called!");
  // });

  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    let range = HashMap::from([("start_date", "2024-07-01"), ("end_date", "2024-08-01")]);
    let bucket_name = "timon";
    let file_name = "temperature";
    let sql_query = "SELECT * FROM temperature LIMIT 25";
    let df_result = cloud_sync::CloudQuerier::query_bucket(&bucket_name, &file_name, range, &sql_query, true).await;
    
    println!("datafusion_query {:?}", df_result);
  });

  // tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
  //   let result = cloud_sync::CloudQuerier::sink_data_to_bucket(
  //     "/tmp/timon/temperature_2024-08-13.parquet",
  //     "temperature.parquet"
  //   ).await;
  //   println!("sink_data_to_bucket result: {:?}", result);
  // });
}
