mod timon_engine;
#[cfg(feature = "s3_sync")]
use crate::timon_engine::{init_bucket, query_bucket, sink_monthly_parquet};
pub use timon_engine::{create_database, create_table, delete_database, delete_table, init_timon, insert, list_databases, list_tables, query};

#[cfg(feature = "s3_sync")]
async fn test_s3_sync() {
  let bucket_endpoint = "http://localhost:9000";
  let bucket_name = "timon";
  let access_key_id = "ahmed";
  let secret_access_key = "ahmed1234";
  let init_bucket_result = init_bucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key).unwrap();
  println!("init_bucket_result: {}", init_bucket_result);

  let range = std::collections::HashMap::from([("start_date", "2024-07-01"), ("end_date", "2024-08-01")]);
  let sql_query = "SELECT * FROM temperature LIMIT 25";
  let df_result = query_bucket(range, &sql_query).await.unwrap();
  println!("query_bucket {:?}", df_result);

  let sink_monthly_parquet_result = sink_monthly_parquet("test", "temperature").await;
  println!("{}", sink_monthly_parquet_result.unwrap());
}

fn main() {
  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    const STORAGE_PATH: &str = "/tmp/timon";
    let timon_result = init_timon(STORAGE_PATH).unwrap();
    println!("init_timon -> {}", timon_result);

    const DATABASE_NAME: &str = "test";
    let database_result = create_database(DATABASE_NAME);
    println!("create_database -> {}", database_result.unwrap());

    let table_schema = r#"
    {
    "temperature": { "type": "float", "required": true },
    "humidity": { "type": "float", "required": true },
    "status": { "type": "string", "required": false }
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
          "timestamp": 1728908268674,
          "humidity": 5.0,
          "temperature": 22.0,
          "status": "active"
        }
      ]
    "#
    .to_string();
    let insertion_result = insert(DATABASE_NAME, "temperature", &json_data);
    println!("insertion_result: {}", insertion_result.unwrap());

    let range: std::collections::HashMap<&str, &str> = std::collections::HashMap::from([("start_date", "2024-10-10"), ("end_date", "2024-11-11")]);
    let sql_query = format!("SELECT * FROM temperature ORDER BY timestamp ASC LIMIT 25");
    let query_result = query(DATABASE_NAME, range, &sql_query).await;
    println!("query_result: {}", query_result.unwrap());

    let delete_table_result = delete_table(DATABASE_NAME, "iot").unwrap();
    println!("delete_table_result -> {}", delete_table_result);
    let delete_database_result = delete_database(DATABASE_NAME).unwrap();
    println!("delete_database_result -> {}", delete_database_result);

    #[cfg(feature = "s3_sync")]
    test_s3_sync().await;
  });
}
