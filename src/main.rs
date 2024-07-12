mod datafusion_query;
pub use datafusion_query::{datafusion_querier, read_parquet_file, write_json_to_parquet};

fn main() {
  const TABLE_NAME: &str = "temperature";
  let file_path = format!("/tmp/timon/{TABLE_NAME}_2024-07-11.parquet").to_string();

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
  let sql_query = format!("select * from {TABLE_NAME} where age > 28").to_string();

  tokio::runtime::Runtime::new().expect("Failed to create runtime").block_on(async {
    let df_result = datafusion_querier(&file_path, TABLE_NAME, &sql_query).await;
    println!("datafusion_query {:?}", df_result);
  })
}
