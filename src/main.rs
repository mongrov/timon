mod read_parquet_file;
mod write_parquet_file;
pub use read_parquet_file::read_parquet_file;
pub use write_parquet_file::write_json_to_parquet;

fn main() {
  // **************** read_parquet_file **************** //
  let file_path = "/tmp/greptimedb/data/greptime/public/1024/1024_0000000000/9d5ef3a4-9499-4da5-87ac-00a666ae70e3.parquet";    
  match read_parquet_file(file_path) {
    Ok(result) => {
      for record in result {
        println!("{}", record);
      }
    }
    Err(e) => eprintln!("Failed to read the Parquet file: {:?}", e),
  }

  // **************** write_json_to_parquet **************** //
  let file_path = "/tmp/greptimedb/sodium/example.parquet";

  let json_data = r#"
  [
      {"name": "Alice", "age": 30},
      {"name": "Bob", "age": 25},
      {"name": "Charlie", "age": 35}
  ]
  "#;

  match write_json_to_parquet(file_path, json_data) {
      Ok(_) => println!("Successfully wrote JSON data to Parquet file {:?}", json_data),
      Err(e) => eprintln!("Failed to write JSON data to Parquet file: {:?}", e),
  }
}
