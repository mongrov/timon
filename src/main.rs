mod read_parquet_file;
pub use read_parquet_file::read_parquet_file;

fn main() {
  let file_path = "/tmp/greptimedb/data/greptime/public/1024/1024_0000000000/9d5ef3a4-9499-4da5-87ac-00a666ae70e3.parquet";    
  match read_parquet_file(file_path) {
    Ok(result) => {
      for record in result {
        println!("{}", record);
      }
    }
    Err(e) => eprintln!("Failed to read the Parquet file: {:?}", e),
  }
}
