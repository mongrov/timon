use std::fs::File;
use std::path::Path;
use std::ffi::{CStr, CString};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use serde_json::json;
use parquet::data_type::{AsBytes, ByteArray, Decimal};
use base64;
use serde_json::Value;

#[no_mangle]
pub extern "C" fn readParquetFile(file_path: *const libc::c_char) -> *mut libc::c_char {
  let c_str = unsafe { CStr::from_ptr(file_path) };
  let file_path = c_str.to_str().unwrap();
  let result = read_parquet_file(file_path);
  
  match result {
    Ok(json_records) => {
      let json_str = serde_json::to_string(&json_records).unwrap();
      let c_string = CString::new(json_str).unwrap();
      c_string.into_raw()
    },
    Err(_) => CString::new("Error reading Parquet file").unwrap().into_raw(),
  }
}

pub fn read_parquet_file(file_path: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
  let file = File::open(&Path::new(file_path))?;
  let reader = SerializedFileReader::new(file)?;
  let mut iter = reader.get_row_iter(None)?;
  
  let mut json_records = Vec::new();

  while let Some(record_result) = iter.next() {
    match record_result {
      Ok(record) => {
        // Convert the record to a JSON-like format
        let json_record = row_to_json(&record);
        json_records.push(json_record);
      },
      Err(_) => {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error reading record")));
      }
    }
  }
  Ok(json_records)
}

fn row_to_json(row: &Row) -> serde_json::Value {
  let mut json_map = serde_json::Map::new();

  for (name, value) in row.get_column_iter() {
    let json_value = match value {
      Field::Null => serde_json::Value::Null,
      Field::Bool(b) => json!(*b),
      Field::Byte(b) => json!(*b),
      Field::Short(s) => json!(*s),
      Field::Int(i) => json!(*i),
      Field::Long(l) => json!(*l),
      Field::UByte(b) => json!(*b),
      Field::UShort(s) => json!(*s),
      Field::UInt(i) => json!(*i),
      Field::ULong(l) => json!(*l),
      Field::Float(f) => json!(*f),
      Field::Double(d) => json!(*d),
      Field::Str(s) => json!(s),
      Field::Bytes(b) => json!(byte_array_to_base64(b)),
      Field::TimestampMicros(t) => json!(t),
      Field::TimestampMillis(t) => json!(t),
      Field::Decimal(d) => json!(decimal_to_string(d)),
      Field::Group(g) => row_to_json(g),
      // Add additional handling for other types if needed
      _ => serde_json::Value::Null,
    };

    json_map.insert(name.clone(), json_value);
  }

  serde_json::Value::Object(json_map)
}

fn byte_array_to_base64(byte_array: &ByteArray) -> String {
  base64::encode(byte_array.data())
}

fn decimal_to_string(decimal: &Decimal) -> String {
  let value = decimal.as_bytes();
  // Assuming the decimal's precision and scale are known
  // You'll need to adapt this to your specific use case
  let precision = decimal.precision();
  let scale = decimal.scale();
  let mut result = String::new();

  // Convert the bytes to a string representation
  if value[0] & 0x80 != 0 {
    result.push('-');
  }

  let int_part = &value[..precision as usize - scale as usize];
  let frac_part = &value[precision as usize - scale as usize..];

  result.push_str(&format!("{:?}", int_part));
  if scale > 0 {
    result.push('.');
    result.push_str(&format!("{:?}", frac_part));
  }

  result
}
