mod datafusion_query;

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
  use crate::datafusion_query::{cloud_sync::DataFusionOutput, datafusion_querier, read_parquet_file, write_json_to_parquet};
  use jni::objects::{JClass, JObject, JString, JValue};
  use jni::sys::jstring;
  use jni::JNIEnv;
  use std::collections::HashMap;
  use tokio::runtime::Runtime;

  #[no_mangle]
  pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_readParquetFile(
    mut env: JNIEnv,
    _class: JClass,
    file_path: JString,
  ) -> jstring {
    let rust_string: String = env.get_string(&file_path).expect("Couldn't get java string!").into();

    match read_parquet_file(&rust_string) {
      Ok(json_records) => {
        let json_str = serde_json::to_string(&json_records).unwrap();
        let output = env.new_string(json_str).expect("Couldn't create java string!");
        output.into_raw() // Use into_raw to return the jstring
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error reading Parquet file: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw() // Use into_raw to return the jstring
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_writeJsonToParquet(
    mut env: JNIEnv,
    _class: JClass,
    file_path: JString,
    json_data: JString,
  ) -> jstring {
    let rust_file_path: String = env.get_string(&file_path).expect("Couldn't get java string!").into();
    let rust_json_data: String = env.get_string(&json_data).expect("Couldn't get java string!").into();

    match write_json_to_parquet(&rust_file_path, &rust_json_data) {
      Ok(_) => {
        let success_message = env
          .new_string("Successfully wrote JSON data to Parquet file")
          .expect("Couldn't create java string!");
        success_message.into_raw() // Use into_raw to return the jstring
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error writing JSON data to Parquet file: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw() // Use into_raw to return the jstring
      }
    }
  }

  fn get_date_range_value(env: &mut JNIEnv, date_range: &JObject, key: &str) -> String {
    // Create the key as a `JString`
    let j_key: JString = env.new_string(key).expect("Couldn't create key string");

    // Convert the JString to JObject
    let j_key_obj: JObject = j_key.into();

    // Define the method name and signature for the Java `get` method of the Map
    let method_name = "get";
    let method_sig = "(Ljava/lang/Object;)Ljava/lang/Object;";

    // Call the `get` method on the `date_range` map object (pass the reference directly)
    let j_value = env
      .call_method(
        date_range, // Do not dereference `*date_range`
        method_name,
        method_sig,
        &[JValue::from(&j_key_obj)], // Pass reference to JObject here
      )
      .expect("Failed to call get method")
      .l() // Get the returned JObject (which should be a String)
      .expect("Invalid value returned from get method");

    // Convert the result to a Rust string
    let rust_value: String = env
      .get_string(&JString::from(j_value))
      .expect("Failed to convert Java String to Rust String")
      .into();

    rust_value
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_datafusionQuerier(
    mut env: JNIEnv,
    _class: JClass,
    base_dir: JString,
    date_range: JObject,
    sql_query: JString,
  ) -> jstring {
    // Convert Java strings to Rust strings
    let rust_base_dir: String = env.get_string(&base_dir).expect("Couldn't get java string!").into();
    let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get java string!").into();

    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    match Runtime::new()
      .unwrap()
      .block_on(datafusion_querier(&rust_base_dir, rust_date_range, &rust_sql_query, true))
    {
      Ok(output) => {
        let json_string = match output {
          DataFusionOutput::Json(s) => s,
          DataFusionOutput::DataFrame(_df) => {
            format!("DataFrame output is not directly convertible to string")
          }
        };
        let java_string = env.new_string(json_string).expect("Couldn't create java string!");
        java_string.into_raw()
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error querying Parquet files: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw() // Use into_raw to return the jstring
      }
    }
  }
}
