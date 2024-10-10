pub mod timon_engine;

#[cfg(target_os = "android")]
pub mod android {
  use crate::timon_engine::{create_database, create_table, delete_database, delete_table, init_timon, insert, list_databases, list_tables, query};
  #[cfg(feature = "s3_sync")]
  use crate::timon_engine::{init_bucket, query_bucket, sink_monthly_parquet};
  use jni::objects::{JClass, JObject, JString, JValue};
  use jni::sys::jstring;
  use jni::JNIEnv;
  use std::collections::HashMap;
  use tokio::runtime::Runtime;

  // ******************************** File Storage ********************************
  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_initTimon(mut env: JNIEnv, _class: JClass, storage_path: JString) -> jstring {
    let rust_storage_path: String = env.get_string(&storage_path).expect("Couldn't get java string!").into();

    match init_timon(&rust_storage_path) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to initialize Timon: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_createDatabase(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();

    match create_database(&rust_db_name) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to create database: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_createTable(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    match create_table(&rust_db_name, &rust_table_name) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to create table: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_listDatabases(env: JNIEnv, _class: JClass) -> jstring {
    match list_databases() {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to list databases: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_listTables(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();

    match list_tables(&rust_db_name) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to list tables: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_deleteDatabase(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();

    match delete_database(&rust_db_name) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to delete database: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_deleteTable(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    match delete_table(&rust_db_name, &rust_table_name) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to delete table: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_insert(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
    json_data: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();
    let rust_json_data: String = env.get_string(&json_data).expect("Couldn't get java string!").into();

    match insert(&rust_db_name, &rust_table_name, &rust_json_data) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error writing JSON data to Parquet file: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw()
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
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_query(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    date_range: JObject,
    sql_query: JString,
  ) -> jstring {
    // Convert Java strings to Rust strings
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get java string!").into();

    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    match Runtime::new().unwrap().block_on(query(&rust_db_name, rust_date_range, &rust_sql_query)) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error querying Parquet files: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw()
      }
    }
  }

  // ******************************** S3 Compatible Storage ********************************
  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_initBucket(
    mut env: JNIEnv,
    _class: JClass,
    bucket_endpoint: JString,
    bucket_name: JString,
    access_key_id: JString,
    secret_access_key: JString,
  ) -> jstring {
    let rust_bucket_endpoint: String = env.get_string(&bucket_endpoint).expect("Couldn't get java string!").into();
    let rust_bucket_name: String = env.get_string(&bucket_name).expect("Couldn't get java string!").into();
    let rust_access_key_id: String = env.get_string(&access_key_id).expect("Couldn't get java string!").into();
    let rust_secret_access_key: String = env.get_string(&secret_access_key).expect("Couldn't get java string!").into();

    match init_bucket(&rust_bucket_endpoint, &rust_bucket_name, &rust_access_key_id, &rust_secret_access_key) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to initialize S3 bucket: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_queryBucket(
    mut env: JNIEnv,
    _class: JClass,
    date_range: JObject,
    sql_query: JString,
  ) -> jstring {
    // Convert Java strings to Rust strings
    let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get java string!").into();

    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    match Runtime::new().unwrap().block_on(query_bucket(rust_date_range, &rust_sql_query)) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(e) => {
        let error_message = env
          .new_string(format!("Error querying Parquet files: {:?}", e))
          .expect("Couldn't create java string!");
        error_message.into_raw()
      }
    }
  }

  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_sinkMonthlyParquet(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    match Runtime::new().unwrap().block_on(sink_monthly_parquet(&rust_db_name, &rust_table_name)) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed sink monthly parquet files: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }
}

#[cfg(target_os = "ios")]
pub mod ios {
  use crate::timon_engine::{create_database, create_table, delete_database, delete_table, init_timon, insert, list_databases, list_tables, query};
  #[cfg(feature = "s3_sync")]
  use crate::timon_engine::{init_bucket, query_bucket, sink_monthly_parquet};
  use libc::c_char;
  use std::collections::HashMap;
  use std::ffi::{CStr, CString};
  use tokio::runtime::Runtime;

  // Helper function to convert C strings to Rust strings
  unsafe fn c_str_to_string(c_str: *const c_char) -> Result<String, String> {
    if c_str.is_null() {
      Err("Null pointer received".to_string())
    } else {
      CStr::from_ptr(c_str)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| format!("Failed to convert C string to Rust string: {:?}", e))
    }
  }

  // Helper function to convert Rust strings to C strings
  fn string_to_c_str(s: String) -> *mut c_char {
    CString::new(s).unwrap().into_raw()
  }

  #[no_mangle]
  pub extern "C" fn rust_string_free(s: *mut c_char) {
    if !s.is_null() {
      unsafe {
        CString::from_raw(s);
      }
    }
  }
  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_initTimon(storage_path: *const c_char) -> *mut c_char {
    unsafe {
      match c_str_to_string(storage_path) {
        Ok(rust_storage_path) => match init_timon(&rust_storage_path) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to initialize Timon: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        Err(err) => {
          let err_message = serde_json::json!({ "error": err }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_createDatabase(db_name: *const c_char) -> *mut c_char {
    unsafe {
      match c_str_to_string(db_name) {
        Ok(rust_db_name) => match create_database(&rust_db_name) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to create database: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        Err(err) => {
          let err_message = serde_json::json!({ "error": err }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_createTable(db_name: *const c_char, table_name: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name)) {
        (Ok(rust_db_name), Ok(rust_table_name)) => match create_table(&rust_db_name, &rust_table_name) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to create table: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        (Err(e), _) | (_, Err(e)) => {
          let err_message = serde_json::json!({ "error": e }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_listDatabases() -> *mut c_char {
    match list_databases() {
      Ok(result) => {
        let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
        string_to_c_str(json_string)
      }
      Err(err) => {
        let err_message = serde_json::json!({ "error": format!("Failed to list databases: {:?}", err) }).to_string();
        string_to_c_str(err_message)
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_listTables(db_name: *const c_char) -> *mut c_char {
    unsafe {
      match c_str_to_string(db_name) {
        Ok(rust_db_name) => match list_tables(&rust_db_name) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to list tables: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        Err(err) => {
          let err_message = serde_json::json!({ "error": err }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_deleteDatabase(db_name: *const c_char) -> *mut c_char {
    unsafe {
      match c_str_to_string(db_name) {
        Ok(rust_db_name) => match delete_database(&rust_db_name) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to delete database: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        Err(err) => {
          let err_message = serde_json::json!({ "error": err }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_deleteTable(db_name: *const c_char, table_name: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name)) {
        (Ok(rust_db_name), Ok(rust_table_name)) => match delete_table(&rust_db_name, &rust_table_name) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to delete table: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        (Err(e), _) | (_, Err(e)) => {
          let err_message = serde_json::json!({ "error": e }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_insert(
    db_name: *const c_char,
    table_name: *const c_char,
    json_data: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name), c_str_to_string(json_data)) {
        (Ok(rust_db_name), Ok(rust_table_name), Ok(rust_json_data)) => match insert(&rust_db_name, &rust_table_name, &rust_json_data) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Error writing JSON data to Parquet file: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        _ => {
          let err_message = serde_json::json!({ "error": "Invalid arguments" }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn Java_com_rustexample_TimonModule_query(
    db_name: *const c_char,
    date_range_json: *const c_char,
    sql_query: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(date_range_json), c_str_to_string(sql_query)) {
        (Ok(rust_db_name), Ok(rust_date_range_json), Ok(rust_sql_query)) => {
          // Parse date_range_json into HashMap
          let rust_date_range: HashMap<String, String> = serde_json::from_str(&rust_date_range_json).unwrap_or_default();
          let start_date = rust_date_range.get("start").cloned().unwrap_or_else(|| "1970-01-01".to_string());
          let end_date = rust_date_range.get("end").cloned().unwrap_or_else(|| "1970-01-02".to_string());

          let mut date_range_map = HashMap::new();
          date_range_map.insert("start_date", start_date.as_str());
          date_range_map.insert("end_date", end_date.as_str());

          match Runtime::new().unwrap().block_on(query(&rust_db_name, date_range_map, &rust_sql_query)) {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Error querying Parquet files: {:?}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({ "error": "Invalid arguments" }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  // ******************************** S3 Compatible Storage ********************************
  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub extern "C" fn Java_com_rustexample_TimonModule_initBucket(
    bucket_endpoint: *const c_char,
    bucket_name: *const c_char,
    access_key_id: *const c_char,
    secret_access_key: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (
        c_str_to_string(bucket_endpoint),
        c_str_to_string(bucket_name),
        c_str_to_string(access_key_id),
        c_str_to_string(secret_access_key),
      ) {
        (Ok(rust_bucket_endpoint), Ok(rust_bucket_name), Ok(rust_access_key_id), Ok(rust_secret_access_key)) => {
          match init_bucket(&rust_bucket_endpoint, &rust_bucket_name, &rust_access_key_id, &rust_secret_access_key) {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Failed to initialize S3 bucket: {:?}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({ "error": "Invalid arguments" }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub extern "C" fn Java_com_rustexample_TimonModule_queryBucket(date_range_json: *const c_char, sql_query: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(date_range_json), c_str_to_string(sql_query)) {
        (Ok(rust_date_range_json), Ok(rust_sql_query)) => {
          // Parse date_range_json into HashMap
          let rust_date_range: HashMap<String, String> = serde_json::from_str(&rust_date_range_json).unwrap_or_default();
          let start_date = rust_date_range.get("start").cloned().unwrap_or_else(|| "1970-01-01".to_string());
          let end_date = rust_date_range.get("end").cloned().unwrap_or_else(|| "1970-01-02".to_string());

          let mut date_range_map = HashMap::new();
          date_range_map.insert("start_date", start_date.as_str());
          date_range_map.insert("end_date", end_date.as_str());

          match Runtime::new().unwrap().block_on(query_bucket(date_range_map, &rust_sql_query)) {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Error querying bucket: {:?}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({ "error": "Invalid arguments" }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  #[cfg(feature = "s3_sync")]
  pub extern "C" fn Java_com_rustexample_TimonModule_sinkMonthlyParquet(db_name: *const c_char, table_name: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name)) {
        (Ok(rust_db_name), Ok(rust_table_name)) => match Runtime::new().unwrap().block_on(sink_monthly_parquet(&rust_db_name, &rust_table_name)) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to sink monthly Parquet files: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        _ => {
          let err_message = serde_json::json!({ "error": "Invalid arguments" }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }
}
