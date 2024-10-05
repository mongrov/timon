mod timon_engine;

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
  use crate::timon_engine::{
    create_database, create_table, delete_database, delete_table, init_bucket, init_timon, insert, list_databases, list_tables, query, query_bucket,
    sink_monthly_parquet,
  };
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
      Ok(success_message) => {
        let output = env.new_string(success_message).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
        let output = env.new_string(result).expect("Couldn't create success string!");
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
      Ok(success_message) => {
        let output = env.new_string(success_message).expect("Couldn't create success string!");
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
      Ok(output) => {
        let java_string = env.new_string(output).expect("Couldn't create java string!");
        java_string.into_raw()
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
      Ok(success_message) => {
        let output = env.new_string(success_message).expect("Couldn't create success string!");
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
      Ok(output) => {
        let java_string = env.new_string(output).expect("Couldn't create java string!");
        java_string.into_raw()
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
  pub unsafe extern "C" fn Java_com_rustexample_TimonModule_sinkMonthlyParquet(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    match Runtime::new().unwrap().block_on(sink_monthly_parquet(&rust_db_name, &rust_table_name)) {
      Ok(success_message) => {
        let output = env.new_string(success_message).expect("Couldn't create success string!");
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
