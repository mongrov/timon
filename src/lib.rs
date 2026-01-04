mod tests;
pub mod timon_engine;

// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
  use crate::timon_engine::errors::TimonError;
  use crate::timon_engine::{
    cloud_fetch_parquet, cloud_fetch_parquet_batch, cloud_sink_parquet, cloud_sync_parquet, create_database, create_table, delete_database,
    delete_table, init_bucket, init_timon, insert, list_databases, list_tables, query,
  };
  use jni::objects::{JClass, JObject, JString, JValue};
  use jni::sys::{jint, jstring};
  use jni::JNIEnv;
  use jni::NativeMethod;
  use std::collections::HashMap;
  use std::ffi::c_void;
  use tokio::runtime::Runtime;

  // ******************************** File Storage ********************************
  #[no_mangle]
  pub unsafe extern "C" fn nativeInitTimon(
    mut env: JNIEnv,
    _class: JClass,
    storage_path: JString,
    bucket_interval: jint,
    username: JString,
  ) -> jstring {
    // Convert `storage_path` from Java `String` to Rust `String`
    let rust_storage_path: String = env.get_string(&storage_path).expect("Couldn't get java string!").into();
    // Convert `bucket_interval` from Java `int` to Rust `u32`
    let rust_bucket_interval: u32 = bucket_interval as u32;
    let rust_username: String = env.get_string(&username).expect("Couldn't get java string!").into();

    match init_timon(&rust_storage_path, rust_bucket_interval, &rust_username) {
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
  pub unsafe extern "C" fn nativeCreateDatabase(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
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
  pub unsafe extern "C" fn nativeCreateTable(mut env: JNIEnv, _class: JClass, db_name: JString, table_name: JString, schema: JString) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();
    let rust_schema: String = env.get_string(&schema).expect("Couldn't get java string!").into();

    match create_table(&rust_db_name, &rust_table_name, &rust_schema) {
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
  pub unsafe extern "C" fn nativeListDatabases(env: JNIEnv, _class: JClass) -> jstring {
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
  pub unsafe extern "C" fn nativeListTables(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
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
  pub unsafe extern "C" fn nativeDeleteDatabase(mut env: JNIEnv, _class: JClass, db_name: JString) -> jstring {
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
  pub unsafe extern "C" fn nativeDeleteTable(mut env: JNIEnv, _class: JClass, db_name: JString, table_name: JString) -> jstring {
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
  pub unsafe extern "C" fn nativeInsert(mut env: JNIEnv, _class: JClass, db_name: JString, table_name: JString, json_data: JString) -> jstring {
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
  pub unsafe extern "C" fn nativeQuery(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    sql_query: JString,
    username: JString,
    limit_partitions: jint,
  ) -> jstring {
    // Convert Java strings to Rust strings
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get db_name java string!").into();
    let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get sql_query java string!").into();
    let rust_username: Option<String> = if username.is_null() {
      None
    } else {
      Some(env.get_string(&username).expect("Couldn't get username java string!").into())
    };

    // Convert limit_partitions: if -1 or 0, use None; otherwise use Some(value)
    let rust_limit_partitions = if limit_partitions > 0 { Some(limit_partitions as usize) } else { None };

    // Call the async query function
    match Runtime::new()
      .unwrap()
      .block_on(query(&rust_db_name, &rust_sql_query, rust_username.as_deref(), rust_limit_partitions))
    {
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
  pub unsafe extern "C" fn nativeInitBucket(
    mut env: JNIEnv,
    _class: JClass,
    bucket_endpoint: JString,
    bucket_name: JString,
    access_key_id: JString,
    secret_access_key: JString,
    bucket_region: JString,
  ) -> jstring {
    let rust_bucket_endpoint: String = env.get_string(&bucket_endpoint).expect("Couldn't get java string!").into();
    let rust_bucket_name: String = env.get_string(&bucket_name).expect("Couldn't get java string!").into();
    let rust_access_key_id: String = env.get_string(&access_key_id).expect("Couldn't get java string!").into();
    let rust_secret_access_key: String = env.get_string(&secret_access_key).expect("Couldn't get java string!").into();
    let rust_bucket_region: String = env.get_string(&bucket_region).expect("Couldn't get java string!").into();

    match init_bucket(
      &rust_bucket_endpoint,
      &rust_bucket_name,
      &rust_access_key_id,
      &rust_secret_access_key,
      &rust_bucket_region,
    ) {
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
  pub unsafe extern "C" fn nativeCloudSyncParquet(
    mut env: JNIEnv,
    _class: JClass,
    db_name: JString,
    table_name: JString,
    date_range: JObject,
    username: JString,
  ) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    let rust_username: Option<String> = if username.is_null() {
      None
    } else {
      Some(env.get_string(&username).expect("Couldn't get username java string!").into())
    };

    match Runtime::new().unwrap().block_on(cloud_sync_parquet(
      &rust_db_name,
      &rust_table_name,
      rust_date_range,
      rust_username.as_deref(),
    )) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed fetch s3 parquet files: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn nativeCloudSinkParquet(mut env: JNIEnv, _class: JClass, db_name: JString, table_name: JString) -> jstring {
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    match Runtime::new().unwrap().block_on(cloud_sink_parquet(&rust_db_name, &rust_table_name)) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed sink parquet files: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn nativeCloudFetchParquet(
    mut env: JNIEnv,
    _class: JClass,
    username: JString,
    db_name: JString,
    table_name: JString,
    date_range: JObject,
  ) -> jstring {
    let rust_username: String = env.get_string(&username).expect("Couldn't get java string!").into();
    let rust_db_name: String = env.get_string(&db_name).expect("Couldn't get java string!").into();
    let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();

    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    match Runtime::new()
      .unwrap()
      .block_on(cloud_fetch_parquet(&rust_username, &rust_db_name, &rust_table_name, rust_date_range))
    {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed fetch s3 parquet files: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub unsafe extern "C" fn nativeCloudFetchParquetBatch(
    mut env: JNIEnv,
    _class: JClass,
    usernames: JObject,
    db_names: JObject,
    table_names: JObject,
    date_range: JObject,
  ) -> jstring {
    // Convert Java String[] arrays to Rust Vec<String>
    let mut rust_usernames: Vec<String> = Vec::new();
    let mut rust_db_names: Vec<String> = Vec::new();
    let mut rust_table_names: Vec<String> = Vec::new();

    // Convert usernames array
    let usernames_array: jni::objects::JObjectArray = usernames.into();
    let usernames_length = env.get_array_length(&usernames_array).expect("Failed to get usernames array length");
    for i in 0..usernames_length {
      let element = env
        .get_object_array_element(&usernames_array, i)
        .expect("Failed to get usernames array element");
      let j_string: JString = element.into();
      let rust_string: String = env.get_string(&j_string).expect("Failed to convert Java String to Rust String").into();
      rust_usernames.push(rust_string);
    }

    // Convert db_names array
    let db_names_array: jni::objects::JObjectArray = db_names.into();
    let db_names_length = env.get_array_length(&db_names_array).expect("Failed to get db_names array length");
    for i in 0..db_names_length {
      let element = env
        .get_object_array_element(&db_names_array, i)
        .expect("Failed to get db_names array element");
      let j_string: JString = element.into();
      let rust_string: String = env.get_string(&j_string).expect("Failed to convert Java String to Rust String").into();
      rust_db_names.push(rust_string);
    }

    // Convert table_names array
    let table_names_array: jni::objects::JObjectArray = table_names.into();
    let table_names_length = env.get_array_length(&table_names_array).expect("Failed to get table_names array length");
    for i in 0..table_names_length {
      let element = env
        .get_object_array_element(&table_names_array, i)
        .expect("Failed to get table_names array element");
      let j_string: JString = element.into();
      let rust_string: String = env.get_string(&j_string).expect("Failed to convert Java String to Rust String").into();
      rust_table_names.push(rust_string);
    }

    // Convert date_range
    let mut rust_date_range: HashMap<&str, &str> = HashMap::new();
    let rust_start = get_date_range_value(&mut env, &date_range, "start");
    let rust_end = get_date_range_value(&mut env, &date_range, "end");
    rust_date_range.insert("start_date", &rust_start);
    rust_date_range.insert("end_date", &rust_end);

    // Convert Vec<String> to &[&str] for the batch function
    let usernames_refs: Vec<&str> = rust_usernames.iter().map(|s| s.as_str()).collect();
    let db_names_refs: Vec<&str> = rust_db_names.iter().map(|s| s.as_str()).collect();
    let table_names_refs: Vec<&str> = rust_table_names.iter().map(|s| s.as_str()).collect();

    match Runtime::new().unwrap().block_on(cloud_fetch_parquet_batch(
      &usernames_refs,
      &db_names_refs,
      &table_names_refs,
      rust_date_range,
    )) {
      Ok(result) => {
        let json_string = result.to_string();
        let output = env.new_string(json_string).expect("Couldn't create success string!");
        output.into_raw()
      }
      Err(err) => {
        let err_message = format!("Failed to batch fetch s3 parquet files: {:?}", err);
        let output = env.new_string(err_message).expect("Couldn't create error string!");
        output.into_raw()
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn JNI_OnLoad(vm: jni::JavaVM, _reserved: *mut std::ffi::c_void) -> jni::sys::jint {
    let mut env = vm.get_env().expect("Failed to get JNIEnv");

    // Get the Application Context
    let activity_thread = env.find_class("android/app/ActivityThread").expect("Failed to find ActivityThread");
    let current_activity_thread = env
      .call_static_method(activity_thread, "currentActivityThread", "()Landroid/app/ActivityThread;", &[])
      .expect("Failed to get currentActivityThread")
      .l()
      .expect("Failed to convert to object");
    let app_context = env
      .call_method(current_activity_thread, "getApplication", "()Landroid/app/Application;", &[])
      .expect("Failed to get Application context")
      .l()
      .expect("Failed to convert to object");

    // Get the Package Name
    let package_name = env
      .call_method(app_context, "getPackageName", "()Ljava/lang/String;", &[])
      .expect("Failed to get package name")
      .l()
      .expect("Failed to convert to object");
    let package_name: String = env
      .get_string(&JString::from(package_name))
      .expect("Failed to convert package name to Rust string")
      .into();

    // Construct the Dynamic Class Name
    let class_name = format!("{}/TimonModule", package_name.replace(".", "/"));
    let class = env.find_class(&class_name).expect(&format!("Failed to find class: {}", class_name));

    let methods = [
      NativeMethod {
        name: "nativeInitTimon".into(),
        sig: "(Ljava/lang/String;ILjava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeInitTimon as *mut c_void,
      },
      NativeMethod {
        name: "nativeCreateDatabase".into(),
        sig: "(Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeCreateDatabase as *mut c_void,
      },
      NativeMethod {
        name: "nativeCreateTable".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeCreateTable as *mut c_void,
      },
      NativeMethod {
        name: "nativeListDatabases".into(),
        sig: "()Ljava/lang/String;".into(),
        fn_ptr: nativeListDatabases as *mut c_void,
      },
      NativeMethod {
        name: "nativeListTables".into(),
        sig: "(Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeListTables as *mut c_void,
      },
      NativeMethod {
        name: "nativeDeleteDatabase".into(),
        sig: "(Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeDeleteDatabase as *mut c_void,
      },
      NativeMethod {
        name: "nativeDeleteTable".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeDeleteTable as *mut c_void,
      },
      NativeMethod {
        name: "nativeInsert".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeInsert as *mut c_void,
      },
      NativeMethod {
        name: "nativeQuery".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;I)Ljava/lang/String;".into(),
        fn_ptr: nativeQuery as *mut c_void,
      },
      NativeMethod {
        name: "nativeInitBucket".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeInitBucket as *mut c_void,
      },
      NativeMethod {
        name: "nativeCloudSyncParquet".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeCloudSyncParquet as *mut c_void,
      },
      NativeMethod {
        name: "nativeCloudSinkParquet".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;".into(),
        fn_ptr: nativeCloudSinkParquet as *mut c_void,
      },
      NativeMethod {
        name: "nativeCloudFetchParquet".into(),
        sig: "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;)Ljava/lang/String;".into(),
        fn_ptr: nativeCloudFetchParquet as *mut c_void,
      },
      NativeMethod {
        name: "nativeCloudFetchParquetBatch".into(),
        sig: "([Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;Ljava/util/Map;)Ljava/lang/String;".into(),
        fn_ptr: nativeCloudFetchParquetBatch as *mut c_void,
      },
    ];

    env.register_native_methods(class, &methods).expect("Failed to register native methods");
    jni::sys::JNI_VERSION_1_8
  }
}

#[cfg(target_os = "ios")]
pub mod ios {
  use crate::timon_engine::errors::TimonError;
  use crate::timon_engine::{
    cloud_fetch_parquet, cloud_fetch_parquet_batch, cloud_sink_parquet, cloud_sync_parquet, create_database, create_table, delete_database,
    delete_table, init_bucket, init_timon, insert, list_databases, list_tables, query,
  };
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
  pub extern "C" fn nativeInitTimon(storage_path: *const c_char, bucket_interval: u32, username: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(storage_path), c_str_to_string(username)) {
        (Ok(rust_storage_path), Ok(rust_username)) => match init_timon(&rust_storage_path, bucket_interval, &rust_username) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to initialize Timon: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        (Err(err), _) | (_, Err(err)) => {
          let err_message = serde_json::json!({ "error": err }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn nativeCreateDatabase(db_name: *const c_char) -> *mut c_char {
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
  pub extern "C" fn nativeCreateTable(db_name: *const c_char, table_name: *const c_char, schema: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name), c_str_to_string(schema)) {
        (Ok(rust_db_name), Ok(rust_table_name), Ok(rust_schema)) => match create_table(&rust_db_name, &rust_table_name, &rust_schema) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to create table: {:?}", err) }).to_string();
            string_to_c_str(err_message)
          }
        },
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
          let err_message = serde_json::json!({ "error": e }).to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn nativeListDatabases() -> *mut c_char {
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
  pub extern "C" fn nativeListTables(db_name: *const c_char) -> *mut c_char {
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
  pub extern "C" fn nativeDeleteDatabase(db_name: *const c_char) -> *mut c_char {
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
  pub extern "C" fn nativeDeleteTable(db_name: *const c_char, table_name: *const c_char) -> *mut c_char {
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
  pub extern "C" fn nativeInsert(db_name: *const c_char, table_name: *const c_char, json_data: *const c_char) -> *mut c_char {
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
  pub extern "C" fn nativeQuery(db_name: *const c_char, sql_query: *const c_char, username: *const c_char, limit_partitions: i32) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(sql_query), c_str_to_string(username).ok()) {
        (Ok(rust_db_name), Ok(rust_sql_query), rust_username) => {
          // Convert limit_partitions: if -1 or 0, use None; otherwise use Some(value)
          let rust_limit_partitions = if limit_partitions > 0 { Some(limit_partitions as usize) } else { None };

          match Runtime::new()
            .unwrap()
            .block_on(query(&rust_db_name, &rust_sql_query, rust_username.as_deref(), rust_limit_partitions))
          {
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
  pub extern "C" fn nativeInitBucket(
    bucket_endpoint: *const c_char,
    bucket_name: *const c_char,
    access_key_id: *const c_char,
    secret_access_key: *const c_char,
    bucket_region: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (
        c_str_to_string(bucket_endpoint),
        c_str_to_string(bucket_name),
        c_str_to_string(access_key_id),
        c_str_to_string(secret_access_key),
        c_str_to_string(bucket_region),
      ) {
        (Ok(rust_bucket_endpoint), Ok(rust_bucket_name), Ok(rust_access_key_id), Ok(rust_secret_access_key), Ok(rust_bucket_region)) => {
          match init_bucket(
            &rust_bucket_endpoint,
            &rust_bucket_name,
            &rust_access_key_id,
            &rust_secret_access_key,
            &rust_bucket_region,
          ) {
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
  pub extern "C" fn nativeCloudSyncParquet(
    db_name: *const c_char,
    table_name: *const c_char,
    date_range_json: *const c_char,
    username: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (
        c_str_to_string(db_name),
        c_str_to_string(table_name),
        c_str_to_string(date_range_json),
        c_str_to_string(username).ok(),
      ) {
        (Ok(rust_db_name), Ok(rust_table_name), Ok(rust_date_range_json), rust_username) => {
          // Parse date_range_json into HashMap
          let rust_date_range: HashMap<String, String> = match serde_json::from_str(&rust_date_range_json) {
            Ok(map) => map,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse date_range_json: {}. Input was: '{}'", e, rust_date_range_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          let start_date = rust_date_range.get("start").cloned().unwrap_or_else(|| {
            println!("Warning: 'start' key not found in date_range_json, using default");
            "1970-01-01".to_string()
          });

          let end_date = rust_date_range.get("end").cloned().unwrap_or_else(|| {
            println!("Warning: 'end' key not found in date_range_json, using default");
            "1970-01-02".to_string()
          });

          let mut date_range_map = HashMap::new();
          date_range_map.insert("start_date", start_date.as_str());
          date_range_map.insert("end_date", end_date.as_str());

          match Runtime::new().unwrap().block_on(cloud_sync_parquet(
            &rust_db_name,
            &rust_table_name,
            date_range_map,
            rust_username.as_deref(),
          )) {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Error syncing parquet files: {}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({
            "error": "Invalid arguments to nativeCloudSyncParquet function. Ensure all parameters are valid strings."
          })
          .to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn nativeCloudSinkParquet(db_name: *const c_char, table_name: *const c_char) -> *mut c_char {
    unsafe {
      match (c_str_to_string(db_name), c_str_to_string(table_name)) {
        (Ok(rust_db_name), Ok(rust_table_name)) => match Runtime::new().unwrap().block_on(cloud_sink_parquet(&rust_db_name, &rust_table_name)) {
          Ok(result) => {
            let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            string_to_c_str(json_string)
          }
          Err(err) => {
            let err_message = serde_json::json!({ "error": format!("Failed to sink Parquet files: {:?}", err) }).to_string();
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
  pub extern "C" fn nativeCloudFetchParquet(
    username: *const c_char,
    db_name: *const c_char,
    table_name: *const c_char,
    date_range_json: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (
        c_str_to_string(username),
        c_str_to_string(db_name),
        c_str_to_string(table_name),
        c_str_to_string(date_range_json),
      ) {
        (Ok(rust_username), Ok(rust_db_name), Ok(rust_table_name), Ok(rust_date_range_json)) => {
          // Parse date_range_json into HashMap
          let rust_date_range: HashMap<String, String> = match serde_json::from_str(&rust_date_range_json) {
            Ok(map) => map,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse date_range_json: {}. Input was: '{}'", e, rust_date_range_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          let start_date = rust_date_range.get("start").cloned().unwrap_or_else(|| {
            println!("Warning: 'start' key not found in date_range_json, using default");
            "1970-01-01".to_string()
          });

          let end_date = rust_date_range.get("end").cloned().unwrap_or_else(|| {
            println!("Warning: 'end' key not found in date_range_json, using default");
            "1970-01-02".to_string()
          });

          let mut date_range_map = HashMap::new();
          date_range_map.insert("start_date", start_date.as_str());
          date_range_map.insert("end_date", end_date.as_str());

          match Runtime::new()
            .unwrap()
            .block_on(cloud_fetch_parquet(&rust_username, &rust_db_name, &rust_table_name, date_range_map))
          {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Failed to fetch s3 Parquet files: {}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({
            "error": "Invalid arguments to nativeCloudFetchParquet function. Ensure all parameters are valid strings."
          })
          .to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }

  #[no_mangle]
  pub extern "C" fn nativeCloudFetchParquetBatch(
    usernames_json: *const c_char,
    db_names_json: *const c_char,
    table_names_json: *const c_char,
    date_range_json: *const c_char,
  ) -> *mut c_char {
    unsafe {
      match (
        c_str_to_string(usernames_json),
        c_str_to_string(db_names_json),
        c_str_to_string(table_names_json),
        c_str_to_string(date_range_json),
      ) {
        (Ok(rust_usernames_json), Ok(rust_db_names_json), Ok(rust_table_names_json), Ok(rust_date_range_json)) => {
          // Parse JSON arrays
          let rust_usernames: Vec<String> = match serde_json::from_str(&rust_usernames_json) {
            Ok(vec) => vec,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse usernames_json: {}. Input was: '{}'", e, rust_usernames_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          let rust_db_names: Vec<String> = match serde_json::from_str(&rust_db_names_json) {
            Ok(vec) => vec,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse db_names_json: {}. Input was: '{}'", e, rust_db_names_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          let rust_table_names: Vec<String> = match serde_json::from_str(&rust_table_names_json) {
            Ok(vec) => vec,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse table_names_json: {}. Input was: '{}'", e, rust_table_names_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          // Parse date_range_json into HashMap
          let rust_date_range: HashMap<String, String> = match serde_json::from_str(&rust_date_range_json) {
            Ok(map) => map,
            Err(e) => {
              let err_message = serde_json::json!({
                "error": format!("Failed to parse date_range_json: {}. Input was: '{}'", e, rust_date_range_json)
              })
              .to_string();
              return string_to_c_str(err_message);
            }
          };

          let start_date = rust_date_range.get("start").cloned().unwrap_or_else(|| {
            println!("Warning: 'start' key not found in date_range_json, using default");
            "1970-01-01".to_string()
          });

          let end_date = rust_date_range.get("end").cloned().unwrap_or_else(|| {
            println!("Warning: 'end' key not found in date_range_json, using default");
            "1970-01-02".to_string()
          });

          let mut date_range_map = HashMap::new();
          date_range_map.insert("start_date", start_date.as_str());
          date_range_map.insert("end_date", end_date.as_str());

          // Convert Vec<String> to &[&str] for the batch function
          let usernames_refs: Vec<&str> = rust_usernames.iter().map(|s| s.as_str()).collect();
          let db_names_refs: Vec<&str> = rust_db_names.iter().map(|s| s.as_str()).collect();
          let table_names_refs: Vec<&str> = rust_table_names.iter().map(|s| s.as_str()).collect();

          match Runtime::new().unwrap().block_on(cloud_fetch_parquet_batch(
            &usernames_refs,
            &db_names_refs,
            &table_names_refs,
            date_range_map,
          )) {
            Ok(result) => {
              let json_string = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
              string_to_c_str(json_string)
            }
            Err(err) => {
              let err_message = serde_json::json!({ "error": format!("Failed to batch fetch s3 Parquet files: {}", err) }).to_string();
              string_to_c_str(err_message)
            }
          }
        }
        _ => {
          let err_message = serde_json::json!({
            "error": "Invalid arguments to nativeCloudFetchParquetBatch function. Ensure all parameters are valid strings."
          })
          .to_string();
          string_to_c_str(err_message)
        }
      }
    }
  }
}
