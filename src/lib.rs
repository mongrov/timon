mod datafusion_query;

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
    use crate::datafusion_query::{
        datafusion_querier,
        read_parquet_file,
        write_json_to_parquet,
    };
    use jni::JNIEnv;
    use jni::objects::{JClass, JString};
    use jni::sys::jstring;

    #[no_mangle]
    pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_readParquetFile(
        mut env: JNIEnv,
        _class: JClass,
        file_path: JString,
    ) -> jstring {
        let rust_string: String = env.get_string(&file_path)
            .expect("Couldn't get java string!")
            .into();

        match read_parquet_file(&rust_string) {
            Ok(json_records) => {
                let json_str = serde_json::to_string(&json_records).unwrap();
                let output = env.new_string(json_str).expect("Couldn't create java string!");
                output.into_raw() // Use into_raw to return the jstring
            },
            Err(_) => {
                let error_message = env.new_string("Error reading Parquet file").expect("Couldn't create java string!");
                error_message.into_raw() // Use into_raw to return the jstring
            },
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
                let success_message = env.new_string("Successfully wrote JSON data to Parquet file").expect("Couldn't create java string!");
                success_message.into_raw() // Use into_raw to return the jstring
            },
            Err(e) => {
                let error_message = env.new_string(format!("Error writing JSON data to Parquet file: {:?}", e))
                    .expect("Couldn't create java string!");

                error_message.into_raw() // Use into_raw to return the jstring
            },
        }
    }
    
    #[no_mangle]
    pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_datafusionQuerier(
        mut env: JNIEnv,
        _class: JClass,
        file_path: JString,
        table_name: JString,
        sql_query: JString,
    ) -> jstring {
        // Convert Java strings to Rust strings
        let rust_file_path: String = env.get_string(&file_path).expect("Couldn't get java string!").into();
        let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();
        let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get java string!").into();
    
        // Call the datafusion_querier function
        match tokio::runtime::Runtime::new().unwrap().block_on(datafusion_querier(&rust_file_path, &rust_table_name, &rust_sql_query)) {
            Ok(json_result) => {
                // Convert Rust string to Java string
                let output = env.new_string(json_result).expect("Couldn't create java string!");
                output.into_raw()
            },
            Err(_) => {
                let error_message = env.new_string("Error querying Parquet file").expect("Couldn't create java string!");
                error_message.into_raw()
            },
        }
    }
}
