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
    use jni::objects::{JClass, JString, JObjectArray};
    use jni::sys::{jobjectArray, jstring};
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
            },
            Err(e) => {
                let error_message = env.new_string(format!("Error reading Parquet file: {:?}", e)).expect("Couldn't create java string!");
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
                let error_message = env.new_string(format!("Error writing JSON data to Parquet file: {:?}", e)).expect("Couldn't create java string!");
                error_message.into_raw() // Use into_raw to return the jstring
            },
        }
    }
    
    #[no_mangle]
    pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_datafusionQuerier(
        mut env: JNIEnv,
        _class: JClass,
        parquet_paths: jobjectArray,
        table_name: JString,
        sql_query: JString,
    ) -> jstring {
        // Convert Java strings to Rust strings
        let parquet_paths_array = JObjectArray::from_raw(parquet_paths);
        let array_len = env.get_array_length(&parquet_paths_array).expect("Couldn't get array length");
        let mut rust_parquet_paths = Vec::new();
        for i in 0..array_len {
            let element = env.get_object_array_element(&parquet_paths_array, i).expect("Couldn't get array element");
            let jstr: JString = element.into();
            let string: String = env.get_string(&jstr).expect("Couldn't get string element").into();
            rust_parquet_paths.push(string);
        }
    
        let rust_table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();
        let rust_sql_query: String = env.get_string(&sql_query).expect("Couldn't get java string!").into();
        // Convert Vec<String> to Vec<&str>
        let rust_parquet_paths: Vec<&str> = rust_parquet_paths.iter().map(|s| &**s).collect();
    
        // Call the datafusion_querier function
        match Runtime::new().unwrap().block_on(datafusion_querier(rust_parquet_paths, &rust_table_name, &rust_sql_query)) {
            Ok(json_result) => {
                // Convert Rust string to Java string
                let output = env.new_string(json_result).expect("Couldn't create java string!");
                output.into_raw()
            },
            Err(e) => {
                let error_message = env.new_string(format!("Error querying Parquet files: {:?}", e)).expect("Couldn't create java string!");
                error_message.into_raw() // Use into_raw to return the jstring
            },
        }
    }
}
