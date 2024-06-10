mod read_parquet_file;
mod write_parquet_file;
pub use read_parquet_file::read_parquet_file;
pub use write_parquet_file::write_json_to_parquet;

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
    use crate::{read_parquet_file::read_parquet_file, write_parquet_file::write_json_to_parquet};
    use jni::JNIEnv;
    use jni::objects::{JClass, JString};
    use jni::sys::jstring;
    use std::ffi::{CStr, CString};

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
                let output = env.new_string(json_str)
                    .expect("Couldn't create java string!");

                output.into_raw() // Use into_raw to return the jstring
            },
            Err(_) => {
                let error_message = env.new_string("Error reading Parquet file")
                    .expect("Couldn't create java string!");

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
        let rust_file_path: String = env.get_string(&file_path)
            .expect("Couldn't get java string!")
            .into();
        let rust_json_data: String = env.get_string(&json_data)
            .expect("Couldn't get java string!")
            .into();

        match write_json_to_parquet(&rust_file_path, &rust_json_data) {
            Ok(_) => {
                let success_message = env.new_string("Successfully wrote JSON data to Parquet file")
                    .expect("Couldn't create java string!");

                success_message.into_raw() // Use into_raw to return the jstring
            },
            Err(e) => {
                let error_message = env.new_string(format!("Error writing JSON data to Parquet file: {:?}", e))
                    .expect("Couldn't create java string!");

                error_message.into_raw() // Use into_raw to return the jstring
            },
        }
    }
}
