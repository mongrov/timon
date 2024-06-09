mod read_parquet_file;
pub use read_parquet_file::read_parquet_file;

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
    use crate::read_parquet_file::read_parquet_file;
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
}
