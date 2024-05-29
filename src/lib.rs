#[no_mangle]
pub extern "C" fn add(left: i32, right: i32) -> i32 {
    left + right
}

/// cbindgen:ignore
#[cfg(target_os = "android")]
pub mod android {
    use crate::add;
    use jni::JNIEnv;
    use jni::objects::JClass;
    use jni::sys::jint;
    
    // to get the fn name check: expo-module.config.json > "Java_" + "expo.modules.testrustmodule.TestRustModule".replace(".", "_")
    #[no_mangle]
    pub unsafe extern "C" fn Java_expo_modules_testrustmodule_TestRustModule_add(
        _env: JNIEnv,
        _class: JClass,
        a: jint,
        b: jint
    ) -> jint {
        add(a, b)
    }
}
