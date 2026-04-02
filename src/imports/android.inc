use crate::timon_engine::*;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jint, jstring};
use jni::JNIEnv;
use jni::NativeMethod;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::LazyLock;
use tokio::runtime::Runtime;
