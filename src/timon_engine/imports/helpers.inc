use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Datelike, Days, Local, NaiveDate, TimeZone, Timelike, Utc};
use datafusion::arrow::array::{
  new_null_array, Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Float64Array, Float64Builder, Int32Array, Int64Array, Int64Builder,
  ListArray, ListBuilder, StringArray, StringBuilder, StringViewArray, StructArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use datafusion::parquet::data_type::{AsBytes, Decimal};
use datafusion::parquet::record::{Field as ParquetField, Row};
use datafusion::scalar::ScalarValue;
use json_rules_engine::{float_greater_than, float_less_than, int_greater_than, int_less_than, Condition};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, metadata, File};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
