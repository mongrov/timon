use crate::timon_engine::{
  cloud_sink_parquet, create_database, create_table, delete_database, delete_table, init_bucket, init_timon, insert, list_databases, list_tables,
  query, query_bucket,
};
use actix_web::{middleware, web, App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_httpauth::middleware::HttpAuthentication;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{env, io};
use utils::{auth_handler, validate_jwt, Claims};
