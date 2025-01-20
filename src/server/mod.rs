mod utils;
use crate::timon_engine::{
  cloud_sync_parquet, create_database, create_table, delete_database, delete_table, init_bucket, init_timon, insert, list_databases, list_tables,
  query, query_bucket,
};
use actix_web::{middleware, web, App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_httpauth::middleware::HttpAuthentication;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{env, io};
use utils::{auth_handler, validate_jwt, Claims};

#[derive(Deserialize)]
pub struct CreateDatabaseRequest {
  db_name: String,
}

#[derive(Deserialize)]
pub struct CreateTableRequest {
  db_name: String,
  table_name: String,
  schema: String,
}

#[derive(Deserialize)]
pub struct InsertRequest {
  db_name: String,
  table_name: String,
  data: String,
}

#[derive(Deserialize)]
pub struct QueryRequest {
  db_name: String,
  sql_query: String,
}

#[derive(Deserialize)]
pub struct QueryBucketRequest {
  date_range: HashMap<String, String>,
  sql_query: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
  result: String,
}

#[derive(Deserialize)]
pub struct SinkParquetRequest {
  db_name: String,
  table_name: String,
}

#[derive(Deserialize)]
pub struct DeleteTableRequest {
  db_name: String,
  table_name: String,
}

/* ******************************** Local File Storage ******************************** */

// Create a database
pub async fn create_database_handler(req: web::Json<CreateDatabaseRequest>) -> impl Responder {
  match create_database(&req.db_name) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// Create a table
pub async fn create_table_handler(req: web::Json<CreateTableRequest>) -> impl Responder {
  match create_table(&req.db_name, &req.table_name, &req.schema) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// Delete a database
pub async fn delete_database_handler(req: web::Json<CreateDatabaseRequest>) -> impl Responder {
  match delete_database(&req.db_name) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// Delete a table
pub async fn delete_table_handler(req: web::Json<DeleteTableRequest>) -> impl Responder {
  match delete_table(&req.db_name, &req.table_name) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// List all databases
pub async fn list_databases_handler() -> impl Responder {
  match list_databases() {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// List all tables in a database
pub async fn list_tables_handler(req: web::Json<CreateDatabaseRequest>) -> impl Responder {
  match list_tables(&req.db_name) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// Insert data
pub async fn insert_handler(req: web::Json<InsertRequest>) -> impl Responder {
  match insert(&req.db_name, &req.table_name, &req.data) {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

// Query data
pub async fn query_handler(req: web::Json<QueryRequest>) -> impl Responder {
  match query(&req.db_name, &req.sql_query).await {
    Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
    Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
  }
}

/* ******************************** S3 Compatible Storage ******************************** */

// Query bucket
pub async fn query_bucket_handler(req: HttpRequest, body: web::Json<QueryBucketRequest>) -> impl Responder {
  // Access claims from the request extensions
  if let Some(claims) = req.extensions().get::<Claims>() {
    let username = &claims.sub;
    let date_range = body
      .date_range
      .iter()
      .map(|(k, v)| (k.as_str(), v.as_str()))
      .collect::<HashMap<&str, &str>>();
    match query_bucket(username, &body.sql_query, date_range).await {
      Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
      Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
    }
  } else {
    HttpResponse::Unauthorized().body("Missing or invalid token")
  }
}

// Sink daily Parquet data
pub async fn cloud_sync_parquet_handler(req: HttpRequest, body: web::Json<SinkParquetRequest>) -> impl Responder {
  // Access claims from the request extensions
  if let Some(claims) = req.extensions().get::<Claims>() {
    let username = &claims.sub;
    match cloud_sync_parquet(username, &body.db_name, &body.table_name).await {
      Ok(result) => HttpResponse::Ok().json(QueryResponse { result: result.to_string() }),
      Err(e) => HttpResponse::InternalServerError().json(format!("Error: {}", e)),
    }
  } else {
    HttpResponse::Unauthorized().body("Missing or invalid token")
  }
}

pub async fn timon_server() -> io::Result<()> {
  let rust_log = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
  env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(rust_log))
    .filter(None, LevelFilter::Info)
    .init();

  let port: u16 = env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
  let address = format!("0.0.0.0:{}", port);

  let storage_path = "tmp/timon";
  match init_timon(storage_path, 30) {
    Ok(res) => println!("Initialized Timon Successfully: {}.", res),
    Err(e) => println!("Error: {}", e),
  }

  let bucket_endpoint = env::var("BUCKET_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
  let bucket_name = env::var("BUCKET_NAME").unwrap_or_else(|_| "timon".to_string());
  let access_key_id = env::var("ACCESS_KEY_ID").unwrap_or_else(|_| "ahmed".to_string());
  let secret_access_key = env::var("SECRET_ACCESS_KEY").unwrap_or_else(|_| "ahmed1234".to_string());
  let bucket_region = env::var("BUCKET_REGION").unwrap_or_else(|_| "us-east-1".to_string());

  match init_bucket(&bucket_endpoint, &bucket_name, &access_key_id, &secret_access_key, &bucket_region) {
    Ok(res) => println!("Initialized Timon Bucket Successfully: {}.", res),
    Err(e) => println!("Error: {}", e),
  }

  HttpServer::new(|| {
    App::new()
      .wrap(middleware::Logger::default())
      // Public routes (no authentication required)
      .service(web::scope("/auth").route("", web::post().to(auth_handler)))
      // Protected routes (authentication required)
      .service(
        web::scope("")
          .wrap(HttpAuthentication::bearer(validate_jwt))
          .route("/database", web::post().to(create_database_handler))
          .route("/table", web::post().to(create_table_handler))
          .route("/database", web::delete().to(delete_database_handler))
          .route("/table", web::delete().to(delete_table_handler))
          .route("/databases", web::get().to(list_databases_handler))
          .route("/tables", web::get().to(list_tables_handler))
          .route("/insert", web::post().to(insert_handler))
          .route("/query", web::get().to(query_handler))
          .route("/query_bucket", web::get().to(query_bucket_handler))
          .route("/sync_bucket", web::post().to(cloud_sync_parquet_handler)),
      )
  })
  .bind(address)?
  .run()
  .await
}
