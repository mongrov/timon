use actix_web::error::ErrorUnauthorized;
use actix_web::{dev::ServiceRequest, web, Error, HttpMessage, HttpResponse, Responder};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Serialize)]
pub struct Claims {
  pub sub: String,
  pub exp: usize, // Expiration time as a UNIX timestamp
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
  username: String,
  password: String,
}

pub async fn auth_handler(data: web::Json<LoginRequest>) -> impl Responder {
  let username = &data.username;
  let password = &data.password;

  // Replace this with a proper database/user validation logic
  if username == "ahmed123" && password == "password123" {
    let secret = env::var("JWT_SECRET").unwrap_or_else(|_| "mysecret".to_string());

    // Generate JWT
    let claims = Claims {
      sub: username.clone(),
      exp: chrono::Utc::now()
        .checked_add_signed(chrono::Duration::hours(720)) // 30-days
        .expect("valid timestamp")
        .timestamp() as usize,
    };

    match encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_ref())) {
      Ok(token) => HttpResponse::Ok().json(serde_json::json!({ "token": token })),
      Err(_) => HttpResponse::InternalServerError().body("Error generating token"),
    }
  } else {
    HttpResponse::Unauthorized().body("Invalid username or password")
  }
}

pub async fn validate_jwt(req: ServiceRequest, credentials: BearerAuth) -> Result<ServiceRequest, (Error, ServiceRequest)> {
  let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "your-secret-key".to_string());
  let token_data = decode::<Claims>(credentials.token(), &DecodingKey::from_secret(secret.as_ref()), &Validation::default());

  match token_data {
    Ok(data) => {
      req.extensions_mut().insert(data.claims); // Insert claims into extensions
      Ok(req)
    }
    Err(_) => Err((ErrorUnauthorized("Invalid token"), req)),
  }
}
