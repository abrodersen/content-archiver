use std::collections::HashMap;
use std::env;
use std::io;

use rocket::figment::providers::Env;
use rocket::futures::TryStreamExt;
use rocket::{Error, State};
use rocket::request::{FromRequest, Request, Outcome};
use rocket::serde::{Serialize, Deserialize, json::Json};
use rocket::http::Status;
use rocket::fairing::AdHoc;
use rocket::outcome::try_outcome;

use rusoto_core::Region;
use rusoto_s3::{S3Client, S3, PutObjectRequest, StreamingBody};

use reqwest::header::CONTENT_TYPE;

use chrono::prelude::{DateTime, Utc};

use url::Url;

#[macro_use] extern crate rocket;

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[derive(Deserialize)]
struct ArchiveRequest {
    pub source: String,
    pub suffix: String,
    pub public: bool,
}

#[derive(Serialize)]
struct ArchiveResult {
    pub location: String,
}

struct BearerToken(pub String);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for BearerToken {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let st = try_outcome!(req.guard::<&'r State<CommonState>>().await);

        fn is_valid<'a>(token: &str, valid: &str) -> bool {
            let token = match token.strip_prefix("Bearer ") {
                Some(x) => x,
                None => return false,
            };

            token == valid
        }

        match req.headers().get_one("authorization") {
            None => Outcome::Failure((Status::BadRequest, ())),
            Some(token) if is_valid(token, &st.bearer_token) => Outcome::Success(BearerToken(token.into())),
            Some(_) => Outcome::Failure((Status::BadRequest, ())),
        }
    }
}

#[derive(Responder)]
#[response(status = 400)]
struct ArchiveFailure {
    inner: Json<ErrorInfo>,
}

impl From<ArchiveError> for ArchiveFailure {
    fn from(error: ArchiveError) -> Self {
        ArchiveFailure {
            inner: ErrorInfo {
                error: error,
            }.into(),
        }
    }
}

#[derive(Serialize)]
enum ArchiveError {
    ContentFetchFailed,
    ContentUploadFailed,
    InvalidConfiguration,
}

#[derive(Serialize)]
struct ErrorInfo {
    error: ArchiveError,
}

#[post("/archive", data = "<request>")]
async fn archive(token: BearerToken, request: Json<ArchiveRequest>, s: &State<CommonState>) -> Result<Json<ArchiveResult>, ArchiveFailure> {
    let resp = match reqwest::get(&request.source).await {
        Ok(r) => r,
        Err(e) => return Err(ArchiveError::ContentFetchFailed.into()),
    };

    if resp.status() != reqwest::StatusCode::OK {
        return Err(ArchiveError::ContentFetchFailed.into());
    }

    let content_length = resp.content_length();
    let content_type = match resp.headers().get(CONTENT_TYPE).map(|t| t.to_str())  {
        Some(Ok(s)) => Some(s.into()),
        Some(Err(_)) => None,
        None => None,
    };

    let stream = resp.bytes_stream().map_err(|e| io::Error::new(io::ErrorKind::Other, e));
    let body = StreamingBody::new(stream);

    let mut put = PutObjectRequest::default();
    put.bucket = s.bucket_name.to_string();
    put.key =  request.0.suffix.to_string();
    put.body = Some(body);
    put.acl = Some("public-read".into());
    put.content_length = content_length.map(|l| l as i64);
    put.content_type = content_type;
    put.cache_control = Some("private, max-age=604800".into());
    put.metadata = Some(HashMap::from([
        ("source".into(), request.0.source.into()),
        ("fetched-at".into(), Utc::now().to_rfc3339())
    ]));

    let _ = match s.client.put_object(put).await {
        Ok(r) => r,
        Err(_) => return Err(ArchiveError::ContentUploadFailed.into()),
    };

    let suffix = format!("/{}/{}", s.bucket_name, request.0.suffix.as_str());
    let url = match s.public_url.join(&suffix) {
        Ok(u) => u,
        Err(_) => return Err(ArchiveError::InvalidConfiguration.into()),
    };

    Ok(ArchiveResult {
        location: url.into(),
    }.into())
}

#[derive(Deserialize)]
struct Config {
    pub bucket_name: String,
    pub bearer_token: String,
    pub public_url: String,
    pub endpoint: String,
}

struct CommonState {
    pub client: S3Client,
    pub bucket_name: String,
    pub bearer_token: String,
    pub public_url: Url,
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    rocket::build()
        .attach(AdHoc::try_on_ignite("load_app_config", |rocket| async {
            info!("loading app config");
            let figment = rocket.figment().clone()
                .merge(Env::prefixed("CONTENT_ARCHIVER_"));
            let config: Config = match figment.extract() {
                Ok(c) => c,
                Err(e) => {
                    error!("failed to load app config: {}", e);
                    return Err(rocket)
                },
            };
            let region = Region::Custom { name: "ceph".into(), endpoint: config.endpoint };
            let client = S3Client::new(region);
            let public_url = match Url::parse(&config.public_url) {
                Ok(u) => u,
                Err(_) => return Err(rocket),
            };
            Ok(rocket.manage(CommonState {
                client: client,
                bucket_name: config.bucket_name,
                bearer_token: config.bearer_token,
                public_url: public_url,
            }))
        }))
        .mount("/", routes![index, archive])
        .ignite().await?
        .launch().await

}
