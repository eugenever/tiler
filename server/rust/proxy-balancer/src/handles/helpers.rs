use std::convert::Infallible;
use std::time::Duration;

use anyhow::anyhow;
use hyper::{
    client::HttpConnector,
    http::header::{self, HeaderMap, HeaderValue},
    Body, Client, Method, Request, Response, StatusCode, Uri,
};
use serde_json::json;
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::structs::ContentType;
use crate::tasks::reload_workers::{MessageMaintenanceWorkers, WorkerData};

pub fn error_response_endpoint(endpoint: &str, err: anyhow::Error) -> Response<Body> {
    event!(Level::ERROR, "Error '{endpoint}': {err}");
    let body = json!({
        "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        "message": err.to_string()
    })
    .to_string();
    response_with_body_and_code(body, StatusCode::INTERNAL_SERVER_ERROR)
}

pub fn error_response(status_code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status_code)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::empty())
        .unwrap()
}

pub async fn worker_response(
    host: String,
    port: i32,
    uri: Uri,
    method: &Method,
    client: &Client<HttpConnector>,
    headers: HeaderMap<HeaderValue>,
    body: Body,
    ct: &str,
    timeout: u64,
) -> Result<Response<Body>, anyhow::Error> {
    let url = format!("http://{host}:{port}{uri}");
    let mut worker_request = Request::builder().method(method).uri(url).body(body)?;
    *worker_request.headers_mut() = headers;

    match tokio::time::timeout(Duration::from_secs(timeout), client.request(worker_request)).await {
        Err(_) => Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::CONTENT_TYPE, ct)
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(Body::from(format!(
                "Timeout: no response in {timeout} seconds."
            )))
            .map_err(anyhow::Error::from),
        Ok(res) => match res {
            Err(err) => {
                let body = json!({
                    "error": format!("{err:?}")
                })
                .to_string();
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header(header::CONTENT_TYPE, ct)
                    .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Body::from(body))
                    .map_err(anyhow::Error::from)
            }
            Ok(r) => Ok(r),
        },
    }
}

pub fn worker_not_found_response(
    datasource_id: &str,
    ct: &str,
) -> Result<Response<Body>, anyhow::Error> {
    let message = format!("DataSource id '{datasource_id}' not found");
    let body = json!({
        "status": StatusCode::NOT_FOUND.as_u16(),
        "message": message
    })
    .to_string();
    let response = Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, ct)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body))
        .map_err(anyhow::Error::from)?;
    return Ok(response);
}

pub fn response_with_body_and_code(body: String, status_code: StatusCode) -> Response<Body> {
    let ct: &str = ContentType::ApplicationJson.into();
    Response::builder()
        .status(status_code)
        .header(header::CONTENT_TYPE, ct)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body))
        .unwrap()
}

pub fn debug_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let body_str = format!("{:?}", req);
    let ct: &str = ContentType::ApplicationJson.into();
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body_str))
        .unwrap();
    Ok(response)
}

pub async fn get_worker_data(
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
) -> Result<WorkerData, anyhow::Error> {
    let (tx_wd, rx_wd) = oneshot::channel();
    tx_mw
        .send_async(MessageMaintenanceWorkers::GetWorkerData { tx_wd })
        .await?;

    match rx_wd.await {
        Err(err) => {
            return Err(anyhow::Error::from(err));
        }
        Ok(opt_wd) => match opt_wd {
            None => {
                return Err(anyhow!("WorkerData is None"));
            }
            Some(wd) => {
                return Ok(wd);
            }
        },
    }
}
