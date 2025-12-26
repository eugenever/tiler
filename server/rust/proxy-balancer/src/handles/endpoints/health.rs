use std::net::IpAddr;

use hyper::{
    client::HttpConnector, http::header, http::request::Parts, Body, Client, Method, Request,
    Response, StatusCode,
};
use serde_json::json;
use tracing::{event, Level};

use crate::defaults::LOCALHOST;
use crate::hyper_reverse_proxy;
use crate::structs::EndPoints;

pub async fn health_endpoint(
    ports: Vec<u16>,
    parts: &Parts,
    client_ip: IpAddr,
    client: Client<HttpConnector>,
    ct: &str,
) -> Result<Response<Body>, anyhow::Error> {
    // check if all workers are available
    let mut error_ports = Vec::new();
    let mut success_ports = Vec::new();

    for p in ports {
        let mut health_request = Request::builder()
            .method(Method::GET)
            .uri(EndPoints::Health.as_ref())
            .body(Body::empty())?;

        *health_request.headers_mut() = parts.headers.clone();
        let res = hyper_reverse_proxy::call(
            client_ip,
            &format!("http://{}:{}", LOCALHOST, p),
            health_request,
            &client,
        )
        .await;

        match res {
            Err(err) => {
                event!(Level::ERROR, "Error health request {:?}, port {}", err, p);
                error_ports.push(p);
            }
            Ok(response) => {
                let status = response.status();
                let b = response.into_body();
                let body_bytes = hyper::body::to_bytes(b).await?;
                let body_json: serde_json::Value = serde_json::from_slice(&body_bytes)?;

                if status != StatusCode::OK {
                    error_ports.push(p);
                } else {
                    let _worker_pid = body_json
                        .get("worker_pid")
                        .and_then(|worker_pid| worker_pid.as_u64())
                        .expect("worker_pid is undefined");
                    success_ports.push(p);
                }
            }
        }
    }

    let body = json!({
        "status": StatusCode::OK.as_u16(),
        "error_ports": error_ports,
        "success_ports": success_ports
    })
    .to_string();
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body))?;
    Ok(response)
}
