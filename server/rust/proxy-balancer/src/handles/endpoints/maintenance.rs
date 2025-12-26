use hyper::body::Bytes;
use hyper::{Body, Method, Response, StatusCode};
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::handles::helpers::response_with_body_and_code;
use crate::structs::EndPoints;
use crate::tasks::{reload_workers::MessageMaintenanceWorkers, semaphore::MessageSemaphore};

pub async fn maintenance_endpoint(
    path: &str,
    method: &Method,
    b_bytes: &Bytes,
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
    tx_sem: flume::Sender<MessageSemaphore>,
) -> Result<Option<Response<Body>>, anyhow::Error> {
    if path.starts_with(EndPoints::AddWorkers.as_ref()) && method == Method::POST {
        let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
        let count = body_json
            .get("count")
            .and_then(|c| c.as_u64())
            .expect("Count workers is undefined");

        tx_mw
            .send_async(MessageMaintenanceWorkers::AddWorkers { count })
            .await?;
        event!(Level::INFO, "Add Python workers, count = {count}");

        return Ok(Some(response_with_body_and_code(
            "Workers successfully added".to_string(),
            StatusCode::OK,
        )));
    } else if path.starts_with(EndPoints::ReloadWorkers.as_ref()) {
        tx_mw
            .send_async(MessageMaintenanceWorkers::ReloadWorkers())
            .await?;
        event!(Level::INFO, "Reload Python workers");

        return Ok(Some(response_with_body_and_code(
            "Workers successfully reloaded".to_string(),
            StatusCode::OK,
        )));
    } else if path.starts_with(EndPoints::TerminateWorkers.as_ref()) {
        tx_mw
            .send_async(MessageMaintenanceWorkers::TerminateWorkers())
            .await?;
        event!(Level::INFO, "Terminate Python workers");

        return Ok(Some(response_with_body_and_code(
            "Workers successfully terminated".to_string(),
            StatusCode::OK,
        )));
    } else if path.starts_with(EndPoints::InfoWorkers.as_ref()) {
        let (tx_iw, rx_iw) = oneshot::channel();
        tx_mw
            .send_async(MessageMaintenanceWorkers::InfoWorkers { tx_iw })
            .await?;

        match rx_iw.await {
            Err(err) => {
                event!(Level::ERROR, "Error receive info worker {:?}", err);
                return Ok(Some(response_with_body_and_code(
                    format!("Error receive info worker {:?}", err),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )));
            }
            Ok(h) => {
                if let Some(iw) = h {
                    let v = serde_json::to_string(&iw)?;
                    return Ok(Some(response_with_body_and_code(v, StatusCode::OK)));
                }
                return Ok(Some(response_with_body_and_code(
                    "Workers info is None".to_string(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )));
            }
        }
    } else if path.starts_with(EndPoints::IncreaseLimitConcurrentRequests.as_ref())
        && method == Method::POST
    {
        let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
        let n = body_json
            .get("n")
            .and_then(|id| id.as_u64())
            .expect("Count permits is undefined");

        tx_sem
            .send_async(MessageSemaphore::AddPermits { n: n as usize })
            .await?;

        return Ok(Some(response_with_body_and_code(
            format!(
                "Limit of concurrent requests successfully increased by {}",
                n
            ),
            StatusCode::OK,
        )));
    } else if path.starts_with(EndPoints::DecreaseLimitConcurrentRequests.as_ref())
        && method == Method::POST
    {
        let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
        let n = body_json
            .get("n")
            .and_then(|id| id.as_u64())
            .expect("Count permits is undefined");

        tx_sem
            .send_async(MessageSemaphore::ForgetPermits { n: n as usize })
            .await?;

        return Ok(Some(response_with_body_and_code(
            format!(
                "Limit of concurrent requests successfully decreased by {}",
                n
            ),
            StatusCode::OK,
        )));
    }

    return Ok(None);
}
