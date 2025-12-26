use anyhow::anyhow;
use hyper::body::Bytes;
use hyper::{client::HttpConnector, http::request::Parts, Body, Client, Method, Response, Uri};
use tokio::sync::oneshot;

use crate::config::Config;
use crate::handles::helpers::{worker_not_found_response, worker_response};
use crate::structs::EndPoints;
use crate::tasks::datasources::MessageDatasource;
use crate::utils::datasource_id_from_uri;

pub async fn master_endpoint(
    path: &str,
    method: &Method,
    b_bytes: Bytes,
    uri: Uri,
    parts: &Parts,
    ct: &str,
    client: Client<HttpConnector>,
    tx: flume::Sender<MessageDatasource>,
    config: &Config,
) -> Result<Option<Response<Body>>, anyhow::Error> {
    if path.starts_with(EndPoints::Tile.as_ref()) {
        let datasource_id = datasource_id_from_uri(path)
            .expect(&format!("Error extract datasource_id from URI: {}", path));

        let (tx_ds, rx_ds) = oneshot::channel();
        tx.send_async(MessageDatasource::GetDataSource {
            datasource_id: datasource_id.to_string(),
            tx_ds,
        })
        .await?;

        /*
            If the host and port parameters are missing,
            the server will assume that the datasource is located on it.
        */

        if let Some(ds) = rx_ds.await? {
            if let Some(host) = ds.host {
                if let Some(port) = ds.port {
                    if let Some(current_addr) = config.address.as_ref() {
                        let addr = format!("{host}:{port}");
                        if *current_addr == addr {
                            // for current machine skip send request
                            return Ok(None);
                        }
                    }

                    let body = Body::empty();
                    let response = worker_response(
                        host,
                        port,
                        uri,
                        method,
                        &client,
                        parts.headers.clone(),
                        body,
                        ct,
                        config.timeout_worker_response,
                    )
                    .await?;
                    return Ok(Some(response));
                }
            }
        } else {
            let response = worker_not_found_response(datasource_id, ct)?;
            return Ok(Some(response));
        }
    } else if path.starts_with(EndPoints::Pyramid.as_ref()) && method == Method::POST {
        let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
        let datasource_id = body_json
            .get("datasource_id")
            .and_then(|id| id.as_str())
            .ok_or(anyhow!("datasource_id is undefined"))?;

        // in case of a delayed launch of the pyramid, we process it in the function 'pyramid_endpoint'
        if let Some(_) = body_json.get("scheduled_for") {
            return Ok(None);
        }

        let (tx_ds, rx_ds) = oneshot::channel();
        tx.send_async(MessageDatasource::GetDataSource {
            datasource_id: datasource_id.to_string(),
            tx_ds,
        })
        .await?;

        if let Some(ds) = rx_ds.await? {
            if let Some(host) = ds.host {
                if let Some(port) = ds.port {
                    if let Some(current_addr) = config.address.as_ref() {
                        let addr = format!("{host}:{port}");
                        if *current_addr == addr {
                            // for current machine skip send request
                            return Ok(None);
                        }
                    }

                    let body = Body::from(b_bytes);
                    let response = worker_response(
                        host,
                        port,
                        uri,
                        method,
                        &client,
                        parts.headers.clone(),
                        body,
                        ct,
                        config.timeout_worker_response,
                    )
                    .await?;
                    return Ok(Some(response));
                }
            }
        } else {
            let response = worker_not_found_response(datasource_id, ct)?;
            return Ok(Some(response));
        }
    }

    return Ok(None);
}
