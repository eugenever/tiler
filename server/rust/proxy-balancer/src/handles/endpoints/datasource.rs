use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::anyhow;
use hyper::{
    body::Bytes, client::HttpConnector, http::header, http::request::Parts, Body, Client, Method,
    Request, Response, StatusCode, Uri,
};
use serde_json::json;
use tracing::{event, Level};

use crate::defaults::{LOCALHOST, MASTER_HEADER};
use crate::handles::helpers::error_response;
use crate::hyper_reverse_proxy::{self, ProxyError};
use crate::structs::EndPoints;
use crate::tasks::datasources::MessageDatasource;
use crate::tasks::sqlite_clients::MessageSQLiteClient;

pub async fn datasource_endpoint(
    port: u16,
    ports: Vec<u16>,
    parts: Parts,
    client_ip: IpAddr,
    client: Client<HttpConnector>,
    tx: flume::Sender<MessageDatasource>,
    req: Request<Body>,
) -> Result<Response<Body>, anyhow::Error> {
    let response = hyper_reverse_proxy::call(
        client_ip,
        &format!("http://{}:{}", LOCALHOST, port),
        req,
        &client,
    )
    .await;

    let mut handles = Vec::with_capacity(ports.len());

    for p in ports {
        let h = parts.headers.clone();
        let client = client.clone();

        let jh = tokio::spawn({
            async move {
                let mut load_dss_request = Request::builder()
                    .method(Method::GET)
                    .uri(EndPoints::DataSources.as_ref())
                    .body(Body::empty())?;

                *load_dss_request.headers_mut() = h;
                let res = hyper_reverse_proxy::call(
                    client_ip,
                    &format!("http://{}:{}", LOCALHOST, p),
                    load_dss_request,
                    &client,
                )
                .await;

                if let Err(_err) = res {
                    event!(Level::ERROR, "Error Reload DataSources request {:?}", &_err);
                }
                Ok::<(), anyhow::Error>(())
            }
        });
        handles.push(jh);
    }
    for jh in handles {
        jh.await??;
    }

    {
        let is_header_master = parts.headers.contains_key(MASTER_HEADER);
        if let Err(err) = tx
            .send_async(MessageDatasource::UpdateDataSources { is_header_master })
            .await
        {
            event!(
                Level::ERROR,
                "Error send message to update DataSources {:?}",
                err
            );
        }
    }

    match response {
        Ok(response) => Ok(response),
        Err(error) => {
            event!(Level::ERROR, "Error request {:?}", error);
            let err_response = error_response(StatusCode::INTERNAL_SERVER_ERROR);
            Ok(err_response)
        }
    }
}

pub async fn datasource_delete_endpoint(
    cwd: &str,
    ports: Vec<u16>,
    parts: Parts,
    uri: Uri,
    b_bytes: Bytes,
    client_ip: IpAddr,
    client: Client<HttpConnector>,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    tx: flume::Sender<MessageDatasource>,
    ct: &str,
) -> Result<Response<Body>, anyhow::Error> {
    let mut is_err = false;
    let mut err: Option<ProxyError> = None;
    let mut handles = Vec::with_capacity(ports.len());

    for p in ports {
        let h = parts.headers.clone();
        let uri = uri.clone();
        let bb = b_bytes.clone();
        let client = client.clone();

        let jh = tokio::spawn({
            async move {
                let mut del_request = Request::builder()
                    .method(Method::DELETE)
                    .uri(uri.clone())
                    .body(Body::from(bb))
                    .unwrap();

                *del_request.headers_mut() = h;
                let res = hyper_reverse_proxy::call(
                    client_ip,
                    &format!("http://{}:{}", LOCALHOST, p),
                    del_request,
                    &client,
                )
                .await;

                let mut err: Option<ProxyError> = None;
                if let Err(_err) = res {
                    event!(
                        Level::ERROR,
                        "Error delete request to datasources {:?}",
                        &_err
                    );
                    err = Some(_err);
                }
                err
            }
        });
        handles.push(jh);
    }

    for jh in handles {
        if let Some(e) = jh.await? {
            err = Some(e);
            is_err = true;
        }
    }

    let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
    let datasource_id = body_json
        .get("datasource_id")
        .and_then(|id| id.as_str())
        .expect("datasource_id is undefined");

    let status_code: StatusCode;
    let message: String;
    if is_err {
        status_code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("Error remove DataSource '{}': {:?}", datasource_id, err);
    } else {
        status_code = StatusCode::OK;
        message = format!("DataSource '{}' successfully remove", datasource_id);
    }

    {
        let mbtiles_db: String = [
            cwd,
            "tiles",
            &datasource_id,
            &format!("{}.mbtiles", datasource_id),
        ]
        .iter()
        .collect::<PathBuf>()
        .into_os_string()
        .into_string()
        .map_err(|err| anyhow!("{err:?}"))?;

        if let Err(err) = tx_sqlite_client
            .send_async(MessageSQLiteClient::RemoveSQLiteClient {
                mbtiles_db,
                remove_tiles_folder: Some(true),
                remove_tiles_db: None,
            })
            .await
        {
            event!(
                Level::ERROR,
                "Error send message to Remove SQLite client {err:?}"
            );
        }

        // this endpoint does not trigger calls 'worker_load_dss' function
        let is_header_master = false;
        if let Err(err) = tx
            .send_async(MessageDatasource::UpdateDataSources { is_header_master })
            .await
        {
            event!(
                Level::ERROR,
                "Error send message to update DataSources {:?}",
                err
            );
        }
    }

    let body = json!({
        "status": status_code.as_u16(),
        "message": message
    })
    .to_string();
    let response = Response::builder()
        .status(status_code)
        .header(header::CONTENT_TYPE, ct)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from(body))?;
    Ok(response)
}
