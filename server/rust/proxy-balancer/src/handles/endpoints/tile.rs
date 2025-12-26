use std::net::IpAddr;

use anyhow::anyhow;
use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use serde_json::json;
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::db::{get_mbtile, DB};
use crate::defaults::{LOCALHOST, MAXZOOM};
use crate::handles::helpers::{error_response, response_with_body_and_code};
use crate::hyper_reverse_proxy;
use crate::structs::ContentType;
use crate::structs::Extension;
use crate::tasks::datasources::MessageDatasource;
use crate::tasks::semaphore::MessageSemaphore;
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use crate::utils::{
    dataset_dir_from_uri, datasource_id_from_uri, file_path_from_uri, get_tile_from_disk,
    mbtiles_path_from_uri, try_init_mbtiles, zxy_from_uri,
};

pub async fn tile_endpoint(
    cwd: String,
    path: &str,
    pool: DB,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    client_ip: IpAddr,
    port: u16,
    req: Request<Body>,
    client: Client<HttpConnector>,
    tx_sem: flume::Sender<MessageSemaphore>,
    tx: flume::Sender<MessageDatasource>,
) -> Result<Response<Body>, anyhow::Error> {
    let dataset_dir = match dataset_dir_from_uri(&cwd, path) {
        Ok(d) => d,
        Err(err) => {
            let body = json!({
                "status": StatusCode::BAD_REQUEST.as_u16(),
                "message": err.to_string()
            })
            .to_string();
            let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
            return Ok(response);
        }
    };

    let datasource_id = datasource_id_from_uri(path)
        .expect(&format!("Error extract datasource_id from URI: {}", path));

    let file_tile_path = match file_path_from_uri(&cwd, path) {
        Ok(p) => p,
        Err(err) => {
            let body = json!({
                "status": StatusCode::BAD_REQUEST.as_u16(),
                "message": err.to_string()
            })
            .to_string();
            let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
            return Ok(response);
        }
    };

    let ext = file_tile_path.extension().ok_or(anyhow!(
        "Extension of tile file '{:?}' is None",
        file_tile_path
    ))?;
    let png: &str = Extension::Png.into();
    let mvt: &str = Extension::Mvt.into();
    let pbf: &str = Extension::Pbf.into();
    let content_type = if ext == png {
        ContentType::Png.as_ref()
    } else if ext == mvt || ext == pbf {
        ContentType::MvtPbf.as_ref()
    } else {
        ContentType::Empty.as_ref()
    };

    let (x, y, z) = match zxy_from_uri(path) {
        Ok(xyz) => xyz,
        Err(err) => {
            let body = json!({
                "status": StatusCode::BAD_REQUEST.as_u16(),
                "message": err.to_string()
            })
            .to_string();
            let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
            return Ok(response);
        }
    };

    if z > MAXZOOM {
        let body = json!({
            "status": StatusCode::BAD_REQUEST.as_u16(),
            "message": format!("Requested zoom '{}' must be in range 0-{}", z, MAXZOOM)
        })
        .to_string();
        let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
        return Ok(response);
    }

    // Check tile on disk
    if let Ok(Some(response)) = get_tile_from_disk(&file_tile_path, content_type).await {
        return Ok(response);
    }

    match mbtiles_path_from_uri(&cwd, path).await {
        Ok(p) => {
            if let Ok(Some(response)) =
                get_mbtile(&p, z as u64, x, y, content_type, tx_sqlite_client).await
            {
                return Ok(response);
            }
        }
        Err(_err) => {
            if let Some(response) = try_init_mbtiles(
                &cwd,
                dataset_dir,
                datasource_id,
                pool,
                Some(tx_sqlite_client.clone()),
            )
            .await
            {
                // return Response with Error
                return Ok(response);
            }
        }
    };

    let (tx_ds, rx_ds) = oneshot::channel();
    tx.send_async(MessageDatasource::GetDataSource {
        datasource_id: datasource_id.to_string(),
        tx_ds,
    })
    .await?;

    let mut use_cache_only = false;
    if let Some(ds) = rx_ds.await? {
        use_cache_only = ds.use_cache_only.unwrap_or(false);
    }

    if use_cache_only {
        let response = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("Content-Length", 0)
            .header("Access-Control-Allow-Origin", "*")
            .header("Cache-Control", "max-age=0")
            .body(Body::empty())?;
        return Ok(response);
    }

    let (tx_permit, rx_permit) = tokio::sync::oneshot::channel();
    if let Err(err) = tx_sem
        .send_async(MessageSemaphore::GetPermit { port, tx_permit })
        .await
    {
        event!(Level::ERROR, "Error send get permit message {err}");
    }
    let permit = rx_permit.await;

    match hyper_reverse_proxy::call(
        client_ip,
        &format!("http://{}:{}", LOCALHOST, port),
        req,
        &client,
    )
    .await
    {
        Ok(response) => {
            if let Ok(p) = permit {
                drop(p);
            }
            return Ok(response);
        }
        Err(error) => {
            if let Ok(p) = permit {
                drop(p);
            }
            event!(Level::ERROR, "Error request {:?}", error);
            let err_response = error_response(StatusCode::INTERNAL_SERVER_ERROR);
            return Ok(err_response);
        }
    }
}
