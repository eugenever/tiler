use std::convert::Infallible;
use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::anyhow;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde_json::json;
use tracing::{event, Level};

use super::helpers::debug_request;
use crate::db::get_mbtile;
use crate::defaults::MAXZOOM;
use crate::handles::helpers::response_with_body_and_code;
use crate::structs::{ContentType, EndPoints, Extension};
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use crate::utils::{
    dataset_dir_from_uri, datasource_id_from_uri, file_path_from_uri, mbtiles_path_from_uri,
    zxy_from_uri,
};

pub async fn handle_cache(
    cwd: String,
    _client_ip: IpAddr,
    req: Request<Body>,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    base_path: PathBuf,
) -> Result<Response<Body>, Infallible> {
    let uri = req.uri().clone();
    let path = uri.path();
    let method = req.method().clone();

    if path.starts_with(EndPoints::Tile.as_ref()) && method == Method::GET {
        match tile_from_cache(&cwd, path, tx_sqlite_client).await {
            Err(err) => {
                event!(Level::ERROR, "Error 'tile_from_cache': {err:?}");
            }
            Ok(response) => return Ok(response),
        }
    } else if path.starts_with(EndPoints::Static.as_ref()) && method == Method::GET {
        match static_files(path, base_path).await {
            Err(err) => {
                let body = json!({
                    "status": StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    "message": err.to_string()
                })
                .to_string();
                let response = response_with_body_and_code(body, StatusCode::INTERNAL_SERVER_ERROR);
                return Ok(response);
            }
            Ok(response) => return Ok(response),
        }
    }

    debug_request(req)
}

async fn static_files(path: &str, base_path: PathBuf) -> Result<Response<Body>, anyhow::Error> {
    let mut file_path = base_path;
    for part in path.split('/').skip(1) {
        // skip the first empty part
        file_path.push(part);
    }

    if let Err(err) = tokio::fs::try_exists(&file_path).await {
        let body = json!({
            "status": StatusCode::BAD_REQUEST.as_u16(),
            "message": err.to_string()
        })
        .to_string();
        let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
        return Ok(response);
    }

    let mime_type = mime_guess::from_path(&file_path).first_or_octet_stream();
    let file_path_str = file_path.to_str().ok_or(anyhow!(
        "Error converting file path '{:?}' to string",
        file_path
    ))?;
    let file_path_dec = urlencoding::decode(file_path_str)?.to_string();

    let response = match tokio::fs::read(&file_path_dec).await {
        Ok(data) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.as_ref())
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "*")
            .header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            .body(Body::from(data))?,
        Err(err) => {
            let body = json!({
                "message": err.to_string()
            })
            .to_string();
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(body))?
        }
    };

    Ok(response)
}

async fn tile_from_cache(
    cwd: &str,
    path: &str,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
) -> Result<Response<Body>, anyhow::Error> {
    let _dataset_dir = match dataset_dir_from_uri(&cwd, path) {
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
    let _datasource_id = datasource_id_from_uri(path)
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

    match mbtiles_path_from_uri(&cwd, path).await {
        Ok(p) => {
            if let Ok(Some(response)) =
                get_mbtile(&p, z as u64, x, y, content_type, tx_sqlite_client).await
            {
                return Ok(response);
            }
        }
        Err(err) => {
            let error = format!("{path}: {err:?}");
            event!(Level::ERROR, "{error}");
            let body = json!({
                "status": StatusCode::BAD_REQUEST.as_u16(),
                "message": error
            })
            .to_string();
            let response = response_with_body_and_code(body, StatusCode::BAD_REQUEST);
            return Ok(response);
        }
    };

    let response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("Content-Length", 0)
        .header("Access-Control-Allow-Origin", "*")
        .header("Cache-Control", "max-age=0")
        .body(Body::empty())?;
    return Ok(response);
}
