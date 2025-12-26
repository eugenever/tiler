use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io::Write};

use anyhow::{anyhow, Error};
use hyper::client::HttpConnector;
use hyper::header::HeaderValue;
use hyper::{Body, Client, Method, Request, Response, StatusCode};
use serde_json::json;
use tracing::{event, Level};

use crate::config::Config;
use crate::db::{init_mbtiles_db, DB};
use crate::defaults::{LOCALHOST, MASTER_HEADER};
use crate::handles::helpers::response_with_body_and_code;
use crate::structs::Extension;
use crate::tasks::datasources::{load_datasource_from_db, MapDataSources};
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use crate::tasks::workers::info_workers;

pub async fn worker_load_dss(
    config: &Config,
    map_dss: &MapDataSources,
    client: &Client<HttpConnector>,
) -> Result<(), anyhow::Error> {
    let mut uries = Vec::with_capacity(map_dss.datasources.len() as usize);
    let v = HeaderValue::from_str(&config.address.clone().unwrap_or("isone".to_string()))?;
    for ds in map_dss.datasources.values() {
        if let Some(ref host) = ds.host.clone() {
            if let Some(port) = ds.port {
                // check is some variant
                if let Some(current_addr) = config.address.as_ref() {
                    let addr = format!("{host}:{port}");
                    if *current_addr == addr {
                        // for current machine skip send request
                        continue;
                    }
                }

                let uri = format!("http://{host}:{port}/api/datasources");
                if uries.iter().any(|u| *u == uri) {
                    continue;
                }
                uries.push(uri.clone());

                // header "Master-Server" to avoid endless sending of requests between some masters
                let req = Request::builder()
                    .method(Method::GET)
                    .uri(uri.clone())
                    .header(MASTER_HEADER, v.clone())
                    .body(Body::empty())?;

                tokio::spawn({
                    let client = client.clone();
                    let host = host.clone();
                    let timeout = config.timeout_worker_response;

                    async move {
                        match tokio::time::timeout(
                            Duration::from_secs(timeout),
                            client.request(req),
                        )
                        .await
                        {
                            Ok(res) => match res {
                                Err(err) => {
                                    event!(
                                        Level::ERROR,
                                        "Error worker '{host}:{port}' load DataSources {err:?}"
                                    );
                                }
                                Ok(_) => {}
                            },
                            Err(_) => {
                                event!(
                                    Level::ERROR,
                                    "Request to worker '{host}:{port}' completed due to timeout"
                                );
                            }
                        }
                    }
                });
            }
        }
    }

    Ok(())
}

pub async fn try_save_process_pid(cwd: &str, workers_pids: Vec<u32>) -> Result<(), anyhow::Error> {
    let mut delay = tokio::time::interval(std::time::Duration::from_secs(1));
    // The first tick completes immediately.
    delay.tick().await;

    let count_attempts = 10;
    for i in 0..count_attempts {
        delay.tick().await;
        let is_workers_run = match save_process_pid(cwd, workers_pids.clone()).await {
            Err(err) => {
                return Err(err);
            }
            Ok(res) => res,
        };
        if is_workers_run {
            break;
        } else {
            if i == count_attempts - 1 {
                return Err(anyhow!(
                    "'try_save_process_pid': no child processes defined"
                ));
            }
        }
    }
    Ok(())
}

pub async fn save_process_pid(cwd: &str, workers_pids: Vec<u32>) -> Result<bool, anyhow::Error> {
    let jh = info_workers(workers_pids);
    match jh.await {
        Err(err) => {
            event!(
                Level::ERROR,
                "'save_process_pid': error get system info workers {err}"
            );
            Err(anyhow!(
                "'save_process_pid': error get system info workers {err}"
            ))
        }
        Ok(iw) => {
            let pids: PathBuf = [cwd, "scripts", "PIDs"].iter().collect();
            let mut file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(pids)?;
            for (wid, childs_id) in &iw.worker_childs {
                if childs_id.len() == 0 {
                    return Ok(false);
                }
                writeln!(file, "{}", wid)?;
                for child_ids in childs_id {
                    writeln!(file, "{}", child_ids.0)?;
                }
            }
            writeln!(file, "{}", std::process::id())?;
            Ok(true)
        }
    }
}

pub async fn get_available_port(
    port_from: u16,
    port_to: u16,
    exclude_ports: &[u16],
) -> Option<u16> {
    for p in port_from..=port_to {
        if port_is_available(p).await {
            if exclude_ports.len() == 0 {
                return Some(p);
            }
            if exclude_ports.contains(&p) {
                continue;
            }
            return Some(p);
        }
    }
    return None;
}

pub async fn port_is_available(port: u16) -> bool {
    match tokio::net::TcpListener::bind((LOCALHOST, port)).await {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub fn datasource_id_from_uri(uri: &str) -> Result<&str, Error> {
    let path_elements: Vec<&str> = uri.split("/").collect();
    if path_elements.len() < 7 {
        return Err(anyhow!("Path of URI is invalid: {:?}", path_elements));
    }
    let datasource_id = path_elements[3];
    Ok(datasource_id)
}

pub fn dataset_dir_from_uri(cwd: &str, uri: &str) -> Result<PathBuf, Error> {
    let path_elements: Vec<&str> = uri.split("/").collect();
    if path_elements.len() < 7 {
        return Err(anyhow!("Path of URI is invalid: {:?}", path_elements));
    }
    let datasource_id = path_elements[3];
    let dataset_dir: PathBuf = [cwd, "tiles", datasource_id].iter().collect();
    Ok(dataset_dir)
}

pub fn dataset_dir_from_ds_id(cwd: &str, datasource_id: &str) -> Result<PathBuf, Error> {
    let dataset_dir: PathBuf = [cwd, "tiles", datasource_id].iter().collect();
    Ok(dataset_dir)
}

pub fn mbtiles_path_from_ds_id(cwd: &str, datasource_id: &str) -> Result<PathBuf, Error> {
    let mbtiles_db: PathBuf = [
        cwd,
        "tiles",
        datasource_id,
        &format!("{datasource_id}.mbtiles"),
    ]
    .iter()
    .collect();
    Ok(mbtiles_db)
}

pub async fn try_init_mbtiles(
    cwd: &str,
    dataset_dir: PathBuf,
    datasource_id: &str,
    pool: DB,
    tx_sqlite_client: Option<flume::Sender<MessageSQLiteClient>>,
) -> Option<Response<Body>> {
    // Check dataset exists
    if tokio::fs::metadata(&dataset_dir).await.is_err() {
        // check 'tiles' directory exists
        let tiles_dir: PathBuf = [cwd, "tiles"].iter().collect();
        if tokio::fs::metadata(&tiles_dir).await.is_err() {
            if let Err(err) = tokio::fs::create_dir(&tiles_dir)
                .await
                .map_err(|err| anyhow!(err))
            {
                event!(Level::ERROR, "Error create 'tiles' directory {err:?}");
            }
        }

        if let Ok(ds) = load_datasource_from_db(&pool, datasource_id).await {
            if let Ok(_) = tokio::fs::create_dir(&dataset_dir)
                .await
                .map_err(|err| anyhow!(err))
            {
                // init MBTiles Database
                if let Some(mbtiles) = ds.mbtiles {
                    if mbtiles {
                        init_mbtiles_db(cwd, ds.identifier.clone(), tx_sqlite_client)
                            .await
                            .expect(&format!(
                                "Error init mbtiles database for ID '{}'",
                                ds.identifier.clone()
                            ));
                    }
                }
            }
            return None;
        } else {
            let body = json!({
                "status": 404,
                "message": format!("DataSource with id '{}' does not exists", datasource_id)
            })
            .to_string();
            let response = response_with_body_and_code(body, StatusCode::NOT_FOUND);
            return Some(response);
        }
    }
    return None;
}

pub fn file_path_from_uri(cwd: &str, uri: &str) -> Result<PathBuf, Error> {
    let path_elements: Vec<&str> = uri.split("/").collect();
    if path_elements.len() < 7 {
        return Err(anyhow!("Path of URI is invalid: {:?}", path_elements));
    }
    let datasource_id = path_elements[3];
    let z = match path_elements[4].parse::<i64>() {
        Ok(v) => v,
        Err(err) => return Err(anyhow!("Error parse Z: {}", err)),
    };
    let x = match path_elements[5].parse::<i64>() {
        Ok(v) => v,
        Err(err) => return Err(anyhow!("Error parse X: {}", err)),
    };

    let file_tile_name: PathBuf = [
        cwd,
        "tiles",
        datasource_id,
        &z.to_string(),
        &x.to_string(),
        path_elements[6],
    ]
    .iter()
    .collect();

    Ok(file_tile_name)
}

pub async fn mbtiles_path_from_uri(cwd: &str, uri: &str) -> Result<PathBuf, Error> {
    let path_elements: Vec<&str> = uri.split("/").collect();
    if path_elements.len() < 7 {
        return Err(anyhow!("Path of URI is invalid: {:?}", path_elements));
    }
    let datasource_id = path_elements[3];
    let datasource_tiles_dir: PathBuf = [cwd, "tiles", datasource_id].iter().collect();
    let mbtile_files = get_mbtiles_paths(datasource_tiles_dir).await?;
    if mbtile_files.len() > 0 {
        return Ok(mbtile_files[0].clone());
    }

    Err(anyhow!(
        "DataBase of MBTiles for DataSource ID '{}' not found",
        datasource_id
    ))
}

async fn get_mbtiles_paths(dir: PathBuf) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut paths = Vec::new();
    let mut dir = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "mbtiles") {
            paths.push(path);
        }
    }
    Ok(paths)
}

pub fn zxy_from_uri(uri: &str) -> Result<(u64, u64, u8), Error> {
    let path_elements: Vec<&str> = uri.split("/").collect();
    if path_elements.len() < 7 {
        return Err(anyhow!("Path of URI is invalid: {:?}", path_elements));
    }
    let z = match path_elements[4].parse::<u8>() {
        Ok(v) => v,
        Err(err) => return Err(anyhow!("Error parse Z: {}", err)),
    };
    let x = match path_elements[5].parse::<u64>() {
        Ok(v) => v,
        Err(err) => return Err(anyhow!("Error parse X: {}", err)),
    };
    let y_ext = path_elements[6].split(".").collect::<Vec<_>>();
    let y = match y_ext[0].parse::<u64>() {
        Ok(v) => v,
        Err(err) => return Err(anyhow!("Error parse Y: {}", err)),
    };

    let png = Extension::Png.as_ref();
    let mvt = Extension::Mvt.as_ref();
    let pbf = Extension::Pbf.as_ref();

    if y_ext[1] != png && y_ext[1] != mvt && y_ext[1] != pbf {
        if y_ext[1].to_lowercase() != png
            && y_ext[1].to_lowercase() != mvt
            && y_ext[1].to_lowercase() != pbf
        {
            return Err(anyhow!(
                "Extension of tile must be '{}', '{}' or '{}'. Got {}",
                png,
                mvt,
                pbf,
                y_ext[1]
            ));
        }
    }

    Ok((x, y, z))
}

pub async fn get_tile_from_disk(
    file_tile_path: &PathBuf,
    content_type: &str,
) -> Result<Option<Response<Body>>, anyhow::Error> {
    if let Ok(md) = tokio::fs::metadata(&file_tile_path).await {
        if md.len() > 0 {
            let tile = tokio::fs::read(file_tile_path).await?;
            let response = tile_response(tile, content_type)?;
            return Ok(Some(response));
        } else {
            let response = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("Content-Length", 0)
                .header("Access-Control-Allow-Origin", "*")
                .header("Cache-Control", "max-age=0")
                .body(Body::empty())?;
            return Ok(Some(response));
        }
    }
    Ok(None)
}

pub fn tile_response(tile: Vec<u8>, content_type: &str) -> Result<Response<Body>, anyhow::Error> {
    let response = if tile.starts_with(b"\x1f\x8b\x08") {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-type", content_type)
            .header("Access-Control-Allow-Origin", "*")
            .header("Cache-Control", "max-age=0")
            .header("Content-Encoding", "gzip")
            .body(Body::from(tile))?
    } else {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-type", content_type)
            .header("Access-Control-Allow-Origin", "*")
            .header("Cache-Control", "max-age=0")
            .body(Body::from(tile))?
    };
    Ok(response)
}
