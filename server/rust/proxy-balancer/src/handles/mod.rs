pub mod cache;
pub mod endpoints;
pub mod helpers;

use std::convert::Infallible;
use std::net::IpAddr;

use hyper::{Body, Method, Request, Response, StatusCode};
use tracing::{event, Level};

use crate::config::Config;
use crate::db::{job::queue::JobDetail, DB};
use crate::defaults::LOCALHOST;
use crate::hyper_reverse_proxy;
use crate::structs::{ContentType, EndPoints};
use crate::tasks::datasources::MessageDatasource;
use crate::tasks::reload_workers::MessageMaintenanceWorkers;
use crate::tasks::semaphore::MessageSemaphore;
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use endpoints::{
    datasource::{datasource_delete_endpoint, datasource_endpoint},
    health::health_endpoint,
    maintenance::maintenance_endpoint,
    master::master_endpoint,
    pyramid::pyramid_endpoint,
    tile::tile_endpoint,
};
use helpers::{debug_request, error_response, error_response_endpoint, get_worker_data};

pub async fn handle(
    cwd: String,
    client_ip: IpAddr,
    mut req: Request<Body>,
    pool: DB,
    config: Config,
    tx: flume::Sender<MessageDatasource>,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
    tx_jd: Option<flume::Sender<JobDetail>>,
    tx_sem: flume::Sender<MessageSemaphore>,
) -> Result<Response<Body>, Infallible> {
    let uri = req.uri().clone();
    let path = uri.path();
    let path_elements: Vec<&str> = path.split("/").collect();

    let method = req.method().clone();
    let (parts, b) = req.into_parts();
    let b_bytes = hyper::body::to_bytes(b).await.unwrap();

    // Maintenance endpoints
    match maintenance_endpoint(path, &method, &b_bytes, tx_mw.clone(), tx_sem.clone()).await {
        Err(err) => {
            let response = error_response_endpoint("maintenance_endpoint", err);
            return Ok(response);
        }
        Ok(value) => {
            if let Some(response) = value {
                return Ok(response);
            }
        }
    }

    let _index;
    let port;
    let ports;
    let client;
    match get_worker_data(tx_mw.clone()).await {
        Err(err) => {
            let response = error_response_endpoint("receive WorkerData", err);
            return Ok(response);
        }
        Ok(wd) => {
            _index = wd.index;
            port = wd.port;
            ports = wd.ports;
            client = wd.client;
        }
    }

    // Rebuild request after consume
    req = Request::builder()
        .method(method.clone())
        .uri(uri.clone())
        .body(Body::from(b_bytes.clone()))
        .unwrap();
    *req.headers_mut() = parts.headers.clone();

    let ct = ContentType::ApplicationJson.as_ref();

    /*
        In master mode, requests to server workers are made for tiles
        and for generating raster pyramids
    */
    if config.master {
        match master_endpoint(
            path,
            &method,
            b_bytes.clone(),
            uri.clone(),
            &parts,
            ct,
            client.clone(),
            tx.clone(),
            &config,
        )
        .await
        {
            Err(err) => {
                let response = error_response_endpoint("master_endpoint", err);
                return Ok(response);
            }
            Ok(value) => {
                if let Some(response) = value {
                    return Ok(response);
                }
            }
        }
    }

    /*
        If the host and port parameters are missing,
        the server will assume that the datasource is located on it.
    */

    // Worker mode at adrress is None (missing)
    if path.starts_with(EndPoints::Tile.as_ref()) {
        match tile_endpoint(
            cwd,
            path,
            pool,
            tx_sqlite_client,
            client_ip,
            port,
            req,
            client,
            tx_sem,
            tx,
        )
        .await
        {
            Err(err) => {
                let response = error_response_endpoint("tile_endpoint", err);
                Ok(response)
            }
            Ok(response) => Ok(response),
        }
    } else if path.starts_with(EndPoints::Pyramid.as_ref()) && method == Method::POST {
        match pyramid_endpoint(
            cwd,
            pool,
            tx_sqlite_client,
            tx_jd,
            client_ip,
            ports[0],
            req,
            &b_bytes,
            client,
        )
        .await
        {
            Err(err) => {
                let response = error_response_endpoint("pyramid_endpoint", err);
                Ok(response)
            }
            Ok(response) => Ok(response),
        }
    } else if path.starts_with(EndPoints::DataSources.as_ref()) && method == Method::DELETE {
        match datasource_delete_endpoint(
            &cwd,
            ports,
            parts,
            uri,
            b_bytes,
            client_ip,
            client,
            tx_sqlite_client,
            tx,
            ct,
        )
        .await
        {
            Err(err) => {
                let response = error_response_endpoint("datasource_delete_endpoint", err);
                Ok(response)
            }
            Ok(response) => Ok(response),
        }
    } else if ((path.starts_with(EndPoints::DataSources.as_ref()) && path_elements.len() == 3)
        || path.starts_with(EndPoints::DataSourcesLoadFiles.as_ref())
        || path.starts_with(EndPoints::DataSourcesReloadFiles.as_ref()))
        && (method == Method::POST
            || method == Method::PUT
            || method == Method::PATCH
            || method == Method::GET)
    {
        match datasource_endpoint(port, ports, parts, client_ip, client, tx, req).await {
            Err(err) => {
                let response = error_response_endpoint("datasource_endpoint", err);
                Ok(response)
            }
            Ok(response) => Ok(response),
        }
    } else if path.starts_with(EndPoints::Health.as_ref()) {
        match health_endpoint(ports, &parts, client_ip, client, ct).await {
            Err(err) => {
                let response = error_response_endpoint("health_endpoint", err);
                Ok(response)
            }
            Ok(response) => Ok(response),
        }
    } else if path.starts_with(EndPoints::Jobs.as_ref()) {
        debug_request(req)
    } else if path.starts_with(EndPoints::Debug.as_ref()) {
        debug_request(req)
    } else {
        match hyper_reverse_proxy::call(
            client_ip,
            &format!("http://{}:{}", LOCALHOST, port),
            req,
            &client,
        )
        .await
        {
            Ok(response) => Ok(response),
            Err(error) => {
                event!(Level::ERROR, "Error request: {:?}", error);
                let err_response = error_response(StatusCode::INTERNAL_SERVER_ERROR);
                Ok(err_response)
            }
        }
    }
}
