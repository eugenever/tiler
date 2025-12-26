use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use hyper::Response;
use hyper::{client::HttpConnector, http::HeaderMap, Body, Client, Method, Request, Uri};
use serde_json::json;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{event, Level};

use super::datasources::MessageDatasource;
use super::reload_workers::MessageMaintenanceWorkers;
use super::sqlite_clients::MessageSQLiteClient;
use crate::config::Config;
use crate::db::{
    job::{
        postgres::PostgresQueue,
        queue::{Job, JobDetail, JobType, Queue},
    },
    DB,
};
use crate::defaults::{JOB_CONCURRENCY, LOCALHOST};
use crate::handles::helpers::{get_worker_data, worker_response};
use crate::hyper_reverse_proxy;
use crate::structs::{ContentType, EndPoints};
use crate::utils::{dataset_dir_from_ds_id, mbtiles_path_from_ds_id, try_init_mbtiles};

pub fn init_job_queue(
    cwd: String,
    pool: DB,
    rx_jd: flume::Receiver<JobDetail>,
    tx: flume::Sender<MessageDatasource>,
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    config: Config,
) -> Result<(JoinHandle<()>, JoinHandle<()>), anyhow::Error> {
    let pg_queue = PostgresQueue::new(pool.clone());
    let jh_add_job = job_add_to_queue(rx_jd, pg_queue.clone());
    let jh_job_worker = job_worker(
        cwd,
        pg_queue,
        pool,
        tx,
        tx_mw,
        tx_sqlite_client,
        config.clone(),
    );
    Ok((jh_add_job, jh_job_worker))
}

pub fn job_add_to_queue(
    rx_jd: flume::Receiver<JobDetail>,
    pg_queue: PostgresQueue,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(jd) = rx_jd.recv_async().await {
            if let Err(err) = pg_queue.push(jd.clone(), jd.scheduled_for).await {
                let _jd = jd.clone();
                let jt = _jd.jt.as_ref();
                event!(Level::ERROR, "Error push job {jt}: {:?}", err);
            }
        }
    })
}

pub fn job_worker(
    cwd: String,
    pg_queue: PostgresQueue,
    pool: DB,
    tx: flume::Sender<MessageDatasource>,
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    config: Config,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client_ip: IpAddr = IpAddr::from_str(LOCALHOST).unwrap();
        loop {
            let jobs = match pg_queue.pull(Some(JOB_CONCURRENCY as i32)).await {
                Ok(jobs) => jobs,
                Err(err) => {
                    event!(Level::ERROR, "Error 'job_worker' pulling jobs: {}", err);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Vec::new()
                }
            };

            if jobs.len() == JOB_CONCURRENCY {
                let job = &jobs[0];

                let _index;
                let _port;
                let ports;
                let client;
                match get_worker_data(tx_mw.clone()).await {
                    Err(err) => {
                        event!(Level::ERROR, "Error 'job_worker' get worker data: {err}");
                        continue;
                    }
                    Ok(wd) => {
                        _index = wd.index;
                        _port = wd.port;
                        ports = wd.ports;
                        client = wd.client;
                    }
                }

                match &job.detail.jt {
                    // Pyramid JOBs
                    JobType::Pyramid { datasource_id } => {
                        if let Err(err) = job_pyramid(
                            cwd.clone(),
                            tx.clone(),
                            datasource_id,
                            &client,
                            &config,
                            pg_queue.clone(),
                            job,
                            tx_sqlite_client.clone(),
                            &pool,
                            client_ip,
                            &ports,
                        )
                        .await
                        {
                            event!(
                                Level::ERROR,
                                "Error 'job_pyramid' for job_id '{}' {:?}",
                                job.job_id.to_string(),
                                err
                            );
                        }
                    }
                    // Any Calculating JOBs
                    JobType::Calculation => {}
                }
            }

            tokio::time::sleep(Duration::from_secs(config.timeout_pull_job)).await;
        }
    })
}

async fn job_pyramid(
    cwd: String,
    tx: flume::Sender<MessageDatasource>,
    datasource_id: &str,
    client: &Client<HttpConnector>,
    config: &Config,
    pg_queue: PostgresQueue,
    job: &Job,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    pool: &DB,
    client_ip: IpAddr,
    ports: &Vec<u16>,
) -> Result<(), anyhow::Error> {
    let (tx_ds, rx_ds) = oneshot::channel();
    tx.send_async(MessageDatasource::GetDataSource {
        datasource_id: datasource_id.to_string(),
        tx_ds,
    })
    .await?;

    let body = json!({
        "datasource_id": datasource_id
    })
    .to_string();

    // in case the DataSource is located on another server
    if let Some(ds) = rx_ds.await? {
        if let Some(host) = ds.host {
            if let Some(port) = ds.port {
                if let Some(current_addr) = config.address.as_ref() {
                    let addr = format!("{host}:{port}");
                    // if NOT current machine then send request
                    if *current_addr != addr {
                        let body = Body::from(body);
                        let uri = Uri::from_str(EndPoints::Pyramid.as_ref())?;
                        let method = Method::POST;
                        let headers = HeaderMap::new();
                        let ct = ContentType::ApplicationJson.as_ref();

                        match worker_response(
                            host,
                            port,
                            uri,
                            &method,
                            &client,
                            headers,
                            body,
                            ct,
                            config.timeout_worker_response,
                        )
                        .await
                        {
                            Ok(response) => {
                                job_processing_result(response, &pg_queue, job).await?;
                            }
                            Err(err) => {
                                event!(
                                    Level::ERROR,
                                    "Error 'job_worker' request to worker {:?}",
                                    err
                                );
                                match pg_queue.fail_job(job.job_id).await {
                                    Err(err) => {
                                        event!(
                                            Level::ERROR,
                                            "Error 'fail_job' for job '{}' {:?}",
                                            job.job_id.to_string(),
                                            err
                                        );
                                    }
                                    Ok(_) => {}
                                };
                            }
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

    // DataSource is on the current server instance
    let dataset_dir = dataset_dir_from_ds_id(&cwd, datasource_id)?;
    let mbtiles_db = mbtiles_path_from_ds_id(&cwd, datasource_id)
        .expect("Error mbtiles_path_from_ds_id")
        .into_os_string()
        .into_string()
        .map_err(|err| anyhow!("{:?}", err))?;

    if let Err(err) = tx_sqlite_client
        .send_async(MessageSQLiteClient::RemoveSQLiteClient {
            mbtiles_db,
            remove_tiles_folder: None,
            remove_tiles_db: Some(true),
        })
        .await
    {
        event!(
            Level::ERROR,
            "Error send message to Remove SQLite client {err:?}"
        );
    }

    if let Some(response) =
        try_init_mbtiles(&cwd, dataset_dir, datasource_id, pool.clone(), None).await
    {
        // return Response with Error
        let b = response.into_body();
        let b_bytes = hyper::body::to_bytes(b).await?;
        let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
        let message = body_json
            .get("message")
            .and_then(|m| m.as_str())
            .ok_or(anyhow!("Message is missing"))?;
        event!(Level::ERROR, "Error 'job_worker' init mbtiles: {}", message);
    }

    /*
        Requests to create pyramids of tiles are sent to one worker
        to prevent the creation of several Python processes controlling multiprocessing
    */
    let request = Request::builder()
        .method(Method::POST)
        .uri(EndPoints::Pyramid.as_ref())
        .body(Body::from(body))?;

    match hyper_reverse_proxy::call(
        client_ip,
        &format!("http://{}:{}", LOCALHOST, ports[0]),
        request,
        &client,
    )
    .await
    {
        Ok(response) => {
            job_processing_result(response, &pg_queue, job).await?;
        }
        Err(err) => {
            event!(
                Level::ERROR,
                "Error 'job_worker' request to worker {:?}",
                err
            );
            match pg_queue.fail_job(job.job_id).await {
                Err(err) => {
                    event!(
                        Level::ERROR,
                        "Error 'fail_job' for job '{}' {:?}",
                        job.job_id.to_string(),
                        err
                    );
                }
                Ok(_) => {}
            };
        }
    }

    Ok(())
}

pub async fn job_processing_result(
    response: Response<Body>,
    pg_queue: &PostgresQueue,
    job: &Job,
) -> Result<(), anyhow::Error> {
    let status = response.status().as_u16();
    let code = ((status as f32 / 100.0) as f32).round() as u16;
    if code == 4 || code == 5 || code == 6 {
        let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
        let err = String::from_utf8(body_bytes.to_vec())?;
        event!(
            Level::ERROR,
            "Error 'job_worker' request to worker: {err:?}"
        );
        match pg_queue.fail_job(job.job_id).await {
            Err(err) => {
                event!(
                    Level::ERROR,
                    "Error 'fail_job' for job '{}' {:?}",
                    job.job_id.to_string(),
                    err
                );
            }
            Ok(_) => {}
        };
    } else {
        match pg_queue.delete_job(job.job_id).await {
            Err(err) => {
                event!(
                    Level::ERROR,
                    "Error delete job '{}' {:?}",
                    job.job_id.to_string(),
                    err
                );
            }
            Ok(_) => {}
        };
    }

    Ok(())
}
