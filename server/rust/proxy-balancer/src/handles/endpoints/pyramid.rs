use std::net::IpAddr;
use std::path::PathBuf;

use anyhow::anyhow;
use hyper::{body::Bytes, client::HttpConnector, Body, Client, Request, Response, StatusCode};
use serde_json::json;
use sqlx::types::Json;
use tracing::{event, Level};

use crate::db::{
    job::queue::{JobDetail, JobType},
    DB,
};
use crate::defaults::LOCALHOST;
use crate::handles::helpers::{error_response, response_with_body_and_code};
use crate::hyper_reverse_proxy;
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use crate::utils::{dataset_dir_from_ds_id, mbtiles_path_from_ds_id, try_init_mbtiles};

pub async fn pyramid_endpoint(
    cwd: String,
    pool: DB,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    opt_tx_jd: Option<flume::Sender<JobDetail>>,
    client_ip: IpAddr,
    port: u16,
    req: Request<Body>,
    b_bytes: &Bytes,
    client: Client<HttpConnector>,
) -> Result<Response<Body>, anyhow::Error> {
    let body_json: serde_json::Value = serde_json::from_slice(&b_bytes)?;
    let datasource_id = body_json
        .get("datasource_id")
        .and_then(|id| id.as_str())
        .expect("datasource_id is undefined");

    if let Some(tx_jd) = opt_tx_jd {
        let scheduled_for = body_json.get("scheduled_for");
        if let Some(v) = scheduled_for {
            if let Some(date_str) = v.as_str() {
                // Format date_str -> 2020-04-12T22:10:57+02:00
                let datetime = chrono::DateTime::parse_from_rfc3339(date_str)?;
                let datetime_utc = datetime.with_timezone(&chrono::Utc);
                let job_detail = JobDetail {
                    jt: JobType::Pyramid {
                        datasource_id: datasource_id.to_string(),
                    },
                    name: "Pyramid".to_string(),
                    scheduled_for: Some(datetime_utc),
                    data: Json(body_json.clone()),
                };
                if let Err(err) = tx_jd.send_async(job_detail).await {
                    event!(
                        Level::ERROR,
                        "Error send job detail for DataSource {datasource_id}: {:?}",
                        err
                    );
                    return Err(anyhow!(
                        "Error send job detail for DataSource {datasource_id}: {:?}",
                        err
                    ));
                }

                let message = format!(
                    "Pyramid for DataSource '{}' successfully scheduled",
                    datasource_id
                );
                let body = json!({
                    "status": StatusCode::ACCEPTED.as_u16(),
                    "message": message,
                })
                .to_string();
                let response = response_with_body_and_code(body, StatusCode::ACCEPTED);
                return Ok(response);
            }
        }
    }

    let dataset_dir = dataset_dir_from_ds_id(&cwd, datasource_id)?;
    let mbtiles_db = mbtiles_path_from_ds_id(&cwd, datasource_id)?
        .into_os_string()
        .into_string()
        .map_err(|err| anyhow!("{err:?}"))?;
    if let Err(err) = tx_sqlite_client
        .send_async(MessageSQLiteClient::RemoveSQLiteClient {
            mbtiles_db: mbtiles_db.clone(),
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

    if let Err(err) = remove_mbtiles_files(&cwd, datasource_id, &mbtiles_db).await {
        event!(
            Level::ERROR,
            "Error remove MBTiles files '{mbtiles_db}' {err:?}"
        );
    }

    if let Some(response) = try_init_mbtiles(&cwd, dataset_dir, datasource_id, pool, None).await {
        // return Response with Error
        return Ok(response);
    }
    /*
        Requests to create pyramids of tiles are sent to one worker
        to prevent the creation of several Python processes controlling multiprocessing
    */
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
            event!(Level::ERROR, "Error request {:?}", error);
            let err_response = error_response(StatusCode::INTERNAL_SERVER_ERROR);
            Ok(err_response)
        }
    }
}

async fn remove_mbtiles_files(
    cwd: &str,
    datasource_id: &str,
    mbtiles_db: &str,
) -> Result<(), anyhow::Error> {
    let wal_file: PathBuf = [
        cwd,
        "tiles",
        datasource_id,
        &format!("{}.mbtiles-wal", datasource_id),
    ]
    .iter()
    .collect();

    let shm_file: PathBuf = [
        cwd,
        "tiles",
        datasource_id,
        &format!("{}.mbtiles-shm", datasource_id),
    ]
    .iter()
    .collect();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    if tokio::fs::metadata(&mbtiles_db).await.is_ok()
        && tokio::fs::metadata(&wal_file).await.is_ok()
        && tokio::fs::metadata(&shm_file).await.is_ok()
    {
        if let Err(_err) = tokio::fs::remove_file(&mbtiles_db).await {}
        if let Err(_err) = tokio::fs::remove_file(&wal_file).await {}
        if let Err(_err) = tokio::fs::remove_file(&shm_file).await {}
    }

    Ok(())
}
