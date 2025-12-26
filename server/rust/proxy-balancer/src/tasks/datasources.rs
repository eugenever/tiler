use std::collections::HashMap;
use std::path::PathBuf;
use std::process::exit;

use anyhow::anyhow;
use hyper::Client;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::sqlite_clients::MessageSQLiteClient;
use crate::config::Config;
use crate::db::{init_mbtiles_db, DB};
use crate::utils::worker_load_dss;

#[derive(Debug, Clone)]
pub struct DataSourceInfo {
    pub host: Option<String>,
    pub port: Option<i32>,
    pub use_cache_only: Option<bool>,
    pub compress_tiles: Option<bool>,
}

pub enum MessageDatasource {
    GetDataSource {
        datasource_id: String,
        tx_ds: oneshot::Sender<Option<DataSourceInfo>>,
    },
    UpdateDataSources {
        is_header_master: bool,
    },
}

#[derive(Debug, Clone)]
pub struct MapDataSources {
    pub datasources: HashMap<String, DataSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSources {
    pub datasources: Vec<DataSource>,
}

#[allow(unused)]
impl DataSources {
    pub fn iter(&self) -> impl Iterator<Item = &DataSource> {
        self.datasources.iter()
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut DataSource> {
        self.datasources.iter_mut()
    }
    pub fn into_iter(self) -> impl Iterator<Item = DataSource> {
        self.datasources.into_iter()
    }
}

#[derive(FromRow, Deserialize, Serialize, Debug, Clone)]
pub struct DataSource {
    pub identifier: String,
    pub data_type: Option<String>,
    pub store_type: Option<String>,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub mbtiles: Option<bool>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub attribution: Option<String>,
    pub minzoom: Option<i16>,
    pub maxzoom: Option<i16>,
    pub bounds: Option<sqlx::types::Json<serde_json::Value>>,
    pub center: Option<sqlx::types::Json<serde_json::Value>>,
    pub data: sqlx::types::Json<serde_json::Value>,
}

pub async fn load_datasource_from_db(pool: &DB, id: &str) -> Result<DataSource, anyhow::Error> {
    let query =
        sqlx::query_as::<_, DataSource>("SELECT * FROM datasource WHERE identifier = $1").bind(id);
    let datasource: DataSource = query.fetch_one(pool).await?;
    Ok(datasource)
}

pub async fn load_datasources_from_db(pool: &DB) -> Result<Vec<DataSource>, anyhow::Error> {
    let query = sqlx::query_as::<_, DataSource>("SELECT * FROM datasource");
    let datasources: Vec<DataSource> = query.fetch_all(pool).await?;
    Ok(datasources)
}

pub async fn init_datasources_tile_dirs(
    cwd: String,
    pool: &DB,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
) -> Result<MapDataSources, anyhow::Error> {
    let datasources = load_datasources_from_db(&pool)
        .await
        .expect("Error load datasources from database");
    let mut map_dss = HashMap::with_capacity(datasources.len());
    let mut handles = Vec::with_capacity(datasources.len());

    for ds in datasources.iter() {
        let ds_tile_dir: PathBuf = [&cwd, "tiles", &ds.identifier].iter().collect();

        let jh = tokio::spawn({
            let tx_sqlite_client = tx_sqlite_client.clone();
            let mbtiles = ds.mbtiles.clone();
            let identifier = ds.identifier.clone();
            let cwd = cwd.clone();

            async move {
                if !tokio::fs::try_exists(&ds_tile_dir).await? {
                    match tokio::fs::create_dir(&ds_tile_dir)
                        .await
                        .map_err(|err| anyhow!(err))
                    {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!(
                                "Error create datasource tile directory '{:?}': {:?}",
                                ds_tile_dir, err
                            );
                            exit(1)
                        }
                    }
                }
                // init mbtiles Database
                if let Some(mbt) = mbtiles {
                    if mbt {
                        init_mbtiles_db(&cwd, identifier.clone(), Some(tx_sqlite_client))
                            .await
                            .expect(&format!(
                                "Error init mbtiles database for ID '{}'",
                                identifier
                            ));
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        });

        handles.push(jh);
        map_dss.insert(ds.identifier.clone(), ds.clone());
    }

    for jh in handles {
        jh.await??;
    }

    Ok(MapDataSources {
        datasources: map_dss,
    })
}

pub fn datasources_maintenance(
    cwd: String,
    pool: DB,
    rx: flume::Receiver<MessageDatasource>,
    config: Config,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut map_dss = init_datasources_tile_dirs(cwd.clone(), &pool, tx_sqlite_client.clone())
            .await
            .expect("Error init datasources tiles directories");

        while let Ok(message) = rx.recv_async().await {
            match message {
                MessageDatasource::GetDataSource {
                    datasource_id,
                    tx_ds,
                } => {
                    if let Some(ds) = map_dss.datasources.get(&datasource_id) {
                        let use_cache_only =
                            ds.data.0.get("use_cache_only").and_then(|v| v.as_bool());
                        let compress_tiles =
                            ds.data.0.get("compress_tiles").and_then(|v| v.as_bool());

                        tx_ds
                            .send(Some(DataSourceInfo {
                                host: ds.host.clone(),
                                port: ds.port,
                                use_cache_only,
                                compress_tiles,
                            }))
                            .expect("Error send DataSourceInfo");
                    } else {
                        tx_ds.send(None).unwrap();
                    }
                }
                MessageDatasource::UpdateDataSources { is_header_master } => {
                    map_dss =
                        init_datasources_tile_dirs(cwd.clone(), &pool, tx_sqlite_client.clone())
                            .await
                            .expect("UpdateDataSources: error init datasources tiles directories");
                    if config.master && !is_header_master {
                        match worker_load_dss(&config, &map_dss, &client).await {
                            Err(_) => {}
                            Ok(_) => {}
                        }
                    }
                }
            }
        }
    })
}
