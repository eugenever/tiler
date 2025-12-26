pub mod error;
pub mod job;

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::anyhow;
use async_sqlite::{ClientBuilder, JournalMode};
use hyper::{Body, Response};
use rusqlite::{named_params, OpenFlags};
use sqlx::{
    postgres::Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Connection, PgConnection, Pool, Row,
};
use tokio::sync::oneshot;
use tracing::{event, Level};

use crate::config::{load_db_config, DBConfig};
use crate::tasks::sqlite_clients::MessageSQLiteClient;
use crate::utils::tile_response;

pub type DB = Pool<Postgres>;

pub async fn create_pg_config_db(db_config: &DBConfig) -> Result<(), anyhow::Error> {
    let db = &db_config.name;
    let sql = format!("SELECT 'CREATE DATABASE {db};' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{db}')");
    let mut conn = match connection_pg_postgres(&db_config).await {
        Err(err) => {
            return Err(anyhow!("{err}"));
        }
        Ok(c) => c,
    };

    let result = sqlx::query(&sql).fetch_optional(&mut conn).await?;
    if let Some(row) = result {
        let sql_create_db: String = row.get(0);
        sqlx::query(&sql_create_db).execute(&mut conn).await?;
    }
    conn.close().await?;

    Ok(())
}

// Connect to DataBase 'postgres'
pub async fn connection_pg_postgres(db_config: &DBConfig) -> Result<PgConnection, anyhow::Error> {
    let conn_str = format!(
        "postgres://{}:{}@{}:{}/{}",
        db_config.user, db_config.pass, db_config.host, db_config.port, "postgres",
    );
    let conn = PgConnection::connect(&conn_str).await?;
    Ok(conn)
}

// Connect to DataBase from Config
pub async fn connection_pg_config_db(db_config: &DBConfig) -> Result<PgConnection, anyhow::Error> {
    let conn_str = format!(
        "postgres://{}:{}@{}:{}/{}",
        db_config.user, db_config.pass, db_config.host, db_config.port, db_config.name,
    );
    let conn = PgConnection::connect(&conn_str).await?;
    Ok(conn)
}

pub async fn init_pg_db(cwd: &str, mut conn: PgConnection) -> Result<PgConnection, anyhow::Error> {
    let queries_path: PathBuf = [cwd, "sql", "0_init.sql"].iter().collect();
    let path = queries_path
        .to_str()
        .ok_or(anyhow!("Error convert 'sql' directiry path"))?;
    let queries = rawsql::Loader::read_queries_from(path)?;

    sqlx::query(
        queries
            .get("create-table-datasource")
            .ok_or(anyhow!("SQL script 'create-table-datasource' not found"))?,
    )
    .execute(&mut conn)
    .await?;

    sqlx::query(
        queries
            .get("create-table-queue")
            .ok_or(anyhow!("SQL script 'create-table-queue' not found"))?,
    )
    .execute(&mut conn)
    .await?;
    sqlx::query(
        queries
            .get("create-index_queue_on_scheduled_for")
            .ok_or(anyhow!(
                "SQL script 'create-index_queue_on_scheduled_for' not found"
            ))?,
    )
    .execute(&mut conn)
    .await?;
    sqlx::query(queries.get("create-index_queue_on_status").ok_or(anyhow!(
        "SQL script 'create-index_queue_on_status' not found"
    ))?)
    .execute(&mut conn)
    .await?;

    Ok(conn)
}

pub async fn init_db(cwd: &str) -> Result<(), anyhow::Error> {
    let db = db_tiler(cwd)?;
    if let Err(_) = tokio::fs::metadata(&db).await {
        let client = ClientBuilder::new()
            .path(&db)
            .journal_mode(JournalMode::Wal)
            .open()
            .await
            .map_err(|err| anyhow!(err))?;

        client
            .conn(|connection| {
                connection.execute(
                    "CREATE TABLE pyramids (
                            id text NOT NULL,
                            dataset text NOT NULL,
                            datasource_id text NOT NULL,
                            start_time timestamp,
                            finish_time timestamp,
                            params text NOT NULL,
                            running integer,
                            complete integer,
                            PRIMARY KEY(id)
                        );",
                    (),
                )
            })
            .await?;

        if let Err(err) = client.close().await {
            event!(Level::ERROR, "Error close connection: {}", err.to_string());
        }
    } else {
        let client = ClientBuilder::new()
            .path(&db)
            .journal_mode(JournalMode::Wal)
            .flags(OpenFlags::SQLITE_OPEN_READ_WRITE)
            .open()
            .await
            .map_err(|err| anyhow!(err))?;

        client
            .conn(|connection| {
                connection.execute(
                    "UPDATE pyramids SET complete = 1 WHERE running = 1 AND finish_time IS NULL;",
                    (),
                )
            })
            .await?;

        if let Err(err) = client.close().await {
            event!(Level::ERROR, "Error close connection: {}", err.to_string());
        }
    }

    Ok(())
}

pub fn db_tiler(cwd: &str) -> Result<PathBuf, anyhow::Error> {
    let db_tiler: PathBuf = [cwd, "data", "tiler.db"].iter().collect();

    Ok(db_tiler)
}

pub async fn check_running_pyramids(cwd: &str) -> Result<bool, anyhow::Error> {
    let db = db_tiler(cwd)?;
    let client = ClientBuilder::new()
        .path(&db)
        .journal_mode(JournalMode::Wal)
        .flags(OpenFlags::SQLITE_OPEN_READ_WRITE)
        .open()
        .await
        .map_err(|err| anyhow!(err))?;

    let running = client
        .conn(move |connection| {
            let mut stmt = connection.prepare("SELECT * FROM pyramids WHERE complete = :comp")?;
            // 2 index -> datasource_id
            let rows = stmt.query_map(named_params! {":comp": 0}, |row| row.get::<_, String>(2))?;
            for row in rows {
                if let Ok(_) = row {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await?;

    if let Err(err) = client.close().await {
        event!(
            Level::ERROR,
            "Error close connection to Tiler DataBase: {}",
            err.to_string()
        );
    }

    Ok(running)
}

pub async fn get_mbtile(
    mbtiles_db: &PathBuf,
    z: u64,
    x: u64,
    y: u64,
    content_type: &str,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
) -> Result<Option<Response<Body>>, anyhow::Error> {
    let (tx_client, rx_client) = oneshot::channel::<Option<async_sqlite::Client>>();
    tx_sqlite_client
        .send_async(MessageSQLiteClient::GetSQLiteClient {
            mbtiles_db: mbtiles_db
                .clone()
                .into_os_string()
                .into_string()
                .map_err(|err| anyhow!("{err:?}"))?,
            tx_client,
        })
        .await?;

    if let Some(sqlite_client) = rx_client.await? {
        let tile_data: Result<Vec<u8>, _> = sqlite_client
                .conn(move |connection| {
                    connection.query_row("SELECT tile_data FROM tiles WHERE zoom_level = (?) AND tile_column = (?) AND tile_row = (?) LIMIT 1;", [z, x, y], |row| row.get(0))
                })
                .await;

        match tile_data {
            Ok(tile) => {
                let response = tile_response(tile, content_type)?;
                return Ok(Some(response));
            }
            Err(async_sqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                // Tile missing
                return Ok(None);
            }
            Err(err) => {
                event!(
                    Level::ERROR,
                    "Error select tile from {mbtiles_db:?}: {err:?}"
                );
                return Ok(None);
            }
        }
    }

    event!(
        Level::ERROR,
        "Error get SQLite client for MBTiles {mbtiles_db:?}"
    );
    return Ok(None);
}

pub async fn init_mbtiles_db(
    cwd: &str,
    datasource_id: String,
    tx_sqlite_client: Option<flume::Sender<MessageSQLiteClient>>,
) -> Result<(), anyhow::Error> {
    let mbtiles_db: PathBuf = [
        cwd,
        "tiles",
        &datasource_id,
        &format!("{}.mbtiles", datasource_id),
    ]
    .iter()
    .collect();

    if let Err(err) = merge_wal_shm_files_to_db(&cwd, &mbtiles_db, &datasource_id).await {
        event!(
            Level::ERROR,
            "Error merge wal and shm files to DataBase {mbtiles_db:?}: {err:?}"
        );
    }

    if !tokio::fs::try_exists(&mbtiles_db).await? {
        let client = ClientBuilder::new()
            .path(&mbtiles_db)
            .journal_mode(JournalMode::Wal)
            .open()
            .await
            .map_err(|err| anyhow!(err))?;

        client
            .conn(|connection| {
                connection.execute(
                    "CREATE TABLE tiles (
                            zoom_level integer NOT NULL,
                            tile_column integer NOT NULL,
                            tile_row integer NOT NULL,
                            tile_data blob,
                            PRIMARY KEY(zoom_level, tile_column, tile_row)
                        );",
                    (),
                )
            })
            .await?;

        client
            .conn(|connection| {
                connection.execute("CREATE TABLE metadata (name text, value text);", ())
            })
            .await?;

        client
            .conn(|connection| {
                connection.execute(
                    "CREATE TABLE grids (
                            zoom_level integer NOT NULL,
                            tile_column integer NOT NULL,
                            tile_row integer NOT NULL,
                            grid blob
                        );",
                    (),
                )
            })
            .await?;

        client
            .conn(|connection| {
                connection.execute(
                    "CREATE TABLE grid_data (
                            zoom_level integer NOT NULL,
                            tile_column integer NOT NULL,
                            tile_row integer NOT NULL,
                            key_name text,
                            key_json text
                        );",
                    (),
                )
            })
            .await?;

        client
            .conn(|connection| {
                connection.execute("CREATE UNIQUE INDEX name on metadata (name);", ())
            })
            .await?;

        if let Err(err) = client.close().await {
            event!(Level::ERROR, "Error close connection: {}", err.to_string());
        }

        // Add client to Pool
        if let Some(_tx_sqlite_client) = tx_sqlite_client {
            add_sqlite_client(
                mbtiles_db,
                _tx_sqlite_client,
                JournalMode::Wal,
                OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .await?;
        }
    }

    Ok(())
}

pub async fn merge_wal_shm_files_to_db(
    cwd: &str,
    mbtiles_db: &PathBuf,
    datasource_id: &str,
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

    // merge WAL file to DataBase after restart server
    if tokio::fs::try_exists(&wal_file).await? || tokio::fs::try_exists(&shm_file).await? {
        let result = ClientBuilder::new()
            .path(&mbtiles_db)
            .journal_mode(JournalMode::Off)
            .open()
            .await
            .map_err(|err| anyhow!(err));

        // When updating DataSources in runtime, the DataBase file may be locked
        if let Ok(cl) = result {
            if let Err(err) = cl.close().await {
                event!(Level::ERROR, "Error close connection: {}", err.to_string());
            }
        }
    }
    Ok(())
}

pub async fn add_sqlite_client(
    mbtiles_db: PathBuf,
    tx_sqlite_client: flume::Sender<MessageSQLiteClient>,
    mode: JournalMode,
    flags: OpenFlags,
) -> Result<(), anyhow::Error> {
    let cl = ClientBuilder::new()
        .path(&mbtiles_db)
        .journal_mode(mode)
        .flags(flags)
        .open()
        .await
        .map_err(|err| anyhow!(err))?;

    tx_sqlite_client
        .send_async(MessageSQLiteClient::AddSQLiteClient {
            mbtiles_db: mbtiles_db
                .clone()
                .into_os_string()
                .into_string()
                .map_err(|err| anyhow!("{err:?}"))?,
            client: cl,
        })
        .await?;

    Ok(())
}

pub async fn pg_pool() -> Result<DB, anyhow::Error> {
    let db_config = load_db_config();
    let uri = format!(
        "postgres://{}:{}@{}:{}/{}",
        db_config.user, db_config.pass, db_config.host, db_config.port, db_config.name,
    );
    let options = PgConnectOptions::from_str(&uri)?.disable_statement_logging();
    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
    {
        Err(err) => return Err(anyhow!("Error connect to PostgreSQL: {:?}", err)),
        Ok(pool) => pool,
    };

    Ok(pool)
}
