use std::collections::HashMap;
use std::path::PathBuf;

use async_sqlite::{Client as SQLiteClient, ClientBuilder, JournalMode};
use rusqlite::OpenFlags;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{event, Level};

pub enum MessageSQLiteClient {
    GetSQLiteClient {
        mbtiles_db: String,
        tx_client: oneshot::Sender<Option<SQLiteClient>>,
    },
    AddSQLiteClient {
        mbtiles_db: String,
        client: SQLiteClient,
    },
    RemoveSQLiteClient {
        mbtiles_db: String,
        remove_tiles_folder: Option<bool>,
        remove_tiles_db: Option<bool>,
    },
    CloseSQLiteClients(),
}

pub fn sqlite_clients_maintenance(rx: flume::Receiver<MessageSQLiteClient>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sqlite_clients: HashMap<String, SQLiteClient> = HashMap::new();

        while let Ok(message) = rx.recv_async().await {
            match message {
                MessageSQLiteClient::GetSQLiteClient {
                    mbtiles_db,
                    tx_client,
                } => {
                    if let Some(client) = sqlite_clients.get(&mbtiles_db) {
                        if let Err(_) = tx_client.send(Some(client.clone())) {}
                    } else {
                        let result = ClientBuilder::new()
                            .path(&mbtiles_db)
                            .journal_mode(JournalMode::Wal)
                            .flags(OpenFlags::SQLITE_OPEN_READ_WRITE)
                            .open()
                            .await;
                        match result {
                            Ok(cl) => {
                                sqlite_clients.insert(mbtiles_db.clone(), cl.clone());
                                if let Err(_) = tx_client.send(Some(cl)) {}
                            }
                            Err(err) => {
                                if let Err(_) = tx_client.send(None) {}
                                event!(Level::ERROR, "SQLite client error: {}", err.to_string());
                            }
                        };
                    }
                }
                MessageSQLiteClient::AddSQLiteClient { mbtiles_db, client } => {
                    sqlite_clients.insert(mbtiles_db, client);
                }
                MessageSQLiteClient::CloseSQLiteClients() => {
                    for client in sqlite_clients.values() {
                        if let Err(err) = client.close().await {
                            event!(Level::ERROR, "Error close client MBTiles {err:?}");
                        }
                    }
                }
                MessageSQLiteClient::RemoveSQLiteClient {
                    mbtiles_db,
                    remove_tiles_folder,
                    remove_tiles_db,
                } => {
                    if let Some(client) = sqlite_clients.remove(&mbtiles_db) {
                        if let Err(err) = client.close().await {
                            event!(
                                Level::ERROR,
                                "Error close client for MBTiles '{mbtiles_db}': {err:?}"
                            );
                        }
                        let jh = tokio::spawn({
                            let mbtiles_db = mbtiles_db.clone();
                            async move {
                                if let Some(is_remove_folder) = remove_tiles_folder {
                                    if is_remove_folder {
                                        if let Some(parent) = PathBuf::from(&mbtiles_db).parent() {
                                            if let Err(_err) =
                                                tokio::fs::remove_dir_all(parent).await
                                            {
                                                /*
                                                    event!(
                                                        Level::ERROR,
                                                        "Impossible to delete directory of MBTiles '{mbtiles_db}': {err:?}"
                                                    );
                                                */
                                            }
                                        }
                                        return;
                                    }
                                }

                                if let Some(is_remove_db) = remove_tiles_db {
                                    if is_remove_db {
                                        if let Err(_err) = tokio::fs::remove_file(&mbtiles_db).await
                                        {
                                            /*
                                                event!(
                                                    Level::INFO,
                                                    "Impossible to delete MBTiles DataBase '{mbtiles_db}': {err:?}"
                                                );
                                            */
                                        }
                                    }
                                }
                            }
                        });
                        if let Err(err) = jh.await {
                            event!(Level::ERROR, "Error remove MBTiles '{mbtiles_db}': {err:?}");
                        }
                    }
                }
            }
        }
    })
}
