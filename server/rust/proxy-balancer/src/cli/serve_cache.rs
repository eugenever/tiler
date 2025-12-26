use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;

use ctrlc;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server;
use tracing::{event, Level};

use crate::config::load_config;
use crate::handles::cache::handle_cache;
use crate::log::init_tracing;
use crate::tasks::sqlite_clients::{sqlite_clients_maintenance, MessageSQLiteClient};

pub async fn command_serve_cache(cwd: String) {
    let config = load_config()
        .await
        .expect("Error load configuration from 'config_app.json'");

    if let Err(err) = init_tracing(&config.log_level_server) {
        eprintln!("Error init tracing: {:?}", err);
        exit(1);
    }

    let (tx_sqlite_client, rx_sqlite_client) = flume::unbounded::<MessageSQLiteClient>();
    let sqlite_clients_handle = sqlite_clients_maintenance(rx_sqlite_client);

    let base_path = PathBuf::from(cwd.clone());

    let make_svc = make_service_fn(|conn: &AddrStream| {
        let remote_addr = conn.remote_addr().ip();
        let tx_sqlite_client = tx_sqlite_client.clone();
        let cwd = cwd.clone();
        let base_path = base_path.clone();

        async move {
            // Request handler
            Ok::<_, Infallible>(service_fn(move |req| {
                let cwd = cwd.clone();
                handle_cache(
                    cwd,
                    remote_addr,
                    req,
                    tx_sqlite_client.clone(),
                    base_path.clone(),
                )
            }))
        }
    });

    ctrlc::set_handler({
        let tx_sqlite_client = tx_sqlite_client.clone();

        move || {
            tx_sqlite_client
                .send(MessageSQLiteClient::CloseSQLiteClients())
                .unwrap();

            event!(Level::INFO, "Terminating...");
            std::thread::sleep(std::time::Duration::from_secs(3));
            sqlite_clients_handle.abort();
            exit(0);
        }
    })
    .expect("Error setting Ctrl-C handler");

    let bind_addr = format!("{}:{}", config.host, config.port);
    let addr: SocketAddr = bind_addr.parse().expect("Could not parse ip:port");

    let server = Server::bind(&addr).serve(make_svc);
    event!(Level::INFO, "Isone Tiler Server running on {:?}", addr);
    if let Err(e) = server.await {
        event!(Level::ERROR, "Server error: {}", e);
    }
}
