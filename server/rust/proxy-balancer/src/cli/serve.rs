use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::exit;

use ctrlc;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{client::HttpConnector, Client, Server};
use tokio::process::Child;
use tokio::task::JoinHandle;
use tracing::{event, Level};

use crate::config::load_config;
use crate::db::{init_db, job::queue::JobDetail, pg_pool};
use crate::environment::setup_envs;
use crate::handles::handle;
use crate::log::init_tracing;
use crate::tasks::datasources::{datasources_maintenance, MessageDatasource};
use crate::tasks::job::init_job_queue;
use crate::tasks::reload_workers::{reload_workers_maintenance, MessageMaintenanceWorkers};
use crate::tasks::semaphore::{semaphore_maintenance, MessageSemaphore};
use crate::tasks::sqlite_clients::{sqlite_clients_maintenance, MessageSQLiteClient};
use crate::tasks::workers::{cmd_run_worker, workers_maintenance};
use crate::utils::{get_available_port, try_save_process_pid};

pub async fn command_serve(cwd: String, address: Option<String>) {
    let vars = setup_envs();
    let mut config = load_config()
        .await
        .expect("Error load configuration from 'config_app.json'");

    config.address = address;
    if let Some(_) = &config.address {
        config.master = true;
    } else {
        config.master = false;
    }

    if let Err(err) = init_tracing(&config.log_level_server) {
        eprintln!("Error init tracing: {:?}", err);
        exit(1);
    }

    // check exist DB tiler.db
    if let Err(err) = init_db(&cwd).await {
        eprintln!("Error initialize DataBase 'tiler.db': {}", err.to_string());
        exit(1);
    }

    let pool = match pg_pool().await {
        Err(err) => {
            eprintln!("{err}");
            exit(1);
        }
        Ok(p) => p,
    };

    let mut childs: HashMap<u16, Child> = HashMap::with_capacity(config.processes_workers as usize);
    let mut ports: Vec<u16> = Vec::with_capacity(config.processes_workers as usize);
    let mut clients: Vec<Client<HttpConnector>> =
        Vec::with_capacity(config.processes_workers as usize);

    for _ in 0..config.processes_workers {
        if let Some(p) = get_available_port(
            config.worker_port_from as u16,
            config.worker_port_to as u16,
            &ports,
        )
        .await
        {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            ports.push(p);
            clients.push(Client::new());
        }
    }

    for p in &ports {
        let worker = cmd_run_worker(&config, &vars, *p);
        childs.insert(*p, worker);
    }

    if childs.len() == 0 {
        eprintln!("Worker processes did not start");
        exit(1);
    }

    if ports.len() == 0 {
        eprintln!("Error request free ports");
        exit(1);
    }

    let workers_pids: Vec<u32> = childs.values().map(|w| w.id().unwrap()).collect();

    let (tx_mw, rx_mw) = flume::unbounded::<MessageMaintenanceWorkers>();
    let wm_handle = workers_maintenance(
        cwd.clone(),
        config.clone(),
        vars.clone(),
        childs,
        ports.clone(),
        clients,
        rx_mw,
    );

    let rw_handle = reload_workers_maintenance(cwd.clone(), tx_mw.clone(), config.clone());

    let (tx_sqlite_client, rx_sqlite_client) = flume::unbounded::<MessageSQLiteClient>();
    let sqlite_clients_handle = sqlite_clients_maintenance(rx_sqlite_client);

    let (tx, rx) = flume::unbounded::<MessageDatasource>();
    let dss_maintenance_handle = datasources_maintenance(
        cwd.clone(),
        pool.clone(),
        rx,
        config.clone(),
        tx_sqlite_client.clone(),
    );

    let (tx_sem, rx_sem) = flume::unbounded::<MessageSemaphore>();
    let (jh_wait_permits, jh_permits_maintenance) =
        semaphore_maintenance(rx_sem, tx_sem.clone(), config.clone(), ports);

    let mut opt_tx_jd: Option<flume::Sender<JobDetail>> = None;
    let mut opt_jd_handles: Option<(JoinHandle<()>, JoinHandle<()>)> = None;
    if config.master {
        let (tx_jd, rx_jd) = flume::unbounded::<JobDetail>();
        let jd_handles = init_job_queue(
            cwd.clone(),
            pool.clone(),
            rx_jd,
            tx.clone(),
            tx_mw.clone(),
            tx_sqlite_client.clone(),
            config.clone(),
        )
        .expect("Error run job queue worker");
        opt_tx_jd = Some(tx_jd);
        opt_jd_handles = Some(jd_handles);
    }

    let bind_addr = format!("{}:{}", config.host, config.port);
    let addr: SocketAddr = bind_addr.parse().expect("Could not parse ip:port");

    // Connection handler
    let make_svc = make_service_fn(|conn: &AddrStream| {
        let remote_addr = conn.remote_addr().ip();
        let p = pool.clone();
        let tx = tx.clone();
        let tx_sqlite_client = tx_sqlite_client.clone();
        let tx_mw = tx_mw.clone();
        let tx_jd = opt_tx_jd.clone();
        let c = config.clone();
        let tx_semaphore = tx_sem.clone();
        let cwd = cwd.clone();

        async move {
            // Request handler
            Ok::<_, Infallible>(service_fn(move |req| {
                let pool = p.clone();
                let config = c.clone();
                let cwd = cwd.clone();

                handle(
                    cwd,
                    remote_addr,
                    req,
                    pool,
                    config,
                    tx.clone(),
                    tx_sqlite_client.clone(),
                    tx_mw.clone(),
                    tx_jd.clone(),
                    tx_semaphore.clone(),
                )
            }))
        }
    });

    ctrlc::set_handler({
        let tx_sqlite_client = tx_sqlite_client.clone();
        let tx_mw = tx_mw.clone();

        move || {
            tx_sqlite_client
                .send(MessageSQLiteClient::CloseSQLiteClients())
                .unwrap();

            tx_mw
                .send(MessageMaintenanceWorkers::TerminateWorkers())
                .unwrap();
            event!(Level::INFO, "Terminate Python workers");

            std::thread::sleep(std::time::Duration::from_secs(3));

            wm_handle.abort();
            dss_maintenance_handle.abort();
            rw_handle.abort();
            sqlite_clients_handle.abort();
            if let Some((jh_add_job, jh_job_worker)) = opt_jd_handles.take() {
                jh_add_job.abort();
                jh_job_worker.abort();
            }
            jh_wait_permits.abort();
            jh_permits_maintenance.abort();

            exit(0);
        }
    })
    .expect("Error setting Ctrl-C handler");

    if let Err(err) = try_save_process_pid(&cwd, workers_pids.clone()).await {
        eprintln!("Error save porcesses PIDs: {}", err);
        exit(1);
    }

    let server = Server::bind(&addr).serve(make_svc);
    event!(Level::INFO, "Isone Tiler Server running on {:?}", addr);
    if let Err(e) = server.await {
        event!(Level::ERROR, "Server error: {}", e);
    }
}
