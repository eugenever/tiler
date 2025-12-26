use std::process::exit;

#[derive(Debug, Clone)]
pub struct Config {
    pub type_server: String,
    pub master: bool,
    pub address: Option<String>,
    pub timeout_worker_response: u64,
    pub timeout_pull_job: u64,
    pub host: String,
    pub port: u64,
    pub log_level_server: String,
    pub log_level_worker: String,
    pub thread_workers: u64,
    pub processes_workers: u64,
    pub blocking_threads: u64,
    pub interface: String,
    pub backlog: u64,
    pub backpressure: u64,
    pub worker_port_from: u64,
    pub worker_port_to: u64,
    pub worker_reload_time: Vec<u32>,
    pub worker_reload_periodicity_days: u32,
    pub worker_reload_repeat_minutes: u64,
    pub worker_reload_repeat_attempts: u64,
    pub terminate_childs_with_python: bool,
    pub max_concurrent_tile_requests: usize,
}

pub async fn load_config() -> Result<Config, anyhow::Error> {
    let data = tokio::fs::read_to_string("config_app.json").await?;
    let config_json: serde_json::Value =
        serde_json::from_str(&data).expect("config_app.json was not well-formatted");

    let type_server = config_json
        .get("server")
        .and_then(|server| server.get("type"))
        .and_then(|type_server| type_server.as_str())
        .expect("Host of server is undefined")
        .to_string();

    if !["granian", "robyn"].contains(&type_server.as_str()) {
        eprintln!(
            "Server type must be 'granian' or 'robyn', got: {}",
            type_server
        );
        exit(1);
    }

    let timeout_worker_response = config_json
        .get("server")
        .and_then(|server| server.get("timeout_worker_response"))
        .and_then(|t| t.as_u64())
        .unwrap_or(5);

    let timeout_pull_job = config_json
        .get("server")
        .and_then(|server| server.get("timeout_pull_job"))
        .and_then(|t| t.as_u64())
        .unwrap_or(60);

    let host = config_json
        .get("server")
        .and_then(|server| server.get("host"))
        .and_then(|host| host.as_str())
        .expect("Host of server is undefined")
        .to_string();

    let port = config_json
        .get("server")
        .and_then(|server| server.get("port"))
        .and_then(|port| port.as_u64())
        .expect("Port of server is undefined");

    let log_level_server = config_json
        .get("server")
        .and_then(|server| server.get("log_level"))
        .and_then(|log_level| log_level.as_str())
        .expect("Log level server is undefined")
        .to_string();

    let log_level_worker = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("log_level"))
        .and_then(|log_level| log_level.as_str())
        .expect("Log level workers of server is undefined")
        .to_string();

    let thread_workers = config_json
        .get("server")
        .and_then(|server| server.get("thread_workers"))
        .and_then(|thread_workers| thread_workers.as_u64())
        .expect("Thread workers of server is undefined");

    let processes_workers = config_json
        .get("server")
        .and_then(|server| server.get("processes_workers"))
        .and_then(|processes_workers| processes_workers.as_u64())
        .expect("Processes workers of server is undefined");

    let blocking_threads = config_json
        .get("server")
        .and_then(|server| server.get("blocking_threads"))
        .and_then(|blocking_threads| blocking_threads.as_u64())
        .unwrap_or(1);

    let interface = config_json
        .get("server")
        .and_then(|server| server.get("interface"))
        .and_then(|interface| interface.as_str())
        .expect("Interface of server is undefined")
        .to_string();

    let backlog = config_json
        .get("server")
        .and_then(|server| server.get("backlog"))
        .and_then(|backlog| backlog.as_u64())
        .unwrap_or(8196);

    let backpressure = config_json
        .get("server")
        .and_then(|server| server.get("backpressure"))
        .and_then(|backpressure| backpressure.as_u64())
        .unwrap_or(200000);

    if type_server == "granian" && interface != "asgi" {
        eprintln!(
            "Granian server interface must be 'asgi', got: {}",
            interface
        );
        exit(1);
    }

    let worker_port_from = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("ports"))
        .and_then(|ports| ports.get("from"))
        .and_then(|from| from.as_u64())
        .expect("Worker 'port from' is undefined");

    let worker_port_to = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("ports"))
        .and_then(|ports| ports.get("to"))
        .and_then(|to| to.as_u64())
        .expect("Worker 'port to' is undefined");

    if worker_port_from > worker_port_to
        || (worker_port_to - worker_port_from + 1) < processes_workers
    {
        eprintln!(
            "Error worker ports values: {}, {}",
            worker_port_from, worker_port_to
        );
        exit(1);
    }

    let worker_reload_time = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("reload_time"))
        .and_then(|reload_time| reload_time.as_str())
        .expect("Worker 'reload time' is undefined");

    let reload_time: Vec<u32> = worker_reload_time
        .split(":")
        .map(|e| e.parse::<u32>().unwrap())
        .collect();

    if reload_time.len() != 3 {
        eprintln!("Error worker 'reload time' value: {}", worker_reload_time);
        exit(1);
    }

    let worker_reload_periodicity_days = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("reload_periodicity_days"))
        .and_then(|reload_periodicity_days| reload_periodicity_days.as_u64())
        .expect("Worker 'reload periodicity days' is undefined")
        as u32;

    let worker_reload_repeat_minutes = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("reload_repeat_minutes"))
        .and_then(|reload_repeat_minutes| reload_repeat_minutes.as_u64())
        .expect("Worker 'reload repeat minutes' is undefined");

    let worker_reload_repeat_attempts = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("reload_repeat_attempts"))
        .and_then(|reload_repeat_attempts| reload_repeat_attempts.as_u64())
        .expect("Worker 'reload repeat attempts' is undefined");

    let terminate_childs_with_python = config_json
        .get("server")
        .and_then(|server| server.get("terminate_childs_with_python"))
        .and_then(|terminate| terminate.as_bool())
        .unwrap_or(false);

    let max_concurrent_tile_requests = config_json
        .get("server")
        .and_then(|server| server.get("worker"))
        .and_then(|worker| worker.get("max_concurrent_tile_requests"))
        .and_then(|max_concurrent_tile_requests| max_concurrent_tile_requests.as_u64())
        .expect("Worker 'max tile concurrent requests' is undefined")
        as usize;

    Ok(Config {
        type_server,
        master: false,
        address: None,
        timeout_worker_response,
        timeout_pull_job,
        host,
        port,
        interface,
        log_level_server,
        log_level_worker,
        processes_workers,
        thread_workers,
        blocking_threads,
        backlog,
        backpressure,
        worker_port_from,
        worker_port_to,
        worker_reload_time: reload_time,
        worker_reload_periodicity_days,
        worker_reload_repeat_minutes,
        worker_reload_repeat_attempts,
        terminate_childs_with_python,
        max_concurrent_tile_requests,
    })
}

#[derive(Debug, Clone)]
pub struct DBConfig {
    pub host: String,
    pub port: String,
    pub name: String,
    pub user: String,
    pub pass: String,
}

pub fn load_db_config() -> DBConfig {
    let host = std::env::var("DBHOST").expect("Environment variable 'DBHOST' is undefined");
    let port = std::env::var("DBPORT").expect("Environment variable 'DBPORT' is undefined");
    let name = std::env::var("DBNAME").expect("Environment variable 'DBNAME' is undefined");
    let user = std::env::var("DBUSER").expect("Environment variable 'DBUSER' is undefined");
    let pass = std::env::var("DBPASS").expect("Environment variable 'DBPASS' is undefined");

    DBConfig {
        host,
        port,
        name,
        user,
        pass,
    }
}
