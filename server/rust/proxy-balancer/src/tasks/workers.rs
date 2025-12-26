use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;

use hyper::{client::HttpConnector, Client};
use serde::Serialize;
use tokio::process::Command;
use tokio::task::JoinHandle;
use tracing::{event, Level};

use super::reload_workers::{MessageMaintenanceWorkers, WorkerData, WorkerState};
use crate::config::Config;
use crate::db::init_db;
use crate::defaults::GRANIAN;
use crate::utils::{get_available_port, try_save_process_pid};

pub fn cmd_run_worker(
    config: &Config,
    vars: &HashMap<&'static str, String>,
    port: u16,
) -> tokio::process::Child {
    if config.type_server == "robyn" {
        Command::new("python")
            .envs(vars)
            .arg("app_robyn.py")
            .arg(format!("--log-level={}", config.log_level_worker))
            .arg(format!("--workers={}", config.thread_workers))
            .arg(format!("--processes={}", 1))
            .arg(format!("--port={}", port))
            .kill_on_drop(true)
            .spawn()
            .expect(&format!("Robyn worker failed to start on port {}", port))
    } else {
        Command::new("granian")
            .envs(vars)
            .arg("app_granian:app")
            .arg(format!("--interface={}", config.interface))
            .arg(format!("--workers={}", 1))
            .arg(format!("--runtime-threads={}", config.thread_workers))
            .arg(format!("--blocking-threads={}", config.blocking_threads))
            .arg(format!("--port={}", port))
            .arg(format!("--backlog={}", config.backlog))
            .arg(format!("--backpressure={}", config.backpressure))
            .arg(format!("--log-config={}", "log_config.json"))
            .kill_on_drop(true)
            .spawn()
            .expect(&format!("Granian worker failed to start on port {}", port))
    }
}

pub async fn run_python_terminate_childs(
    cwd: &str,
    vars: HashMap<&'static str, String>,
) -> Result<(), anyhow::Error> {
    let script: PathBuf = [cwd, "scripts", "terminate_childs.py"].iter().collect();
    let out = Command::new("python")
        .envs(vars)
        .arg(script)
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    let stdout = String::from_utf8(out.stdout)?;
    event!(Level::INFO, "Output terminate_childs:\n{}", stdout);
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct SystemInfoWorkers {
    pub worker_childs: HashMap<u32, Vec<(u32, u32)>>,
    pub worker_memory: HashMap<u32, Vec<u64>>,
}

pub fn is_process_run(name: &'static str) -> JoinHandle<bool> {
    tokio::task::spawn_blocking(move || {
        let sys = sysinfo::System::new_all();
        let mut is_run = false;
        for _ in sys.processes_by_exact_name(name.as_ref()) {
            is_run = true;
        }
        is_run
    })
}

pub fn info_workers(workers_pids: Vec<u32>) -> JoinHandle<SystemInfoWorkers> {
    tokio::task::spawn_blocking(move || {
        let sys = sysinfo::System::new_all();
        let mut worker_childs = HashMap::new();
        let mut worker_memory = HashMap::new();
        for pid in workers_pids {
            #[cfg(target_os = "linux")]
            let _pid = pid as i32;
            #[cfg(target_os = "windows")]
            let _pid = pid;

            let rp = remoteprocess::Process::new(_pid).unwrap();
            let rp_childs = rp.child_processes().unwrap();
            let mut active_childs = Vec::with_capacity(rp_childs.len());
            let mut memory_childs = Vec::with_capacity(rp_childs.len());
            for (pid_ch, pid_par) in rp_childs.iter() {
                let mut is_active_ch = false;
                let mut is_active_par = false;
                if let Some(p) = sys.process(sysinfo::Pid::from_u32(*pid_ch as u32)) {
                    is_active_ch = true;
                    let mem = p.memory();
                    memory_childs.push(mem);
                }
                if let Some(_p) = sys.process(sysinfo::Pid::from_u32(*pid_par as u32)) {
                    is_active_par = true;
                }
                if is_active_ch && is_active_par {
                    active_childs.push((*pid_ch as u32, *pid_par as u32));
                }
            }
            worker_childs.insert(pid, active_childs);
            worker_memory.insert(pid, memory_childs);
        }

        SystemInfoWorkers {
            worker_childs,
            worker_memory,
        }
    })
}

pub fn workers_maintenance(
    cwd: String,
    config: Config,
    vars: HashMap<&'static str, String>,
    mut childs: HashMap<u16, tokio::process::Child>,
    mut ports: Vec<u16>,
    mut clients: Vec<Client<HttpConnector>>,
    rx: flume::Receiver<MessageMaintenanceWorkers>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut index_port = 0;
        let mut count_ports = ports.len();
        let mut workers_pids: Vec<u32> = childs.values().map(|w| w.id().unwrap()).collect();
        let mut last_reload_time: Option<chrono::DateTime<chrono::Utc>> = None;

        while let Ok(message) = rx.recv_async().await {
            match message {
                MessageMaintenanceWorkers::GetWorkerData { tx_wd } => {
                    if count_ports == 0 {
                        if let Err(_) = tx_wd.send(None) {
                            event!(Level::ERROR, "Error send None");
                        }
                    } else {
                        // simple balancing based on the order of workers
                        let index = {
                            if index_port > (count_ports - 1) as u16 {
                                index_port = 1;
                                0
                            } else {
                                index_port += 1;
                                (index_port - 1) as usize
                            }
                        };
                        let port = ports[index];
                        if let Err(_) = tx_wd.send(Some(WorkerData {
                            port,
                            index,
                            ports: ports.clone(),
                            client: clients[index].clone(),
                        })) {
                            event!(Level::ERROR, "Error send port {port}");
                        }
                    }
                }
                MessageMaintenanceWorkers::InfoWorkers { tx_iw } => {
                    let jh = info_workers(workers_pids.clone());
                    match jh.await {
                        Err(err) => {
                            event!(Level::ERROR, "Error get system info workers {err}");
                        }
                        Ok(iw) => {
                            if let Err(_) = tx_iw.send(Some(iw)) {
                                event!(Level::ERROR, "Error send system info workers");
                            }
                        }
                    }
                }
                MessageMaintenanceWorkers::AddWorkers { count } => {
                    for _ in 0..count {
                        if let Some(p) = get_available_port(
                            config.worker_port_from as u16,
                            config.worker_port_to as u16,
                            &ports,
                        )
                        .await
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                            let worker = cmd_run_worker(&config, &vars, p);
                            childs.insert(p, worker);
                            ports.push(p);
                            clients.push(Client::new());
                        }
                    }
                    count_ports = ports.len();

                    workers_pids = childs.values().map(|w| w.id().unwrap()).collect();
                    if let Err(err) = try_save_process_pid(&cwd, workers_pids.clone()).await {
                        event!(Level::ERROR, "Error save porcesses PIDs: {}", err);
                    }
                }
                MessageMaintenanceWorkers::ReloadWorkers() => {
                    for (_port, child) in childs.iter() {
                        kill_tree::tokio::kill_tree(child.id().unwrap())
                            .await
                            .unwrap();
                    }

                    if config.terminate_childs_with_python {
                        if let Err(err) = run_python_terminate_childs(&cwd, vars.clone()).await {
                            event!(Level::ERROR, "Error run_python_terminate_childs: {}", err);
                        }
                    }

                    childs.clear();
                    ports.clear();
                    clients.clear();

                    for _ in 0..60 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        if let Ok(is_run) = is_process_run(GRANIAN).await {
                            if !is_run {
                                break;
                            }
                        };
                    }

                    for _ in 0..config.processes_workers {
                        if let Some(p) = get_available_port(
                            config.worker_port_from as u16,
                            config.worker_port_to as u16,
                            &ports,
                        )
                        .await
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            let worker = cmd_run_worker(&config, &vars, p);
                            childs.insert(p, worker);
                            ports.push(p);
                            clients.push(Client::new());
                        }
                    }
                    count_ports = ports.len();

                    if let Err(err) = init_db(&cwd).await {
                        event!(
                            Level::ERROR,
                            "Error initialize DataBase 'tiler.db' at reload workers: {}",
                            err.to_string()
                        );
                    }

                    workers_pids = childs.values().map(|w| w.id().unwrap()).collect();
                    if let Err(err) = try_save_process_pid(&cwd, workers_pids.clone()).await {
                        event!(Level::ERROR, "Error save porcesses PIDs: {}", err);
                    }
                }
                MessageMaintenanceWorkers::TerminateWorkers() => {
                    for (_port, child) in childs.iter() {
                        kill_tree::tokio::kill_tree(child.id().unwrap())
                            .await
                            .unwrap();
                    }

                    if config.terminate_childs_with_python {
                        if let Err(err) = run_python_terminate_childs(&cwd, vars.clone()).await {
                            event!(Level::ERROR, "Error run_python_terminate_childs: {}", err);
                        }
                    }

                    childs.clear();
                    ports.clear();
                    clients.clear();
                    count_ports = ports.len();
                }
                MessageMaintenanceWorkers::GetWorkerState { tx_ws } => {
                    if let Some(lrt) = last_reload_time {
                        let now = chrono::Utc::now();
                        let diff_secs = now.signed_duration_since(lrt).num_seconds();

                        // maximum reboot frequency in seconds
                        let state = if diff_secs < 60 {
                            WorkerState::Reloading
                        } else {
                            last_reload_time = Some(chrono::Utc::now());
                            WorkerState::Running
                        };

                        if let Err(_) = tx_ws.send(state) {
                            event!(Level::ERROR, "Error send worker state");
                        }
                    } else {
                        last_reload_time = Some(chrono::Utc::now());
                        if let Err(_) = tx_ws.send(WorkerState::Running) {
                            event!(Level::ERROR, "Error send worker state");
                        }
                    }
                }
            }
        }
    })
}
