use std::time::Duration;

use hyper::client::HttpConnector;
use hyper::Client;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_schedule::Job;
use tracing::{event, Level};

use super::workers::SystemInfoWorkers;
use crate::config::Config;
use crate::db::check_running_pyramids;

trait DurationExt {
    fn from_hours(hours: u64) -> Duration;
    fn from_minutes(hours: u64) -> Duration;
}

impl DurationExt for Duration {
    fn from_hours(hours: u64) -> Duration {
        Duration::from_secs(hours * 60 * 60)
    }
    fn from_minutes(minutes: u64) -> Duration {
        Duration::from_secs(minutes * 60)
    }
}

pub struct WorkerData {
    pub port: u16,
    pub index: usize,
    pub ports: Vec<u16>,
    pub client: Client<HttpConnector>,
}

pub enum WorkerState {
    Running,
    Reloading,
}

pub enum MessageMaintenanceWorkers {
    GetWorkerData {
        tx_wd: oneshot::Sender<Option<WorkerData>>,
    },
    InfoWorkers {
        tx_iw: oneshot::Sender<Option<SystemInfoWorkers>>,
    },
    AddWorkers {
        count: u64,
    },
    ReloadWorkers(),
    TerminateWorkers(),
    GetWorkerState {
        tx_ws: oneshot::Sender<WorkerState>,
    },
}

pub fn reload_workers_maintenance(
    cwd: String,
    tx_mw: flume::Sender<MessageMaintenanceWorkers>,
    config: Config,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio_schedule::every(config.worker_reload_periodicity_days)
            .day()
            .at(
                config.worker_reload_time[0],
                config.worker_reload_time[1],
                config.worker_reload_time[2],
            )
            .perform(|| async {

                // does not allow double calling
                let (tx_ws, rx_ws) = oneshot::channel();
                if let Err(err)  = tx_mw.send(MessageMaintenanceWorkers::GetWorkerState { tx_ws }) {
                    event!(Level::ERROR, "Error get worker state: {err}");
                }
                match rx_ws.await {
                    Err(err) => {
                        event!(Level::ERROR, "Error receive worker state: {err}");
                    }
                    Ok(ws) => {
                        match ws {
                            WorkerState::Reloading => return,
                            WorkerState::Running => {}
                        }
                    }
                }

                let mut delay = tokio::time::interval(Duration::from_minutes(
                    config.worker_reload_repeat_minutes,
                ));

                for i in 0..config.worker_reload_repeat_attempts {
                    // Approximately 0ms have elapsed.
                    // The first tick completes immediately.
                    delay.tick().await;

                    match check_running_pyramids(&cwd).await {
                        Err(err) => {
                            event!(
                                Level::ERROR,
                                "Error '{i}' repeat check running pyramids: {}",
                                err
                            );
                        }
                        Ok(running) => {
                            if !running {
                                tx_mw
                                    .send_async(MessageMaintenanceWorkers::ReloadWorkers())
                                    .await
                                    .unwrap();
                                event!(Level::INFO, "Reload Python workers after attemp '{i}'");
                                drop(delay);
                                break;
                            } else {
                                event!(Level::INFO, "NOT Reload Python workers after attemp '{i}', pyramid is running");
                            }
                        }
                    }
                }
            })
            .await;
    })
}
