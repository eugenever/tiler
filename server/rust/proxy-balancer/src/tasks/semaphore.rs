use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{oneshot, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{event, Level};

use crate::config::Config;

#[derive(Debug)]
pub enum MessageSemaphore {
    GetPermit {
        port: u16,
        tx_permit: oneshot::Sender<tokio::sync::OwnedSemaphorePermit>,
    },
    ReleasedPermit {
        port: u16,
    },
    AddPermits {
        n: usize,
    },
    ForgetPermits {
        n: usize,
    },
}

#[derive(Debug)]
pub enum MessageChangeLimitCR {
    Increase { n: usize },
    Decrease { n: usize },
}

pub fn semaphore_maintenance(
    rx: flume::Receiver<MessageSemaphore>,
    tx: flume::Sender<MessageSemaphore>,
    config: Config,
    ports: Vec<u16>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let max_concurrent_tile_requests = config.max_concurrent_tile_requests;

    // Shared semaphores between tasks
    let semaphores_map: Arc<RwLock<HashMap<u16, Arc<Semaphore>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    for port in ports.iter() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_tile_requests));
        if let Ok(mut semaphores_map_guard) = semaphores_map.try_write() {
            if !semaphores_map_guard.contains_key(port) {
                semaphores_map_guard.insert(*port, semaphore);
            }
            drop(semaphores_map_guard);
        }
    }

    let (tx_change_permits, rx_change_permits) = flume::unbounded::<MessageChangeLimitCR>();

    let jh_wait_permits = tokio::spawn({
        let rx_change_permits = rx_change_permits.clone();
        let semaphores_map = semaphores_map.clone();
        let mut number_concurrent_requests = max_concurrent_tile_requests;

        let mut delay = tokio::time::interval(std::time::Duration::from_millis(5));

        async move {
            loop {
                delay.tick().await;

                let mut delta_permits = 0;
                let mut actual_n = 0;

                if let Ok(message) = rx_change_permits.try_recv() {
                    match message {
                        MessageChangeLimitCR::Increase { n } => {
                            delta_permits = n as isize;
                        }
                        MessageChangeLimitCR::Decrease { n } => {
                            delta_permits = -1 * n as isize;
                        }
                    }
                }

                let tx = tx.clone();
                let sems_map_guard = semaphores_map.read().await;
                for (port, semaphore) in sems_map_guard.iter() {
                    if semaphore.available_permits() == number_concurrent_requests {
                        if let Err(err) = tx
                            .send_async(MessageSemaphore::ReleasedPermit { port: *port })
                            .await
                        {
                            event!(Level::ERROR, "Error send message 'released permit': {err}");
                        }
                    }

                    if delta_permits > 0 {
                        semaphore.add_permits(delta_permits as usize);
                    } else if delta_permits < 0 {
                        actual_n = semaphore.forget_permits(delta_permits.abs() as usize);
                    }
                }

                if sems_map_guard.len() > 0 {
                    if delta_permits > 0 {
                        number_concurrent_requests += delta_permits as usize;
                    } else if delta_permits < 0 {
                        number_concurrent_requests -= actual_n;
                    }
                }
            }
        }
    });

    let jh_permits_maintenance = tokio::spawn({
        let semaphores_map = semaphores_map.clone();
        let mut senders_map: HashMap<u16, VecDeque<oneshot::Sender<OwnedSemaphorePermit>>> =
            HashMap::new();

        async move {
            while let Ok(message) = rx.recv_async().await {
                match message {
                    MessageSemaphore::ReleasedPermit { port } => {
                        if senders_map.contains_key(&port) {
                            if let Some(deque) = senders_map.get_mut(&port) {
                                if let Some(sender) = deque.pop_front() {
                                    let mut semaphores_map_guard = semaphores_map.write().await;
                                    if let Some(sm) = semaphores_map_guard.get_mut(&port) {
                                        if sm.available_permits() > 0 {
                                            let permit = sm.clone().acquire_owned().await.unwrap();
                                            if let Err(err) = sender.send(permit) {
                                                // Send error occurs when the receiver is removed,
                                                // for example when the request is cancelled by the client (MapLibre)
                                                drop(err);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    MessageSemaphore::GetPermit { port, tx_permit } => {
                        let mut semaphores_map_guard = semaphores_map.write().await;
                        if semaphores_map_guard.contains_key(&port) {
                            if let Some(sm) = semaphores_map_guard.get_mut(&port) {
                                if sm.available_permits() > 0 {
                                    let permit = sm.clone().acquire_owned().await.unwrap();
                                    if let Err(err) = tx_permit.send(permit) {
                                        // Send error occurs when the receiver is removed,
                                        // for example when the request is cancelled by the client (MapLibre)
                                        drop(err);
                                    }
                                } else {
                                    if !senders_map.contains_key(&port) {
                                        let mut deque = VecDeque::new();
                                        deque.push_back(tx_permit);
                                        senders_map.insert(port, deque);
                                    } else {
                                        if let Some(deque) = senders_map.get_mut(&port) {
                                            deque.push_back(tx_permit);
                                        }
                                    }
                                }
                            }
                        } else {
                            let semaphore =
                                Arc::new(tokio::sync::Semaphore::new(max_concurrent_tile_requests));
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            if let Err(err) = tx_permit.send(permit) {
                                // Send error occurs when the receiver is removed,
                                // for example when the request is cancelled by the client (MapLibre)
                                drop(err);
                            }
                            semaphores_map_guard.insert(port, semaphore);
                        }
                    }
                    MessageSemaphore::AddPermits { n } => {
                        if let Err(err) = tx_change_permits
                            .send_async(MessageChangeLimitCR::Increase { n })
                            .await
                        {
                            event!(Level::ERROR, "Error send increase message: {}", err);
                        }
                    }
                    MessageSemaphore::ForgetPermits { n } => {
                        if let Err(err) = tx_change_permits
                            .send_async(MessageChangeLimitCR::Decrease { n })
                            .await
                        {
                            event!(Level::ERROR, "Error send decrease message: {}", err);
                        }
                    }
                }
            }
        }
    });

    (jh_wait_permits, jh_permits_maintenance)
}
