use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crossbeam_channel::{bounded, Sender, TrySendError};
use log::{error, trace, warn};

use crate::interactions::types::{ActivationCompleted, PetEvent, RunStarted, RunSummary};

pub trait CapWarmNotifier: Send + Sync {
    fn send_run_started(&self, evt: RunStarted);
    fn send_pet(&self, evt: PetEvent);
    fn send_activation_completed(&self, evt: ActivationCompleted);
    fn send_run_summary(&self, evt: RunSummary);
}

#[derive(Clone, Default)]
pub struct NoopNotifier;

impl CapWarmNotifier for NoopNotifier {
    fn send_run_started(&self, _evt: RunStarted) {}

    fn send_pet(&self, _evt: PetEvent) {}

    fn send_activation_completed(&self, _evt: ActivationCompleted) {}

    fn send_run_summary(&self, _evt: RunSummary) {}
}

#[derive(Clone, Default)]
pub struct RecordingNotifier {
    pub run_started: Arc<Mutex<Vec<RunStarted>>>,
    pub pet: Arc<Mutex<Vec<PetEvent>>>,
    pub activation_completed: Arc<Mutex<Vec<ActivationCompleted>>>,
    pub run_summary: Arc<Mutex<Vec<RunSummary>>>,
}

impl CapWarmNotifier for RecordingNotifier {
    fn send_run_started(&self, evt: RunStarted) {
        self.run_started.lock().unwrap().push(evt);
    }

    fn send_pet(&self, evt: PetEvent) {
        self.pet.lock().unwrap().push(evt);
    }

    fn send_activation_completed(&self, evt: ActivationCompleted) {
        self.activation_completed.lock().unwrap().push(evt);
    }

    fn send_run_summary(&self, evt: RunSummary) {
        self.run_summary.lock().unwrap().push(evt);
    }
}

pub enum CapWarmEvent {
    RunStarted(RunStarted),
    Pet(PetEvent),
    ActivationCompleted(ActivationCompleted),
    RunSummary(RunSummary),
}

pub struct HttpCapWarmNotifier {
    tx: Sender<CapWarmEvent>,
}

impl HttpCapWarmNotifier {
    pub fn new(base_url: String) -> Self {
        // Use bounded channel for backpressure
        let (tx, rx) = bounded(10000); 
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build http client");
        
        let base_url = base_url.trim_end_matches('/').to_string();

        thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                let (url, body) = match event {
                    CapWarmEvent::RunStarted(evt) => (
                        format!("{}/run/start", base_url),
                        serde_json::to_value(&evt).unwrap(),
                    ),
                    CapWarmEvent::Pet(evt) => (
                        format!("{}/pet", base_url),
                        serde_json::to_value(&evt).unwrap(),
                    ),
                    CapWarmEvent::ActivationCompleted(evt) => (
                        format!("{}/activation/complete", base_url),
                        serde_json::to_value(&evt).unwrap(),
                    ),
                    CapWarmEvent::RunSummary(evt) => (
                        format!("{}/run/summary", base_url),
                        serde_json::to_value(&evt).unwrap(),
                    ),
                };

                // Retry logic (max 3 times)
                let mut attempts = 0;
                while attempts < 3 {
                    match client.post(&url).json(&body).send() {
                        Ok(resp) => {
                            if resp.status().is_success() {
                                trace!("Sent event to {}", url);
                                break;
                            } else {
                                error!("Failed to send event to {}: status {}", url, resp.status());
                            }
                        }
                        Err(e) => {
                            error!("Failed to send event to {}: {} (attempt {})", url, e, attempts + 1);
                        }
                    }
                    attempts += 1;
                    if attempts < 3 {
                        thread::sleep(Duration::from_millis(50 * attempts));
                    }
                }
            }
        });

        Self {
            tx,
        }
    }

    fn send_event(&self, event: CapWarmEvent) {
        match self.tx.try_send(event) {
            Ok(_) => {}
            Err(TrySendError::Full(_)) => {
                // Drop event if channel is full (backpressure)
                // In a real system we might want to increment a dropped counter
                warn!("CapWarmNotifier channel full, dropping event");
            }
            Err(TrySendError::Disconnected(_)) => {
                error!("CapWarmNotifier channel disconnected");
            }
        }
    }
}

impl CapWarmNotifier for HttpCapWarmNotifier {
    fn send_run_started(&self, evt: RunStarted) {
        self.send_event(CapWarmEvent::RunStarted(evt));
    }

    fn send_pet(&self, evt: PetEvent) {
        self.send_event(CapWarmEvent::Pet(evt));
    }

    fn send_activation_completed(&self, evt: ActivationCompleted) {
        self.send_event(CapWarmEvent::ActivationCompleted(evt));
    }

    fn send_run_summary(&self, evt: RunSummary) {
        self.send_event(CapWarmEvent::RunSummary(evt));
    }
}
