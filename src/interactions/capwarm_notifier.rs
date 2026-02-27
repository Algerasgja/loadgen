use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender};
use std::thread;

use crate::interactions::types::{ActivationCompleted, PetEvent, RunStarted};

pub trait CapWarmNotifier: Send + Sync {
    fn send_run_started(&self, evt: RunStarted);
    fn send_pet(&self, evt: PetEvent);
    fn send_activation_completed(&self, evt: ActivationCompleted);
}

#[derive(Clone, Default)]
pub struct NoopNotifier;

impl CapWarmNotifier for NoopNotifier {
    fn send_run_started(&self, _evt: RunStarted) {}

    fn send_pet(&self, _evt: PetEvent) {}

    fn send_activation_completed(&self, _evt: ActivationCompleted) {}
}

#[derive(Clone, Default)]
pub struct RecordingNotifier {
    pub run_started: Arc<Mutex<Vec<RunStarted>>>,
    pub pet: Arc<Mutex<Vec<PetEvent>>>,
    pub activation_completed: Arc<Mutex<Vec<ActivationCompleted>>>,
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
}

pub enum CapWarmEvent {
    RunStarted(RunStarted),
    Pet(PetEvent),
    ActivationCompleted(ActivationCompleted),
}

pub struct HttpCapWarmNotifier {
    tx: Mutex<Sender<CapWarmEvent>>,
}

impl HttpCapWarmNotifier {
    pub fn new(base_url: String) -> Self {
        let (tx, rx) = channel();
        let client = reqwest::blocking::Client::new();
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
                };

                // Simple retry logic or just log error
                if let Err(e) = client.post(&url).json(&body).send() {
                    eprintln!("Failed to send event to {}: {}", url, e);
                }
            }
        });

        Self {
            tx: Mutex::new(tx),
        }
    }
}

impl CapWarmNotifier for HttpCapWarmNotifier {
    fn send_run_started(&self, evt: RunStarted) {
        let _ = self.tx.lock().unwrap().send(CapWarmEvent::RunStarted(evt));
    }

    fn send_pet(&self, evt: PetEvent) {
        let _ = self.tx.lock().unwrap().send(CapWarmEvent::Pet(evt));
    }

    fn send_activation_completed(&self, evt: ActivationCompleted) {
        let _ = self
            .tx
            .lock()
            .unwrap()
            .send(CapWarmEvent::ActivationCompleted(evt));
    }
}
