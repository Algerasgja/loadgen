use std::sync::{Arc, Mutex};

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
