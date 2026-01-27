use std::collections::HashMap;

use crate::interactions::openwhisk_client::ActivationRecord;
use crate::interactions::types::{FuncId, RunId};
use crate::run::state::{RunState, TerminationReason};

#[derive(Clone, Debug, Default)]
pub struct RunManager {
    runs: HashMap<RunId, RunState>,
    activation_to_run: HashMap<String, RunId>,
}

impl RunManager {
    pub fn create_run(&mut self, state: RunState) {
        self.runs.insert(state.run_id.clone(), state);
    }

    pub fn get(&self, run_id: &RunId) -> Option<&RunState> {
        self.runs.get(run_id)
    }

    pub fn get_mut(&mut self, run_id: &RunId) -> Option<&mut RunState> {
        self.runs.get_mut(run_id)
    }

    pub fn inflight_count(&self) -> usize {
        self.runs
            .values()
            .map(|s| s.inflight_activations.len())
            .sum()
    }

    pub fn on_invoked(
        &mut self,
        run_id: &RunId,
        activation_id: String,
        func: FuncId,
        start_ts: u64,
    ) -> Result<(), String> {
        let state = self
            .runs
            .get_mut(run_id)
            .ok_or_else(|| "unknown run".to_string())?;
        state.on_invoked(activation_id.clone(), func, start_ts);
        self.activation_to_run.insert(activation_id, run_id.clone());
        Ok(())
    }

    pub fn on_activation_completed(
        &mut self,
        activation: &ActivationRecord,
        children_len: usize,
    ) -> RunStepOutcome {
        let run_id = match self.activation_to_run.remove(&activation.activation_id) {
            Some(v) => v,
            None => return RunStepOutcome::UnknownRun,
        };
        let state = match self.runs.get_mut(&run_id) {
            Some(v) => v,
            None => return RunStepOutcome::UnknownRun,
        };

        let completed = state
            .on_completed(&activation.activation_id)
            .unwrap_or_else(|| activation.func.clone());

        if let Some(reason) = state.should_terminate(children_len) {
            state.mark_finished(reason.clone());
            return RunStepOutcome::Finished { reason };
        }

        RunStepOutcome::Continue {
            completed_func: completed,
            prefix: state.prefix.clone(),
            run_id,
        }
    }
}

#[derive(Clone, Debug)]
pub enum RunStepOutcome {
    Continue {
        completed_func: FuncId,
        prefix: Vec<FuncId>,
        run_id: RunId,
    },
    Finished {
        reason: TerminationReason,
    },
    UnknownRun,
}
