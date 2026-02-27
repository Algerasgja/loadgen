use std::collections::HashMap;

use crate::interactions::capwarm_notifier::CapWarmNotifier;
use crate::interactions::openwhisk_client::ActivationRecord;
use crate::interactions::types::{ActivationCompleted, FuncId, RunId};
use crate::run::state::{RunState, TerminationReason};
use crate::util::now_millis;

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
        notifier: &dyn CapWarmNotifier,
    ) -> RunStepOutcome {
        let run_id = match self.activation_to_run.remove(&activation.activation_id) {
            Some(v) => v,
            None => return RunStepOutcome::UnknownRun,
        };
        let state = match self.runs.get_mut(&run_id) {
            Some(v) => v,
            None => return RunStepOutcome::UnknownRun,
        };

        let (completed, prev_end_ts) = state
            .on_completed(&activation.activation_id, activation.end_ts)
            .unwrap_or_else(|| (activation.func.clone(), 0));

        let transition_time = activation.start_ts.saturating_sub(prev_end_ts);

        notifier.send_activation_completed(ActivationCompleted {
            workflow_id: state.workflow_id.clone(),
            run_id: run_id.clone(),
            request_id: state.request_id.clone(),
            prefix: state.prefix.clone(),
            func: completed.clone(),
            activation_id: activation.activation_id.clone(),
            start_ts: activation.start_ts,
            end_ts: activation.end_ts,
            exec_duration: activation.exec_duration,
            cold_start_duration: activation.cold_start_duration,
            transition_time,
            timestamp: now_millis(),
        });

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
