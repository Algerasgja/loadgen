use std::collections::HashMap;

use crate::interactions::types::{FuncId, RequestId, RunId, WorkflowId};

#[derive(Clone, Debug)]
pub struct RunState {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub hop_index: usize,
    pub max_hops: usize,
    pub prefix: Vec<FuncId>,
    pub current_func: Option<FuncId>,
    pub branch_probabilities: HashMap<FuncId, Vec<(FuncId, f64)>>,
    pub inflight_activations: HashMap<String, InflightActivation>,
    pub status: RunStatus,
    pub last_end_ts: u64,
}

impl RunState {
    pub fn new(
        workflow_id: WorkflowId,
        run_id: RunId,
        request_id: RequestId,
        max_hops: usize,
        branch_probabilities: HashMap<FuncId, Vec<(FuncId, f64)>>,
        start_time: u64,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            request_id,
            hop_index: 0,
            max_hops: max_hops.max(1),
            prefix: Vec::new(),
            current_func: None,
            branch_probabilities,
            inflight_activations: HashMap::new(),
            status: RunStatus::Running,
            last_end_ts: start_time,
        }
    }

    pub fn start_at(&mut self, func: FuncId) {
        self.current_func = Some(func);
    }

    pub fn on_invoked(&mut self, activation_id: String, func: FuncId, start_ts: u64) {
        self.inflight_activations.insert(
            activation_id,
            InflightActivation {
                func,
                start_ts,
            },
        );
    }

    pub fn on_completed(&mut self, activation_id: &str, current_end_ts: u64) -> Option<(FuncId, u64)> {
        let inflight = self.inflight_activations.remove(activation_id)?;
        let func = inflight.func;
        
        let prev_end_ts = self.last_end_ts;
        self.last_end_ts = current_end_ts;

        self.prefix.push(func.clone());
        self.hop_index = self.hop_index.saturating_add(1);
        self.current_func = None;
        Some((func, prev_end_ts))
    }

    pub fn set_current(&mut self, func: FuncId) {
        self.current_func = Some(func);
    }

    pub fn should_terminate(&self, children_len: usize) -> Option<TerminationReason> {
        if self.hop_index >= self.max_hops {
            return Some(TerminationReason::ReachedMaxHops);
        }
        if children_len == 0 {
            return Some(TerminationReason::NoChildren);
        }
        None
    }

    pub fn mark_finished(&mut self, reason: TerminationReason) {
        self.status = RunStatus::Completed { reason };
        self.current_func = None;
        self.inflight_activations.clear();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InflightActivation {
    pub func: FuncId,
    pub start_ts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminationReason {
    NoChildren,
    ReachedMaxHops,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Completed { reason: TerminationReason },
}
