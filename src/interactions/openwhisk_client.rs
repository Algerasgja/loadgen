use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::interactions::types::{Duration, FuncId, RequestId, RunId, Timestamp, WorkflowId};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationRecord {
    pub activation_id: String,
    pub func: FuncId,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    pub exec_duration: Duration,
    pub cold_start_duration: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationContext {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub prefix: Vec<FuncId>,
    pub curr_func: FuncId,
    pub timestamp: Timestamp,
    pub params: Option<Value>, // Extra parameters (e.g. memory, warmup)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationHandle {
    pub activation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedActivation {
    pub ctx: InvocationContext,
    pub record: ActivationRecord,
}

pub trait OpenWhiskClient: Send + Sync {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle;
    fn poll_completed(&self) -> Vec<CompletedActivation>;
}
