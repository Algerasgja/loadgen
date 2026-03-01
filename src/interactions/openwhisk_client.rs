use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::interactions::types::{Duration, FuncId, RequestId, RunId, Timestamp, WorkflowId};

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

#[derive(Clone)]
pub struct MockOpenWhiskClient {
    pub records: Arc<Mutex<VecDeque<ActivationRecord>>>,
}

impl MockOpenWhiskClient {
    pub fn new(records: Vec<ActivationRecord>) -> Self {
        Self {
            records: Arc::new(Mutex::new(VecDeque::from(records))),
        }
    }
}

impl OpenWhiskClient for MockOpenWhiskClient {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle {
        let activation_id = format!("mock-{}", uuid::Uuid::new_v4());
        ActivationHandle { activation_id }
    }

    fn poll_completed(&self) -> Vec<CompletedActivation> {
        let mut out = Vec::new();
        let mut recs = self.records.lock().unwrap();
        if let Some(r) = recs.pop_front() {
             out.push(CompletedActivation {
                ctx: InvocationContext {
                    workflow_id: WorkflowId("mock".to_string()),
                    run_id: RunId("mock".to_string()),
                    request_id: RequestId("mock".to_string()),
                    prefix: vec![],
                    curr_func: r.func.clone(),
                    timestamp: 0,
                },
                record: r,
            });
        }
        out
    }
}
