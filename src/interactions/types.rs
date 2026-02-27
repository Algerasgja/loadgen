use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct WorkflowId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct RunId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct RequestId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct FuncId(pub String);

pub type Timestamp = u64;
pub type Duration = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RunStarted {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PetEvent {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub prefix: Vec<FuncId>,
    pub curr_func: FuncId,
    pub next_func: FuncId,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ActivationCompleted {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub prefix: Vec<FuncId>,
    pub func: FuncId,
    pub activation_id: String,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    pub exec_duration: Duration,
    pub cold_start_duration: Option<Duration>,
    pub transition_time: Duration,
    pub timestamp: Timestamp,
}

