use crate::interactions::types::{RequestId, RunId};

pub fn make_run_id(value: impl Into<String>) -> RunId {
    RunId(value.into())
}

pub fn make_request_id(value: impl Into<String>) -> RequestId {
    RequestId(value.into())
}

