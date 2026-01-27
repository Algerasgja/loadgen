use crate::interactions::types::{FuncId, PetEvent, Timestamp, WorkflowId};

pub fn build_pet_event(
    workflow_id: WorkflowId,
    run_id: crate::interactions::types::RunId,
    request_id: crate::interactions::types::RequestId,
    prefix: Vec<FuncId>,
    curr_func: FuncId,
    next_func: FuncId,
    timestamp: Timestamp,
) -> PetEvent {
    PetEvent {
        workflow_id,
        run_id,
        request_id,
        prefix,
        curr_func,
        next_func,
        timestamp,
    }
}

