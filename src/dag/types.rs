use crate::interactions::types::{FuncId, WorkflowId};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub FuncId);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DagId(pub WorkflowId);

