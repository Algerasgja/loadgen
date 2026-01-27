use crate::interactions::types::FuncId;

#[derive(Clone, Debug)]
pub struct Choice {
    pub next: FuncId,
    pub weight: f64,
}

