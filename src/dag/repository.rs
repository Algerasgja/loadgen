use std::collections::HashMap;

use crate::interactions::types::{FuncId, WorkflowId};

//定义DAG仓库，包含函数之间的依赖关系以及DAG之间的跳转概率
#[derive(Debug, Clone)]
pub struct DagRepository {
    start_nodes: HashMap<WorkflowId, FuncId>,
    edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>>,
    branch_probs: HashMap<(WorkflowId, FuncId), Vec<(FuncId, f64)>>,
    context_aware_nodes: HashMap<WorkflowId, FuncId>, // One context-aware node per workflow
    
    // New fields
    pub iat: HashMap<WorkflowId, f64>,
    pub cv: HashMap<WorkflowId, f64>,
    pub func_memory: HashMap<(WorkflowId, FuncId), usize>,
}

impl DagRepository {
    pub fn new(
        start_nodes: HashMap<WorkflowId, FuncId>,
        edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>>,
    ) -> Self {
        Self {
            start_nodes,
            edges,
            branch_probs: HashMap::new(),
            context_aware_nodes: HashMap::new(),
            iat: HashMap::new(),
            cv: HashMap::new(),
            func_memory: HashMap::new(),
        }
    }
    
    pub fn set_metadata(&mut self, wf: &WorkflowId, iat: f64, cv: f64) {
        self.iat.insert(wf.clone(), iat);
        self.cv.insert(wf.clone(), cv);
    }
    
    pub fn set_memory(&mut self, wf: &WorkflowId, func: FuncId, mem: usize) {
        self.func_memory.insert((wf.clone(), func), mem);
    }
    
    pub fn get_memory(&self, wf: &WorkflowId, func: &FuncId) -> Option<usize> {
        self.func_memory.get(&(wf.clone(), func.clone())).cloned()
    }
    
    pub fn mark_context_aware(&mut self, wf: &WorkflowId, func: FuncId) {
        self.context_aware_nodes.insert(wf.clone(), func);
    }
    
    pub fn get_context_aware_node(&self, wf: &WorkflowId) -> Option<&FuncId> {
        self.context_aware_nodes.get(wf)
    }

    pub fn start_node(&self, workflow_id: &WorkflowId) -> Option<&FuncId> {
        self.start_nodes.get(workflow_id)
    }

    pub fn children(&self, workflow_id: &WorkflowId, curr: &FuncId) -> Vec<FuncId> {
        self.edges
            .get(&(workflow_id.clone(), curr.clone()))
            .cloned()
            .unwrap_or_default()
    }

    pub fn from_nodes(&self, workflow_id: &WorkflowId) -> Vec<FuncId> {
        self.edges
            .keys()
            .filter_map(|(w, from)| {
                if w == workflow_id {
                    Some(from.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn set_edge_prob(
        &mut self,
        workflow_id: &WorkflowId,
        from: &FuncId,
        to: &FuncId,
        prob: f64,
    ) -> Result<(), String> {
        let children = self.children(workflow_id, from);
        if !children.contains(to) {
            return Err("target is not a child edge".to_string());
        }

        let key = (workflow_id.clone(), from.clone());
        let entry = self.branch_probs.entry(key).or_default();
        entry.push((to.clone(), prob));
        Ok(())
    }

    pub fn set_branch_probs(
        &mut self,
        wf: &WorkflowId,
        from: &FuncId,
        dist: Vec<(FuncId, f64)>,
    ) -> Result<(), String> {
        self.branch_probs
            .insert((wf.clone(), from.clone()), dist);
        Ok(())
    }

    pub fn branch_edges(&self, workflow_id: &WorkflowId, from: &FuncId) -> Vec<(FuncId, f64)> {
        self.branch_probs
            .get(&(workflow_id.clone(), from.clone()))
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    }
}
