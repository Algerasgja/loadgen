use std::collections::HashMap;

use crate::interactions::types::{FuncId, WorkflowId};

//定义DAG仓库，包含函数之间的依赖关系以及DAG之间的跳转概率
#[derive(Clone, Debug)]
pub struct DagRepository {
    start: HashMap<WorkflowId, FuncId>,
    edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>>,
    branch_probs: HashMap<(WorkflowId, FuncId), HashMap<FuncId, f64>>,
}

impl DagRepository {
    pub fn new(start: HashMap<WorkflowId, FuncId>, edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>>) -> Self {
        Self {
            start,
            edges,
            branch_probs: HashMap::new(),
        }
    }

    pub fn start_node(&self, workflow_id: &WorkflowId) -> Option<&FuncId> {
        self.start.get(workflow_id)
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
        entry.insert(to.clone(), prob);
        Ok(())
    }

    pub fn set_branch_probs(
        &mut self,
        workflow_id: &WorkflowId,
        from: &FuncId,
        probs: Vec<(FuncId, f64)>,
    ) -> Result<(), String> {
        let children = self.children(workflow_id, from);
        for (to, _) in probs.iter() {
            if !children.contains(to) {
                return Err("target is not a child edge".to_string());
            }
        }

        let key = (workflow_id.clone(), from.clone());
        let mut map: HashMap<FuncId, f64> = HashMap::new();
        for (to, p) in probs {
            map.insert(to, p);
        }
        self.branch_probs.insert(key, map);
        Ok(())
    }

    pub fn branch_edges(&self, workflow_id: &WorkflowId, from: &FuncId) -> Vec<(FuncId, f64)> {
        self.branch_probs
            .get(&(workflow_id.clone(), from.clone()))
            .map(|m| m.iter().map(|(k, v)| (k.clone(), *v)).collect())
            .unwrap_or_default()
    }
}
