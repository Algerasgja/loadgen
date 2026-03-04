use std::collections::HashMap;

use crate::dag::repository::DagRepository;
use crate::interactions::types::{FuncId, WorkflowId};
use rand::SeedableRng;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum Period {
    Morning,
    Evening,
}

pub type ContextProbTable = HashMap<(Vec<FuncId>, Period, FuncId), Vec<(FuncId, f64)>>;

/// Generate context-aware probability table.
/// Logic:
/// 1. Identify "branching nodes" (children > 1) in the DAG.
/// 2. For each branching node, perform a search to find paths (prefixes) leading to it.
/// 3. For each (Prefix, Node), generate two distributions:
///    - Morning: Heavily favor Child A (0.9).
///    - Evening: Heavily favor Child B (0.9).
///    - Other children share the remaining 0.1 probability.
pub fn generate_context_aware_table(
    repo: &DagRepository,
    workflow_id: &WorkflowId,
    _seed: u64,
) -> ContextProbTable {
    // Use StdRng if needed, currently deterministic
    // let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut table = ContextProbTable::new();
    let start_node = match repo.start_node(workflow_id) {
        Some(s) => s,
        None => return table,
    };

    // 1. Identify branching nodes
    // User requirement: Only ONE context-aware node per workflow.
    // We rely on repository having marked it.
    let target_node = match repo.get_context_aware_node(workflow_id) {
        Some(n) => n.clone(),
        None => return table, // No context-aware node marked
    };

    let mut stack: Vec<(FuncId, Vec<FuncId>)> = Vec::new();
    stack.push((start_node.clone(), Vec::new()));

    // Limit max paths to avoid explosion
    let max_paths = 10000;
    let mut paths_found = 0;

    while let Some((curr, prefix)) = stack.pop() {
        paths_found += 1;
        if paths_found > max_paths {
            break;
        }

        let children = repo.children(workflow_id, &curr);
        
        // If this is the TARGET node, generate entries
        if curr == target_node {
            // Generate Morning Distribution
            let morning_dist = generate_skewed_dist(&children, 0, 0.9); // Favor 1st child
            table.insert((prefix.clone(), Period::Morning, curr.clone()), morning_dist);

            // Generate Evening Distribution
            // Favor a different child (e.g., 2nd child, or last)
            let favor_idx = if children.len() > 1 { 1 } else { 0 };
            let evening_dist = generate_skewed_dist(&children, favor_idx, 0.9);
            table.insert((prefix.clone(), Period::Evening, curr.clone()), evening_dist);
        }

        // Continue traversal
        let mut new_prefix = prefix.clone();
        new_prefix.push(curr.clone());
        
        for child in children {
            stack.push((child, new_prefix.clone()));
        }
    }

    table
}

fn generate_skewed_dist(children: &[FuncId], favor_idx: usize, favor_prob: f64) -> Vec<(FuncId, f64)> {
    let n = children.len();
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![(children[0].clone(), 1.0)];
    }

    let mut dist = Vec::with_capacity(n);
    let remaining_prob = 1.0 - favor_prob;
    let other_prob = remaining_prob / (n - 1) as f64;

    for (i, child) in children.iter().enumerate() {
        let p = if i == favor_idx {
            favor_prob
        } else {
            other_prob
        };
        dist.push((child.clone(), p));
    }
    dist
}
