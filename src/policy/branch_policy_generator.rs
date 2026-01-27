use std::collections::HashMap;

use crate::dag::repository::DagRepository;
use crate::interactions::types::{FuncId, WorkflowId};
use crate::policy::generator::StaticProbabilityPolicy;

pub fn generate_initial_branch_policy(
    repo: &DagRepository,
    workflow_id: &WorkflowId,
    seed: u64,
) -> StaticProbabilityPolicy {
    let mut rng = rand::SeedableRng::seed_from_u64(seed);
    let mut probs_by_from: HashMap<FuncId, Vec<(FuncId, f64)>> = HashMap::new();
    for from in repo.from_nodes(workflow_id) {
        let children = repo.children(workflow_id, &from);
        if children.len() <= 1 {
            continue;
        }

        let preset = repo.branch_edges(workflow_id, &from);
        let dist = if preset.is_empty() {
            random_dist(&children, &mut rng)
        } else {
            normalize(preset)
        };
        probs_by_from.insert(from, dist);
    }
    StaticProbabilityPolicy::new(probs_by_from, seed)
}

fn normalize(mut dist: Vec<(FuncId, f64)>) -> Vec<(FuncId, f64)> {
    let sum: f64 = dist.iter().map(|(_, p)| *p).sum();
    if !sum.is_finite() || sum <= 0.0 {
        return Vec::new();
    }
    for (_, p) in dist.iter_mut() {
        *p /= sum;
    }
    dist
}

fn random_dist(children: &[FuncId], rng: &mut rand::rngs::StdRng) -> Vec<(FuncId, f64)> {
    if children.len() == 2 {
        let u: f64 = rand::Rng::gen(rng);
        return vec![(children[0].clone(), u), (children[1].clone(), 1.0 - u)];
    }

    let mut weights: Vec<f64> = Vec::with_capacity(children.len());
    for _ in 0..children.len() {
        let w: f64 = rand::Rng::gen(rng);
        weights.push(w.max(1e-12));
    }
    let sum: f64 = weights.iter().sum();
    children
        .iter()
        .cloned()
        .zip(weights.into_iter().map(|w| w / sum))
        .collect()
}
