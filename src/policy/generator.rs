use std::collections::HashMap;

use crate::interactions::types::FuncId;

pub trait BranchPolicy: Send + Sync {
    fn choose_next(&self, from: &FuncId, children: &[FuncId], prefix: &[FuncId]) -> Option<FuncId>;
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicPolicy {
    by_prefix: HashMap<Vec<FuncId>, FuncId>,
}

impl DeterministicPolicy {
    pub fn new(by_prefix: HashMap<Vec<FuncId>, FuncId>) -> Self {
        Self { by_prefix }
    }
}

impl BranchPolicy for DeterministicPolicy {
    fn choose_next(&self, _from: &FuncId, children: &[FuncId], prefix: &[FuncId]) -> Option<FuncId> {
        if let Some(next) = self.by_prefix.get(prefix) {
            if children.contains(next) {
                return Some(next.clone());
            }
        }
        children.first().cloned()
    }
}

#[derive(Clone, Debug)]
pub struct StaticProbabilityPolicy {
    probs_by_from: HashMap<FuncId, Vec<(FuncId, f64)>>,
    rng: std::sync::Arc<std::sync::Mutex<rand::rngs::StdRng>>,
}

impl StaticProbabilityPolicy {
    pub fn new(
        probs_by_from: HashMap<FuncId, Vec<(FuncId, f64)>>,
        seed: u64,
    ) -> Self {
        Self {
            probs_by_from,
            rng: std::sync::Arc::new(std::sync::Mutex::new(
                rand::SeedableRng::seed_from_u64(seed),
            )),
        }
    }

    pub fn dist(&self, from: &FuncId) -> Option<Vec<(FuncId, f64)>> {
        self.probs_by_from.get(from).cloned()
    }

    fn sample_from_dist(
        &self,
        dist: &[(FuncId, f64)],
    ) -> Option<FuncId> {
        if dist.is_empty() {
            return None;
        }
        let mut rng = self.rng.lock().unwrap();
        let r: f64 = rand::Rng::gen(&mut *rng);
        let mut acc = 0.0;
        for (to, p) in dist {
            acc += *p;
            if r < acc {
                return Some(to.clone());
            }
        }
        dist.last().map(|(to, _)| to.clone())
    }
}

impl BranchPolicy for StaticProbabilityPolicy {
    fn choose_next(&self, from: &FuncId, children: &[FuncId], _prefix: &[FuncId]) -> Option<FuncId> {
        if children.is_empty() {
            return None;
        }
        if children.len() == 1 {
            return Some(children[0].clone());
        }

        if let Some(dist) = self.probs_by_from.get(from) {
            if let Some(next) = self.sample_from_dist(dist) {
                if children.contains(&next) {
                    return Some(next);
                }
            }
        }
        children.first().cloned()
    }
}
