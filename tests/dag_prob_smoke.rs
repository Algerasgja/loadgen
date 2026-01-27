use std::collections::HashMap;

use loadgen::dag::repository::DagRepository;
use loadgen::interactions::types::{FuncId, WorkflowId};
use loadgen::policy::branch_policy_generator::generate_initial_branch_policy;

#[test]
fn generated_branch_prob_sum_is_one() {
    let w = WorkflowId("wf".to_string());
    let a = FuncId("A".to_string());
    let b = FuncId("B".to_string());
    let c = FuncId("C".to_string());

    let repo = DagRepository::new(
        HashMap::from([(w.clone(), a.clone())]),
        HashMap::from([((w.clone(), a.clone()), vec![b.clone(), c.clone()])]),
    );

    let policy = generate_initial_branch_policy(&repo, &w, 1);
    let dist = policy.dist(&a).expect("missing dist for branch node");
    let sum: f64 = dist.iter().map(|(_, p)| *p).sum();
    assert!((sum - 1.0).abs() < 1e-9);
    for (_, p) in dist {
        assert!(p >= 0.0 && p <= 1.0);
    }
}
