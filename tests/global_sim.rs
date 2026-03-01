use std::collections::HashMap;

use loadgen::dag::repository::DagRepository;
use loadgen::engine::dag_execution_engine::DagExecutionEngine;
use loadgen::interactions::capwarm_notifier::RecordingNotifier;
use loadgen::interactions::openwhisk_client::{ActivationRecord, MockOpenWhiskClient};
use loadgen::interactions::types::{FuncId, WorkflowId};
use loadgen::workload::arrival::ArrivalProcess;
use loadgen::workload::scheduler::WorkloadScheduler;

#[test]
fn sim_one_second_requests_single_engine_multi_run_branch_and_termination() {
    let workflow_id = WorkflowId("wf".to_string());
    let a = FuncId("A".to_string());
    let b = FuncId("B".to_string());
    let c = FuncId("C".to_string());
    let d = FuncId("D".to_string());

    let mut repo = DagRepository::new(
        HashMap::from([(workflow_id.clone(), a.clone())]),
        HashMap::from([
            ((workflow_id.clone(), a.clone()), vec![b.clone(), c.clone()]),
            ((workflow_id.clone(), b.clone()), vec![d.clone()]),
            ((workflow_id.clone(), c.clone()), vec![d.clone()]),
        ]),
    );
    repo.set_branch_probs(&workflow_id, &a, vec![(b.clone(), 0.2), (c.clone(), 0.8)])
        .unwrap();

    let target_rps = 200.0;
    let mean_interval_ms: f64 = (1000.0_f64 / target_rps).max(1.0);
    let total_runs_limit = 10_000;
    let max_concurrency = 20;

    let per_run_acts = 3;
    let mut records: Vec<ActivationRecord> = Vec::new();
    let max_possible_runs_in_one_second = 1001usize;
    for i in 0..(max_possible_runs_in_one_second * per_run_acts) {
        records.push(ActivationRecord {
            activation_id: String::new(),
            func: FuncId("X".to_string()),
            start_ts: (i as u64) * 10,
            end_ts: (i as u64) * 10 + 5,
            exec_duration: 5,
            cold_start_duration: None,
        });
    }
    let ow = MockOpenWhiskClient::new(records);
    let notifier = RecordingNotifier::default();

    let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut scheduler = WorkloadScheduler::new(
        workflow_id.clone(),
        ArrivalProcess::FixedIntervalMs(mean_interval_ms as u64),
        usize::MAX,
        total_runs_limit,
        in_flight.clone(),
        1,
        0,
    );

    let mut engine = DagExecutionEngine::new(&repo, &ow, &notifier, max_concurrency, in_flight.clone(), 7);

    let start_ms = 0_u64;
    let end_ms = start_ms + 1000;
    let mut now_ms = start_ms;
    let mut issued = 0usize;
    while now_ms <= end_ms {
        if let Some(req) = scheduler.try_issue(now_ms) {
            engine.enqueue_new_run(req, 64);
            issued += 1;
        }
        engine.tick();
        now_ms += 1;
    }

    while engine.has_work() {
        engine.tick();
    }

    let run_started = notifier.run_started.lock().unwrap().clone();
    assert_eq!(run_started.len(), issued);

    let pets = notifier.pet.lock().unwrap().clone();
    for p in pets.iter() {
        let children = repo.children(&p.workflow_id, &p.curr_func);
        assert!(children.contains(&p.next_func));
    }

    let mut branch_count_b = 0usize;
    let mut branch_count_c = 0usize;
    for p in pets.iter().filter(|p| p.curr_func == a) {
        if p.next_func == b {
            branch_count_b += 1;
        } else if p.next_func == c {
            branch_count_c += 1;
        }
    }
    assert_eq!(branch_count_b + branch_count_c, issued);
    let b_ratio = (branch_count_b as f64) / (issued as f64);
    assert!((b_ratio - 0.2).abs() < 0.1);

    let completed = notifier.activation_completed.lock().unwrap().clone();
    let mut by_run: HashMap<String, Vec<FuncId>> = HashMap::new();
    for e in completed.iter() {
        by_run.entry(e.run_id.0.clone()).or_default().push(e.func.clone());
    }
    assert_eq!(by_run.len(), issued);
    for seq in by_run.values() {
        assert_eq!(seq.len(), 3);
        assert_eq!(seq[0], a);
        assert_eq!(seq[2], d);
    }

    assert_eq!(pets.len(), issued * 2);
}
