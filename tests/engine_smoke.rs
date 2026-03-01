use std::collections::HashMap;

use loadgen::dag::repository::DagRepository;
use loadgen::engine::dag_execution_engine::DagExecutionEngine;
use loadgen::interactions::capwarm_notifier::RecordingNotifier;
use loadgen::interactions::openwhisk_client::{ActivationRecord, MockOpenWhiskClient};
use loadgen::interactions::types::{FuncId, RequestId, RunId, WorkflowId};

#[test]
fn runs_linear_dag_and_emits_events() {
    let w = WorkflowId("wf".to_string());
    let a = FuncId("A".to_string());
    let b = FuncId("B".to_string());
    let c = FuncId("C".to_string());

    let repo = DagRepository::new(
        HashMap::from([(w.clone(), a.clone())]),
        HashMap::from([
            ((w.clone(), a.clone()), vec![b.clone()]),
            ((w.clone(), b.clone()), vec![c.clone()]),
        ]),
    );

    let ow = MockOpenWhiskClient::new(vec![
        ActivationRecord {
            activation_id: "actA".to_string(),
            func: a.clone(),
            start_ts: 1,
            end_ts: 2,
            exec_duration: 10,
            cold_start_duration: Some(3),
        },
        ActivationRecord {
            activation_id: "actB".to_string(),
            func: b.clone(),
            start_ts: 3,
            end_ts: 4,
            exec_duration: 11,
            cold_start_duration: None,
        },
        ActivationRecord {
            activation_id: "actC".to_string(),
            func: c.clone(),
            start_ts: 5,
            end_ts: 6,
            exec_duration: 12,
            cold_start_duration: None,
        },
    ]);

    let notifier = RecordingNotifier::default();
    let inflight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut engine = DagExecutionEngine::new(&repo, &ow, &notifier, 8, inflight, 1);
    engine.enqueue_new_run(
        loadgen::workload::scheduler::NewRunRequest {
            workflow_id: w.clone(),
            run_id: RunId("run1".to_string()),
            request_id: RequestId("req1".to_string()),
            start_time: 0,
        },
        64,
    );
    while engine.has_work() {
        engine.tick();
    }

    assert_eq!(notifier.run_started.lock().unwrap().len(), 1);
    assert_eq!(notifier.activation_completed.lock().unwrap().len(), 3);
    assert_eq!(notifier.pet.lock().unwrap().len(), 2);

    let pets = notifier.pet.lock().unwrap().clone();
    assert_eq!(pets[0].curr_func, a);
    assert_eq!(pets[0].next_func, b);
    assert_eq!(pets[1].curr_func, b);
    assert_eq!(pets[1].next_func, c);
}
