use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use loadgen::interactions::types::WorkflowId;
use loadgen::workload::arrival::ArrivalProcess;
use loadgen::workload::scheduler::WorkloadScheduler;

#[test]
fn scheduler_issues_n_requests_and_respects_concurrency() {
    let in_flight: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let mut scheduler = WorkloadScheduler::new(
        WorkflowId("wf".to_string()),
        ArrivalProcess::FixedIntervalMs(10),
        1,
        2,
        in_flight.clone(),
        1,
        0,
    );

    let r1 = scheduler.try_issue(0).expect("first request");
    assert_eq!(r1.run_id.0, "run-0");
    assert_eq!(in_flight.load(Ordering::Relaxed), 1);

    assert!(scheduler.try_issue(10).is_none());

    in_flight.fetch_sub(1, Ordering::Relaxed);
    let r2 = scheduler.try_issue(10).expect("second request");
    assert_eq!(r2.run_id.0, "run-1");
    assert_eq!(in_flight.load(Ordering::Relaxed), 1);

    in_flight.fetch_sub(1, Ordering::Relaxed);
    assert!(scheduler.try_issue(20).is_none());
    assert!(scheduler.is_done());
}

