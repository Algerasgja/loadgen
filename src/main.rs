fn main() {
    use loadgen::config::Config;
    use loadgen::dag::loader::load_from_dir;
    use loadgen::engine::dag_execution_engine::DagExecutionEngine;
    use loadgen::interactions::capwarm_notifier::NoopNotifier;
    use loadgen::interactions::openwhisk_actions::OpenWhiskActionProvisioner;
    use loadgen::interactions::openwhisk_client::{
        ActivationRecord, MockOpenWhiskClient, OpenWhiskCliClient, OpenWhiskClient,
    };
    use loadgen::interactions::types::FuncId;
    use loadgen::util::now_millis;
    use loadgen::workload::arrival::ArrivalProcess;
    use loadgen::workload::scheduler::WorkloadScheduler;

    let cfg_path = std::env::var("LOADGEN_CONFIG").unwrap_or_else(|_| "loadgen.yaml".to_string());
    let cfg = Config::from_yaml_file(std::path::Path::new(&cfg_path)).expect("load config failed");

    let mean_interval_ms = (1000.0 / cfg.workload.target_rps.max(0.001)).max(1.0);
    let stddev_ms = (mean_interval_ms * cfg.workload.stddev_ratio).max(1.0);

    let (repo, workflow_id) = load_from_dir(std::path::Path::new(&cfg.dag_dir))
        .expect("load DAGs failed");
    let notifier = NoopNotifier::default();

    let ow: Box<dyn OpenWhiskClient> = if !cfg.openwhisk.wsk_path.is_empty() {
        if cfg.openwhisk.auto_create_actions != 0 {
            let provisioner = OpenWhiskActionProvisioner::new(
                cfg.openwhisk.wsk_path.clone(),
                cfg.openwhisk.action_kind.clone(),
            );
            let demo_dir = std::path::Path::new(&cfg.openwhisk.demo_dir);
            provisioner
                .ensure_actions_for_workflow(&repo, &workflow_id, demo_dir)
                .expect("ensure actions failed");
        }
        Box::new(OpenWhiskCliClient::new(
            cfg.openwhisk.wsk_path.clone(),
            cfg.openwhisk.poll_interval_ms,
            cfg.openwhisk.poll_batch_size,
        ))
    } else {
        let per_run_acts = 3;
        let mut records: Vec<ActivationRecord> = Vec::new();
        for i in 0..(cfg.workload.total_runs * per_run_acts) {
            let start_ts = (i as u64) * 10;
            let end_ts = start_ts + 5;
            records.push(ActivationRecord {
                activation_id: String::new(),
                func: FuncId("X".to_string()),
                start_ts,
                end_ts,
                exec_duration: 5,
                cold_start_duration: None,
            });
        }
        Box::new(MockOpenWhiskClient::new(records))
    };
    let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut scheduler = WorkloadScheduler::new(
        workflow_id.clone(),
        ArrivalProcess::NormalIntervalMs {
            mean_interval_ms,
            stddev_ms,
            min_interval_ms: 1,
        },
        usize::MAX,
        cfg.workload.total_runs,
        in_flight.clone(),
        42,
        now_millis(),
    );

    let mut engine = DagExecutionEngine::new(
        &repo,
        ow.as_ref(),
        &notifier,
        cfg.workload.max_concurrency,
        7,
    );

    let start_ms = now_millis();
    let end_ms = start_ms + 1000;
    let mut now_ms = start_ms;
    while now_ms <= end_ms {
        if let Some(req) = scheduler.try_issue(now_ms) {
            engine.enqueue_new_run(req, 64);
        }
        engine.tick();
        now_ms = now_ms.saturating_add(1);
    }

    while engine.has_work() {
        engine.tick();
    }

    println!("done: runs={}", cfg.workload.total_runs);
}
