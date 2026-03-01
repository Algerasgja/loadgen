fn main() {
    use loadgen::config::Config;
    use loadgen::dag::loader::load_from_dir;
    use loadgen::engine::dag_execution_engine::DagExecutionEngine;
    use loadgen::interactions::capwarm_notifier::{CapWarmNotifier, HttpCapWarmNotifier, NoopNotifier};
    use loadgen::interactions::openwhisk_client::{
        ActivationRecord, MockOpenWhiskClient, OpenWhiskClient,
    };
    use loadgen::interactions::http_openwhisk_client::HttpOpenWhiskClient;
    use loadgen::interactions::types::FuncId;
    use loadgen::util::now_millis;
    use loadgen::workload::arrival::ArrivalProcess;
    use loadgen::workload::scheduler::WorkloadScheduler;
    use log::{info, error};

    env_logger::init();

    let cfg_path = std::env::var("LOADGEN_CONFIG").unwrap_or_else(|_| "loadgen.yaml".to_string());
    info!("Loading config from: {}", cfg_path);
    let cfg = Config::from_yaml_file(std::path::Path::new(&cfg_path)).expect("load config failed");
    info!("Config loaded: target_rps={}, concurrency={}, total_runs={}", 
        cfg.workload.target_rps, cfg.workload.max_concurrency, cfg.workload.total_runs);

    let mean_interval_ms = (1000.0 / cfg.workload.target_rps.max(0.001)).max(1.0);
    let stddev_ms = (mean_interval_ms * cfg.workload.stddev_ratio).max(1.0);

    let repos = load_from_dir(std::path::Path::new(&cfg.dag_dir))
        .expect("load DAGs failed");
    info!("Loaded {} workflows from {}", repos.len(), cfg.dag_dir);
    
    // Extract workflow IDs for scheduler
    let workflow_ids: Vec<_> = repos.iter().map(|(_, wf)| wf.clone()).collect();
    
    let notifier: Box<dyn CapWarmNotifier> = if !cfg.cap_warm.url.is_empty() {
        info!("Using HttpCapWarmNotifier with URL: {}", cfg.cap_warm.url);
        Box::new(HttpCapWarmNotifier::new(cfg.cap_warm.url.clone()))
    } else {
        info!("Using NoopNotifier (CAP-Warm URL not configured)");
        Box::new(NoopNotifier::default())
    };

    let ow: Box<dyn OpenWhiskClient> = if !cfg.openwhisk.api_host.is_empty() && !cfg.openwhisk.auth_key.is_empty() {
        info!("Using HttpOpenWhiskClient with API Host: {}", cfg.openwhisk.api_host);
        Box::new(HttpOpenWhiskClient::new(
            cfg.openwhisk.api_host.clone(),
            cfg.openwhisk.auth_key.clone(),
            cfg.openwhisk.poll_interval_ms,
            cfg.openwhisk.poll_batch_size,
        ))
    } else {
        info!("Using MockOpenWhiskClient");
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
        workflow_ids,
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
        &repos,
        ow.as_ref(),
        notifier.as_ref(),
        cfg.workload.max_concurrency,
        in_flight.clone(),
        7,
    );

    let start_ms = now_millis();
    let end_ms = start_ms + 1000;
    let mut now_ms = start_ms;
    while now_ms <= end_ms {
        if let Some(req) = scheduler.try_issue(now_ms) {
            info!("Issued new run: {} (workflow: {})", req.run_id.0, req.workflow_id.0);
            engine.enqueue_new_run(req, 64);
        }
        engine.tick();
        now_ms = now_ms.saturating_add(1);
    }

    info!("Finished issuing runs, waiting for completions...");
    while engine.has_work() {
        engine.tick();
    }

    info!("All work completed. Total runs: {}", cfg.workload.total_runs);
}
