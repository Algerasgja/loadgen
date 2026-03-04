fn main() {
    use loadgen::config::Config;
    use loadgen::dag::loader::load_single_file;
    use loadgen::engine::dag_execution_engine::DagExecutionEngine;
    use loadgen::interactions::action_provisioner::HttpActionProvisioner;
    use loadgen::interactions::capwarm_notifier::{CapWarmNotifier, HttpCapWarmNotifier, NoopNotifier};
    use loadgen::interactions::openwhisk_client::{
        ActivationRecord, OpenWhiskClient,
    };
    use loadgen::interactions::http_openwhisk_client::HttpOpenWhiskClient;
    use loadgen::interactions::types::FuncId;
    use loadgen::util::now_millis;
    use loadgen::workload::arrival::ArrivalProcess;
    use loadgen::workload::scheduler::WorkloadScheduler;
    use log::{info, error};

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cfg_path = std::env::var("LOADGEN_CONFIG").unwrap_or_else(|_| "loadgen.yaml".to_string());
    info!("Loading config from: {}", cfg_path);
    let cfg = Config::from_yaml_file(std::path::Path::new(&cfg_path)).expect("load config failed");
    info!("Config loaded: duration_seconds={}, drift_mode={:?}", 
        cfg.workload.duration_seconds, cfg.workload.drift_mode);

    let mean_interval_ms = 200.0; // Default fallback, overridden by RealWorld
    let stddev_ms = 20.0;

    // Load single specified DAG
    let (repo, workflow_id) = load_single_file(std::path::Path::new(&cfg.dag_dir), &cfg.dag_file)
        .expect("load DAG failed");
    let repos = vec![(repo.clone(), workflow_id.clone())];
    info!("Loaded workflow {} from {}", workflow_id.0, cfg.dag_file);
    
    // Log Repository Details
    info!("=== DAG Repository Details ===");
    info!("Workflow: {}", workflow_id.0);
    info!("Start Node: {:?}", repo.start_node(&workflow_id).unwrap().0);
    
    info!("-- Metadata --");
    if let (Some(iat), Some(cv)) = (repo.iat.get(&workflow_id), repo.cv.get(&workflow_id)) {
        info!("IAT: {:.4}, CV: {:.4}", iat, cv);
    } else {
        info!("IAT/CV: Not set");
    }
    
    info!("-- Context Aware Node --");
    if let Some(node) = repo.get_context_aware_node(&workflow_id) {
        info!("Node: {}", node.0);
    } else {
        info!("None");
    }
    
    info!("-- Functions & Memory --");
    // Iterate all known nodes (start + all children)
    let mut all_nodes = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    let start = repo.start_node(&workflow_id).unwrap();
    queue.push_back(start.clone());
    all_nodes.insert(start.clone());
    
    while let Some(curr) = queue.pop_front() {
        let mem = repo.get_memory(&workflow_id, &curr).unwrap_or(0);
        info!("Func: {}, Memory: {} MB", curr.0, mem);
        
        for child in repo.children(&workflow_id, &curr) {
            if all_nodes.insert(child.clone()) {
                queue.push_back(child);
            }
        }
    }
    
    info!("-- Edges & Probabilities --");
    for from in &all_nodes {
        let children = repo.children(&workflow_id, from);
        if !children.is_empty() {
            let probs = repo.branch_edges(&workflow_id, from);
            if !probs.is_empty() {
                let prob_str: Vec<String> = probs.iter().map(|(t, p)| format!("{}={:.2}", t.0, p)).collect();
                info!("{} -> {:?} (Probs: {})", from.0, children.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(), prob_str.join(", "));
            } else {
                info!("{} -> {:?}", from.0, children.iter().map(|c| c.0.as_str()).collect::<Vec<_>>());
            }
        }
    }
    info!("==============================");
    
    // Use single workflow ID for scheduler
    let workflow_ids = vec![workflow_id.clone()];
    
    // Provision Actions (Compile and Create)
    if let Some(provisioner) = HttpActionProvisioner::new_from_config(&cfg) {
        // Collect all functions
        let mut functions = std::collections::HashSet::new();
        // Just BFS to find all functions again?
        // Or expose them from repo?
        // Let's just traverse start node and children
        let start_node = repo.start_node(&workflow_id).unwrap();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(start_node.clone());
        functions.insert(start_node.clone());
        
        while let Some(curr) = queue.pop_front() {
            for child in repo.children(&workflow_id, &curr) {
                if functions.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
        }
        
        info!("Ensuring actions for {} functions...", functions.len());
        for func in functions {
            // Use python function
            let code_path = std::path::Path::new("demos/function.py");
            if let Err(e) = provisioner.ensure_action(&func.0, code_path) {
                error!("Failed to ensure action {}: {}", func.0, e);
                // Depending on requirements, we might panic or continue
                panic!("Action provisioning failed");
            }
        }
    } else {
        info!("Skipping action provisioning (no API host/key)");
    }
    
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
        error!("OpenWhisk config missing. Mock client removed.");
        panic!("OpenWhisk config required");
    };
    let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut scheduler = WorkloadScheduler::new(
        workflow_ids,
        ArrivalProcess::NormalIntervalMs {
            mean_interval_ms,
            stddev_ms,
            min_interval_ms: 1,
        },
        cfg.workload.duration_seconds,
        in_flight.clone(),
        42,
        now_millis(),
    );
    
    // Set RealWorld params if available in repo
    if let (Some(&iat), Some(&cv)) = (repo.iat.get(&workflow_id), repo.cv.get(&workflow_id)) {
        info!("Using RealWorld arrival process: IAT={}, CV={}", iat, cv);
        scheduler.set_real_world_params(iat, cv, now_millis());
    } else {
        info!("Using Configured arrival process (RealWorld params missing)");
    }

    let mut engine = DagExecutionEngine::new(
        &repos,
        ow.as_ref(),
        notifier.as_ref(),
        0, // No concurrency limit
        in_flight.clone(),
        7,
        cfg.workload.duration_seconds,
        cfg.workload.drift_mode,
        now_millis(),
    );

    let start_ms = now_millis();
    // Simulate time loop
    let mut now_ms = start_ms;
    
    // Loop until duration is exceeded
    while !scheduler.is_done(now_ms) {
        if let Some(req) = scheduler.try_issue(now_ms) {
            info!("Issued new run: {} (workflow: {})", req.run_id.0, req.workflow_id.0);
            engine.enqueue_new_run(req, 64);
        }
        engine.tick();
        
        // Advance time - for simulation we might jump?
        // But for RealWorld logic we need precise seconds.
        // If we are just simulating locally without sleeps, we might run too fast?
        // But in real deployment, we rely on `now_millis()` being actual time?
        // Wait, `now_millis()` returns system time.
        // But here we are incrementing `now_ms` manually?
        // If we want real-time execution, we should sleep.
        // `scheduler.try_issue` expects `now_ms`.
        // If we use system time:
        now_ms = now_millis();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    info!("Finished issuing runs (duration exceeded), waiting for completions...");
    while engine.has_work() {
        engine.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    info!("All work completed. Duration: {}s", cfg.workload.duration_seconds);
}
