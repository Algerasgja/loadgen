use std::collections::{HashMap, VecDeque};

use crate::config::DriftMode;
use crate::dag::repository::DagRepository;
use crate::engine::pet::build_pet_event;
use crate::interactions::capwarm_notifier::CapWarmNotifier;
use crate::interactions::openwhisk_client::{ActivationRecord, CompletedActivation, InvocationContext, OpenWhiskClient};
use crate::interactions::types::{ActivationCompleted, FuncId, RunId, RunStarted, WorkflowId};
use crate::policy::branch_prob_table::generate_branch_prob_table;
use crate::policy::context_aware::{generate_context_aware_table, ContextProbTable, Period};
use crate::run::manager::{RunManager, RunStepOutcome};
use crate::run::state::RunState;
use crate::util::now_millis;
use crate::workload::scheduler::NewRunRequest;
use serde_json::json;

pub struct DagExecutionEngine<'a> {
    repos: HashMap<WorkflowId, &'a DagRepository>,
    branch_probs: HashMap<WorkflowId, HashMap<FuncId, Vec<(FuncId, f64)>>>,
    context_probs: HashMap<WorkflowId, ContextProbTable>,
    ow: &'a dyn OpenWhiskClient,
    notifier: &'a dyn CapWarmNotifier,
    pub run_manager: RunManager,
    max_concurrency: usize,
    in_flight_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    seed: u64,
    rng: rand::rngs::StdRng,
    pending_starts: VecDeque<NewRunRequest>,
    pending_invokes: VecDeque<(RunId, InvocationContext)>,
    
    // Context-aware & Drift state
    current_period: Period,
    drift_mode: DriftMode,
    duration_seconds: u64,
    start_time_ms: u64,
}

impl<'a> DagExecutionEngine<'a> {
    pub fn new(
        repos_vec: &'a Vec<(DagRepository, WorkflowId)>,
        ow: &'a dyn OpenWhiskClient,
        notifier: &'a dyn CapWarmNotifier,
        max_concurrency: usize,
        in_flight_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        seed: u64,
        duration_seconds: u64,
        drift_mode: DriftMode,
        start_time_ms: u64,
    ) -> Self {
        let mut repos = HashMap::new();
        let mut branch_probs = HashMap::new();
        let mut context_probs = HashMap::new();
        
        for (repo, wf_id) in repos_vec {
            repos.insert(wf_id.clone(), repo);
            // Pre-generate branch probabilities for each workflow
            let probs = generate_branch_prob_table(repo, wf_id, seed);
            branch_probs.insert(wf_id.clone(), probs);
            
            // Pre-generate context-aware probabilities
            let ctx_probs = generate_context_aware_table(repo, wf_id, seed);
            context_probs.insert(wf_id.clone(), ctx_probs);
        }

        Self {
            repos,
            branch_probs,
            context_probs,
            ow,
            notifier,
            run_manager: RunManager::default(),
            max_concurrency: max_concurrency.max(1),
            in_flight_counter,
            seed,
            rng: rand::SeedableRng::seed_from_u64(seed),
            pending_starts: VecDeque::new(),
            pending_invokes: VecDeque::new(),
            current_period: Period::Morning,
            drift_mode,
            duration_seconds,
            start_time_ms,
        }
    }

    pub fn enqueue_new_run(&mut self, req: NewRunRequest, max_hops: usize) {
        let state = RunState::new(
            req.workflow_id.clone(),
            RunId(req.run_id.0.clone()),
            req.request_id.clone(),
            max_hops,
            req.start_time,
        );
        self.run_manager.create_run(state);
        self.pending_starts.push_back(req);
    }

    pub fn has_work(&self) -> bool {
        !self.pending_starts.is_empty()
            || !self.pending_invokes.is_empty()
            || self.run_manager.inflight_count() > 0
    }

    pub fn tick(&mut self) {
        let completions = self.ow.poll_completed();
        for c in completions {
            self.handle_completion(c);
        }

        // Remove concurrency limit check
        // while self.run_manager.inflight_count() < self.max_concurrency {
        loop {
            if let Some((run_id, ctx)) = self.pending_invokes.pop_front() {
                log::info!("Invoking function {} for run {}", ctx.curr_func.0, run_id.0);
                let handle = self.ow.invoke_nonblocking(ctx.clone());
                let _ = self.run_manager.on_invoked(
                    &run_id,
                    handle.activation_id,
                    ctx.curr_func.clone(),
                    ctx.timestamp,
                );
                if let Some(s) = self.run_manager.get_mut(&run_id) {
                    s.set_current(ctx.curr_func);
                }
                continue;
            }

            if let Some(req) = self.pending_starts.pop_front() {
                log::info!("Starting run {} (workflow {})", req.run_id.0, req.workflow_id.0);
                self.start_run(req);
                continue;
            }

            break;
        }
    }

    fn start_run(&mut self, req: NewRunRequest) {
        let run_id = RunId(req.run_id.0.clone());
        let workflow_id = req.workflow_id.clone();
        let request_id = req.request_id.clone();
        
        let repo = self.repos.get(&workflow_id).expect("workflow repo not found");
        let start = repo
            .start_node(&workflow_id)
            .expect("missing start node")
            .clone();

        if let Some(s) = self.run_manager.get_mut(&run_id) {
            s.start_at(start.clone());
        }

        self.notifier.send_run_started(RunStarted {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
            request_id: request_id.clone(),
            timestamp: now_millis(),
        });

        // Get function memory
        let memory = repo.get_memory(&workflow_id, &start).unwrap_or(50);

        let ctx = InvocationContext {
            workflow_id,
            run_id: run_id.clone(),
            request_id,
            prefix: Vec::new(),
            curr_func: start,
            timestamp: now_millis(),
            params: Some(json!({
                "memory": memory,
                "warmup": false
            })),
        };
        let handle = self.ow.invoke_nonblocking(ctx.clone());
        let _ = self
            .run_manager
            .on_invoked(&run_id, handle.activation_id, ctx.curr_func.clone(), ctx.timestamp);
    }

    fn handle_completion(&mut self, completion: CompletedActivation) {
        let ActivationRecord {
            activation_id,
            func,
            start_ts,
            end_ts,
            exec_duration,
            cold_start_duration,
        } = completion.record;

        // Duplicate send_activation_completed removed here (handled by RunManager)

        let repo = self.repos.get(&completion.ctx.workflow_id).expect("workflow repo not found");
        let children = repo.children(&completion.ctx.workflow_id, &func);
        let outcome = self
            .run_manager
            .on_activation_completed(&ActivationRecord {
                activation_id: activation_id.clone(),
                func: func.clone(),
                start_ts,
                end_ts,
                exec_duration,
                cold_start_duration,
            }, children.len(), self.notifier, &self.in_flight_counter);

        let (run_id, prefix_after) = match outcome {
            RunStepOutcome::Continue { run_id, prefix, .. } => (run_id, prefix),
            RunStepOutcome::Finished { .. } => {
                // Check Drift based on time
                if self.drift_mode == DriftMode::Drift {
                    let now = now_millis();
                    let elapsed = now.saturating_sub(self.start_time_ms);
                    if elapsed >= (self.duration_seconds * 1000 / 2) {
                        if self.current_period != Period::Evening {
                            // Switch period
                            self.current_period = Period::Evening;
                            log::info!("Concept Drift: Switching to Evening period after {} ms (Halfway of {}s)", 
                                elapsed, self.duration_seconds);
                        }
                    }
                }
                return;
            }
            RunStepOutcome::UnknownRun => return,
        };

        // Context-aware Branch Selection
        let wf_ctx_probs = self.context_probs.get(&completion.ctx.workflow_id).expect("workflow ctx probs not found");
        let wf_branch_probs = self.branch_probs.get(&completion.ctx.workflow_id).expect("workflow branch probs not found");
        
        // Try context-aware first
        let ctx_key = (prefix_after.clone(), self.current_period, func.clone());
        let next = if let Some(dist) = wf_ctx_probs.get(&ctx_key) {
             use rand::Rng;
            let r: f64 = self.rng.gen();
            let mut acc = 0.0;
            let mut selected = None;
            for (candidate, p) in dist {
                acc += p;
                if r <= acc {
                    selected = Some(candidate.clone());
                    break;
                }
            }
            selected.unwrap_or_else(|| children[0].clone())
        } else if let Some(dist) = wf_branch_probs.get(&func) {
            // Use static branch probabilities
            use rand::Rng;
            let r: f64 = self.rng.gen();
            let mut acc = 0.0;
            let mut selected = None;
            for (candidate, p) in dist {
                acc += p;
                if r <= acc {
                    selected = Some(candidate.clone());
                    break;
                }
            }
            selected.unwrap_or_else(|| children[0].clone())
        } else {
             // Fallback or deterministic choice
             children.first().cloned().unwrap_or_else(|| func.clone())
        };

        let workflow_id = completion.ctx.workflow_id.clone();
        let request_id = completion.ctx.request_id.clone();

        self.notifier.send_pet(build_pet_event(
            workflow_id.clone(),
            run_id.clone(),
            request_id.clone(),
            prefix_after.clone(),
            func,
            next.clone(),
            now_millis(),
        ));

        let next_mem = repo.get_memory(&workflow_id, &next).unwrap_or(50);

        let next_ctx = InvocationContext {
            workflow_id,
            run_id,
            request_id,
            prefix: prefix_after.clone(),
            curr_func: next.clone(),
            timestamp: now_millis(),
            params: Some(json!({
                "memory": next_mem,
                "warmup": false
            })),
        };

        self.pending_invokes.push_back((next_ctx.run_id.clone(), next_ctx));
    }
}
