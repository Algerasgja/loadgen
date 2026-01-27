use std::collections::VecDeque;

use crate::dag::repository::DagRepository;
use crate::engine::pet::build_pet_event;
use crate::interactions::capwarm_notifier::CapWarmNotifier;
use crate::interactions::openwhisk_client::{ActivationRecord, CompletedActivation, InvocationContext, OpenWhiskClient};
use crate::interactions::types::{ActivationCompleted, FuncId, RunId, RunStarted};
use crate::policy::branch_prob_table::generate_branch_prob_table;
use crate::run::manager::{RunManager, RunStepOutcome};
use crate::run::state::RunState;
use crate::util::now_millis;
use crate::workload::scheduler::NewRunRequest;

pub struct DagExecutionEngine<'a> {
    repo: &'a DagRepository,
    ow: &'a dyn OpenWhiskClient,
    notifier: &'a dyn CapWarmNotifier,
    pub run_manager: RunManager,
    max_concurrency: usize,
    seed: u64,
    rng: rand::rngs::StdRng,
    pending_starts: VecDeque<NewRunRequest>,
    pending_invokes: VecDeque<(RunId, InvocationContext)>,
}

impl<'a> DagExecutionEngine<'a> {
    pub fn new(
        repo: &'a DagRepository,
        ow: &'a dyn OpenWhiskClient,
        notifier: &'a dyn CapWarmNotifier,
        max_concurrency: usize,
        seed: u64,
    ) -> Self {
        Self {
            repo,
            ow,
            notifier,
            run_manager: RunManager::default(),
            max_concurrency: max_concurrency.max(1),
            seed,
            rng: rand::SeedableRng::seed_from_u64(seed),
            pending_starts: VecDeque::new(),
            pending_invokes: VecDeque::new(),
        }
    }

    pub fn enqueue_new_run(&mut self, req: NewRunRequest, max_hops: usize) {
        let branch_probs = generate_branch_prob_table(self.repo, &req.workflow_id, self.seed);
        let state = RunState::new(
            req.workflow_id.clone(),
            RunId(req.run_id.0.clone()),
            req.request_id.clone(),
            max_hops,
            branch_probs,
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

        while self.run_manager.inflight_count() < self.max_concurrency {
            if let Some((run_id, ctx)) = self.pending_invokes.pop_front() {
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
        let start = self
            .repo
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

        let ctx = InvocationContext {
            workflow_id,
            run_id: run_id.clone(),
            request_id,
            prefix: Vec::new(),
            curr_func: start,
            timestamp: now_millis(),
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

        self.notifier.send_activation_completed(ActivationCompleted {
            workflow_id: completion.ctx.workflow_id.clone(),
            run_id: completion.ctx.run_id.clone(),
            request_id: completion.ctx.request_id.clone(),
            prefix: completion.ctx.prefix.clone(),
            func: func.clone(),
            activation_id: activation_id.clone(),
            start_ts,
            end_ts,
            exec_duration,
            cold_start_duration,
            timestamp: now_millis(),
        });

        let children = self.repo.children(&completion.ctx.workflow_id, &func);
        let outcome = self
            .run_manager
            .on_activation_completed(&ActivationRecord {
                activation_id: activation_id.clone(),
                func: func.clone(),
                start_ts,
                end_ts,
                exec_duration,
                cold_start_duration,
            }, children.len());

        let (run_id, prefix_after) = match outcome {
            RunStepOutcome::Continue { run_id, prefix, .. } => (run_id, prefix),
            RunStepOutcome::Finished { .. } => return,
            RunStepOutcome::UnknownRun => return,
        };

        let next = choose_next_from_run_probs(
            self.run_manager.get(&run_id).unwrap(),
            &func,
            &children,
            &mut self.rng,
        )
            .unwrap_or_else(|| children[0].clone());

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

        let next_ctx = InvocationContext {
            workflow_id,
            run_id: run_id.clone(),
            request_id,
            prefix: prefix_after,
            curr_func: next,
            timestamp: now_millis(),
        };

        if self.run_manager.inflight_count() < self.max_concurrency {
            let handle = self.ow.invoke_nonblocking(next_ctx.clone());
            let _ = self.run_manager.on_invoked(
                &run_id,
                handle.activation_id,
                next_ctx.curr_func.clone(),
                next_ctx.timestamp,
            );
            if let Some(s) = self.run_manager.get_mut(&run_id) {
                s.set_current(next_ctx.curr_func);
            }
        } else {
            self.pending_invokes.push_back((run_id, next_ctx));
        }
    }
}

fn choose_next_from_run_probs(
    state: &RunState,
    from: &FuncId,
    children: &[FuncId],
    rng: &mut rand::rngs::StdRng,
) -> Option<FuncId> {
    if children.is_empty() {
        return None;
    }
    if children.len() == 1 {
        return Some(children[0].clone());
    }
    let dist = state.branch_probabilities.get(from)?;
    let r: f64 = rand::Rng::gen(rng);
    let mut acc = 0.0;
    for (to, p) in dist {
        acc += *p;
        if r < acc {
            if children.contains(to) {
                return Some(to.clone());
            }
        }
    }
    dist.last().map(|(to, _)| to.clone())
}
