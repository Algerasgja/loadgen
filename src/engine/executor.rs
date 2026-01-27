use crate::dag::repository::DagRepository;
use crate::engine::pet::build_pet_event;
use crate::interactions::capwarm_notifier::CapWarmNotifier;
use crate::interactions::openwhisk_client::OpenWhiskClient;
use crate::interactions::types::{ActivationCompleted, RunStarted};
use crate::run::manager::{RunManager, RunStepOutcome};
use crate::run::state::RunState;
use crate::util::now_millis;

pub fn run_once(
    repo: &DagRepository,
    ow: &dyn OpenWhiskClient,
    notifier: &dyn CapWarmNotifier,
    state: &mut RunState,
) {
    let mut run_manager = RunManager::default();
    let run_id = state.run_id.clone();
    run_manager.create_run(state.clone());

    notifier.send_run_started(RunStarted {
        workflow_id: state.workflow_id.clone(),
        run_id: state.run_id.clone(),
        request_id: state.request_id.clone(),
        timestamp: now_millis(),
    });

    let start = repo
        .start_node(&state.workflow_id)
        .expect("missing start node")
        .clone();
    run_manager
        .get_mut(&run_id)
        .expect("run exists")
        .start_at(start);

    loop {
        let (workflow_id, request_id, prefix_snapshot, curr) = {
            let s = run_manager.get(&run_id).expect("run exists");
            (
                s.workflow_id.clone(),
                s.request_id.clone(),
                s.prefix.clone(),
                s.current_func.clone().expect("missing current func"),
            )
        };
        let activation = ow.invoke_blocking(&curr);

        notifier.send_activation_completed(ActivationCompleted {
            workflow_id,
            run_id: run_id.clone(),
            request_id,
            prefix: prefix_snapshot,
            func: curr.clone(),
            activation_id: activation.activation_id.clone(),
            start_ts: activation.start_ts,
            end_ts: activation.end_ts,
            exec_duration: activation.exec_duration,
            cold_start_duration: activation.cold_start_duration,
            timestamp: now_millis(),
        });

        let children = repo.children(&run_manager.get(&run_id).unwrap().workflow_id, &curr);
        let outcome = run_manager.on_activation_completed(&run_id, &activation, children.len());
        match outcome {
            RunStepOutcome::Finished { .. } => {
                break;
            }
            RunStepOutcome::Continue { .. } => {}
            RunStepOutcome::UnknownRun => {
                break;
            }
        }

        let next = {
            let s = run_manager.get(&run_id).expect("run exists");
            choose_next_from_run_probs(s, &curr, &children)
        }
            .unwrap_or_else(|| children[0].clone());

        let pet_prefix = run_manager.get(&run_id).unwrap().prefix.clone();
        notifier.send_pet(build_pet_event(
            run_manager.get(&run_id).unwrap().workflow_id.clone(),
            run_id.clone(),
            run_manager.get(&run_id).unwrap().request_id.clone(),
            pet_prefix,
            curr,
            next.clone(),
            now_millis(),
        ));

        run_manager
            .get_mut(&run_id)
            .expect("run exists")
            .set_current(next);
    }

    if let Some(final_state) = run_manager.get(&run_id).cloned() {
        *state = final_state;
    }
}

fn choose_next_from_run_probs(
    state: &RunState,
    from: &crate::interactions::types::FuncId,
    children: &[crate::interactions::types::FuncId],
) -> Option<crate::interactions::types::FuncId> {
    if children.is_empty() {
        return None;
    }
    if children.len() == 1 {
        return Some(children[0].clone());
    }
    let dist = state.branch_probabilities.get(from)?;
    let r: f64 = rand::random();
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
