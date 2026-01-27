use crate::interactions::types::{RequestId, RunId, WorkflowId};
use crate::workload::arrival::ArrivalProcess;

#[derive(Clone, Debug)]
pub struct NewRunRequest {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub start_time: u64,
}

#[derive(Clone, Debug)]
pub struct WorkloadScheduler {
    pub workflow_id: WorkflowId,
    pub arrival: ArrivalProcess,
    pub max_concurrency: usize,
    pub total_runs: usize,
    pub in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub seed: u64,
    issued_runs: usize,
    next_fire_ms: u64,
    id_counter: u64,
    rng: rand::rngs::StdRng,
}

impl WorkloadScheduler {
    pub fn new(
        workflow_id: WorkflowId,
        arrival: ArrivalProcess,
        max_concurrency: usize,
        total_runs: usize,
        in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        seed: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            workflow_id,
            arrival,
            max_concurrency: max_concurrency.max(1),
            total_runs,
            in_flight,
            seed,
            issued_runs: 0,
            next_fire_ms: now_ms,
            id_counter: 0,
            rng: rand::SeedableRng::seed_from_u64(seed),
        }
    }

    pub fn is_done(&self) -> bool {
        self.issued_runs >= self.total_runs
    }

    pub fn try_issue(&mut self, now_ms: u64) -> Option<NewRunRequest> {
        if self.is_done() {
            return None;
        }

        if now_ms < self.next_fire_ms {
            return None;
        }

        let inflight = self
            .in_flight
            .load(std::sync::atomic::Ordering::Relaxed);
        if inflight >= self.max_concurrency {
            return None;
        }

        let req = NewRunRequest {
            workflow_id: self.workflow_id.clone(),
            run_id: RunId(format!("run-{}", self.id_counter)),
            request_id: RequestId(format!("req-{}", self.id_counter)),
            start_time: now_ms,
        };

        self.id_counter += 1;
        self.issued_runs += 1;
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let interval_ms = self.sample_interval_ms();
        self.next_fire_ms = self.next_fire_ms.saturating_add(interval_ms);

        Some(req)
    }

    pub fn wait_time_ms(&self, now_ms: u64) -> u64 {
        self.next_fire_ms.saturating_sub(now_ms)
    }

    fn sample_interval_ms(&mut self) -> u64 {
        match self.arrival {
            ArrivalProcess::FixedIntervalMs(ms) => ms.max(1),
            ArrivalProcess::NormalIntervalMs {
                mean_interval_ms,
                stddev_ms,
                min_interval_ms,
            } => {
                let stddev_ms = if stddev_ms.is_finite() && stddev_ms > 0.0 {
                    stddev_ms
                } else {
                    1.0
                };
                let mean_interval_ms = if mean_interval_ms.is_finite() && mean_interval_ms > 0.0 {
                    mean_interval_ms
                } else {
                    1.0
                };
                let min_interval_ms = min_interval_ms.max(1);

                let normal =
                    rand_distr::Normal::new(mean_interval_ms, stddev_ms).unwrap_or_else(|_| {
                        rand_distr::Normal::new(1.0, 1.0).expect("normal fallback")
                    });
                let sample: f64 = rand::Rng::sample(&mut self.rng, normal);
                let ms = sample.round() as i64;
                (ms.max(min_interval_ms as i64)) as u64
            }
        }
    }
}
