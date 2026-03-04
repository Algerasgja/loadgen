use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use rand::Rng;
use rand_distr::Distribution;

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
struct RealWorldState {
    iat: f64,
    cv: f64,
    accumulated_fraction: f64,
    last_second: u64,
}

#[derive(Clone, Debug)]
pub struct WorkloadScheduler {
    pub workflow_ids: Vec<WorkflowId>,
    pub arrival: ArrivalProcess,
    pub duration_seconds: u64,
    pub in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub seed: u64,
    issued_runs: usize,
    next_fire_ms: u64,
    id_counter: u64,
    rng: rand::rngs::StdRng,
    
    // RealWorld state
    real_world_state: Option<RealWorldState>,
    pending_buffer: VecDeque<NewRunRequest>,
    
    start_time_ms: u64,
}

impl WorkloadScheduler {
    pub fn new(
        workflow_ids: Vec<WorkflowId>,
        arrival: ArrivalProcess,
        duration_seconds: u64,
        in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        seed: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            workflow_ids,
            arrival,
            duration_seconds,
            in_flight,
            seed,
            issued_runs: 0,
            next_fire_ms: now_ms,
            id_counter: 0,
            rng: rand::SeedableRng::seed_from_u64(seed),
            real_world_state: None,
            pending_buffer: VecDeque::new(),
            start_time_ms: now_ms,
        }
    }
    
    pub fn set_real_world_params(&mut self, iat: f64, cv: f64, now_ms: u64) {
        self.real_world_state = Some(RealWorldState {
            iat,
            cv,
            accumulated_fraction: 0.0,
            last_second: now_ms / 1000,
        });
    }

    pub fn is_done(&self, now_ms: u64) -> bool {
        now_ms >= self.start_time_ms + (self.duration_seconds * 1000)
    }

    //尝试按照频率生成模式(arrival:固定模式或者正态分布模式)生成新的run请求
    //实现了并发数控制
    pub fn try_issue(&mut self, now_ms: u64) -> Option<NewRunRequest> {
        if self.is_done(now_ms) {
            return None;
        }
        
        let _inflight = self.in_flight.load(Ordering::Relaxed);
        // User requested to remove concurrency limit
        // if inflight >= self.max_concurrency {
        //     return None;
        // }

        // RealWorld Logic
        if let Some(ref mut state) = self.real_world_state {
            // 1. Drain pending buffer
            if let Some(req) = self.pending_buffer.pop_front() {
                // Update start time to now? Or keep original scheduled time?
                // Usually we want to issue now.
                let mut req = req;
                req.start_time = now_ms;
                
                self.issued_runs += 1;
                self.in_flight.fetch_add(1, Ordering::Relaxed);
                return Some(req);
            }
            
            // 2. Check if new second started
            let current_sec = now_ms / 1000;
            if current_sec > state.last_second {
                state.last_second = current_sec;
                
                // Generate_Request_Count Logic
                let avg_freq = 1.0 / state.iat;
                let std_dev = avg_freq * state.cv;
                let mut stochastic_freq = 0.0;
                
                // Rejection sampling
                let normal = rand_distr::Normal::new(avg_freq, std_dev).unwrap_or_else(|_| {
                    rand_distr::Normal::new(avg_freq, 1.0).expect("normal fallback")
                });
                
                for _ in 0..100 {
                    let sample: f64 = self.rng.sample(normal);
                    if sample > 0.0 {
                        stochastic_freq = sample;
                        break;
                    }
                }
                if stochastic_freq <= 0.0 {
                    log::warn!("Failed to sample positive freq, using avg");
                    stochastic_freq = avg_freq;
                }
                
                let total_freq = stochastic_freq + state.accumulated_fraction;
                let request_count = total_freq.floor() as usize;
                state.accumulated_fraction = total_freq - request_count as f64;
                
                log::info!("LoadGen: Generating {} requests for second {} (Target Freq: {:.2})", 
                    request_count, current_sec, stochastic_freq);
                
                // Generate requests
                for _ in 0..request_count {
                    if self.is_done(now_ms) {
                        break;
                    }
                    
                    use rand::seq::SliceRandom;
                    let wf_id = self.workflow_ids.choose(&mut self.rng).expect("no workflows").clone();
                    
                    let req = NewRunRequest {
                        workflow_id: wf_id,
                        run_id: RunId(format!("run-{}", self.id_counter)),
                        request_id: RequestId(format!("req-{}", self.id_counter)),
                        start_time: now_ms, // Scheduled for this second
                    };
                    self.id_counter += 1;
                    self.pending_buffer.push_back(req);
                }
                
                // Try to issue immediately
                return self.try_issue(now_ms);
            }
            
            return None;
        }

        // Original Logic
        if now_ms < self.next_fire_ms {
            return None;
        }

        // Remove concurrency check here too
        // let inflight = self
        //     .in_flight
        //     .load(std::sync::atomic::Ordering::Relaxed);
        // if inflight >= self.max_concurrency {
        //     return None;
        // }

        use rand::seq::SliceRandom;
        let wf_id = self.workflow_ids.choose(&mut self.rng).expect("no workflows available").clone();

        let req = NewRunRequest {
            workflow_id: wf_id,
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

    //根据arrival模式采样新的run请求的时间间隔
    //如果是固定模式，直接返回固定值
    //如果是正态分布模式，根据正态分布采样值，确保不小于min_interval_ms
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
