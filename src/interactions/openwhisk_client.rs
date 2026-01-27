use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::interactions::types::{Duration, FuncId, RequestId, RunId, Timestamp, WorkflowId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationRecord {
    pub activation_id: String,
    pub func: FuncId,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    pub exec_duration: Duration,
    pub cold_start_duration: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationContext {
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub request_id: RequestId,
    pub prefix: Vec<FuncId>,
    pub curr_func: FuncId,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationHandle {
    pub activation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedActivation {
    pub ctx: InvocationContext,
    pub record: ActivationRecord,
}

pub trait OpenWhiskClient: Send + Sync {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle;
    fn poll_completed(&self) -> Vec<CompletedActivation>;
}

#[derive(Clone)]
pub struct OpenWhiskCliClient {
    wsk_path: String,
    poll_interval_ms: u64,
    poll_batch_size: usize,
    state: Arc<Mutex<CliState>>,
}

#[derive(Clone, Debug)]
struct CliState {
    queue: VecDeque<String>,
    inflight: std::collections::HashMap<String, InvocationContext>,
    last_poll: Option<std::time::Instant>,
}

impl OpenWhiskCliClient {
    pub fn new(wsk_path: String, poll_interval_ms: u64, poll_batch_size: usize) -> Self {
        Self {
            wsk_path,
            poll_interval_ms,
            poll_batch_size: poll_batch_size.max(1),
            state: Arc::new(Mutex::new(CliState {
                queue: VecDeque::new(),
                inflight: std::collections::HashMap::new(),
                last_poll: None,
            })),
        }
    }

    fn run_wsk(&self, args: &[String]) -> String {
        let out = std::process::Command::new(&self.wsk_path)
            .args(args)
            .output()
            .expect("wsk command failed");
        if !out.status.success() {
            panic!(
                "wsk exit {}: {}",
                out.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn parse_activation_id_from_invoke(output: &str) -> Option<String> {
        let needle = "with id ";
        let idx = output.find(needle)?;
        let rest = &output[(idx + needle.len())..];
        let id = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        if id.is_empty() {
            None
        } else {
            Some(id)
        }
    }

    fn parse_number_field(json: &str, key: &str) -> Option<u64> {
        let pat = format!("\"{}\":", key);
        let idx = json.find(&pat)?;
        let rest = &json[(idx + pat.len())..];
        let num = rest
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>();
        num.parse().ok()
    }
}

impl OpenWhiskClient for OpenWhiskCliClient {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle {
        let mut args: Vec<String> = Vec::new();
        args.push("action".to_string());
        args.push("invoke".to_string());
        args.push(ctx.curr_func.0.clone());
        args.push("--blocking".to_string());
        args.push("false".to_string());
        args.push("--result".to_string());
        args.push("false".to_string());

        let prefix: String = ctx
            .prefix
            .iter()
            .map(|f| f.0.clone())
            .collect::<Vec<_>>()
            .join(",");
        args.push("--param".to_string());
        args.push("workflow_id".to_string());
        args.push(ctx.workflow_id.0.clone());
        args.push("--param".to_string());
        args.push("run_id".to_string());
        args.push(ctx.run_id.0.clone());
        args.push("--param".to_string());
        args.push("request_id".to_string());
        args.push(ctx.request_id.0.clone());
        args.push("--param".to_string());
        args.push("prefix".to_string());
        args.push(prefix);
        args.push("--param".to_string());
        args.push("timestamp".to_string());
        args.push(ctx.timestamp.to_string());

        let out = self.run_wsk(&args);
        let activation_id = Self::parse_activation_id_from_invoke(&out).expect("parse activation id");

        let mut st = self.state.lock().unwrap();
        st.queue.push_back(activation_id.clone());
        st.inflight.insert(activation_id.clone(), ctx);

        ActivationHandle { activation_id }
    }

    fn poll_completed(&self) -> Vec<CompletedActivation> {
        {
            let mut st = self.state.lock().unwrap();
            if let Some(last) = st.last_poll {
                if last.elapsed() < std::time::Duration::from_millis(self.poll_interval_ms) {
                    return Vec::new();
                }
            }
            st.last_poll = Some(std::time::Instant::now());
        }

        let mut out: Vec<CompletedActivation> = Vec::new();

        for _ in 0..self.poll_batch_size {
            let activation_id = {
                let mut st = self.state.lock().unwrap();
                st.queue.pop_front()
            };
            let Some(activation_id) = activation_id else { break };

            let mut args: Vec<String> = Vec::new();
            args.push("activation".to_string());
            args.push("get".to_string());
            args.push(activation_id.clone());
            args.push("--json".to_string());

            let json = self.run_wsk(&args);
            let end_ts = match Self::parse_number_field(&json, "end") {
                Some(v) => v,
                None => {
                    let mut st = self.state.lock().unwrap();
                    st.queue.push_back(activation_id);
                    continue;
                }
            };
            let start_ts = Self::parse_number_field(&json, "start").unwrap_or(end_ts);
            let exec_duration = Self::parse_number_field(&json, "duration").unwrap_or_else(|| end_ts.saturating_sub(start_ts));

            let ctx = {
                let st = self.state.lock().unwrap();
                st.inflight.get(&activation_id).cloned()
            };
            let Some(ctx) = ctx else { continue };

            {
                let mut st = self.state.lock().unwrap();
                st.inflight.remove(&activation_id);
            }

            out.push(CompletedActivation {
                ctx: ctx.clone(),
                record: ActivationRecord {
                    activation_id,
                    func: ctx.curr_func.clone(),
                    start_ts,
                    end_ts,
                    exec_duration,
                    cold_start_duration: None,
                },
            });
        }

        out
    }
}

#[derive(Clone, Default)]
pub struct MockOpenWhiskClient {
    next_records: Arc<Mutex<VecDeque<ActivationRecord>>>,
    completed: Arc<Mutex<VecDeque<CompletedActivation>>>,
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl MockOpenWhiskClient {
    pub fn new(records: Vec<ActivationRecord>) -> Self {
        Self {
            next_records: Arc::new(Mutex::new(records.into())),
            completed: Arc::new(Mutex::new(VecDeque::new())),
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl OpenWhiskClient for MockOpenWhiskClient {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle {
        let mut rec = self
            .next_records
            .lock()
            .unwrap()
            .pop_front()
            .expect("mock activation queue empty");
        rec.func = ctx.curr_func.clone();
        if rec.activation_id.is_empty() {
            let n = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rec.activation_id = format!("act-{}-{}", ctx.run_id.0, n);
        }
        let activation_id = rec.activation_id.clone();

        self.completed
            .lock()
            .unwrap()
            .push_back(CompletedActivation { ctx, record: rec });

        ActivationHandle { activation_id }
    }

    fn poll_completed(&self) -> Vec<CompletedActivation> {
        let mut q = self.completed.lock().unwrap();
        let mut out = Vec::with_capacity(q.len());
        while let Some(item) = q.pop_front() {
            out.push(item);
        }
        out
    }
}
