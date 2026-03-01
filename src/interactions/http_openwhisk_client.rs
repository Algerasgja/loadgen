use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::interactions::types::FuncId;
use crate::interactions::openwhisk_client::{
    ActivationHandle, ActivationRecord, CompletedActivation, InvocationContext, OpenWhiskClient,
};
use reqwest::blocking::Client;
use serde_json::Value;
use log::{debug, error, info, trace};

#[derive(Clone)]
pub struct HttpOpenWhiskClient {
    api_host: String,
    auth_key: String,
    namespace: String,
    client: Client,
    poll_interval_ms: u64,
    poll_batch_size: usize,
    state: Arc<Mutex<HttpState>>,
}

#[derive(Debug)]
struct HttpState {
    queue: VecDeque<String>,
    inflight: std::collections::HashMap<String, InvocationContext>,
    last_poll: Option<std::time::Instant>,
}

impl HttpOpenWhiskClient {
    pub fn new(
        api_host: String,
        auth_key: String,
        poll_interval_ms: u64,
        poll_batch_size: usize,
    ) -> Self {
        // Default namespace is usually "guest" or "_"
        // auth_key format: UUID:KEY
        let namespace = "guest".to_string(); 

        Self {
            api_host: api_host.trim_end_matches('/').to_string(),
            auth_key,
            namespace,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("build http client"),
            poll_interval_ms,
            poll_batch_size: poll_batch_size.max(1),
            state: Arc::new(Mutex::new(HttpState {
                queue: VecDeque::new(),
                inflight: std::collections::HashMap::new(),
                last_poll: None,
            })),
        }
    }

    fn get_auth_header(&self) -> String {
        use base64::Engine;
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(&self.auth_key)
        )
    }
}

impl OpenWhiskClient for HttpOpenWhiskClient {
    fn invoke_nonblocking(&self, ctx: InvocationContext) -> ActivationHandle {
        let url = format!(
            "{}/api/v1/namespaces/{}/actions/{}?blocking=false&result=false",
            self.api_host, self.namespace, ctx.curr_func.0
        );

        let prefix: String = ctx
            .prefix
            .iter()
            .map(|f| f.0.clone())
            .collect::<Vec<_>>()
            .join(",");

        let body = serde_json::json!({
            "workflow_id": ctx.workflow_id.0,
            "run_id": ctx.run_id.0,
            "request_id": ctx.request_id.0,
            "prefix": prefix,
            "timestamp": ctx.timestamp
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", self.get_auth_header())
            .json(&body)
            .send()
            .expect("invoke request failed");

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            error!("Invoke failed for {}: {} - {}", ctx.curr_func.0, status, text);
            panic!("invoke failed: {} - {}", status, text);
        }

        let json: Value = resp.json().expect("parse invoke response");
        let activation_id = json["activationId"]
            .as_str()
            .expect("missing activationId")
            .to_string();
        
        debug!("Invoked action {} -> {}", ctx.curr_func.0, activation_id);

        let mut st = self.state.lock().unwrap();
        st.queue.push_back(activation_id.clone());
        st.inflight.insert(activation_id.clone(), ctx);

        ActivationHandle { activation_id }
    }

    fn poll_completed(&self) -> Vec<CompletedActivation> {
        {
            let mut st = self.state.lock().unwrap();
            if let Some(last) = st.last_poll {
                if last.elapsed() < Duration::from_millis(self.poll_interval_ms) {
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

            let url = format!(
                "{}/api/v1/namespaces/{}/activations/{}",
                self.api_host, self.namespace, activation_id
            );

            let resp = match self
                .client
                .get(&url)
                .header("Authorization", self.get_auth_header())
                .send()
            {
                Ok(r) => r,
                Err(e) => {
                    error!("poll request failed for {}: {}", activation_id, e);
                    let mut st = self.state.lock().unwrap();
                    st.queue.push_back(activation_id);
                    continue;
                }
            };

            if resp.status().as_u16() == 404 {
                // Not ready yet
                trace!("Activation {} not ready (404)", activation_id);
                let mut st = self.state.lock().unwrap();
                st.queue.push_back(activation_id);
                continue;
            }

            if !resp.status().is_success() {
                error!("Poll failed for {}: {}", activation_id, resp.status());
                let mut st = self.state.lock().unwrap();
                st.queue.push_back(activation_id);
                continue;
            }

            let json: Value = match resp.json() {
                Ok(v) => v,
                Err(e) => {
                    error!("Parse poll response failed for {}: {}", activation_id, e);
                    continue;
                }
            };

            let end = json["end"].as_u64();
            if end.is_none() {
                // Still running? Or incomplete record. Re-queue.
                trace!("Activation {} still running (no end ts)", activation_id);
                let mut st = self.state.lock().unwrap();
                st.queue.push_back(activation_id);
                continue;
            }

            let start_ts = json["start"].as_u64().unwrap_or(0);
            let end_ts = end.unwrap();
            let duration = json["duration"].as_u64().unwrap_or(0);
            
            // "annotations": [{"key": "initTime", "value": 123}, ...]
            let mut cold_start_duration = None;
            if let Some(anns) = json["annotations"].as_array() {
                for ann in anns {
                    if ann["key"] == "initTime" {
                        cold_start_duration = ann["value"].as_u64();
                        break;
                    }
                }
            }

            let mut st = self.state.lock().unwrap();
            if let Some(ctx) = st.inflight.remove(&activation_id) {
                info!("Activation completed: {} ({}) duration={}ms", ctx.curr_func.0, activation_id, duration);
                out.push(CompletedActivation {
                    ctx,
                    record: ActivationRecord {
                        activation_id: activation_id.clone(),
                        func: FuncId(json["name"].as_str().unwrap_or("?").to_string()),
                        start_ts,
                        end_ts,
                        exec_duration: duration,
                        cold_start_duration,
                    },
                });
            }
        }

        out
    }
}
