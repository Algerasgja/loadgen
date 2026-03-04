use std::path::Path;
use log::debug;
use reqwest::blocking::Client;
use base64::Engine;

use crate::interactions::types::FuncId;

#[derive(Clone)]
pub struct HttpActionProvisioner {
    api_host: String,
    auth_key: String,
    client: Client,
    namespace: String,
}

use crate::config::Config;

impl HttpActionProvisioner {
    pub fn new_from_config(cfg: &Config) -> Option<Self> {
        if !cfg.openwhisk.api_host.is_empty() && !cfg.openwhisk.auth_key.is_empty() {
            Some(Self::new(cfg.openwhisk.api_host.clone(), cfg.openwhisk.auth_key.clone()))
        } else {
            None
        }
    }

    pub fn new(api_host: String, auth_key: String) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .danger_accept_invalid_certs(true)
            .build()
            .expect("build http client");
        
        // Assume default namespace or parse from key? Usually "_" or "guest"
        // Key format: uuid:key
        // Default namespace is usually "_"
        let namespace = "_".to_string();

        Self {
            api_host,
            auth_key,
            client,
            namespace,
        }
    }

    fn get_auth_header(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        let encoded = general_purpose::STANDARD.encode(&self.auth_key);
        format!("Basic {}", encoded)
    }

    pub fn ensure_action(&self, name: &str, _code_path: &Path) -> Result<(), String> {
        // 1. Check if action exists
        let url = format!("{}/api/v1/namespaces/{}/actions/{}", self.api_host, self.namespace, name);
        let resp = self.client.get(&url)
            .header("Authorization", self.get_auth_header())
            .send()
            .map_err(|e| format!("check action failed: {}", e))?;

        if resp.status().is_success() {
            debug!("Action {} exists", name);
            return Ok(());
        }

        // Action missing
        Err(format!("Action {} not found in OpenWhisk", name))
    }
}
