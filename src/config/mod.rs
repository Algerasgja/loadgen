#[derive(Clone, Debug)]
pub struct Config {
    pub dag_dir: String,
    pub dag_file: String, // New: Specific DAG file to load
    pub workload: WorkloadConfig,
    pub openwhisk: OpenWhiskConfig,
    pub cap_warm: CapWarmConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dag_dir: "dags".to_string(),
            dag_file: "DAG-real.dag".to_string(), // Default DAG file
            workload: WorkloadConfig::default(),
            openwhisk: OpenWhiskConfig::default(),
            cap_warm: CapWarmConfig::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub duration_seconds: u64,
    pub drift_mode: DriftMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriftMode {
    Stable,
    Drift,
}

impl Default for DriftMode {
    fn default() -> Self {
        DriftMode::Stable
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            duration_seconds: 60,
            drift_mode: DriftMode::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OpenWhiskConfig {
    pub api_host: String,
    pub auth_key: String,
    pub poll_interval_ms: u64,
    pub poll_batch_size: usize,
}

impl Default for OpenWhiskConfig {
    fn default() -> Self {
        Self {
            api_host: "http://localhost:31001".to_string(), // Default OW API host
            auth_key: String::new(),
            poll_interval_ms: 50,
            poll_batch_size: 64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CapWarmConfig {
    pub url: String,
}

impl Default for CapWarmConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
        }
    }
}

impl Config {
    pub fn from_yaml_file(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("read config {:?} failed: {}", path, e))?;
        parse_yaml_minimal(&content)
    }
}

fn parse_yaml_minimal(input: &str) -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut section: Option<&str> = None;

    for (idx, raw) in input.lines().enumerate() {
        let line_no = idx + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let indent = raw.chars().take_while(|c| *c == ' ').count();
        let is_section_line = trimmed.ends_with(':') && !trimmed.contains(' ');
        if indent == 0 && is_section_line {
            let name = trimmed.trim_end_matches(':');
            section = Some(name);
            continue;
        }

        let (key, value) = split_key_value(trimmed)
            .ok_or_else(|| format!("line {}: expected key: value", line_no))?;

        if indent == 0 {
            section = None;
        }

        match section {
            None => match key {
                "dag_dir" => cfg.dag_dir = parse_string(value),
                "dag_file" => cfg.dag_file = parse_string(value),
                _ => return Err(format!("line {}: unknown key {}", line_no, key)),
            },
            Some("workload") => match key {
                "duration_seconds" => cfg.workload.duration_seconds = parse_u64(value, line_no)?,
                "drift_mode" => cfg.workload.drift_mode = parse_drift_mode(value, line_no)?,
                // Ignore old keys or error if strict
                "target_rps" | "total_runs" | "max_concurrency" | "stddev_ratio" => {
                    // Just ignore or log warning? For now ignore.
                },
                _ => return Err(format!("line {}: unknown workload key {}", line_no, key)),
            },
            Some("openwhisk") => match key {
                "api_host" => cfg.openwhisk.api_host = parse_string(value),
                "auth_key" => cfg.openwhisk.auth_key = parse_string(value),
                "poll_interval_ms" => cfg.openwhisk.poll_interval_ms = parse_u64(value, line_no)?,
                "poll_batch_size" => cfg.openwhisk.poll_batch_size = parse_usize(value, line_no)?,
                _ => return Err(format!("line {}: unknown openwhisk key {}", line_no, key)),
            },
            Some("cap_warm") => match key {
                "url" => cfg.cap_warm.url = parse_string(value),
                _ => return Err(format!("line {}: unknown cap_warm key {}", line_no, key)),
            },
            Some(other) => return Err(format!("line {}: unknown section {}", line_no, other)),
        }
    }

    Ok(cfg)
}

fn split_key_value(s: &str) -> Option<(&str, &str)> {
    let mut it = s.splitn(2, ':');
    let key = it.next()?.trim();
    let raw_val = it.next()?.trim();
    
    // Remove inline comments starting with '#'
    let val = raw_val.split('#').next().unwrap_or("").trim();
    
    if key.is_empty() {
        None
    } else {
        Some((key, val))
    }
}

fn parse_string(s: &str) -> String {
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

fn parse_u64(s: &str, line_no: usize) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|_| format!("line {}: bad u64", line_no))
}

fn parse_usize(s: &str, line_no: usize) -> Result<usize, String> {
    s.parse::<usize>()
        .map_err(|_| format!("line {}: bad usize", line_no))
}

fn parse_f64(s: &str, line_no: usize) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|_| format!("line {}: bad f64", line_no))
}

fn parse_drift_mode(s: &str, line_no: usize) -> Result<DriftMode, String> {
    // Handle potential quotes and trimming
    let s = s.trim();
    let val = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        &s[1..s.len() - 1]
    } else {
        s
    };

    match val.to_lowercase().as_str() {
        "stable" => Ok(DriftMode::Stable),
        "drift" => Ok(DriftMode::Drift),
        _ => Err(format!("line {}: bad drift_mode (expected 'stable' or 'drift'), got '{}'", line_no, val)),
    }
}
