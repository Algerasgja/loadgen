#[derive(Clone, Debug)]
pub struct Config {
    pub dag_dir: String,
    pub workload: WorkloadConfig,
    pub openwhisk: OpenWhiskConfig,
    pub cap_warm: CapWarmConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dag_dir: "dags".to_string(),
            workload: WorkloadConfig::default(),
            openwhisk: OpenWhiskConfig::default(),
            cap_warm: CapWarmConfig::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub target_rps: f64,
    pub total_runs: usize,
    pub max_concurrency: usize,
    pub stddev_ratio: f64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            target_rps: 5.0,
            total_runs: 3,
            max_concurrency: 2,
            stddev_ratio: 0.1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OpenWhiskConfig {
    pub wsk_path: String,
    pub poll_interval_ms: u64,
    pub poll_batch_size: usize,
    pub demo_dir: String,
    pub action_kind: String,
    pub auto_create_actions: usize,
}

impl Default for OpenWhiskConfig {
    fn default() -> Self {
        Self {
            wsk_path: String::new(),
            poll_interval_ms: 50,
            poll_batch_size: 64,
            demo_dir: "python_demo".to_string(),
            action_kind: "python:3".to_string(),
            auto_create_actions: 1,
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
                _ => return Err(format!("line {}: unknown key {}", line_no, key)),
            },
            Some("workload") => match key {
                "target_rps" => cfg.workload.target_rps = parse_f64(value, line_no)?,
                "total_runs" => cfg.workload.total_runs = parse_usize(value, line_no)?,
                "max_concurrency" => cfg.workload.max_concurrency = parse_usize(value, line_no)?,
                "stddev_ratio" => cfg.workload.stddev_ratio = parse_f64(value, line_no)?,
                _ => return Err(format!("line {}: unknown workload key {}", line_no, key)),
            },
            Some("openwhisk") => match key {
                "wsk_path" => cfg.openwhisk.wsk_path = parse_string(value),
                "poll_interval_ms" => cfg.openwhisk.poll_interval_ms = parse_u64(value, line_no)?,
                "poll_batch_size" => cfg.openwhisk.poll_batch_size = parse_usize(value, line_no)?,
                "demo_dir" => cfg.openwhisk.demo_dir = parse_string(value),
                "action_kind" => cfg.openwhisk.action_kind = parse_string(value),
                "auto_create_actions" => cfg.openwhisk.auto_create_actions = parse_usize(value, line_no)?,
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
    let val = it.next()?.trim();
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
