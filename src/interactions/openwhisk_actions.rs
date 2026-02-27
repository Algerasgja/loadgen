use std::collections::{HashSet, VecDeque};
use std::path::Path;

use crate::dag::repository::DagRepository;
use crate::interactions::types::{FuncId, WorkflowId};

pub struct OpenWhiskActionProvisioner {
    wsk_path: String,
    kind: String,
}

impl OpenWhiskActionProvisioner {
    pub fn new(wsk_path: String, kind: String) -> Self {
        Self { wsk_path, kind }
    }

    pub fn ensure_actions_for_workflow(
        &self,
        repo: &DagRepository,
        workflow_id: &WorkflowId,
        demo_dir: &Path,
    ) -> Result<Vec<FuncId>, String> {
        let funcs = collect_reachable_funcs(repo, workflow_id)?;
        for f in funcs.iter() {
            if self.action_exists(&f.0) {
                continue;
            }
            let file_py = demo_dir.join(format!("{}.py", f.0));
            let file_js = demo_dir.join(format!("{}.js", f.0));
            let file = if file_py.exists() {
                file_py
            } else if file_js.exists() {
                file_js
            } else {
                return Err(format!("missing demo file for {} in {:?}", f.0, demo_dir));
            };
            self.create_action(&f.0, &file)?;
        }
        Ok(funcs)
    }

    fn action_exists(&self, name: &str) -> bool {
        let args = vec![
            "action".to_string(),
            "get".to_string(),
            name.to_string(),
        ];
        run_wsk_status(&self.wsk_path, &args).is_ok()
    }

    fn create_action(&self, name: &str, file: &Path) -> Result<(), String> {
        // println!("Creating action {} with kind {} from {:?}", name, self.kind, file);
        let args = vec![
            "action".to_string(),
            "create".to_string(),
            name.to_string(),
            file.to_string_lossy().to_string(),
            "--kind".to_string(),
            self.kind.clone(),
        ];
        run_wsk_status(&self.wsk_path, &args).map(|_| ())
    }
}

fn collect_reachable_funcs(repo: &DagRepository, workflow_id: &WorkflowId) -> Result<Vec<FuncId>, String> {
    let start = repo
        .start_node(workflow_id)
        .cloned()
        .ok_or_else(|| "missing start node".to_string())?;
    let mut visited: HashSet<FuncId> = HashSet::new();
    let mut q: VecDeque<FuncId> = VecDeque::new();
    visited.insert(start.clone());
    q.push_back(start);

    while let Some(curr) = q.pop_front() {
        for child in repo.children(workflow_id, &curr) {
            if visited.insert(child.clone()) {
                q.push_back(child);
            }
        }
    }

    let mut out: Vec<FuncId> = visited.into_iter().collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn run_wsk_status(wsk_path: &str, args: &[String]) -> Result<String, String> {
    let out = std::process::Command::new(wsk_path)
        .args(args)
        .output()
        .map_err(|e| format!("wsk exec failed: {}", e))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

