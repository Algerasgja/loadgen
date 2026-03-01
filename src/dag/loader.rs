use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::dag::repository::DagRepository;
use crate::interactions::types::{FuncId, WorkflowId};

pub fn load_from_dir(dir: &Path) -> Result<Vec<(DagRepository, WorkflowId)>, String> {
    let mut files: Vec<PathBuf> = Vec::new();
    let entries = fs::read_dir(dir).map_err(|e| format!("read_dir failed: {}", e))?;
    for ent in entries {
        let ent = ent.map_err(|e| format!("read_dir entry failed: {}", e))?;
        let p = ent.path();
        if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("dag") {
            files.push(p);
        }
    }
    files.sort();
    if files.is_empty() {
        return Err("no .dag files found".to_string());
    }

    let mut results = Vec::new();

    for file in files {
        let content =
            fs::read_to_string(&file).map_err(|e| format!("read file {:?}: {}", file, e))?;
        let (wf, wf_start, wf_edges, wf_probs) =
            parse_dag_file(&content).map_err(|e| format!("{:?}: {}", file, e))?;
        
        let mut start: HashMap<WorkflowId, FuncId> = HashMap::new();
        let mut edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>> = HashMap::new();
        
        start.insert(wf.clone(), wf_start);
        for (from, tos) in wf_edges {
            edges.insert((wf.clone(), from), tos);
        }
        
        let mut repo = DagRepository::new(start, edges);
        for (from, dist) in wf_probs {
            repo.set_branch_probs(&wf, &from, dist)?;
        }
        
        results.push((repo, wf));
    }

    Ok(results)
}

fn parse_dag_file(
    content: &str,
) -> Result<
    (
        WorkflowId,
        FuncId,
        Vec<(FuncId, Vec<FuncId>)>,
        Vec<(FuncId, Vec<(FuncId, f64)>)>,
    ),
    String,
> {
    let mut workflow: Option<WorkflowId> = None;
    let mut start: Option<FuncId> = None;
    let mut edges: Vec<(FuncId, Vec<FuncId>)> = Vec::new();
    let mut probs: Vec<(FuncId, Vec<(FuncId, f64)>)> = Vec::new();

    for (idx, raw) in content.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(rest) = line.strip_prefix("workflow ") {
            let id = rest.trim();
            if id.is_empty() {
                return Err(format!("line {}: missing workflow id", line_no));
            }
            workflow = Some(WorkflowId(id.to_string()));
            continue;
        }

        if let Some(rest) = line.strip_prefix("start ") {
            let f = rest.trim();
            if f.is_empty() {
                return Err(format!("line {}: missing start func", line_no));
            }
            start = Some(FuncId(f.to_string()));
            continue;
        }

        if let Some(rest) = line.strip_prefix("edge ") {
            let parts: Vec<&str> = rest.split("->").collect();
            if parts.len() != 2 {
                return Err(format!("line {}: bad edge syntax", line_no));
            }
            let from = parts[0].trim();
            let tos = parts[1]
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| FuncId(s.to_string()))
                .collect::<Vec<_>>();
            if from.is_empty() || tos.is_empty() {
                return Err(format!("line {}: bad edge syntax", line_no));
            }
            edges.push((FuncId(from.to_string()), tos));
            continue;
        }

        if let Some(rest) = line.strip_prefix("prob ") {
            let mut it = rest.split_whitespace();
            let from = it.next().unwrap_or("").trim();
            if from.is_empty() {
                return Err(format!("line {}: missing prob from", line_no));
            }
            let mut dist: Vec<(FuncId, f64)> = Vec::new();
            for kv in it {
                let mut kv_it = kv.splitn(2, '=');
                let to = kv_it.next().unwrap_or("").trim();
                let p = kv_it.next().unwrap_or("").trim();
                if to.is_empty() || p.is_empty() {
                    return Err(format!("line {}: bad prob entry", line_no));
                }
                let prob: f64 = p
                    .parse()
                    .map_err(|_| format!("line {}: bad prob number", line_no))?;
                dist.push((FuncId(to.to_string()), prob));
            }
            if dist.is_empty() {
                return Err(format!("line {}: empty prob dist", line_no));
            }
            probs.push((FuncId(from.to_string()), dist));
            continue;
        }

        return Err(format!("line {}: unknown directive", line_no));
    }

    let wf = workflow.ok_or_else(|| "missing workflow".to_string())?;
    let st = start.ok_or_else(|| "missing start".to_string())?;
    Ok((wf, st, edges, probs))
}

