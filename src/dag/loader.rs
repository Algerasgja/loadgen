use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use rand::Rng;

use crate::dag::repository::DagRepository;
use crate::interactions::types::{FuncId, WorkflowId};

fn get_python_value(arg: &str, seed: f64) -> Result<f64, String> {
    // Path relative to binary or current working directory
    // Local execution: real-world-emulation/RealWorldAppEmulation.py
    let script_path = "real-world-emulation/RealWorldAppEmulation.py";
    if !Path::new(script_path).exists() {
        // Fallback for container path if local fails (optional, but requested to fix for local)
        let container_path = "real-world/real-world-emulation/RealWorldAppEmulation.py";
        if Path::new(container_path).exists() {
             let output = Command::new("python3")
                .arg(container_path)
                .arg(arg)
                .arg(seed.to_string())
                .output()
                .map_err(|e| format!("python execution failed: {}", e))?;
             if !output.status.success() {
                return Err(format!("python script error: {}", String::from_utf8_lossy(&output.stderr)));
             }
             let stdout = String::from_utf8_lossy(&output.stdout);
             return stdout.trim().parse::<f64>().map_err(|e| format!("parse python output failed: {} ({})", e, stdout));
        }
        return Err(format!("Python script not found at {} or {}", script_path, container_path));
    }

    let output = Command::new("python3")
        .arg(script_path)
        .arg(arg)
        .arg(seed.to_string())
        .output()
        .map_err(|e| format!("python execution failed: {}", e))?;
        
    if !output.status.success() {
        return Err(format!("python script error: {}", String::from_utf8_lossy(&output.stderr)));
    }
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse::<f64>().map_err(|e| format!("parse python output failed: {} ({})", e, stdout))
}

pub fn load_single_file(dir: &Path, filename: &str) -> Result<(DagRepository, WorkflowId), String> {
    let file = dir.join(filename);
    if !file.exists() {
        return Err(format!("DAG file not found: {:?}", file));
    }

    let content = fs::read_to_string(&file).map_err(|e| format!("read file {:?}: {}", file, e))?;
    let (wf, wf_start, wf_edges, wf_probs) =
        parse_dag_file(&content).map_err(|e| format!("{:?}: {}", file, e))?;
    
    let mut start: HashMap<WorkflowId, FuncId> = HashMap::new();
    let mut edges: HashMap<(WorkflowId, FuncId), Vec<FuncId>> = HashMap::new();
    
    start.insert(wf.clone(), wf_start.clone());
    for (from, tos) in wf_edges {
        edges.insert((wf.clone(), from), tos);
    }
    
    let mut repo = DagRepository::new(start, edges);
    for (from, dist) in wf_probs {
        repo.set_branch_probs(&wf, &from, dist)?;
    }

    // 1. IAT and CV from Python
    let seed: f64 = rand::thread_rng().gen();
    let iat = get_python_value("IAT", seed)?;
    let cv = get_python_value("CV", seed)?;
    repo.set_metadata(&wf, iat, cv);
    
    // 2. Generate Memory for all nodes
    let mut all_nodes = std::collections::HashSet::new();
    all_nodes.insert(wf_start.clone());
    // Traverse edges to find all nodes
    // repo.edges is private? No, we just constructed it. But we moved it into repo.
    // repo.edges is private field. We can use methods.
    // We can iterate over the parsed edges.
    // Re-parse edges or just iterate?
    // We have edges variable but we moved it? No, `edges` was passed to `new`.
    // Wait, `edges` ownership was moved.
    // We can re-traverse using repo methods if we expose keys?
    // Or just collect from `wf_edges`.
    // `wf_edges` is `Vec<(FuncId, Vec<FuncId>)>`.
    // Wait, `parse_dag_file` returns `wf_edges`. We iterated it to build `edges` map.
    // `wf_edges` is still available? No, `for (from, tos) in wf_edges` consumes it.
    // We should clone or collect first.
    // Or simpler: repo has `from_nodes`.
    // And `children`.
    
    // Let's iterate over `wf_edges` BEFORE consuming.
    // Ah, I already consumed it.
    // Let's modify the loop.
    
    // Actually, let's just use BFS from start node to find all reachable nodes.
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(wf_start.clone());
    let mut visited = std::collections::HashSet::new();
    visited.insert(wf_start.clone());
    
    while let Some(curr) = queue.pop_front() {
        // Assign memory
        let mem = rand::thread_rng().gen_range(10..=100);
        repo.set_memory(&wf, curr.clone(), mem);
        
        for child in repo.children(&wf, &curr) {
            if visited.insert(child.clone()) {
                queue.push_back(child);
            }
        }
    }
    
    // Auto-detect a "deep" branching node to mark as context-aware
        // Heuristic: Find the LAST branching node (children > 1) in BFS order.
        let start_node = wf_start.clone();
        let mut best_candidate: Option<FuncId> = None;
        
        // Simple BFS to find candidates
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(start_node.clone());
        let mut visited = std::collections::HashSet::new();
        visited.insert(start_node.clone());
        
        while let Some(curr) = queue.pop_front() {
            let children = repo.children(&wf, &curr);
            if children.len() > 1 && curr != start_node {
                // Found a candidate! Since we traverse BFS (level-order), updating best_candidate
                // will eventually result in the "deepest" or "last found" branching node.
                best_candidate = Some(curr.clone());
            }
            for child in children {
                if visited.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
        }
        
        if let Some(node) = best_candidate {
            repo.mark_context_aware(&wf, node);
        }
    
    Ok((repo, wf))
}


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
        
        start.insert(wf.clone(), wf_start.clone());
    for (from, tos) in wf_edges {
        edges.insert((wf.clone(), from), tos);
    }
        
        let mut repo = DagRepository::new(start, edges);
        for (from, dist) in wf_probs {
            repo.set_branch_probs(&wf, &from, dist)?;
        }
        
        // Auto-detect a "deep" branching node to mark as context-aware
        // Heuristic: Find the LAST branching node (children > 1) in BFS order.
        let start_node = wf_start.clone();
        let mut best_candidate: Option<FuncId> = None;
        
        // Simple BFS to find candidates
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(start_node.clone());
        let mut visited = std::collections::HashSet::new();
        visited.insert(start_node.clone());
        
        while let Some(curr) = queue.pop_front() {
            let children = repo.children(&wf, &curr);
            if children.len() > 1 && curr != start_node {
                // Found a candidate! Since we traverse BFS (level-order), updating best_candidate
                // will eventually result in the "deepest" or "last found" branching node.
                best_candidate = Some(curr.clone());
            }
            for child in children {
                if visited.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
        }
        
        if let Some(node) = best_candidate {
            repo.mark_context_aware(&wf, node);
        }
        
        results.push((repo, wf));
    }

    Ok(results)
}


//将文件中的内容转换为对应的结构体(WorkflowId, FuncId, Vec<(FuncId, Vec<FuncId>)>, Vec<(FuncId, Vec<(FuncId, f64)>)>)
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

