//! `--env KEY=VALUE` and `--workers N`: the environment the spawned `logit` gets on top of this
//! process's own, recorded in the results file so a result names the arm it measured.
//!
//! `--workers` is the environment route to tokio's worker count: it sets `TOKIO_WORKER_THREADS`,
//! which tokio reads only when the runtime builder sets no worker count, so a worker count set in
//! the config wins over it. It's independent of `--pin-child`: the pin decides which cores the
//! child may run on, and tokio otherwise sizes its pool from that affinity mask.

use anyhow::bail;
use std::collections::BTreeMap;

/// The variable `--workers` sets.
pub const TOKIO_WORKER_THREADS: &str = "TOKIO_WORKER_THREADS";

/// What the child's environment adds to this process's own.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChildEnv {
    /// Every `--env` pair, as given.
    pub env: BTreeMap<String, String>,
    pub workers: Option<u32>,
}

impl ChildEnv {
    /// Fails on a key given twice, or on `TOKIO_WORKER_THREADS` set both by `--env` and
    /// `--workers`: either would leave the recorded arm ambiguous.
    pub fn new(pairs: Vec<(String, String)>, workers: Option<u32>) -> anyhow::Result<ChildEnv> {
        let mut env = BTreeMap::new();
        for (key, value) in pairs {
            if env.insert(key.clone(), value).is_some() {
                bail!("--env {key} is given more than once");
            }
        }
        if workers.is_some() && env.contains_key(TOKIO_WORKER_THREADS) {
            bail!("--workers sets {TOKIO_WORKER_THREADS}; drop one of --workers and --env");
        }
        Ok(ChildEnv { env, workers })
    }

    /// The variables to set on the child's `Command`.
    pub fn vars(&self) -> Vec<(String, String)> {
        let mut vars: Vec<(String, String)> =
            self.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        if let Some(workers) = self.workers {
            vars.push((TOKIO_WORKER_THREADS.to_string(), workers.to_string()));
        }
        vars
    }

    /// `, workers 2, env A=1 B=2` for a header line; empty when nothing is set.
    pub fn describe(&self) -> String {
        let mut out = String::new();
        if let Some(workers) = self.workers {
            out.push_str(&format!(", workers {workers}"));
        }
        if !self.env.is_empty() {
            let pairs: Vec<String> = self.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
            out.push_str(&format!(", env {}", pairs.join(" ")));
        }
        out
    }
}

/// `--env`'s parser: splits on the first `=`, so a value may itself contain `=`.
pub fn parse_env_pair(arg: &str) -> Result<(String, String), String> {
    let (key, value) =
        arg.split_once('=').ok_or_else(|| format!("`{arg}` is not KEY=VALUE (no `=`)"))?;
    if key.is_empty() {
        return Err(format!("`{arg}` has an empty KEY"));
    }
    Ok((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_may_contain_an_equals_sign() {
        assert_eq!(
            parse_env_pair("RUST_LOG=a=b,c=d").unwrap(),
            ("RUST_LOG".to_string(), "a=b,c=d".to_string())
        );
    }

    #[test]
    fn an_empty_value_is_allowed() {
        assert_eq!(parse_env_pair("A=").unwrap(), ("A".to_string(), String::new()));
    }

    #[test]
    fn a_pair_needs_an_equals_sign_and_a_key() {
        assert!(parse_env_pair("NOEQUALS").is_err());
        assert!(parse_env_pair("=value").is_err());
    }

    #[test]
    fn workers_sets_tokio_worker_threads_beside_the_env_pairs() {
        let env = ChildEnv::new(vec![("A".into(), "1".into())], Some(2)).unwrap();
        assert_eq!(
            env.vars(),
            vec![
                ("A".to_string(), "1".to_string()),
                (TOKIO_WORKER_THREADS.to_string(), "2".into())
            ]
        );
        assert_eq!(env.describe(), ", workers 2, env A=1");
    }

    #[test]
    fn a_duplicate_key_or_a_second_worker_count_is_refused() {
        assert!(
            ChildEnv::new(vec![("A".into(), "1".into()), ("A".into(), "2".into())], None).is_err()
        );
        assert!(ChildEnv::new(vec![(TOKIO_WORKER_THREADS.into(), "2".into())], Some(1)).is_err());
        assert!(ChildEnv::new(vec![(TOKIO_WORKER_THREADS.into(), "2".into())], None).is_ok());
    }
}
