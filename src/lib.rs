//! SigmaOS bigdata — distributed data processing primitives.
//!
//! Partitioning strategies, job tracking, and dataset statistics for
//! large-scale data processing. JSON-backed job store.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Errors produced by this component.
#[derive(Debug, thiserror::Error)]
pub enum BigDataError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("job not found: {0}")]
    NotFound(String),
    #[error("validation error: {0}")]
    Validation(String),
}

/// Partitioning strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionStrategy {
    /// Round-robin across N partitions.
    RoundRobin,
    /// Hash of key field mod N.
    Hash,
    /// Range partition by numeric field.
    Range,
}

/// Partition a set of records into N buckets.
pub fn partition(
    records: &[Value],
    n: usize,
    strategy: PartitionStrategy,
    key_field: Option<&str>,
) -> Result<Vec<Vec<Value>>, BigDataError> {
    if n == 0 {
        return Err(BigDataError::Validation("zero partitions".into()));
    }
    let mut buckets: Vec<Vec<Value>> = (0..n).map(|_| Vec::new()).collect();
    match strategy {
        PartitionStrategy::RoundRobin => {
            for (i, r) in records.iter().enumerate() {
                buckets[i % n].push(r.clone());
            }
        }
        PartitionStrategy::Hash => {
            let field = key_field
                .ok_or_else(|| BigDataError::Validation("hash partition needs key field".into()))?;
            for r in records {
                let key = r.get(field).map(|v| v.to_string()).unwrap_or_default();
                let hash = fnv_hash(key.as_bytes());
                buckets[(hash as usize) % n].push(r.clone());
            }
        }
        PartitionStrategy::Range => {
            let field = key_field.ok_or_else(|| {
                BigDataError::Validation("range partition needs key field".into())
            })?;
            // compute min/max
            let mut nums: Vec<f64> = records
                .iter()
                .filter_map(|r| r.get(field).and_then(|v| v.as_f64()))
                .collect();
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if nums.is_empty() {
                return Ok(buckets);
            }
            let min = nums.first().copied().unwrap();
            let max = nums.last().copied().unwrap();
            let width = if max > min {
                (max - min) / n as f64
            } else {
                1.0
            };
            for r in records {
                let v = r.get(field).and_then(|x| x.as_f64()).unwrap_or(min);
                let idx = (((v - min) / width) as usize).min(n - 1);
                buckets[idx].push(r.clone());
            }
        }
    }
    Ok(buckets)
}

/// FNV-1a hash.
fn fnv_hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Dataset statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatasetStats {
    pub records: usize,
    pub fields: Vec<String>,
    pub size_bytes: u64,
}

/// Compute basic stats for a record set.
pub fn stats(records: &[Value]) -> DatasetStats {
    let mut fields = std::collections::BTreeSet::new();
    for r in records {
        if let Some(obj) = r.as_object() {
            for k in obj.keys() {
                fields.insert(k.clone());
            }
        }
    }
    DatasetStats {
        records: records.len(),
        fields: fields.into_iter().collect(),
        size_bytes: serde_json::to_string(records)
            .map(|s| s.len() as u64)
            .unwrap_or(0),
    }
}

/// Job status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

/// A processing job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub input_path: PathBuf,
    pub output_path: PathBuf,
    pub partitions: usize,
    pub strategy: PartitionStrategy,
    #[serde(default = "default_key_field")]
    pub key_field: Option<String>,
    #[serde(default = "default_job_status")]
    pub status: JobStatus,
    #[serde(default)]
    pub records_processed: usize,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub created_unix: u64,
}

fn default_key_field() -> Option<String> {
    None
}

fn default_job_status() -> JobStatus {
    JobStatus::Pending
}

/// Job store.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobStore {
    #[serde(default)]
    pub jobs: BTreeMap<String, Job>,
}

impl JobStore {
    /// Create a job.
    pub fn create(
        &mut self,
        name: &str,
        input: PathBuf,
        output: PathBuf,
        partitions: usize,
        strategy: PartitionStrategy,
        key_field: Option<String>,
    ) -> Result<Job, BigDataError> {
        if !input.is_file() {
            return Err(BigDataError::Validation(format!(
                "input not a file: {}",
                input.display()
            )));
        }
        if partitions == 0 {
            return Err(BigDataError::Validation("partitions must be > 0".into()));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let job = Job {
            id: id.clone(),
            name: name.to_string(),
            input_path: input,
            output_path: output,
            partitions,
            strategy,
            key_field,
            status: JobStatus::Pending,
            records_processed: 0,
            error: None,
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        self.jobs.insert(id.clone(), job.clone());
        Ok(job)
    }

    /// Execute a job: read JSON array input, partition, write partitioned
    /// output files (output_path is a directory prefix -> _0.json etc.).
    pub fn run(&mut self, id: &str) -> Result<(), BigDataError> {
        let job = self
            .jobs
            .get_mut(id)
            .ok_or_else(|| BigDataError::NotFound(id.into()))?;
        job.status = JobStatus::Running;
        let input = job.input_path.clone();
        let output = job.output_path.clone();
        let n = job.partitions;
        let strategy = job.strategy;
        let key = job.key_field.clone();

        let text = std::fs::read_to_string(&input)?;
        let v: Value = serde_json::from_str(&text)?;
        let records = match v {
            Value::Array(a) => a,
            Value::Object(o) => match o.get("records") {
                Some(Value::Array(a)) => a.clone(),
                _ => return Err(BigDataError::Validation("no records array".into())),
            },
            _ => return Err(BigDataError::Validation("input must be array".into())),
        };
        let total = records.len();
        let buckets = partition(&records, n, strategy, key.as_deref())?;

        // output_path treated as directory; write part-N.json files
        std::fs::create_dir_all(&output)?;
        for (i, bucket) in buckets.iter().enumerate() {
            let f = output.join(format!("part-{i}.json"));
            std::fs::write(&f, serde_json::to_string_pretty(bucket)?)?;
        }

        job.status = JobStatus::Completed;
        job.records_processed = total;
        job.error = None;
        Ok(())
    }

    /// Get a job.
    pub fn get(&self, id: &str) -> Option<&Job> {
        self.jobs.get(id)
    }

    /// List jobs.
    pub fn list(&self) -> Vec<&Job> {
        self.jobs.values().collect()
    }

    /// Save to JSON.
    pub fn save(&self, path: &Path) -> Result<(), BigDataError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    /// Load from JSON.
    pub fn load(path: &Path) -> Result<Self, BigDataError> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| BigDataError::NotFound(path.display().to_string()))?;
        Ok(serde_json::from_str(&text)?)
    }
}

/// Default job store path.
pub fn default_store_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("org", "sigmaos", "sigmaos")
        .map(|p| p.data_dir().join("bigdata").join("jobs.json"))
}
