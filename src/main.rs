//! CLI for sigmaos-bigdata.

use clap::{Parser, Subcommand, ValueEnum};
use sigmaos_bigdata::{default_store_path, partition, stats, BigDataError, JobStore, PartitionStrategy};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sigmaos-bigdata", about = "SigmaOS big data processing")]
struct Cli {
    #[arg(long, global = true)]
    store: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, ValueEnum)]
enum StratArg {
    RoundRobin,
    Hash,
    Range,
}

impl From<StratArg> for PartitionStrategy {
    fn from(s: StratArg) -> Self {
        match s {
            StratArg::RoundRobin => PartitionStrategy::RoundRobin,
            StratArg::Hash => PartitionStrategy::Hash,
            StratArg::Range => PartitionStrategy::Range,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Show stats for a JSON records file.
    Stats { file: PathBuf },
    /// Partition a JSON records file into N buckets (dry run, prints sizes).
    Partition {
        file: PathBuf,
        #[arg(long, default_value_t = 4)]
        n: usize,
        #[arg(long, value_enum, default_value_t = StratArg::RoundRobin)]
        strategy: StratArg,
        #[arg(long)]
        key: Option<String>,
    },
    /// Create a processing job.
    Create {
        name: String,
        input: PathBuf,
        output: PathBuf,
        #[arg(long, default_value_t = 4)]
        partitions: usize,
        #[arg(long, value_enum, default_value_t = StratArg::RoundRobin)]
        strategy: StratArg,
        #[arg(long)]
        key: Option<String>,
    },
    /// Run a job.
    Run { id: String },
    /// List jobs.
    List,
    /// Print store path.
    Path,
}

fn store_path(s: &Option<PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    s.clone()
        .or_else(default_store_path)
        .ok_or_else(|| "cannot determine store path".into())
}

fn load(s: &Option<PathBuf>) -> Result<JobStore, Box<dyn std::error::Error>> {
    let p = store_path(s)?;
    if p.exists() {
        Ok(JobStore::load(&p)?)
    } else {
        let m = JobStore::default();
        m.save(&p)?;
        Ok(m)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();
    let mut store = load(&cli.store)?;
    match cli.command {
        Commands::Stats { file } => {
            let text = std::fs::read_to_string(&file)?;
            let v: serde_json::Value = serde_json::from_str(&text)?;
            let records = match v {
                serde_json::Value::Array(a) => a,
                serde_json::Value::Object(o) => match o.get("records") {
                    Some(serde_json::Value::Array(a)) => a.clone(),
                    _ => return Err(BigDataError::Validation("no records".into()).into()),
                },
                _ => return Err("invalid input".into()),
            };
            let s = stats(&records);
            println!("records: {}", s.records);
            println!("fields: {}", s.fields.join(", "));
            println!("size: {} bytes", s.size_bytes);
        }
        Commands::Partition { file, n, strategy, key } => {
            let text = std::fs::read_to_string(&file)?;
            let v: serde_json::Value = serde_json::from_str(&text)?;
            let records = match v {
                serde_json::Value::Array(a) => a,
                _ => return Err("input must be JSON array".into()),
            };
            let buckets = partition(&records, n, strategy.into(), key.as_deref())?;
            for (i, b) in buckets.iter().enumerate() {
                println!("partition {i}: {} records", b.len());
            }
        }
        Commands::Create { name, input, output, partitions, strategy, key } => {
            let job = store.create(&name, input, output, partitions, strategy.into(), key)?;
            store.save(&store_path(&cli.store)?)?;
            println!("created job {} ({})", job.id, job.name);
        }
        Commands::Run { id } => {
            store.run(&id)?;
            store.save(&store_path(&cli.store)?)?;
            let j = store.get(&id).unwrap();
            println!("job {} -> {:?} ({} records)", id, j.status, j.records_processed);
        }
        Commands::List => {
            for j in store.list() {
                println!("{} [{:?}] {} -> {} ({} partitions, {:?})",
                    j.id, j.status, j.name, j.output_path.display(), j.partitions, j.strategy);
            }
        }
        Commands::Path => println!("{}", store_path(&cli.store)?.display()),
    }
    Ok(())
}
