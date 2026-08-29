use anyhow::Context;
use clap::{Parser, Subcommand};

mod pipeline;

#[derive(Parser)]
#[command(name = "logit", version, about = "A logging, metrics, and tracing multiplexer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the JSON Schema for the config file format (ADR 0003) to stdout.
    Schema,
    /// Validate a config file against the schema and print a summary.
    Validate { path: std::path::PathBuf },
    /// Run logit with the given config file.
    Run { path: std::path::PathBuf },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Schema => {
            let schema = logit_config::json_schema();
            println!("{}", serde_json::to_string_pretty(&schema)?);
            Ok(())
        }
        Command::Validate { path } => {
            let raw = std::fs::read_to_string(&path)?;
            let _config: logit_config::Config = serde_norway::from_str(&raw)?;
            println!("{} is valid", path.display());
            Ok(())
        }
        Command::Run { path } => {
            // Schema/Validate stay synchronous above -- only Run needs an async runtime, so only
            // Run pays for building one.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            runtime.block_on(pipeline::run_pipelines(path))
        }
    }
}
