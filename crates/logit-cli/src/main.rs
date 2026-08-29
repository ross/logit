use clap::{Parser, Subcommand};

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
            // TODO: build the tokio runtime, load `path`, wire inputs -> transforms -> outputs
            // per pipeline. This is the v0.1 vertical slice's entry point and is deliberately not
            // implemented in this design/scaffolding pass -- see docs/OVERVIEW.md.
            anyhow::bail!("not yet implemented: {} (see docs/OVERVIEW.md)", path.display())
        }
    }
}
