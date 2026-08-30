use anyhow::Context;
use clap::{Parser, Subcommand};

mod config;
mod dot;
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
    /// Print the config's resolved component graph as graphviz DOT (docs/design/pipeline-graph.md).
    Graph { path: std::path::PathBuf },
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
            // Strict: an unset `!env` variable fails here too, so `validate` is a real preflight
            // -- run it on the host before restarting the service and it catches a missing
            // secret before `run` would.
            let config = config::load(&path, config::MissingEnv::Error)?;
            // Same semantic checks `logit run` makes before spawning anything (empty component
            // graph, unknown/self-referencing sources, cycles, arity violations, unimplemented
            // kinds) -- shared so `validate` can't silently pass a config `run` would reject.
            pipeline::validate_semantics(config)?;
            println!("{} is valid", path.display());
            Ok(())
        }
        Command::Run { path } => {
            // Schema/Validate/Graph stay synchronous above -- only Run needs an async runtime, so
            // only Run pays for building one.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            runtime.block_on(pipeline::run_pipelines(path))
        }
        Command::Graph { path } => {
            // Lenient: `logit graph` renders a config's shape, not its values, and its whole
            // point is rendering *something* useful even for an otherwise-broken config -- an
            // unset `!env` variable substitutes a placeholder (with a warning from `config::load`)
            // rather than failing outright. That placeholder is always a string, though, so it can
            // still fail to type-check against a non-string field (a `Duration`, an `f64`, ...) --
            // `config::load_for_graph` degrades to `Loaded::Lenient` rather than erroring out in
            // exactly that case, so DOT still prints even then (docs/known-gaps.md).
            match config::load_for_graph(&path)? {
                config::Loaded::Config(config) => {
                    // Print the DOT first, always -- then report validation problems on stderr
                    // without suppressing it. A cyclic or otherwise-broken config is exactly what
                    // this command is most useful for: a cycle is far easier to see rendered than
                    // parsed out of an error message naming two component ids
                    // (docs/design/pipeline-graph.md).
                    println!("{}", dot::render(&config));
                    if let Err(err) = pipeline::validate_semantics(config) {
                        eprintln!("warning: {err}");
                        std::process::exit(1);
                    }
                }
                config::Loaded::Lenient { value, error } => {
                    println!("{}", dot::render_lenient(&value));
                    eprintln!(
                        "warning: config did not fully resolve ({error:#}) -- rendering topology \
                         only; semantic validation skipped"
                    );
                }
            }
            Ok(())
        }
    }
}
