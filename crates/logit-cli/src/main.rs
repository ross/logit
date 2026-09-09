use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};

mod config;
mod dot;
mod pipeline;

/// jemalloc rather than the platform default (glibc malloc on this project's `debian:bookworm-slim`
/// runtime image) -- see `docs/adr/jemalloc-global-allocator.md` and
/// `docs/design/memory.md`. `logit` is exactly the workload glibc's arena model handles worst: a
/// long-lived, multi-threaded process churning small short-lived allocations forever, where RSS
/// drifts upward for days without the working set growing.
///
/// Behind a default-on feature so both allocators stay measurable -- `--no-default-features` builds
/// against the system allocator, which is what makes "is jemalloc actually helping here?" a
/// question with an answer rather than an assumption.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "logit", version, about = "A logging, metrics, and tracing multiplexer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// `tracing`'s `EnvFilter` syntax (e.g. `debug`, `logit_pipeline=trace,info`) -- what severity
    /// (and per-module override) `logit`'s own self-logging reports at. Only `run` installs a
    /// subscriber (docs/plans/operator-surface.md); every other command stays print-only.
    #[arg(long, env = "LOGIT_LOG", default_value = "info", global = true)]
    log_level: String,
    /// `text` is one line per event, human-formatted; `json` is one JSON object per line
    /// (`timestamp`, `level`, `target`, `component`, `key`, `message`, every one of them
    /// top-level -- `flatten_event`, and `target` left displayed, so a collector reads the
    /// fields named here rather than unwrapping a nested `fields` object) -- for a log
    /// collector. Both formats write to stderr; see `init_logging`.
    #[arg(long, value_enum, default_value = "text", global = true)]
    log_format: LogFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Print the JSON Schema for the config file format (ADR `config-yaml-jsonschema`) to stdout.
    Schema,
    /// Validate a config file against the schema and print a summary.
    Validate { path: std::path::PathBuf },
    /// Run logit with the given config file.
    Run { path: std::path::PathBuf },
    /// Print the config's resolved component graph as graphviz DOT (docs/design/pipeline-graph.md).
    Graph { path: std::path::PathBuf },
}

/// Builds and installs the process-wide `tracing` subscriber for `Command::Run` -- the only
/// subcommand that runs long enough, or does enough on `logit`'s own behalf, to want leveled
/// self-logging (`Schema`/`Validate`/`Graph` are print-only and stay exactly that way).
///
/// A bad `--log-level`/`LOGIT_LOG` directive is a config error the same as a bad `bind` address:
/// reported and exited on the spot, before anything else has started.
///
/// Everything the subscriber renders goes to stderr, never stdout: `stdio_out` defaults to
/// `target: stdout` (`StdioTarget::Stdout`), so stdout belongs to the pipeline's own event
/// stream -- `logit run c.yaml > events.log` has to stay parseable, and lifecycle lines
/// interleaved into it would corrupt exactly the output an operator is capturing.
fn init_logging(level: &str, format: LogFormat) -> anyhow::Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_new(level)
        .with_context(|| format!("--log-level/LOGIT_LOG: '{level}' is not a valid directive"))?;
    let registry = tracing_subscriber::registry().with(filter);
    match format {
        LogFormat::Text => {
            let layer =
                tracing_subscriber::fmt::layer().with_target(false).with_writer(std::io::stderr);
            registry.with(layer).init();
        }
        LogFormat::Json => {
            // `target` stays displayed here (unlike the text arm): every `logit` event sets it
            // explicitly to `"logit"`, and it's one of the six fields `--log-format`'s doc
            // comment promises a collector. `flatten_event` lifts `message`/`component`/`key`
            // out of the nested `fields` object the JSON formatter otherwise wraps them in.
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(std::io::stderr);
            registry.with(layer).init();
        }
    }
    Ok(())
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
            // An unset `!env` variable fails here too, so `validate` is a real preflight -- run
            // it on the host before restarting the service and it catches a missing secret
            // before `run` would.
            let config = config::load(&path)?;
            // Same semantic checks `logit run` makes before spawning anything (empty component
            // graph, unknown/self-referencing sources, cycles, arity violations, unimplemented
            // kinds) -- shared so `validate` can't silently pass a config `run` would reject.
            pipeline::validate_semantics(config)?;
            println!("{} is valid", path.display());
            Ok(())
        }
        Command::Run { path } => {
            init_logging(&cli.log_level, cli.log_format)?;
            // Schema/Validate/Graph stay synchronous above -- only Run needs an async runtime, so
            // only Run pays for building one.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            runtime.block_on(pipeline::run_pipelines(path))
        }
        Command::Graph { path } => {
            // Every `!env` reference must resolve here too, same as `run`/`validate` -- no
            // lenient mode that renders a config's shape with its secrets left unset
            // (docs/adr/env-yaml-tag.md's Alternatives).
            let config = config::load(&path)?;
            // Print the DOT first, always -- then report validation problems on stderr without
            // suppressing it. A cyclic or otherwise-broken config is exactly what this command is
            // most useful for: a cycle is far easier to see rendered than parsed out of an error
            // message naming two component ids (docs/design/pipeline-graph.md).
            println!("{}", dot::render(&config));
            if let Err(err) = pipeline::validate_semantics(config) {
                eprintln!("warning: {err}");
                std::process::exit(1);
            }
            Ok(())
        }
    }
}
