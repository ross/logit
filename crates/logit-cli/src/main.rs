use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};

mod admin;
mod config;
mod dot;
mod pipeline;

/// jemalloc rather than glibc malloc (the `debian:bookworm-slim` runtime image's default): a
/// long-lived, multi-threaded process churning small short-lived allocations is the workload
/// glibc's arena model handles worst, with RSS drifting upward for days without the working set
/// growing. See `docs/adr/jemalloc-global-allocator.md`.
///
/// Behind a default-on feature so both allocators stay measurable: `--no-default-features` builds
/// against the system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "logit", version, about = "A logging, metrics, and tracing multiplexer.")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Self-logging filter for `run`, in `tracing`'s `EnvFilter` syntax (e.g. `debug`,
    /// `logit_pipeline=trace,info`). Other commands don't self-log.
    #[arg(long, env = "LOGIT_LOG", default_value = "info", global = true)]
    log_level: String,
    /// Self-logging format, written to stderr: `text` is one human-formatted line per event;
    /// `json` is one JSON object per line for a log collector, with `timestamp`, `level`,
    /// `target`, `component`, `key`, and `message` all top-level.
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
    /// Print the config file format's JSON Schema to stdout.
    Schema,
    /// Check a config file's schema, `!env` references, and component graph without running it.
    Validate { path: std::path::PathBuf },
    /// Run logit with the given config file.
    Run { path: std::path::PathBuf },
    /// Print a config file's component graph as graphviz DOT, even if the config is invalid.
    Graph { path: std::path::PathBuf },
    /// Probe a running `logit`'s `/readyz`: print its status word, and exit 0 on `200` or 1
    /// otherwise. The target's config must set `admin.bind`; the container image's `HEALTHCHECK`
    /// runs this.
    Ready {
        /// The target's admin URL: `http://` plus its `admin.bind` address.
        #[arg(long, default_value = "http://127.0.0.1:9600")]
        admin: String,
    },
}

/// Installs the process-wide `tracing` subscriber; only `Command::Run` calls it.
///
/// `Command::Run` calls this before loading the config, so a bad `--log-level`/`LOGIT_LOG`
/// directive fails fast whatever the config holds, and `pipeline::run_pipelines`'s `starting` log
/// fires even for a config that then fails resolution.
///
/// Output goes to stderr, never stdout: `stdio_out` defaults to `target: stdout`, and
/// `logit run c.yaml > events.log` has to stay parseable.
///
/// `telemetry_layer` is stacked in unconditionally, inactive until `pipeline::run_pipelines` calls
/// `TelemetryLayer::activate` once the config's `internal` component is known. That happens after
/// this call, and there's no stable API to add a layer to an `.init()`-ed subscriber.
///
/// `--log-level` filters only the stderr `fmt` layer; `telemetry_layer` carries
/// `logit_core::TelemetryLayer::capture_filter` of its own. See the comment in the body for why.
fn init_logging(
    level: &str,
    format: LogFormat,
    telemetry_layer: logit_core::TelemetryLayer,
) -> anyhow::Result<()> {
    use tracing_subscriber::layer::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_new(level)
        .with_context(|| format!("--log-level/LOGIT_LOG: '{level}' is not a valid directive"))?;
    // `with_filter` on each layer, never a bare `.with(filter)` on the registry. A registry-level
    // `EnvFilter` is global: `Layered::enabled`/`register_callsite` short-circuit the whole stack
    // and `tracing-core` caches a `never` verdict per callsite for the process's life, so
    // `--log-level error` would drop every `warn` before `TelemetryLayer::on_event` ran,
    // downgrading `internal: { logs: warn }` to `error` uncounted. Per layer, `--log-level` is
    // stderr verbosity and `internal.logs` is what the pipeline captures.
    let registry = tracing_subscriber::registry()
        .with(telemetry_layer.with_filter(logit_core::TelemetryLayer::capture_filter()));
    match format {
        LogFormat::Text => {
            let layer = tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr)
                .with_filter(filter);
            registry.with(layer).init();
        }
        LogFormat::Json => {
            // `target` stays displayed (unlike the text arm): it's one of the six top-level fields
            // `--log-format` promises a collector. `flatten_event` lifts `message`/`component`/
            // `key` out of the nested `fields` object the JSON formatter otherwise uses.
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(std::io::stderr)
                .with_filter(filter);
            registry.with(layer).init();
        }
    }
    Ok(())
}

/// `GET {admin}/readyz`: the status word on `200`, else an error `main` prints and exits 1 on.
///
/// Uses `hyper_util`'s legacy client, not `reqwest`: the crate takes no new HTTP client
/// dependency (docs/plans/operator-surface.md).
fn check_ready(admin: &str) -> anyhow::Result<String> {
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;
    runtime.block_on(async {
        let uri: hyper::Uri = format!("{}/readyz", admin.trim_end_matches('/'))
            .parse()
            .with_context(|| format!("--admin: '{admin}' is not a valid URL"))?;
        let client = Client::builder(TokioExecutor::new())
            .build_http::<http_body_util::Empty<bytes::Bytes>>();
        let response =
            client.get(uri).await.with_context(|| format!("requesting {admin}/readyz"))?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .context("reading the /readyz response body")?
            .to_bytes();
        let word = String::from_utf8_lossy(&body).trim().to_string();
        if status.is_success() {
            Ok(word)
        } else {
            anyhow::bail!("{word} ({status})")
        }
    })
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
            // An unset `!env` variable fails here too, so `validate` catches a missing secret on
            // the host before a restart would.
            let config = config::load(&path)?;
            // The graph checks `run` makes before spawning anything. `build_spec`'s checks (a
            // syslog `sd_id`, a referenced file) run only under `run` (docs/deploying.md's
            // "`logit validate` as a preflight").
            pipeline::validate_semantics(config)?;
            println!("{} is valid", path.display());
            Ok(())
        }
        Command::Run { path } => {
            let telemetry_layer = logit_core::TelemetryLayer::new();
            init_logging(&cli.log_level, cli.log_format, telemetry_layer.clone())?;
            // Only `Run` pays for an async runtime.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            match runtime.block_on(pipeline::run_pipelines(path, telemetry_layer)) {
                Ok(()) => Ok(()),
                Err(err) => {
                    // 1 for a startup failure, 2 for a runtime failure after the process was ready
                    // (`docs/deploying.md`'s "Probes and exit codes"). Prints `anyhow`'s `Error:
                    // {:?}` by hand because returning the error from `main` would exit 1.
                    eprintln!("Error: {:?}", err.error());
                    std::process::exit(err.exit_code());
                }
            }
        }
        Command::Graph { path } => {
            // Every `!env` reference must resolve here too; there's no lenient mode
            // (docs/adr/env-yaml-tag.md's "Alternatives considered").
            let config = config::load(&path)?;
            // DOT first, always, then any validation error on stderr: a cycle is easier to see
            // rendered than read out of an error naming two component ids.
            println!("{}", dot::render(&config));
            if let Err(err) = pipeline::validate_semantics(config) {
                eprintln!("warning: {err}");
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Ready { admin } => match check_ready(&admin) {
            Ok(word) => {
                println!("{word}");
                Ok(())
            }
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        },
    }
}
