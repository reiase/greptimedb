use clap::{Parser, Subcommand};
use cmd::error::Error;
use cmd::options::{Options, TopLevelOptions};
use cmd::subcmd::{repl, standalone};
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use futures::executor::block_on;

#[derive(Parser)]
#[command(name = "Engram")]
#[command(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Engram {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Standalone(standalone::Standalone),
    REPL(repl::REPL),
}

impl Commands {
    pub fn execute(self, opts: Options) -> Result<(), Error> {
        match (self, opts) {
            (Commands::Standalone(cmd), Options::Standalone(opts)) => {
                block_on(cmd.execute(*opts))
            }
            (Commands::REPL(cmd), Options::Cli(_)) => block_on(cmd.execute()),
            _ => unreachable!(),
        }
    }
    pub fn load_options(&self) -> Result<Options, Error> {
        match self {
            Commands::Standalone(cmd) => cmd.load_options(TopLevelOptions::default()),
            Commands::REPL(cmd) => cmd.load_options(TopLevelOptions::default()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli: Engram = Engram::parse();
    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging(
        "Engram",
        &LoggingOptions::default(),
        TracingOptions::default(),
    );

    let opts = cli.command.load_options()?;
    return cli.command.execute(opts);
}
