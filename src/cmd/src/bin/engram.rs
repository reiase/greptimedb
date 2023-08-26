use clap::{Parser, Subcommand};
use cmd::error::Error;
use cmd::options::{Options, TopLevelOptions};
use cmd::standalone;
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
    Standalone(standalone::Command),
    REPL,
}

impl Commands {
    pub fn execute(self, opts: Options) -> Result<(), Error> {
        match (self, opts) {
            (Commands::Standalone(cmd), Options::Standalone(opts)) => {
                let instance = block_on(cmd.build(opts.fe_opts, opts.dn_opts));
                block_on(instance.unwrap().start())
            }
            (Commands::REPL, Options::Cli(_)) => todo!(),
            _ => unreachable!(),
        }
    }
    pub fn load_options(&self) -> Result<Options, Error> {
        match self {
            Commands::Standalone(cmd) => cmd.load_options(TopLevelOptions::default()),
            Commands::REPL => todo!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli: Engram = Engram::parse();
    common_telemetry::set_panic_hook();
    common_telemetry::init_default_metrics_recorder();
    let _guard = common_telemetry::init_global_logging("Engram", &LoggingOptions::default(), TracingOptions::default());

    let opts = cli.command.load_options()?;
    return cli.command.execute(opts);
}
