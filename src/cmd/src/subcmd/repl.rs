use clap::Parser;
use common_telemetry::logging::LoggingOptions;
use futures::executor::block_on;
use crate::{error::Result, options::{TopLevelOptions, Options}, cli::{Repl, AttachCommand}};

#[derive(Clone, Debug, Parser)]
pub struct REPL {
    #[arg(short, long)]
    pub grpc_addr: String,
    #[arg(short, long)]
    pub meta_addr: Option<String>,
    #[arg(short, long)]
    pub disable_helper: bool,
}

impl REPL{
    pub async fn execute(self) -> Result<()> {
        let cmd = AttachCommand {
            grpc_addr: self.grpc_addr,
            meta_addr: self.meta_addr,
            disable_helper: self.disable_helper,
        };
        let mut repl = block_on(Repl::try_new(&cmd))?;
        repl.run().await
    }

    pub fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        let mut logging_opts = LoggingOptions::default();
        if let Some(dir) = top_level_opts.log_dir {
            logging_opts.dir = dir;
        }
        if top_level_opts.log_level.is_some() {
            logging_opts.level = top_level_opts.log_level;
        }
        Ok(Options::Cli(Box::new(logging_opts)))
    }
}