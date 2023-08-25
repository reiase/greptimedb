use clap::Parser;
use clap::Subcommand;

use cmd::standalone;

#[derive(Parser)]
#[command(name = "Engram")]
#[command(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Engram {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Standalone{
        #[arg(short, long)]
        host: String,
        #[arg(short, long)]
        port: i32,
    },
    REPL,
}

impl Commands {
    fn execute() {
    }
}

fn main() {
    let cli = Engram::parse();
}