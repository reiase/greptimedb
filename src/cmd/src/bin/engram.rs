use clap::Parser;
use clap::Subcommand;

#[derive(Parser)]
#[command(name = "Engram")]
#[command(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Engram {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Standalone,
    REPL,
}

fn main() {
    let cli = Engram::parse();
}