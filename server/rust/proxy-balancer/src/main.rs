mod cli;
mod config;
mod db;
mod defaults;
mod environment;
mod handles;
mod hyper_reverse_proxy;
mod log;
mod structs;
mod tasks;
mod utils;

use clap::{Parser, Subcommand};
use cli::{init::command_init, serve::command_serve, serve_cache::command_serve_cache};
use environment::get_cwd;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Init,
    Serve {
        #[arg(long)]
        address: Option<String>,
    },
    ServeCache,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cwd = if let Ok(curr_dir) = get_cwd() {
        curr_dir
    } else {
        eprintln!("Error get current working directory");
        exit(1);
    };

    let args = Args::parse();

    match args.cmd {
        Commands::Init => command_init(cwd).await,
        Commands::Serve { address } => command_serve(cwd, address).await,
        Commands::ServeCache => command_serve_cache(cwd).await,
    }
}
