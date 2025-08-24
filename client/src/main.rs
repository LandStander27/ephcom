use anyhow::Context;
use clap::{Args as ClapArgs, Parser, Subcommand};
use ephcom_common::prelude::*;
use sha2::{Digest, Sha256};
use std::io::Write;
use tracing_subscriber::field::MakeExt;

#[allow(unused)]
use tracing::{debug, error, info, trace, warn};

#[derive(Parser, Debug)]
#[command(name = "ephcom", version = version::version)]
#[command(about = "Connect to an Ephcom server", long_about = None)]
struct Args {
	#[arg(short, long, help = "increase verbosity")]
	verbose: bool,

	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
	Create(CreateCommand),
	Join(JoinCommand),
}

#[derive(ClapArgs, Debug)]
struct CreateCommand {
	#[arg(required = true, help = "base URL of server")]
	url: String,
}

#[derive(ClapArgs, Debug)]
struct JoinCommand {
	#[arg(required = true, help = "URL shared from room host")]
	url: String,
}

mod client;

async fn prompt_password() -> anyhow::Result<String> {
	print!("Password ? ");
	std::io::stdout()
		.flush()
		.context("could not flush stdout")?;
	let mut buffer = String::new();
	let stdin = std::io::stdin();
	stdin
		.read_line(&mut buffer)
		.context("could not read from stdin")?;
	let hash = Sha256::digest(buffer.trim());
	let hash_string = hex::encode(hash);
	return Ok(hash_string);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();

	let filter = tracing_subscriber::EnvFilter::builder().parse(format!("ephcom={}", if args.verbose { "trace" } else { "debug" }))?;
	let subscriber = tracing_subscriber::fmt()
		.compact()
		.with_file(args.verbose)
		.with_line_number(args.verbose)
		.with_thread_ids(args.verbose)
		.with_target(args.verbose)
		// .without_time()
		.map_fmt_fields(|f| f.debug_alt())
		.with_env_filter(filter)
		.finish();
	tracing::subscriber::set_global_default(subscriber)?;

	trace!("registered logger");

	match args.command {
		Commands::Create(create) => {
			let mut client = client::Client::new(format!("{}/create", create.url), create.url, true);
			let hash = prompt_password().await?;
			client.connect().await?;

			let request = Response::CreateRequest(CreateRequest { pass_hash: hash });
			client.send(request).await?;
			client.start().await?;
		}
		Commands::Join(join) => {
			let mut client = client::Client::new(join.url, "", false);
			let hash = prompt_password().await?;
			client.connect().await?;

			let request = Response::JoinRequest(JoinRequest { pass_hash: hash });
			client.send(request).await?;
			client.start().await?;
		}
	}

	return Ok(());
}
