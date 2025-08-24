use actix_web::{App, HttpRequest, HttpResponse, HttpServer, middleware::Logger, web};
use anyhow::Context;
use clap::Parser;
use ephcom_common::prelude::*;
use futures_channel::mpsc::{self, UnboundedSender};
use rand::prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing_subscriber::field::MakeExt;

#[allow(unused)]
use tracing::{debug, error, info, trace};

mod websocket;

#[derive(Parser, Debug)]
#[command(name = "ephcom-server", version = version::version)]
#[command(about = "Run the Ephcom server", long_about = None)]
struct Args {
	#[arg(short, long, help = "increase verbosity")]
	verbose: bool,
}

#[derive(Clone, Debug)]
struct Room {
	password_hash: String,
	host: UnboundedSender<Response>,
	guest: Option<UnboundedSender<Response>>,
}

#[derive(Clone, Default)]
struct Data {
	rooms: Arc<Mutex<HashMap<String, Room>>>,
}

async fn chat_create(req: HttpRequest, stream: web::Payload, data: web::Data<Data>) -> Result<HttpResponse, actix_web::Error> {
	let (res, mut session, mut stream) = actix_ws::handle(&req, stream)?;
	actix_web::rt::spawn(async move {
		let password_hash = if let Some(Ok(actix_ws::Message::Text(str))) = stream.recv().await {
			let str = str.to_string();
			if let Ok(Response::CreateRequest(req)) = Response::decode(&str) {
				req.pass_hash
			} else {
				error!("invalid handshake: {}", str);
				websocket::send_error(&mut session, "invalid handshake").await;
				return;
			}
		} else {
			error!("handshake failed");
			websocket::send_error(&mut session, "invalid handshake").await;
			session.close(None).await.unwrap_or_else(|e| {
				error!("could not close websocket: {}", e);
			});
			return;
		};

		let mut rng = rand::rng();
		let (tx, rx) = mpsc::unbounded::<Response>();
		let id: String = (0..6)
			.map(|_| rng.sample(rand::distr::Alphabetic) as char)
			.collect();
		{
			let mut rooms = data.rooms.lock().await;
			rooms.insert(
				id.clone(),
				Room {
					host: tx,
					guest: None,
					password_hash,
				},
			);
		}

		if let Err(e) = session
			.text(Response::encode(&Response::CreatedRoom(CreatedRoom { id: id.clone() })).unwrap())
			.await
		{
			error!("{e}");
		}

		if let Err(e) = websocket::handle(data.rooms.clone(), id, rx, session.clone(), stream, true).await {
			error!("host error: {e}");
		}

		if let Err(e) = session
			.close(None)
			.await
			.context("could not send close msg")
		{
			error!("{e}");
		}
	});
	return Ok(res);
}

async fn chat_connect(req: HttpRequest, stream: web::Payload, data: web::Data<Data>, id: web::Path<String>) -> Result<HttpResponse, actix_web::Error> {
	let (res, mut session, mut stream) = actix_ws::handle(&req, stream)?;
	actix_web::rt::spawn(async move {
		let (tx, rx) = mpsc::unbounded::<Response>();
		let id = id.into_inner();

		let password_hash = if let Some(Ok(actix_ws::Message::Text(str))) = stream.recv().await {
			let str = str.to_string();
			if let Ok(Response::JoinRequest(handshake)) = Response::decode(&str) {
				handshake.pass_hash
			} else {
				error!("invalid handshake: {}", str);
				websocket::send_error(&mut session, "invalid handshake").await;
				return;
			}
		} else {
			error!("handshake failed");
			websocket::send_error(&mut session, "invalid handshake").await;
			session.close(None).await.unwrap_or_else(|e| {
				error!("could not close websocket: {}", e);
			});
			return;
		};

		{
			let mut rooms = data.rooms.lock().await;
			if let Some(room) = rooms.get_mut(&id) {
				if room.password_hash != password_hash {
					let _ = session
						.text(Response::encode(&Response::PasswordWrong(PasswordWrong)).unwrap())
						.await;
					error!("invalid password");
					session.close(None).await.unwrap_or_else(|e| {
						error!("could not close websocket: {}", e);
					});
					return;
				} else {
					let _ = session
						.text(Response::encode(&Response::PasswordCorrect(PasswordCorrect)).unwrap())
						.await;
				}
				room.guest = Some(tx);
				if let Err(e) = room.host.unbounded_send(Response::JoinedRoom(JoinedRoom)) {
					let _ = session
						.text(Response::encode(&Response::Error(ErrorResponse { message: e.to_string() })).unwrap())
						.await;
					error!("{e}");
				}
			}
		}

		if let Err(e) = websocket::handle(data.rooms.clone(), id, rx, session.clone(), stream, false).await {
			error!("guest error: {e}");
		}

		if let Err(e) = session
			.close(None)
			.await
			.context("could not send close msg")
		{
			error!("{e}");
		}
	});
	return Ok(res);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let args = Args::parse();

	let filter = tracing_subscriber::EnvFilter::builder().parse(format!("actix_server=info,ephcom_server={}", if args.verbose { "trace" } else { "debug" }))?;
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

	let data = Data::default();
	HttpServer::new(move || {
		App::new()
			.wrap(Logger::default())
			.app_data(web::Data::new(data.clone()))
			.route("/chat/{id}", web::get().to(chat_connect))
			.route("/create", web::get().to(chat_create))
	})
	.bind("0.0.0.0:8080")?
	.run()
	.await?;

	return Ok(());
}
