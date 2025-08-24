use anyhow::{Context, anyhow};
use ephcom_common::prelude::*;
use futures_util::{
	SinkExt, StreamExt,
	stream::{SplitSink, SplitStream},
};
use http::Uri;
use rustyline_async::{Readline, ReadlineEvent, SharedWriter};
use std::str::FromStr;
use std::{io::Write, sync::Arc};
use tokio::sync::Mutex;
use tokio::{net::TcpStream, sync::Notify};
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream};

#[allow(unused)]
use tracing::{debug, error, info, trace, warn};

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct Client {
	is_host: bool,
	uri: String,
	base_uri: String,
	sink: Option<Arc<Mutex<Sink>>>,
	stream: Option<Arc<Mutex<Stream>>>,
}

impl Client {
	pub fn new(url: impl Into<String>, base_url: impl Into<String>, is_host: bool) -> Self {
		return Self {
			is_host,
			uri: url.into(),
			base_uri: base_url.into(),
			sink: None,
			stream: None,
		};
	}

	pub async fn send(&mut self, msg: Response) -> anyhow::Result<()> {
		if let Some(sink) = self.sink.as_mut() {
			sink.lock()
				.await
				.send(Message::text(Response::encode(&msg).context("encoding failure")?))
				.await?;
		} else {
			return Err(anyhow!("connection not open"));
		}

		return Ok(());
	}

	pub async fn connect(&mut self) -> anyhow::Result<()> {
		let uri = Uri::from_str(&format!("ws://{}", self.uri))?;
		let (client, _) = ClientBuilder::from_uri(uri)
			.connect()
			.await
			.context("could not connect")?;

		let (sink, stream) = client.split();

		self.sink = Some(Arc::new(Mutex::new(sink)));
		self.stream = Some(Arc::new(Mutex::new(stream)));
		return Ok(());
	}

	pub async fn start(&mut self) -> anyhow::Result<()> {
		if self.stream.is_none() || self.sink.is_none() {
			return Err(anyhow!("connection not open"));
		}

		let exiting = Arc::new(Notify::new());
		let (t, stdout) = self
			.send_messages(exiting.clone())
			.await
			.context("could not send")?;
		let t2 = self
			.listen_incoming(stdout, exiting.clone())
			.await
			.context("could not listen")?;

		t.await??;
		t2.await??;

		return Ok(());
	}

	async fn send_messages(&self, exiting: Arc<Notify>) -> anyhow::Result<(tokio::task::JoinHandle<anyhow::Result<()>>, SharedWriter)> {
		let sink = self.sink.as_ref().unwrap().clone();
		let (mut rl, stdout) = Readline::new("> ".to_string())?;
		let mut thread_stdout = stdout.clone();
		let t = tokio::spawn(async move {
			loop {
				let msg = tokio::select! {
					biased;
					_ = exiting.notified() => break,
					msg = rl.readline() => msg?,
				};
				match msg {
					ReadlineEvent::Line(s) => {
						let s = s.trim();
						rl.add_history_entry(s.to_string());
						sink.lock()
							.await
							.send(Message::text(Response::encode(&Response::Message(ephcom_common::Message { text: s.to_string() }))?))
							.await
							.context("could not send message over sink")?;
					}
					ReadlineEvent::Interrupted => {
						writeln!(thread_stdout, "^C")?;
						sink.lock()
							.await
							.send(Message::close(None, ""))
							.await
							.context("could not send close msg")?;
						break;
					}
					_ => todo!(),
				}
			}

			rl.flush()?;
			exiting.notify_waiters();
			return Ok(());
		});
		return Ok((t, stdout));
	}

	async fn listen_incoming(&self, mut stdout: SharedWriter, exiting: Arc<Notify>) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<()>>> {
		let stream = self.stream.as_ref().unwrap().clone();
		let uri = self.base_uri.clone();
		let t = tokio::spawn(async move {
			loop {
				let mut stream = stream.lock().await;
				let msg = tokio::select! {
					biased;
					_ = exiting.notified() => break,
					msg = stream.next() => msg
				};
				if let Some(msg) = msg {
					let msg = msg.context("could not recv over stream")?;
					if msg.is_text() {
						let json = msg.as_text().unwrap();
						let msg = Response::decode(json).context("could not decode json into message")?;
						match msg {
							Response::Message(msg) => {
								writeln!(stdout, "other: {}", msg.text.trim())?;
							}
							Response::CreatedRoom(recv) => {
								writeln!(stdout, "system: Room created! Share this URL to have someone join you! {}/chat/{}", uri, recv.id)?;
							}
							Response::PasswordCorrect(_) => {
								writeln!(stdout, "system: Joined!")?;
							}
							Response::PasswordWrong(_) => {
								writeln!(stdout, "system: Incorrect password")?;
							}
							Response::JoinedRoom(_) => {
								writeln!(stdout, "system: Guest joined!")?;
							}
							Response::LeftRoom(_) => {
								writeln!(stdout, "system: Guest left")?;
							}
							Response::DeletedRoom(_) => {
								writeln!(stdout, "system: Room was deleted")?;
							}
							_ => todo!("{msg:?}"),
						}
					} else if msg.is_close() {
						break;
					}
				}
			}

			exiting.notify_waiters();
			return Ok(());
		});
		return Ok(t);
	}
}
