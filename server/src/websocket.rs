use actix_ws::{MessageStream, Session};
use anyhow::{Context, anyhow};
use ephcom_common::prelude::*;
use futures_channel::mpsc::UnboundedReceiver;
use futures_util::{
	StreamExt, TryFutureExt, TryStreamExt,
	future::{self, Either},
	pin_mut,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[allow(unused)]
use tracing::{debug, error, info, trace};

use crate::Room;

pub async fn send_error(session: &mut Session, msg: impl std::fmt::Display) {
	let _ = session
		.text(Response::encode(&Response::Error(ErrorResponse { message: msg.to_string() })).unwrap())
		.await;
}

pub async fn handle(
	rooms: Arc<Mutex<HashMap<String, Room>>>,
	id: String,
	rx: UnboundedReceiver<Response>,
	mut session: Session,
	stream: MessageStream,
	is_host: bool,
) -> anyhow::Result<()> {
	let send_incoming = stream
		.try_for_each(async |msg| {
			let incoming_json = match msg {
				actix_ws::Message::Text(str) => str.to_string(),
				actix_ws::Message::Close(_) => {
					info!("websocket closed");
					let mut rooms = rooms.lock().await;
					if let Some(room) = rooms.get_mut(&id) {
						let tx = if is_host {
							room.guest.take()
						} else {
							Some(room.host.clone())
						};
						if let Some(tx) = tx {
							tx.unbounded_send(if is_host {
								Response::DeletedRoom(DeletedRoom)
							} else {
								Response::LeftRoom(LeftRoom)
							})
							.map_err(|e| actix_ws::ProtocolError::Io(std::io::Error::other(e)))?;
						}
					}
					return Err(actix_ws::ProtocolError::Io(std::io::Error::other("websocket closed")));
				}
				_ => return Ok(()),
			};

			let message = match Response::decode(incoming_json).map_err(|_e| actix_ws::ProtocolError::Io(std::io::Error::other("invalid json"))) {
				Ok(o) => o,
				Err(e) => {
					let rooms = rooms.lock().await;
					if let Some(room) = rooms.get(&id) {
						let tx = if is_host {
							Some(&room.host)
						} else {
							room.guest.as_ref()
						};
						if let Some(tx) = tx {
							tx.unbounded_send(Response::Error(ErrorResponse {
								message: format!("invalid json: {e}"),
							}))
							.map_err(|e| actix_ws::ProtocolError::Io(std::io::Error::other(e)))?;
						}
					}
					return Ok(());
				}
			};
			// let json = Response::encode(&message).map_err(|_e| actix_ws::ProtocolError::Io(std::io::Error::other("could not encode message as json")))?;
			if let Response::Message(message) = message {
				let rooms = rooms.lock().await;
				if let Some(room) = rooms.get(&id) {
					let tx = if is_host {
						room.guest.as_ref()
					} else {
						Some(&room.host)
					};
					if let Some(tx) = tx {
						tx.unbounded_send(Response::Message(message))
							.map_err(|e| actix_ws::ProtocolError::Io(std::io::Error::other(e)))?;
					}
				}
			}

			return Ok(());
		})
		.map_err(|e| anyhow!("{e}"));
	let from_other = async {
		let mut stream = rx.map(|msg| {
			let json = Response::encode(&msg)?;
			return Ok::<String, String>(json);
		});

		while let Some(msg) = stream.next().await {
			let msg = match msg.map_err(|e| anyhow!("json error: {e}")) {
				Ok(o) => o,
				Err(e) => {
					send_error(&mut session, e).await;
					continue;
				}
			};
			session
				.text(msg)
				.await
				.context("could not send message over ws")?;
		}

		return Ok(());
	};

	pin_mut!(send_incoming, from_other);
	match future::select(send_incoming, from_other).await {
		Either::Left((left, _)) => left,
		Either::Right((right, _)) => right,
	}?;

	return Ok(());
}
