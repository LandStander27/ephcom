use serde::{Deserialize, Serialize};

pub mod prelude;

pub trait Json<T: for<'de> Deserialize<'de> + Serialize = Self> {
	fn decode<S: Into<String>>(json: S) -> Result<T, String> {
		return match serde_json::from_str(&json.into()) {
			Ok(s) => Ok(s),
			Err(e) => {
				return Err(e.to_string());
			}
		};
	}

	fn encode(message: &T) -> Result<String, String> {
		return match serde_json::to_string(message) {
			Ok(j) => Ok(j),
			Err(e) => {
				return Err(e.to_string());
			}
		};
	}
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JoinedRoom;

#[derive(Serialize, Deserialize, Clone)]
pub struct JoinRequest {
	pub pass_hash: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LeftRoom;

#[derive(Serialize, Deserialize, Clone)]
pub struct DeletedRoom;

#[derive(Serialize, Deserialize, Clone)]
pub struct CreateRequest {
	pub pass_hash: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CreatedRoom {
	pub id: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PasswordWrong;

#[derive(Serialize, Deserialize, Clone)]
pub struct PasswordCorrect;

#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
	pub text: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Error {
	pub message: String,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Response {
	JoinRequest(JoinRequest),
	CreateRequest(CreateRequest),
	PasswordCorrect(PasswordCorrect),
	PasswordWrong(PasswordWrong),

	JoinedRoom(JoinedRoom),
	LeftRoom(LeftRoom),
	DeletedRoom(DeletedRoom),
	CreatedRoom(CreatedRoom),
	Message(Message),
	Error(Error),
}

impl Json for Response {}
