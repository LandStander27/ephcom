use serde::{Deserialize, Serialize};

pub mod prelude;

#[derive(Debug)]
pub struct JsonError {
	inner: serde_json::Error,
}

impl From<serde_json::Error> for JsonError {
	fn from(value: serde_json::Error) -> Self {
		return Self { inner: value };
	}
}

impl std::fmt::Display for JsonError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		return self.inner.fmt(f);
	}
}

impl std::error::Error for JsonError {}

pub trait Json<T: for<'de> Deserialize<'de> + Serialize = Self> {
	fn decode<S: Into<String>>(json: S) -> Result<T, JsonError> {
		return serde_json::from_str(&json.into()).map_err(JsonError::from);
	}

	fn encode(message: &T) -> Result<String, JsonError> {
		return serde_json::to_string(message).map_err(JsonError::from);
	}
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinedRoom;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JoinRequest {
	pub pass_hash: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeftRoom;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeletedRoom;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateRequest {
	pub pass_hash: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreatedRoom {
	pub id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PasswordWrong;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PasswordCorrect;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Message {
	pub text: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Error {
	pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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
