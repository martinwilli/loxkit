//! WebSocket transport and the 8-byte binary framing.
//!
//! The Miniserver speaks a framed protocol over a WebSocket: text frames carry
//! JSON command replies (and the structure file), while binary messages are
//! preceded by their own 8-byte header frame whose payload arrives in the next
//! frame. This module connects over plain `ws://`, classifies inbound frames,
//! and correlates a sent text command with its JSON reply.
//!
//! No transport-layer TLS: server identity comes from the app-layer public-key
//! pin (a later commit), so no `wss://` and no rustls/native-tls dependency.

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// WebSocket resource path the Miniserver serves.
const WS_PATH: &str = "/ws/rfc6455";
/// Subprotocol the Miniserver requires in the upgrade request.
const SUBPROTOCOL: &str = "remotecontrol";

/// The class of a binary message, from byte 1 of its 8-byte header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {
    /// Plain text (a command response follows as a text frame).
    Text,
    /// A binary file.
    BinaryFile,
    /// A value-state table (uuid + f64 entries).
    ValueStates,
    /// A text-state table.
    TextStates,
    /// A daytimer table.
    Daytimer,
    /// Server going out of service.
    OutOfService,
    /// A keepalive response (empty payload).
    Keepalive,
    /// A weather table.
    Weather,
}

impl MessageType {
    /// Map the header type byte to a [`MessageType`].
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::Text,
            1 => Self::BinaryFile,
            2 => Self::ValueStates,
            3 => Self::TextStates,
            4 => Self::Daytimer,
            5 => Self::OutOfService,
            6 => Self::Keepalive,
            7 => Self::Weather,
            _ => return None,
        })
    }
}

/// The parsed 8-byte binary header that precedes a Miniserver message.
///
/// Little-endian, packed: `[0x03, type, info, reserved, len:u32]`. The high bit
/// of the info byte marks an estimated length, in which case an exact header
/// frame follows before the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub msg_type: MessageType,
    pub len: u32,
    pub estimated: bool,
}

impl MessageHeader {
    /// Size of the header on the wire.
    pub const SIZE: usize = 8;

    /// Parse an 8-byte header frame.
    pub fn parse(bytes: &[u8]) -> Result<Self, TransportError> {
        if bytes.len() < Self::SIZE {
            return Err(TransportError::ShortHeader(bytes.len()));
        }
        if bytes[0] != 0x03 {
            return Err(TransportError::BadHeaderId(bytes[0]));
        }
        let msg_type = MessageType::from_u8(bytes[1]).ok_or(TransportError::UnknownType(bytes[1]))?;
        let estimated = bytes[2] & 0x80 != 0;
        // bytes[3] is reserved.
        let len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        Ok(Self {
            msg_type,
            len,
            estimated,
        })
    }
}

/// A classified inbound message.
#[derive(Clone, Debug)]
pub enum Inbound {
    /// A text frame: a JSON command reply or the structure file.
    Text(String),
    /// A binary message: its parsed header plus the payload bytes.
    Binary {
        header: MessageHeader,
        payload: Vec<u8>,
    },
}

/// The `{"LL":{...}}` envelope every command reply is wrapped in.
#[derive(Clone, Debug, Deserialize)]
pub struct LlResponse {
    #[serde(rename = "LL")]
    pub ll: LlBody,
}

/// The body of an `LL` envelope.
#[derive(Clone, Debug, Deserialize)]
pub struct LlBody {
    /// The command this reply answers.
    pub control: String,
    /// Status code; the wire spells it `Code` and sometimes as a string.
    #[serde(rename = "Code", alias = "code", deserialize_with = "de_code")]
    pub code: i64,
    /// The command-specific value (string, object, ...).
    #[serde(default)]
    pub value: serde_json::Value,
}

impl LlResponse {
    /// Parse an `LL` envelope from a text-frame reply.
    ///
    /// Public so the crypto layer can re-parse the plaintext it recovers from a
    /// `fenc` reply, whose decrypted body is itself a full `LL` envelope.
    pub fn parse(text: &str) -> Result<Self, TransportError> {
        Ok(serde_json::from_str(text)?)
    }

    /// The reply status code.
    pub fn code(&self) -> i64 {
        self.ll.code
    }

    /// The command the reply answers.
    pub fn control(&self) -> &str {
        &self.ll.control
    }

    /// The command-specific value.
    pub fn value(&self) -> &serde_json::Value {
        &self.ll.value
    }
}

/// Deserialize the status code whether it arrives as a JSON number or a string.
fn de_code<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrInt {
        Int(i64),
        Str(String),
    }
    match StrOrInt::deserialize(deserializer)? {
        StrOrInt::Int(i) => Ok(i),
        StrOrInt::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

/// Errors from the transport layer.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("websocket header too short: got {0} bytes, need 8")]
    ShortHeader(usize),
    #[error("bad websocket header id 0x{0:02x}, expected 0x03")]
    BadHeaderId(u8),
    #[error("unknown message type byte {0}")]
    UnknownType(u8),
    #[error("unexpected websocket frame in binary sequence")]
    UnexpectedFrame,
    #[error("connection closed (code {0:?})")]
    Closed(Option<u16>),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("malformed JSON response: {0}")]
    Json(#[from] serde_json::Error),
}

/// The concrete WebSocket stream: plaintext only, so the TLS arm of
/// `MaybeTlsStream` is never taken.
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A connected Miniserver WebSocket, split into a writer and a reader.
pub struct Transport {
    write: SplitSink<Ws, Message>,
    read: SplitStream<Ws>,
    /// A header whose payload has not been read yet. Buffered so [`next_inbound`]
    /// stays cancel-safe: a `select!` caller cancels the read to service a timer,
    /// and on the next call we resume with the header intact rather than reading
    /// the payload frame as if it were a new header (which would desync).
    ///
    /// [`next_inbound`]: Transport::next_inbound
    pending_header: Option<MessageHeader>,
}

impl Transport {
    /// Connect to `ws://{host}:{port}/ws/rfc6455` with the `remotecontrol`
    /// subprotocol.
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        let url = format!("ws://{host}:{port}{WS_PATH}");
        let mut request = url.into_client_request()?;
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(SUBPROTOCOL));
        let (ws, _response) = connect_async(request).await?;
        let (write, read) = ws.split();
        Ok(Self {
            write,
            read,
            pending_header: None,
        })
    }

    /// Send a text command and await its `LL` JSON reply.
    ///
    /// Before the event stream is enabled the box does not push tables, so any
    /// stray binary message seen while awaiting the reply is skipped.
    pub async fn command(&mut self, cmd: &str) -> Result<LlResponse, TransportError> {
        self.send_text(cmd).await?;
        loop {
            match self.next_inbound().await? {
                Inbound::Text(text) => return LlResponse::parse(&text),
                Inbound::Binary { .. } => continue,
            }
        }
    }

    /// Send a raw text command without awaiting a reply.
    pub async fn send_text(&mut self, cmd: &str) -> Result<(), TransportError> {
        self.write.send(Message::Text(cmd.into())).await?;
        Ok(())
    }

    /// Read and classify the next inbound message, resolving the binary
    /// header/payload two-frame dance (including the estimated-then-exact case).
    ///
    /// Cancel-safe: the supervisor drives this in a `select!` and cancels it when
    /// a keepalive or refresh timer fires. The resolved header is buffered before
    /// its payload is awaited, so a cancellation mid-dance resumes on the next
    /// call instead of reading the payload as a bogus header.
    pub async fn next_inbound(&mut self) -> Result<Inbound, TransportError> {
        // Resume a dance a previous (cancelled) call left mid-flight.
        if let Some(header) = self.pending_header.take() {
            return self.read_payload(header).await;
        }
        match self.read_message().await? {
            Message::Text(t) => Ok(Inbound::Text(t.as_str().to_owned())),
            Message::Binary(b) => {
                let mut header = MessageHeader::parse(b.as_ref())?;
                if header.estimated {
                    // An exact header frame precedes the real payload. Cancelling
                    // here only drops the (redundant) estimated header; the exact
                    // one is a valid standalone header on the next call.
                    let Message::Binary(exact) = self.read_message().await? else {
                        return Err(TransportError::UnexpectedFrame);
                    };
                    header = MessageHeader::parse(exact.as_ref())?;
                }
                self.read_payload(header).await
            }
            _ => Err(TransportError::UnexpectedFrame),
        }
    }

    /// Read the payload frame for an already-resolved `header`.
    ///
    /// The header is re-buffered before the payload read is awaited and cleared
    /// only once it resolves, so a cancellation leaves the header in place for
    /// [`next_inbound`](Self::next_inbound) to resume from.
    async fn read_payload(&mut self, header: MessageHeader) -> Result<Inbound, TransportError> {
        if header.len == 0 {
            return Ok(Inbound::Binary {
                header,
                payload: Vec::new(),
            });
        }
        self.pending_header = Some(header);
        let msg = self.read_message().await;
        // Reached only if the read resolved (a cancellation drops the future
        // before this line, keeping pending_header set for the resume path).
        self.pending_header = None;
        match msg? {
            Message::Binary(payload) => Ok(Inbound::Binary {
                header,
                payload: payload.to_vec(),
            }),
            // Text-class messages announce a header, then deliver the body as a
            // text frame.
            Message::Text(t) => Ok(Inbound::Text(t.as_str().to_owned())),
            _ => Err(TransportError::UnexpectedFrame),
        }
    }

    /// Close the connection with a normal-closure (1000) status.
    pub async fn close(&mut self) -> Result<(), TransportError> {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        };
        self.write.send(Message::Close(Some(frame))).await?;
        Ok(())
    }

    /// Pull the next content frame, answering pings and skipping pongs/raw
    /// frames; a close or an ended stream surfaces as [`TransportError::Closed`],
    /// carrying the peer's close code when it sent one (so the supervisor can
    /// tell a routine drop from a 4003 lockout or a 4004/4005 user change).
    async fn read_message(&mut self) -> Result<Message, TransportError> {
        loop {
            let msg = self.read.next().await.ok_or(TransportError::Closed(None))??;
            match msg {
                Message::Ping(payload) => self.write.send(Message::Pong(payload)).await?,
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Close(frame) => {
                    return Err(TransportError::Closed(frame.map(|f| u16::from(f.code))));
                }
                content => return Ok(content),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_value_states_header() {
        // id=0x03, type=2 (value-states), info=0, reserved=0, len=48 (LE).
        let bytes = [0x03, 0x02, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00];
        let h = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(h.msg_type, MessageType::ValueStates);
        assert_eq!(h.len, 48);
        assert!(!h.estimated);
    }

    #[test]
    fn parses_estimated_header() {
        // info high bit set => length estimated; len=256 (LE).
        let bytes = [0x03, 0x03, 0x80, 0x00, 0x00, 0x01, 0x00, 0x00];
        let h = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(h.msg_type, MessageType::TextStates);
        assert_eq!(h.len, 256);
        assert!(h.estimated);
    }

    #[test]
    fn parses_keepalive_header() {
        let bytes = [0x03, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let h = MessageHeader::parse(&bytes).unwrap();
        assert_eq!(h.msg_type, MessageType::Keepalive);
        assert_eq!(h.len, 0);
        assert!(!h.estimated);
    }

    #[test]
    fn rejects_malformed_headers() {
        assert!(matches!(
            MessageHeader::parse(&[0x00, 0, 0, 0, 0, 0, 0, 0]),
            Err(TransportError::BadHeaderId(0)),
        ));
        assert!(matches!(
            MessageHeader::parse(&[0x03, 0x00]),
            Err(TransportError::ShortHeader(2)),
        ));
        assert!(matches!(
            MessageHeader::parse(&[0x03, 0x09, 0, 0, 0, 0, 0, 0]),
            Err(TransportError::UnknownType(9)),
        ));
    }

    #[test]
    fn parses_code_from_string_or_number() {
        let as_str: LlResponse =
            serde_json::from_str(r#"{"LL":{"control":"c","Code":"200","value":"x"}}"#).unwrap();
        assert_eq!(as_str.code(), 200);
        assert_eq!(as_str.control(), "c");

        let as_num: LlResponse =
            serde_json::from_str(r#"{"LL":{"control":"c","code":420,"value":1}}"#).unwrap();
        assert_eq!(as_num.code(), 420);
    }

    // The tungstenite handshake callback's error type (a full HTTP response)
    // is large; we only ever return Ok, so the lint is noise here.
    #[allow(clippy::result_large_err)]
    #[tokio::test]
    async fn command_round_trips_against_mock() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_hdr_async;
        use tokio_tungstenite::tungstenite::handshake::server::{
            ErrorResponse, Request, Response,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // Echo the subprotocol; the client fails the handshake otherwise.
            let mut ws = accept_hdr_async(stream, |_req: &Request, mut resp: Response| {
                resp.headers_mut()
                    .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(SUBPROTOCOL));
                Ok::<Response, ErrorResponse>(resp)
            })
            .await
            .unwrap();

            let msg = ws.next().await.unwrap().unwrap();
            assert_eq!(msg.into_text().unwrap().as_str(), "jdev/cfg/apiKey");

            let reply = r#"{"LL":{"control":"jdev/cfg/apiKey","value":"1.2.3.4","Code":"200"}}"#;
            ws.send(Message::Text(reply.into())).await.unwrap();
            let _ = ws.next().await; // wait for the client's close
        });

        let mut transport = Transport::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        let resp = transport.command("jdev/cfg/apiKey").await.unwrap();
        assert_eq!(resp.control(), "jdev/cfg/apiKey");
        assert_eq!(resp.code(), 200);
        assert_eq!(resp.value().as_str(), Some("1.2.3.4"));

        transport.close().await.unwrap();
        server.await.unwrap();
    }

    // The binary two-frame dance (header frame, then payload frame) must resolve
    // into a single `Inbound::Binary`; this is the path a cancelled read resumes.
    #[allow(clippy::result_large_err)]
    #[tokio::test]
    async fn next_inbound_resolves_header_then_payload() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_hdr_async;
        use tokio_tungstenite::tungstenite::handshake::server::{
            ErrorResponse, Request, Response,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_hdr_async(stream, |_req: &Request, mut resp: Response| {
                resp.headers_mut()
                    .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(SUBPROTOCOL));
                Ok::<Response, ErrorResponse>(resp)
            })
            .await
            .unwrap();

            // A value-states header (type 2, len 24) then a 24-byte payload.
            let header = [0x03, 0x02, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00];
            ws.send(Message::Binary(header.to_vec().into())).await.unwrap();
            ws.send(Message::Binary(vec![0xabu8; 24].into())).await.unwrap();
            let _ = ws.next().await; // wait for the client's close
        });

        let mut transport = Transport::connect(&addr.ip().to_string(), addr.port())
            .await
            .unwrap();
        match transport.next_inbound().await.unwrap() {
            Inbound::Binary { header, payload } => {
                assert_eq!(header.msg_type, MessageType::ValueStates);
                assert_eq!(header.len, 24);
                assert_eq!(payload, vec![0xabu8; 24]);
            }
            other => panic!("expected a binary message, got {other:?}"),
        }
        assert!(transport.pending_header.is_none(), "header must be cleared after the payload");

        transport.close().await.unwrap();
        server.await.unwrap();
    }
}
