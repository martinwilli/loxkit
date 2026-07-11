//! RSA/AES session key exchange and the encrypted command channel.
//!
//! The Miniserver declines credential commands over a clear channel, so before
//! authenticating we set up an application-layer cipher: fetch the server's RSA
//! public key, hand it a freshly generated AES-256 session secret, and from
//! then on wrap every command through that secret.
//!
//! The flow this module drives (see [`handshake`]):
//!
//! 1. `getPublicKey` -> parse the SPKI RSA key ([`ServerKey::fetch`]).
//! 2. Verify the key's fingerprint against the configured pin, failing closed
//!    before we ever trust it ([`ServerKey::verify_pin`]).
//! 3. Generate a 32-byte AES key + 16-byte IV, RSA-encrypt them, and run
//!    `keyexchange` to activate encryption ([`Session::establish`]).
//! 4. Wrap subsequent commands as `enc` (plaintext reply) or `fenc` (encrypted
//!    reply) through the [`Session`].
//!
//! # Landmines baked in here
//!
//! - The AES key/IV travel as **hex text** inside the RSA payload
//!   (`hexkey:hexiv`) but are used as **raw bytes** for AES.
//! - AES-256-CBC with **zero-byte** padding (not PKCS7); replies are
//!   zero-terminated, so the plaintext is right-stripped of `\0`.
//! - The `keyexchange` base64 rides raw, but `enc`/`fenc` ciphertext is
//!   URL-encoded into the command path.

use std::time::{Duration, Instant};

use aes::Aes256;
use aes::cipher::{BlockModeDecrypt as _, BlockModeEncrypt as _, KeyIvInit as _};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use block_padding::{NoPadding, ZeroPadding};
use rand::RngCore as _;
use rand::rngs::OsRng;
use rsa::pkcs8::DecodePublicKey as _;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

use crate::transport::{Inbound, LlResponse, Transport, TransportError};

/// Resource path that returns the server's RSA public key. Served over HTTP,
/// not the websocket (the websocket rejects it), so it is fetched separately.
const PATH_GET_PUBLIC_KEY: &str = "jdev/sys/getPublicKey";
/// Command prefix that hands the server our RSA-wrapped session secret.
const CMD_KEY_EXCHANGE: &str = "jdev/sys/keyexchange/";
/// Command prefix for an encrypted request with a plaintext reply.
const CMD_ENC: &str = "jdev/sys/enc/";
/// Command prefix for an encrypted request *and* an encrypted reply.
const CMD_FENC: &str = "jdev/sys/fenc/";

/// Rotate the command salt after this many uses.
const SALT_MAX_USES: u32 = 30;
/// Rotate the command salt once it reaches this age.
const SALT_MAX_AGE: Duration = Duration::from_secs(3600);

type Aes256CbcEnc = cbc::Encryptor<Aes256>;
type Aes256CbcDec = cbc::Decryptor<Aes256>;

/// Errors from the crypto handshake and the encrypted channel.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("network error fetching the public key: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed HTTP reply fetching the public key")]
    HttpReply,
    #[error("getPublicKey failed with code {0}")]
    PublicKey(i64),
    #[error("getPublicKey reply was not a string")]
    PublicKeyShape,
    #[error("public key PEM had no base64 body")]
    PemBody,
    #[error("base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("could not parse the server SPKI public key: {0}")]
    Spki(String),
    #[error("RSA encryption failed: {0}")]
    Rsa(#[from] rsa::Error),
    #[error("keyexchange failed with code {0}")]
    KeyExchange(i64),
    #[error("could not decrypt the encrypted reply")]
    Decrypt,
    #[error(transparent)]
    Pin(#[from] PinError),
}

/// A server-key pin failure.
///
/// Both variants are fatal: the connection flow must refuse to start rather
/// than encrypt a session key to an unvouched server. The supervisor treats
/// this as terminal (a human decision), never a transient drop to retry.
#[derive(Debug, thiserror::Error)]
pub enum PinError {
    #[error(
        "no server key pin configured. the server presented this key:\n\
         \n    server_key_pin: \"{presented}\"\n\n\
         confirm this is your Miniserver, then paste that line into the config."
    )]
    Missing { presented: String },
    #[error(
        "server key pin mismatch, refusing to continue:\n\
         \n    configured: {configured}\n    presented:  {presented}\n\n\
         the server key changed. either a Miniserver factory reset rotated it, \
         or someone is machine-in-the-middle on this connection."
    )]
    Mismatch {
        configured: String,
        presented: String,
    },
}

/// The server's RSA public key, fetched over HTTP.
///
/// Retains the raw SPKI DER so the key can be fingerprinted and pinned before
/// we trust it in the key exchange.
pub struct ServerKey {
    public_key: RsaPublicKey,
    spki_der: Vec<u8>,
}

impl ServerKey {
    /// Fetch and parse the server's public key.
    ///
    /// `getPublicKey` is served over HTTP, not the websocket, so this opens a
    /// short-lived TCP connection and speaks a minimal HTTP/1.1 GET rather than
    /// pull in an HTTP client crate.
    pub async fn fetch(host: &str, port: u16) -> Result<Self, CryptoError> {
        let body = http_get(host, port, PATH_GET_PUBLIC_KEY).await?;
        let resp = LlResponse::parse(&body)?;
        if resp.code() != 200 {
            return Err(CryptoError::PublicKey(resp.code()));
        }
        let cert = resp.value().as_str().ok_or(CryptoError::PublicKeyShape)?;
        Self::from_cert_pem(cert)
    }

    /// Parse the mislabeled `CERTIFICATE` PEM the server returns into an SPKI
    /// RSA key. Separated from [`fetch`](Self::fetch) so it is unit-testable
    /// without a live transport.
    fn from_cert_pem(cert_pem: &str) -> Result<Self, CryptoError> {
        let body = spki_base64_body(cert_pem)?;
        let spki_der = BASE64.decode(&body)?;
        let pem = rewrite_to_public_key_pem(&body);
        let public_key =
            RsaPublicKey::from_public_key_pem(&pem).map_err(|e| CryptoError::Spki(e.to_string()))?;
        Ok(Self {
            public_key,
            spki_der,
        })
    }

    /// The `sha256:<hex>` fingerprint of the DER SPKI public key.
    ///
    /// This equals `openssl pkey -pubin -outform DER | sha256sum`, so an
    /// operator can reproduce and vouch for it by hand.
    pub fn fingerprint(&self) -> String {
        let digest = Sha256::digest(&self.spki_der);
        format!("sha256:{}", hex_encode(digest.as_slice()))
    }

    /// Check the presented key against the configured pin, failing closed.
    ///
    /// Pass `None` when no pin is configured yet; the returned [`PinError`]
    /// then carries the exact `server_key_pin:` line to paste.
    pub fn verify_pin(&self, configured: Option<&str>) -> Result<(), PinError> {
        let presented = self.fingerprint();
        match configured {
            None => Err(PinError::Missing { presented }),
            Some(pin) if pin.trim() == presented => Ok(()),
            Some(pin) => Err(PinError::Mismatch {
                configured: pin.trim().to_owned(),
                presented,
            }),
        }
    }
}

/// Strip the PEM armor and all whitespace, leaving just the base64 SPKI body.
///
/// The Miniserver returns the key wrapped in `CERTIFICATE` armor (a misnomer;
/// it is a plain SPKI public key) with the body on a single line. This tolerates
/// either armor label and any (or no) internal newlines.
fn spki_base64_body(pem: &str) -> Result<String, CryptoError> {
    let body: String = pem
        .replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
        .replace("-----BEGIN PUBLIC KEY-----", "")
        .replace("-----END PUBLIC KEY-----", "")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if body.is_empty() {
        return Err(CryptoError::PemBody);
    }
    Ok(body)
}

/// Re-wrap a base64 SPKI body as a well-formed `PUBLIC KEY` PEM (64-col lines),
/// the shape the RSA parser expects.
fn rewrite_to_public_key_pem(body: &str) -> String {
    let mut out = String::from("-----BEGIN PUBLIC KEY-----\n");
    let chars: Vec<char> = body.chars().collect();
    for chunk in chars.chunks(64) {
        out.extend(chunk);
        out.push('\n');
    }
    out.push_str("-----END PUBLIC KEY-----\n");
    out
}

/// An established encrypted channel: the AES session secret plus the rotating
/// command salt.
///
/// The auth layer rides this: it sends `getkey2`/`getjwt` through
/// [`fenc_command`](Self::fenc_command) so both the challenge and the token stay
/// confidential on a clear transport.
pub struct Session {
    /// Raw AES-256 key (used as bytes; travels to the server as hex).
    key: [u8; 32],
    /// Raw AES-CBC IV (used as bytes; travels to the server as hex).
    iv: [u8; 16],
    /// Current command salt (hex); rotated periodically.
    salt: String,
    /// Times the current salt has been used.
    uses: u32,
    /// When the current salt was generated.
    salt_since: Instant,
}

impl Session {
    /// Run the key exchange: generate a session secret, RSA-encrypt it to the
    /// server's key, and activate encryption via `keyexchange`.
    pub async fn establish(
        transport: &mut Transport,
        server_key: &ServerKey,
    ) -> Result<Self, CryptoError> {
        let session = Self::generate();
        // The key/IV cross the wire as hex text inside the RSA payload, though
        // they are raw bytes for AES.
        let secret = format!(
            "{}:{}",
            hex_encode(&session.key),
            hex_encode(&session.iv)
        );
        let mut rng = OsRng;
        let wrapped = server_key
            .public_key
            .encrypt(&mut rng, Pkcs1v15Encrypt, secret.as_bytes())?;
        let payload = BASE64.encode(&wrapped);
        // keyexchange base64 rides raw (no URL-encoding), unlike enc/fenc.
        let resp = transport
            .command(&format!("{CMD_KEY_EXCHANGE}{payload}"))
            .await?;
        if resp.code() != 200 {
            return Err(CryptoError::KeyExchange(resp.code()));
        }
        Ok(session)
    }

    /// Generate a fresh session secret (raw key + IV) and an initial salt.
    fn generate() -> Self {
        let mut key = [0u8; 32];
        let mut iv = [0u8; 16];
        let mut rng = OsRng;
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut iv);
        Self {
            key,
            iv,
            salt: generate_salt(),
            uses: 0,
            salt_since: Instant::now(),
        }
    }

    /// Send an `enc`-wrapped command; the reply comes back in plaintext.
    pub async fn enc_command(
        &mut self,
        transport: &mut Transport,
        cmd: &str,
    ) -> Result<LlResponse, CryptoError> {
        let wrapped = self.wrap(cmd, CMD_ENC);
        Ok(transport.command(&wrapped).await?)
    }

    /// Send a `fenc`-wrapped command and decrypt the reply.
    ///
    /// Unlike `enc`, a `fenc` reply is fully encrypted: the whole reply frame is
    /// base64 ciphertext (no `LL` wrapper), whose plaintext is itself a full
    /// `LL` envelope. So read the raw text frame directly rather than through
    /// [`command`](Transport::command) (which would try to parse the ciphertext
    /// as JSON), decrypt it, and hand back the recovered envelope.
    pub async fn fenc_command(
        &mut self,
        transport: &mut Transport,
        cmd: &str,
    ) -> Result<LlResponse, CryptoError> {
        let wrapped = self.wrap(cmd, CMD_FENC);
        transport.send_text(&wrapped).await?;
        // Skip any stray binary frame while awaiting the ciphertext reply.
        let raw = loop {
            match transport.next_inbound().await? {
                Inbound::Text(t) => break t,
                Inbound::Binary { .. } => continue,
            }
        };
        let ciphertext = BASE64.decode(raw.trim())?;
        let plaintext = self.decrypt(&ciphertext)?;
        Ok(LlResponse::parse(&plaintext)?)
    }

    /// Build an `enc`/`fenc` command string: salt-prefix, AES-encrypt, base64,
    /// then URL-encode into the given command path.
    fn wrap(&mut self, cmd: &str, endpoint: &str) -> String {
        let plaintext = self.salted_plaintext(cmd);
        let ciphertext = self.encrypt(plaintext.as_bytes());
        let encoded = url_encode_base64(&BASE64.encode(&ciphertext));
        format!("{endpoint}{encoded}")
    }

    /// Prefix a command with the salt, rotating it when stale.
    ///
    /// A fresh salt is announced once via `nextSalt/{prev}/{new}/…`; steady
    /// state is `salt/{current}/…`.
    fn salted_plaintext(&mut self, cmd: &str) -> String {
        let stale = self.uses >= SALT_MAX_USES || self.salt_since.elapsed() >= SALT_MAX_AGE;
        let prefix = if stale {
            let prev = std::mem::replace(&mut self.salt, generate_salt());
            self.uses = 0;
            self.salt_since = Instant::now();
            format!("nextSalt/{prev}/{}", self.salt)
        } else {
            format!("salt/{}", self.salt)
        };
        self.uses += 1;
        format!("{prefix}/{cmd}")
    }

    /// AES-256-CBC encrypt with zero-byte padding.
    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        Aes256CbcEnc::new_from_slices(&self.key, &self.iv)
            .expect("32-byte key and 16-byte IV are valid")
            .encrypt_padded_vec::<ZeroPadding>(plaintext)
    }

    /// AES-256-CBC decrypt, then right-strip the server's zero termination.
    fn decrypt(&self, ciphertext: &[u8]) -> Result<String, CryptoError> {
        let plaintext = Aes256CbcDec::new_from_slices(&self.key, &self.iv)
            .expect("32-byte key and 16-byte IV are valid")
            .decrypt_padded_vec::<NoPadding>(ciphertext)
            .map_err(|_| CryptoError::Decrypt)?;
        let end = plaintext.iter().rposition(|&b| b != 0).map_or(0, |p| p + 1);
        String::from_utf8(plaintext[..end].to_vec()).map_err(|_| CryptoError::Decrypt)
    }
}

/// Fetch the server key, verify it against the configured pin, then run the key
/// exchange and return the live session.
///
/// The pin gate sits between the fetch and the exchange by construction: we
/// refuse to encrypt our session key to a server we have not vouched for. The
/// `host`/`port` drive the HTTP `getPublicKey` fetch; the key exchange rides the
/// already-connected websocket `transport`.
pub async fn handshake(
    transport: &mut Transport,
    host: &str,
    port: u16,
    configured_pin: Option<&str>,
) -> Result<Session, CryptoError> {
    let server_key = ServerKey::fetch(host, port).await?;
    server_key.verify_pin(configured_pin)?;
    Session::establish(transport, &server_key).await
}

/// Minimal HTTP/1.1 GET over a fresh TCP connection, returning the response body.
///
/// The Miniserver frames replies with `Content-Length` and honors
/// `Connection: close`, so reading to EOF yields the whole message; the body is
/// whatever follows the blank line.
async fn http_get(host: &str, port: u16, path: &str) -> Result<String, CryptoError> {
    let mut stream = TcpStream::connect((host, port)).await?;
    let request = format!("GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    let (_headers, body) = text.split_once("\r\n\r\n").ok_or(CryptoError::HttpReply)?;
    Ok(body.to_owned())
}

/// Generate a short random command salt as hex.
fn generate_salt() -> String {
    let mut buf = [0u8; 2];
    OsRng.fill_bytes(&mut buf);
    hex_encode(&buf)
}

/// Lower-case hex encoding.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Percent-encode a base64 string for embedding in a Miniserver command path.
///
/// Base64 output only contains `+`, `/`, `=` beyond the URL-safe set; the server
/// URL-decodes the command tail, so escaping all three is safe.
fn url_encode_base64(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '+' => out.push_str("%2B"),
            '/' => out.push_str("%2F"),
            '=' => out.push_str("%3D"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // A synthetic RSA public key (freshly generated offline, NOT any real
    // server's), wrapped exactly as the Miniserver returns it: `CERTIFICATE`
    // armor with the whole thing on a single line, no internal newlines.
    const SYNTH_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA58XDe2vyExplc7E+d4QMyyvb8/j5zpAiowbAJ9u0NZPJePtYYnDJKJBj+c/2U+57H071Wgky+xXBiY0pPCkJPR7yuu+lKgNpHOTRORwobCxC/oXwg2OcTbznW2WmtzhI4eWrKaZiHPj+D+BZe03H76XeYG6BEdwD3+7Kg9VIqQM41Ro1TdBeECDaVjY6BI+DXXyY+/N3pH2m2yGmsIgQl8VQZ1qVsprGuusrEsLS1tLQzkQgNWbcwPxNa86tBIcE27IjKIUxyZJbUGTKwsvZl6tIQXNjKjpPw6vXuyMFal/5y35z5q9PDmDx/J+HBM/eL/1rTkPTEx6Xdf4LpExM7wIDAQAB-----END CERTIFICATE-----";

    // The precomputed SHA-256 of that key's DER SPKI, i.e. the pin an operator
    // would paste. Reproducible with:
    //   openssl pkey -pubin -outform DER | sha256sum
    const SYNTH_PIN: &str =
        "sha256:1e6eea5c17ed190396f589153450a51ca435feeacd80d7354c32718e29e31252";

    #[test]
    fn aes_zero_pad_cbc_round_trips() {
        let key = [0x11u8; 32];
        let iv = [0x22u8; 16];
        let session = Session {
            key,
            iv,
            salt: "abcd".to_owned(),
            uses: 0,
            salt_since: Instant::now(),
        };
        // Not a block multiple, so zero padding kicks in.
        let plaintext = b"salt/abcd/jdev/sys/getkey2/some-user";
        let ciphertext = session.encrypt(plaintext);
        assert_eq!(ciphertext.len() % 16, 0);
        assert_ne!(ciphertext, plaintext);
        let recovered = session.decrypt(&ciphertext).unwrap();
        // Right-strip of the zero padding recovers the exact plaintext.
        assert_eq!(recovered.as_bytes(), plaintext);
    }

    #[test]
    fn aes_round_trips_block_aligned_plaintext() {
        let session = Session {
            key: [0xabu8; 32],
            iv: [0xcdu8; 16],
            salt: "beef".to_owned(),
            uses: 0,
            salt_since: Instant::now(),
        };
        // Exactly 16 bytes: no padding is added, and nothing is stripped.
        let plaintext = b"0123456789abcdef";
        let ciphertext = session.encrypt(plaintext);
        assert_eq!(ciphertext.len(), 16);
        assert_eq!(session.decrypt(&ciphertext).unwrap().as_bytes(), plaintext);
    }

    #[test]
    fn rewrites_certificate_pem_to_public_key() {
        let body = spki_base64_body(SYNTH_CERT_PEM).unwrap();
        let pem = rewrite_to_public_key_pem(&body);
        assert!(pem.starts_with("-----BEGIN PUBLIC KEY-----\n"));
        assert!(pem.trim_end().ends_with("-----END PUBLIC KEY-----"));
        assert!(!pem.contains("CERTIFICATE"));
        // Body lines (between the markers) are wrapped at 64 columns.
        for line in pem.lines().filter(|l| !l.contains("PUBLIC KEY")) {
            assert!(line.len() <= 64, "over-long PEM line: {line:?}");
        }
    }

    #[test]
    fn parses_rewritten_public_key() {
        // The rewrite must yield a PEM the RSA parser accepts.
        assert!(ServerKey::from_cert_pem(SYNTH_CERT_PEM).is_ok());
    }

    #[test]
    fn fingerprint_matches_known_vector() {
        let key = ServerKey::from_cert_pem(SYNTH_CERT_PEM).unwrap();
        assert_eq!(key.fingerprint(), SYNTH_PIN);
    }

    #[test]
    fn verify_pin_accepts_match() {
        let key = ServerKey::from_cert_pem(SYNTH_CERT_PEM).unwrap();
        assert!(key.verify_pin(Some(SYNTH_PIN)).is_ok());
        // Surrounding whitespace in the configured value is tolerated.
        assert!(key.verify_pin(Some(&format!("  {SYNTH_PIN}  "))).is_ok());
    }

    #[test]
    fn verify_pin_rejects_mismatch_carrying_both_pins() {
        let key = ServerKey::from_cert_pem(SYNTH_CERT_PEM).unwrap();
        let wrong = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let err = key.verify_pin(Some(wrong)).unwrap_err();
        assert!(matches!(err, PinError::Mismatch { .. }));
        let msg = err.to_string();
        assert!(msg.contains(wrong), "mismatch must show the configured pin");
        assert!(msg.contains(SYNTH_PIN), "mismatch must show the presented pin");
    }

    #[test]
    fn verify_pin_rejects_missing_with_paste_line() {
        let key = ServerKey::from_cert_pem(SYNTH_CERT_PEM).unwrap();
        let err = key.verify_pin(None).unwrap_err();
        assert!(matches!(err, PinError::Missing { .. }));
        let msg = err.to_string();
        assert!(msg.contains(SYNTH_PIN), "missing must show the presented pin");
        assert!(
            msg.contains(&format!("server_key_pin: \"{SYNTH_PIN}\"")),
            "missing must print the exact paste-ready line"
        );
    }

    #[test]
    fn rejects_empty_pem_body() {
        let empty = "-----BEGIN CERTIFICATE----------END CERTIFICATE-----";
        assert!(matches!(
            spki_base64_body(empty),
            Err(CryptoError::PemBody)
        ));
    }

    #[test]
    fn url_encodes_base64_special_chars() {
        assert_eq!(url_encode_base64("ab+cd/ef=="), "ab%2Bcd%2Fef%3D%3D");
        assert_eq!(url_encode_base64("plainABC123"), "plainABC123");
    }

    #[test]
    fn salt_rotation_announces_next_salt() {
        let mut session = Session {
            key: [0u8; 32],
            iv: [0u8; 16],
            salt: "aaaa".to_owned(),
            uses: 0,
            salt_since: Instant::now(),
        };
        // Steady state uses the current salt.
        let first = session.salted_plaintext("cmd");
        assert_eq!(first, "salt/aaaa/cmd");
        // Force staleness: the next command announces a rotation.
        session.uses = SALT_MAX_USES;
        let rotated = session.salted_plaintext("cmd");
        assert!(rotated.starts_with("nextSalt/aaaa/"));
        assert!(rotated.ends_with("/cmd"));
        assert_eq!(session.uses, 1);
        // And settles back to the new salt.
        let after = session.salted_plaintext("cmd");
        assert!(after.starts_with("salt/"));
        assert!(!after.starts_with("salt/aaaa/"));
    }
}
