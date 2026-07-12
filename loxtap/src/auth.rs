//! Token authentication and the token lifecycle.
//!
//! Once the encrypted channel is up ([`crate::crypto`]), we exchange the login
//! credentials for a long-lived token, all over the confidential `fenc` channel
//! so neither the password challenge nor the token is sniffable on a clear
//! transport.
//!
//! The flow this module drives (see [`authenticate`]):
//!
//! 1. `getkey2/{user}` (`fenc`) -> `{key, salt, hashAlg}`. We require
//!    `hashAlg == SHA256` and refuse anything else: a `SHA1` or absent value
//!    means the Miniserver still stores the password with an outdated hash.
//! 2. Derive two hashes with **different case rules**:
//!    - `pwHash = UPPERCASE( hex( SHA256( password + ":" + salt ) ) )`
//!    - `authHash = hex( HMAC-SHA256( key = hex(getkey2.key), user + ":" + pwHash ) )`
//!      left lowercase. Crossing the two case rules is a silent 401, so they are
//!      kept apart in [`password_hash`] and [`auth_hash`].
//! 3. `getjwt/{authHash}/{user}/{permission}/{uuid}/{info}` (`fenc`) -> the
//!    [`Token`] (`token`, `validUntil`, `key`).
//!
//! `permission = 4` (App): a daemon needs a weeks-long token, not the
//! short-lived Web permission.
//!
//! Once a token is held, the supervisor keeps it alive: [`refresh_token`] extends
//! it before expiry and [`kill_token`] invalidates it on shutdown. Both use a
//! *different* hash from login ([`token_hash`]): the plain `getkey` key (not
//! getkey2's) over the token itself, not `user:pwHash`.
//!
//! On-disk token persistence and `authwithtoken` reuse are deferred; Milestone 1
//! holds the token in memory only and does a fresh getjwt on every reconnect.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit as _, Mac as _};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tracing::warn;

use crate::config::Config;
use crate::crypto::{CryptoError, Session};
use crate::identity::ClientIdentity;
use crate::transport::{Transport, TransportError};

/// Command prefix that returns the per-user hashing key, salt and algorithm.
const CMD_GET_KEY2: &str = "jdev/sys/getkey2/";
/// Command prefix that exchanges the credential hash for a JSON web token.
const CMD_GET_JWT: &str = "jdev/sys/getjwt/";
/// Command that returns the ephemeral key used to hash a token on refresh/kill.
const CMD_GET_KEY: &str = "jdev/sys/getkey";
/// Command prefix that extends a token's validity.
const CMD_REFRESH_JWT: &str = "jdev/sys/refreshjwt/";
/// Command prefix that invalidates a token.
const CMD_KILL_TOKEN: &str = "jdev/sys/killtoken/";

/// Token permission level. `4` (App) yields a long-lived token, as a daemon
/// needs; `2` (Web) is short-lived, so do not use it here.
const TOKEN_PERMISSION: u8 = 4;

/// The hash algorithm we require the Miniserver to use, since Config 10.3.
const REQUIRED_HASH_ALG: &str = "SHA256";

/// Seconds between the unix epoch and the Loxone epoch (2009-01-01 00:00:00 UTC).
/// The server reports token expiry in seconds since this later epoch.
const LOXONE_EPOCH_OFFSET_SECS: u64 = 1_230_768_000;

type HmacSha256 = Hmac<Sha256>;

/// Errors from the token handshake.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("getkey2 failed with code {0}")]
    GetKey2(i64),
    #[error("getjwt failed with code {0}")]
    GetJwt(i64),
    #[error("getkey failed with code {0}")]
    GetKey(i64),
    #[error("refreshjwt failed with code {0}")]
    RefreshJwt(i64),
    #[error("killtoken failed with code {0}")]
    KillToken(i64),
    #[error("{context} reply missing the {field} field")]
    MissingField {
        context: &'static str,
        field: &'static str,
    },
    #[error(
        "the Miniserver returned password hash algorithm {found:?}, but this daemon \
         requires {REQUIRED_HASH_ALG}. Update the Miniserver firmware, or reset this \
         user's password so it is re-stored with the current hash."
    )]
    UnsupportedHashAlg { found: String },
    #[error("the getkey2 key was not valid hex")]
    KeyDecode,
}

impl AuthError {
    fn missing(context: &'static str, field: &'static str) -> Self {
        Self::MissingField { context, field }
    }
}

/// A live authentication token plus the material needed to refresh it.
///
/// The `token` and `key` are runtime secrets, so [`fmt::Debug`](std::fmt::Debug)
/// redacts them. The supervisor (a later commit) reads [`valid_until`] and
/// [`refresh_at`] to schedule a proactive refresh, and [`key`]/[`hash_alg`] to
/// hash the token for the refresh/kill commands.
///
/// [`valid_until`]: Token::valid_until
/// [`refresh_at`]: Token::refresh_at
/// [`key`]: Token::key
/// [`hash_alg`]: Token::hash_alg
#[derive(Clone)]
pub struct Token {
    /// The opaque JWT the server issued.
    token: String,
    /// Absolute expiry, converted from the Loxone epoch into a UTC instant.
    valid_until: SystemTime,
    /// When this token was obtained, so a refresh fraction of the lifetime is
    /// computable without a separate clock reading.
    issued_at: SystemTime,
    /// Hex key the server returned for hashing the token on refresh/kill.
    key: String,
    /// The negotiated hash algorithm (always SHA256 here; kept for refresh).
    hash_alg: String,
}

impl Token {
    /// The opaque token string, to be echoed back on reconnect via
    /// `authwithtoken` (a later commit).
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Absolute expiry as a UTC instant.
    pub fn valid_until(&self) -> SystemTime {
        self.valid_until
    }

    /// When the token was obtained.
    pub fn issued_at(&self) -> SystemTime {
        self.issued_at
    }

    /// The token-hashing key the server returned with the token.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The negotiated hash algorithm.
    pub fn hash_alg(&self) -> &str {
        &self.hash_alg
    }

    /// The instant at which to refresh, at `fraction` of the token lifetime
    /// (e.g. `0.9` for 90%). The supervisor caps this against a shorter ceiling.
    pub fn refresh_at(&self, fraction: f64) -> SystemTime {
        let lifetime = self
            .valid_until
            .duration_since(self.issued_at)
            .unwrap_or(Duration::ZERO);
        self.issued_at + lifetime.mul_f64(fraction)
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token")
            .field("token", &"<redacted>")
            .field("valid_until", &self.valid_until)
            .field("issued_at", &self.issued_at)
            .field("key", &"<redacted>")
            .field("hash_alg", &self.hash_alg)
            .finish()
    }
}

/// Authenticate to the Miniserver and obtain a token.
///
/// Runs the getkey2 -> hash -> getjwt sequence over the encrypted `session`,
/// which must already be [established](Session::establish). Both commands go
/// through `fenc` so the challenge and the token stay confidential.
pub async fn authenticate(
    session: &mut Session,
    transport: &mut Transport,
    config: &Config,
    identity: &ClientIdentity,
) -> Result<Token, AuthError> {
    // Step 1: getkey2 -> the per-user hashing key, salt and algorithm.
    let cmd = format!("{CMD_GET_KEY2}{}", config.user);
    let resp = session.fenc_command(transport, &cmd).await?;
    if resp.code() != 200 {
        return Err(AuthError::GetKey2(resp.code()));
    }
    let challenge = KeyChallenge::from_reply(resp.value())?;

    // Step 2: derive the two hashes (different case rules, kept apart).
    let pw_hash = password_hash(&config.password, &challenge.salt);
    let auth_hash = auth_hash(&config.user, &pw_hash, &challenge.key);

    // Step 3: getjwt -> the token. It must be sent encrypted or the server
    // declines it with 400, so it also rides fenc.
    let cmd = format!(
        "{CMD_GET_JWT}{auth_hash}/{}/{}/{}/{}",
        config.user, TOKEN_PERMISSION, identity.uuid, identity.info,
    );
    let resp = session.fenc_command(transport, &cmd).await?;
    if resp.code() != 200 {
        return Err(AuthError::GetJwt(resp.code()));
    }
    Token::from_reply(resp.value(), &challenge.hash_alg, SystemTime::now())
}

/// `tokenHash = hex( HMAC-SHA256( key = hex_decode(getkey), msg = token ) )`.
///
/// The refresh/kill hash is deliberately unlike the login [`auth_hash`]: the key
/// comes from the plain `getkey` (not getkey2), and the message is the *token*
/// itself, not `user:pwHash`. Left lowercase, like `authHash`.
pub fn token_hash(getkey_hex: &str, token: &str) -> Result<String, AuthError> {
    let key = hex_decode(getkey_hex)?;
    let mut mac = HmacSha256::new_from_slice(&key).expect("HMAC accepts any key length");
    mac.update(token.as_bytes());
    Ok(hex_lower(&mac.finalize().into_bytes()))
}

/// Fetch the ephemeral key used to hash the token for refresh/kill.
///
/// Goes through the `enc` channel: a live box *closes the connection* (WS 1000)
/// on a plain `getkey` once encryption is active, so it must be encrypted like
/// the other credential-adjacent commands. The reply rides `enc` rather than
/// `fenc` because the key itself is not a secret (it only proves token
/// possession in combination with the token), so a plaintext reply is fine.
async fn fetch_token_key(
    session: &mut Session,
    transport: &mut Transport,
) -> Result<String, AuthError> {
    let resp = session.enc_command(transport, CMD_GET_KEY).await?;
    if resp.code() != 200 {
        return Err(AuthError::GetKey(resp.code()));
    }
    resp.value()
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| AuthError::missing("getkey", "value"))
}

/// Refresh the token before it expires, returning the updated [`Token`].
///
/// Hashes the current token with a fresh `getkey` key ([`token_hash`]) and sends
/// `refreshjwt` over `fenc`, so a new token in the reply stays confidential on a
/// clear transport. Code 477 means the token already lapsed, so fall back to a
/// full re-authentication on the same session rather than give up.
pub async fn refresh_token(
    session: &mut Session,
    transport: &mut Transport,
    token: &Token,
    config: &Config,
    identity: &ClientIdentity,
) -> Result<Token, AuthError> {
    let key = fetch_token_key(session, transport).await?;
    let hash = token_hash(&key, token.token())?;
    let cmd = format!("{CMD_REFRESH_JWT}{hash}/{}", config.user);
    let resp = session.fenc_command(transport, &cmd).await?;
    match resp.code() {
        200 => Token::from_refresh_reply(resp.value(), token, SystemTime::now()),
        477 => {
            warn!("token expired before refresh, re-authenticating from scratch");
            authenticate(session, transport, config, identity).await
        }
        c => Err(AuthError::RefreshJwt(c)),
    }
}

/// Invalidate the token on the server, freeing its slot.
///
/// Best-effort on graceful shutdown: hashes the token the same way as
/// [`refresh_token`] and sends `killtoken` over `fenc`. Killing the very token
/// that authenticates the session makes the Miniserver close the socket, so a
/// disconnect right after the command is the success signal, not a failure.
pub async fn kill_token(
    session: &mut Session,
    transport: &mut Transport,
    token: &Token,
    user: &str,
) -> Result<(), AuthError> {
    let key = fetch_token_key(session, transport).await?;
    let hash = token_hash(&key, token.token())?;
    let cmd = format!("{CMD_KILL_TOKEN}{hash}/{user}");
    match session.fenc_command(transport, &cmd).await {
        Ok(resp) if resp.code() == 200 => Ok(()),
        Ok(resp) => Err(AuthError::KillToken(resp.code())),
        // The token is gone and the server hung up: that is the confirmation.
        Err(CryptoError::Transport(TransportError::Closed(_))) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The material a getkey2 reply carries.
struct KeyChallenge {
    /// The user salt to fold into the password hash.
    salt: String,
    /// The HMAC key, decoded from the reply's hex.
    key: Vec<u8>,
    /// The negotiated (and required-to-be-SHA256) hash algorithm.
    hash_alg: String,
}

impl KeyChallenge {
    /// Parse and validate a getkey2 reply value.
    ///
    /// A non-SHA256 (or absent) `hashAlg` is a hard error, never a fallback:
    /// this daemon does not implement the legacy SHA1/gettoken path.
    fn from_reply(value: &Value) -> Result<Self, AuthError> {
        let hash_alg = match value.get("hashAlg").and_then(Value::as_str) {
            Some(alg) if alg.eq_ignore_ascii_case(REQUIRED_HASH_ALG) => alg.to_owned(),
            other => {
                return Err(AuthError::UnsupportedHashAlg {
                    found: other.unwrap_or("(absent)").to_owned(),
                });
            }
        };
        let key_hex = value
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| AuthError::missing("getkey2", "key"))?;
        let salt = value
            .get("salt")
            .and_then(Value::as_str)
            .ok_or_else(|| AuthError::missing("getkey2", "salt"))?
            .to_owned();
        Ok(Self {
            salt,
            key: hex_decode(key_hex)?,
            hash_alg,
        })
    }
}

impl Token {
    /// Build a token from a getjwt reply value.
    ///
    /// `hash_alg` comes from the earlier getkey2 negotiation (getjwt does not
    /// echo it); `issued_at` is the moment of the request, so the lifetime is
    /// known for refresh scheduling.
    fn from_reply(
        value: &Value,
        hash_alg: &str,
        issued_at: SystemTime,
    ) -> Result<Self, AuthError> {
        let token = value
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| AuthError::missing("getjwt", "token"))?
            .to_owned();
        let valid_until_lox = value
            .get("validUntil")
            .and_then(as_u64_flexible)
            .ok_or_else(|| AuthError::missing("getjwt", "validUntil"))?;
        // The server may hand back a fresh token-hashing key; keep it if so.
        let key = value
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        Ok(Self {
            token,
            valid_until: loxone_epoch_to_system_time(valid_until_lox),
            issued_at,
            key,
            hash_alg: hash_alg.to_owned(),
        })
    }

    /// Build an updated token from a `refreshjwt` reply.
    ///
    /// The reply may return a fresh token JWT or only a new `validUntil`; keep
    /// the previous token and key when the reply omits them. `issued_at` is reset
    /// to now so the next refresh is scheduled off the new lifetime.
    fn from_refresh_reply(
        value: &Value,
        previous: &Token,
        issued_at: SystemTime,
    ) -> Result<Self, AuthError> {
        let token = value
            .get("token")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| previous.token.clone());
        let valid_until_lox = value
            .get("validUntil")
            .and_then(as_u64_flexible)
            .ok_or_else(|| AuthError::missing("refreshjwt", "validUntil"))?;
        let key = value
            .get("key")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| previous.key.clone());
        Ok(Self {
            token,
            valid_until: loxone_epoch_to_system_time(valid_until_lox),
            issued_at,
            key,
            hash_alg: previous.hash_alg.clone(),
        })
    }
}

/// `pwHash = UPPERCASE( hex( SHA256( password + ":" + salt ) ) )`.
///
/// The uppercasing is load-bearing: the server folds this exact string into the
/// HMAC below, so lowercase hex silently mismatches.
fn password_hash(password: &str, salt: &str) -> String {
    let digest = Sha256::digest(format!("{password}:{salt}").as_bytes());
    hex_upper(digest.as_slice())
}

/// `authHash = hex( HMAC-SHA256( key, user + ":" + pwHash ) )`, left lowercase.
///
/// The key is the raw bytes decoded from the getkey2 `key` hex.
fn auth_hash(user: &str, pw_hash: &str, key: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(format!("{user}:{pw_hash}").as_bytes());
    hex_lower(&mac.finalize().into_bytes())
}

/// Convert a Loxone-epoch second count into an absolute UTC instant.
fn loxone_epoch_to_system_time(secs_since_lox_epoch: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(LOXONE_EPOCH_OFFSET_SECS + secs_since_lox_epoch)
}

/// Read a JSON value as `u64`, whether it arrives as a number or a string.
fn as_u64_flexible(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

/// Decode a hex string into bytes.
fn hex_decode(s: &str) -> Result<Vec<u8>, AuthError> {
    if !s.len().is_multiple_of(2) {
        return Err(AuthError::KeyDecode);
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| AuthError::KeyDecode))
        .collect()
}

/// Lowercase hex encoding.
fn hex_lower(bytes: &[u8]) -> String {
    hex_with(bytes, false)
}

/// Uppercase hex encoding.
fn hex_upper(bytes: &[u8]) -> String {
    hex_with(bytes, true)
}

fn hex_with(bytes: &[u8], upper: bool) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        if upper {
            let _ = write!(s, "{b:02X}");
        } else {
            let _ = write!(s, "{b:02x}");
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Synthetic (invented) credential tuple — never the real service account.
    // The expected hashes below were derived independently with Python's
    // hashlib/hmac over this exact tuple, then pasted here:
    //   pw   = sha256(f"{PASSWORD}:{SALT}").hexdigest().upper()
    //   auth = hmac(bytes.fromhex(KEY_HEX), f"{USER}:{pw}", sha256).hexdigest()
    const USER: &str = "svc-logger";
    const PASSWORD: &str = "Corr3ct-Horse-Batt3ry";
    const SALT: &str = "a1b2c3d4e5f6";
    const KEY_HEX: &str = "0011223344556677889900aabbccddeeff00112233445566778899aabbccddee";
    const EXPECT_PW_HASH: &str =
        "986BA72F7AE4737CDA36C855BBE9D40189F6D835110952E5F892C8ABFE114F9D";
    const EXPECT_AUTH_HASH: &str =
        "2ae3595f21afe2df510e77e430e2c6e6934e68687144aed80931a1f365119efe";

    #[test]
    fn password_hash_is_uppercase_and_matches_vector() {
        let pw = password_hash(PASSWORD, SALT);
        assert_eq!(pw, EXPECT_PW_HASH);
        assert_eq!(pw, pw.to_uppercase(), "pwHash must be uppercase hex");
    }

    #[test]
    fn auth_hash_is_lowercase_and_matches_vector() {
        let key = hex_decode(KEY_HEX).unwrap();
        let auth = auth_hash(USER, EXPECT_PW_HASH, &key);
        assert_eq!(auth, EXPECT_AUTH_HASH);
        assert_eq!(auth, auth.to_lowercase(), "authHash must stay lowercase hex");
    }

    #[test]
    fn getkey2_accepts_sha256_case_insensitively() {
        let reply = json!({"key": KEY_HEX, "salt": SALT, "hashAlg": "sha256"});
        let challenge = KeyChallenge::from_reply(&reply).unwrap();
        assert_eq!(challenge.salt, SALT);
        assert_eq!(challenge.key, hex_decode(KEY_HEX).unwrap());
        assert_eq!(challenge.hash_alg, "sha256");
    }

    #[test]
    fn getkey2_rejects_sha1_without_panicking() {
        let reply = json!({"key": KEY_HEX, "salt": SALT, "hashAlg": "SHA1"});
        match KeyChallenge::from_reply(&reply) {
            Err(AuthError::UnsupportedHashAlg { found }) => assert_eq!(found, "SHA1"),
            _ => panic!("expected UnsupportedHashAlg for a SHA1 reply"),
        }
    }

    #[test]
    fn getkey2_rejects_absent_hash_alg() {
        let reply = json!({"key": KEY_HEX, "salt": SALT});
        match KeyChallenge::from_reply(&reply) {
            Err(AuthError::UnsupportedHashAlg { found }) => assert_eq!(found, "(absent)"),
            _ => panic!("expected UnsupportedHashAlg for an absent hashAlg"),
        }
    }

    #[test]
    fn getkey2_reports_missing_key() {
        let reply = json!({"salt": SALT, "hashAlg": "SHA256"});
        assert!(matches!(
            KeyChallenge::from_reply(&reply),
            Err(AuthError::MissingField {
                context: "getkey2",
                field: "key"
            })
        ));
    }

    #[test]
    fn loxone_epoch_maps_to_utc() {
        // validUntil == 0 is the Loxone epoch itself, 2009-01-01 00:00:00 UTC.
        let at_epoch = loxone_epoch_to_system_time(0);
        assert_eq!(
            at_epoch.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            LOXONE_EPOCH_OFFSET_SECS
        );
        // A sample expiry cross-checked against Python's datetime.
        let sample = loxone_epoch_to_system_time(554_400_000);
        assert_eq!(sample.duration_since(UNIX_EPOCH).unwrap().as_secs(), 1_785_168_000);
    }

    #[test]
    fn getjwt_reply_parses_into_token() {
        let issued = UNIX_EPOCH + Duration::from_secs(1_785_000_000);
        let reply = json!({
            "token": "eyJhbGciOi.header.sig",
            "validUntil": 554_400_000u64,
            "key": "abcdef",
            "tokenRights": 4,
        });
        let token = Token::from_reply(&reply, "SHA256", issued).unwrap();
        assert_eq!(token.token(), "eyJhbGciOi.header.sig");
        assert_eq!(token.key(), "abcdef");
        assert_eq!(token.hash_alg(), "SHA256");
        assert_eq!(
            token.valid_until().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1_785_168_000
        );
        // Refresh at 90% of the (issued .. valid_until) span.
        let lifetime = 1_785_168_000 - 1_785_000_000;
        let expect_refresh = 1_785_000_000 + (lifetime as f64 * 0.9) as u64;
        assert_eq!(
            token
                .refresh_at(0.9)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expect_refresh
        );
    }

    #[test]
    fn getjwt_reply_tolerates_string_valid_until() {
        let reply = json!({"token": "t", "validUntil": "554400000"});
        let token = Token::from_reply(&reply, "SHA256", SystemTime::now()).unwrap();
        assert_eq!(
            token.valid_until().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1_785_168_000
        );
        // An absent key is tolerated (kept empty).
        assert_eq!(token.key(), "");
    }

    #[test]
    fn getjwt_reports_missing_token() {
        let reply = json!({"validUntil": 1});
        assert!(matches!(
            Token::from_reply(&reply, "SHA256", SystemTime::now()),
            Err(AuthError::MissingField {
                context: "getjwt",
                field: "token"
            })
        ));
    }

    #[test]
    fn token_debug_redacts_secrets() {
        let reply = json!({"token": "super-secret-jwt", "validUntil": 1, "key": "s3cr3tkey"});
        let token = Token::from_reply(&reply, "SHA256", SystemTime::now()).unwrap();
        let rendered = format!("{token:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("super-secret-jwt"));
        assert!(!rendered.contains("s3cr3tkey"));
    }

    #[test]
    fn hex_decode_rejects_odd_and_non_hex() {
        assert!(matches!(hex_decode("abc"), Err(AuthError::KeyDecode)));
        assert!(matches!(hex_decode("zz"), Err(AuthError::KeyDecode)));
        assert_eq!(hex_decode("00ff10").unwrap(), vec![0x00, 0xff, 0x10]);
    }

    #[test]
    fn token_hash_matches_independent_vector() {
        // Derived independently with Python's hmac/hashlib over this synthetic
        // (key, token) pair:
        //   hmac.new(bytes.fromhex(KEY), TOKEN.encode(), sha256).hexdigest()
        const KEY_HEX: &str =
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        const TOKEN: &str = "eyJhbGciOiJIUzI1NiJ9.synthetic-token-body.sig";
        const EXPECT: &str =
            "7dd15b5e261db5f2528618b6a86939cb677caf18bffdc6dba037d4d00e81654f";
        let th = token_hash(KEY_HEX, TOKEN).unwrap();
        assert_eq!(th, EXPECT);
        assert_eq!(th, th.to_lowercase(), "tokenHash must stay lowercase hex");
    }

    #[test]
    fn token_hash_rejects_non_hex_key() {
        assert!(matches!(token_hash("zz", "tok"), Err(AuthError::KeyDecode)));
    }

    #[test]
    fn refresh_reply_advances_validity_and_keeps_token_when_absent() {
        // A refresh reply carrying only a new validUntil keeps the token/key and
        // resets issued_at, so the next refresh is scheduled off the new life.
        let issued = UNIX_EPOCH + Duration::from_secs(1_785_000_000);
        let base = Token::from_reply(
            &json!({"token": "orig-token", "validUntil": 100u64, "key": "origkey"}),
            "SHA256",
            issued,
        )
        .unwrap();
        let refreshed_at = UNIX_EPOCH + Duration::from_secs(1_785_100_000);
        let updated =
            Token::from_refresh_reply(&json!({"validUntil": 554_400_000u64}), &base, refreshed_at)
                .unwrap();
        assert_eq!(updated.token(), "orig-token");
        assert_eq!(updated.key(), "origkey");
        assert_eq!(updated.issued_at(), refreshed_at);
        assert_eq!(
            updated.valid_until().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1_785_168_000
        );
    }

    #[test]
    fn refresh_reply_takes_new_token_when_present() {
        let base = Token::from_reply(
            &json!({"token": "orig", "validUntil": 1u64, "key": "k0"}),
            "SHA256",
            SystemTime::now(),
        )
        .unwrap();
        let updated = Token::from_refresh_reply(
            &json!({"token": "fresh", "validUntil": 554_400_000u64, "key": "k1"}),
            &base,
            SystemTime::now(),
        )
        .unwrap();
        assert_eq!(updated.token(), "fresh");
        assert_eq!(updated.key(), "k1");
    }

    #[test]
    fn refresh_reply_requires_valid_until() {
        let base = Token::from_reply(
            &json!({"token": "orig", "validUntil": 1u64}),
            "SHA256",
            SystemTime::now(),
        )
        .unwrap();
        assert!(matches!(
            Token::from_refresh_reply(&json!({"token": "fresh"}), &base, SystemTime::now()),
            Err(AuthError::MissingField {
                context: "refreshjwt",
                field: "validUntil"
            })
        ));
    }
}
