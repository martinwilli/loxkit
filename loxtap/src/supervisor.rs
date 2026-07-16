//! The connection supervisor: the layer that turns one live session into a
//! long-running daemon.
//!
//! A single session ([`connect_and_run`]) is: connect, run the pin-gated crypto
//! handshake, authenticate for a token, load the structure map, then stream and
//! dispatch value events. This module wraps that in the resilience the "runs in
//! the demo, dies overnight" gap needs:
//!
//! - **Reconnect** with exponential backoff and jitter on any drop or close;
//!   every reconnect re-runs the full handshake, so the server-key pin is
//!   re-verified each time.
//! - **Keepalive** every ~240 s (the server drops an idle socket after five
//!   minutes), sent as a plain `keepalive`.
//! - **Token refresh** proactively at ~90% of the token lifetime (capped at a
//!   day), and reactively when a refresh sees code 477 (expired).
//! - **Graceful shutdown** on Ctrl-C / SIGTERM: kill the token, then close the
//!   socket with a normal-closure (1000) frame.
//!
//! # Close codes
//!
//! Not every disconnect is equal ([`classify_close`]): a 4003 (blocked for too
//! many failed logins) must *not* be retried fast, or the lockout only extends;
//! 4004/4005 (the user changed) drop the token and re-authenticate clean; 4007
//! (update in progress) / 4008 (no event slots) and routine drops back off
//! normally. A pin mismatch or a credential/firmware error is terminal — a human
//! decision, so the supervisor exits rather than loop.
//!
//! # Concurrency
//!
//! The read loop and the timed senders share one transport. A `select!` branch
//! cannot hold `&mut transport` for the read while another branch sends on it, so
//! the timer branches only *flag* their work; the actual keepalive/refresh sends
//! run at the top of the loop with exclusive access. That keeps [`next_inbound`]
//! the sole transport borrow inside the `select!`. Because a timer firing cancels
//! the in-flight read, `next_inbound` is cancel-safe by design.
//!
//! [`next_inbound`]: crate::transport::Transport::next_inbound

use std::time::{Duration, SystemTime};

use rand::Rng;
use tokio::sync::watch;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, info, warn};

use crate::auth::{self, AuthError, Token, authenticate};
use crate::config::Config;
use crate::crypto::{CryptoError, Session, handshake};
use crate::events;
use crate::sink::{self, Sink};
use crate::structure::{Structure, fetch_structure};
use crate::transport::{Inbound, Transport, TransportError};

/// Fraction of a token's lifetime at which to refresh proactively.
const REFRESH_FRACTION: f64 = 0.9;
/// Never wait longer than this between refreshes, even for a long-lived token.
const REFRESH_CAP: Duration = Duration::from_secs(24 * 60 * 60);
/// After a refresh fails on a still-live socket, retry it this soon.
const REFRESH_RETRY: Duration = Duration::from_secs(60 * 60);
/// A 4003 lockout must not be hammered; wait this long and log loudly.
const BLOCKED_BACKOFF: Duration = Duration::from_secs(15 * 60);
/// The (plain) keepalive command.
const KEEPALIVE: &str = "keepalive";

/// How the supervisor should react after a session ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryKind {
    /// A routine drop or a retryable close (4007 update, 4008 no slots).
    Normal,
    /// 4003: blocked for too many failed logins. Wait long, do not hammer.
    Blocked,
    /// 4004/4005: the user changed. Drop the token and re-auth from scratch.
    DropToken,
}

/// The outcome of one supervised session.
enum SessionOutcome {
    /// Graceful shutdown: the token was killed and the socket closed.
    Shutdown,
    /// Unrecoverable (bad pin, bad credentials, unsupported firmware): exit.
    Terminal(anyhow::Error),
    /// The connection ended; reconnect per the retry kind.
    Retry(RetryKind),
}

/// Everything one established session owns.
type Established = (Transport, Session, Token, Structure);

/// Run the daemon until a terminal error or a shutdown signal.
///
/// Loops over supervised sessions, applying the reconnect policy between them.
/// Returns `Ok` on a clean shutdown and `Err` on a terminal condition a human
/// must resolve (a pin mismatch, bad credentials, or unsupported firmware).
pub async fn run(config: &Config) -> anyhow::Result<()> {
    let sink = sink::build(config).await?;
    let mut shutdown = install_shutdown();
    let base = Duration::from_secs(config.reconnect.backoff_base_secs);
    let max = Duration::from_secs(config.reconnect.backoff_max_secs);
    let mut rng = rand::thread_rng();
    let mut attempt: u32 = 0;

    loop {
        match connect_and_run(config, sink.as_ref(), &mut shutdown, &mut attempt).await {
            SessionOutcome::Shutdown => {
                info!("shutdown complete");
                return Ok(());
            }
            SessionOutcome::Terminal(e) => return Err(e),
            SessionOutcome::Retry(kind) => {
                let delay = match kind {
                    RetryKind::DropToken => {
                        info!("server reports the user changed; re-authenticating from scratch");
                        attempt = 0;
                        Duration::ZERO
                    }
                    RetryKind::Blocked => {
                        warn!(
                            seconds = BLOCKED_BACKOFF.as_secs(),
                            "Miniserver blocked this client (too many failed logins); \
                             backing off long to avoid extending the lockout"
                        );
                        BLOCKED_BACKOFF
                    }
                    RetryKind::Normal => {
                        let ceiling = backoff_ceiling(attempt, base, max);
                        let delay = jittered_backoff(ceiling, &mut rng);
                        attempt = attempt.saturating_add(1);
                        delay
                    }
                };
                if !delay.is_zero() {
                    info!(seconds = delay.as_secs(), "reconnecting after backoff");
                }
                if wait_or_shutdown(delay, &mut shutdown).await {
                    info!("shutdown requested during backoff");
                    return Ok(());
                }
            }
        }
    }
}

/// Stand up one session, then run it until it ends.
async fn connect_and_run(
    config: &Config,
    sink: &dyn Sink,
    shutdown: &mut watch::Receiver<bool>,
    attempt: &mut u32,
) -> SessionOutcome {
    // Establishing (connect, pin handshake, auth, structure) can be interrupted
    // by a shutdown; there is no token yet, so dropping the socket is clean
    // enough at this stage.
    let established = tokio::select! {
        biased;
        _ = shutdown_requested(shutdown) => return SessionOutcome::Shutdown,
        result = establish(config) => result,
    };
    let (mut transport, mut session, mut token, structure) = match established {
        Ok(v) => v,
        Err(outcome) => return outcome,
    };
    // A full connection stood up, so reset the backoff schedule.
    *attempt = 0;
    info!(
        valid_until = ?token.valid_until(),
        series = structure.len(),
        "connected"
    );
    event_loop(
        &mut transport,
        &mut session,
        &mut token,
        &structure,
        sink,
        config,
        shutdown,
    )
    .await
}

/// Connect, verify the pin, authenticate, and load the structure map.
async fn establish(config: &Config) -> Result<Established, SessionOutcome> {
    let mut transport = Transport::connect(&config.host, config.port)
        .await
        .map_err(|e| retryable("connect", e))?;
    let mut session = match handshake(
        &mut transport,
        &config.host,
        config.port,
        Some(&config.server_key_pin),
    )
    .await
    {
        Ok(s) => s,
        // A pin mismatch or a missing pin is a human decision, never a retry.
        Err(CryptoError::Pin(pe)) => return Err(SessionOutcome::Terminal(pe.into())),
        Err(e) => return Err(retryable("handshake", e)),
    };
    let token = match authenticate(&mut session, &mut transport, config).await {
        Ok(t) => t,
        Err(e) if is_terminal_auth(&e) => return Err(SessionOutcome::Terminal(e.into())),
        Err(e) => return Err(retryable("authentication", e)),
    };
    let structure = fetch_structure(&mut transport)
        .await
        .map_err(|e| retryable("structure fetch", e))?;
    Ok((transport, session, token, structure))
}

/// Enable the event stream and pump it, keeping the session alive.
async fn event_loop(
    transport: &mut Transport,
    session: &mut Session,
    token: &mut Token,
    structure: &Structure,
    sink: &dyn Sink,
    config: &Config,
    shutdown: &mut watch::Receiver<bool>,
) -> SessionOutcome {
    if let Err(e) = events::enable_updates(transport).await {
        return retryable("enable updates", e);
    }

    let keepalive_period = Duration::from_secs(config.reconnect.keepalive_secs.max(1));
    let mut keepalive = tokio::time::interval(keepalive_period);
    keepalive.tick().await; // drop the immediate first tick

    let refresh_sleep = tokio::time::sleep(refresh_deadline(token, REFRESH_CAP, SystemTime::now()));
    tokio::pin!(refresh_sleep);

    // A `select!` branch cannot hold `&mut transport` for the read while another
    // branch sends on it, so the timer branches only flag their work; the sends
    // run here, at the top of the loop, with exclusive transport access.
    let mut keepalive_due = false;
    let mut refresh_due = false;

    loop {
        if *shutdown.borrow_and_update() {
            return graceful_shutdown(transport, session, token, config).await;
        }
        if keepalive_due {
            keepalive_due = false;
            if let Err(e) = transport.send_text(KEEPALIVE).await {
                return retryable("keepalive", e);
            }
            debug!("keepalive sent");
        }
        if refresh_due {
            refresh_due = false;
            match auth::refresh_token(session, transport, token, config).await {
                Ok(new) => {
                    *token = new;
                    let delay = refresh_deadline(token, REFRESH_CAP, SystemTime::now());
                    refresh_sleep.as_mut().reset(TokioInstant::now() + delay);
                    info!(valid_until = ?token.valid_until(), "token refreshed");
                }
                Err(e) => {
                    warn!(
                        retry_secs = REFRESH_RETRY.as_secs(),
                        "token refresh failed, will retry: {e:#}"
                    );
                    refresh_sleep
                        .as_mut()
                        .reset(TokioInstant::now() + REFRESH_RETRY);
                }
            }
        }

        tokio::select! {
            biased;
            // Shutdown/keepalive/refresh only flag their work; it is serviced at
            // the top of the next iteration so the read stays the lone borrow.
            _ = shutdown_requested(shutdown) => {}
            _ = keepalive.tick() => keepalive_due = true,
            _ = &mut refresh_sleep => refresh_due = true,
            inbound = transport.next_inbound() => match inbound {
                Ok(Inbound::Binary { header, payload }) => {
                    events::handle_table(header.msg_type, &payload, structure, sink).await;
                }
                Ok(Inbound::Text(text)) => {
                    debug!(%text, "unexpected text frame in event stream");
                }
                Err(TransportError::Closed(code)) => {
                    let kind = classify_close(code);
                    warn!(?code, ?kind, "connection closed");
                    return SessionOutcome::Retry(kind);
                }
                Err(e) => return retryable("event stream", e),
            },
        }
    }
}

/// Kill the token and close the socket for a graceful stop.
async fn graceful_shutdown(
    transport: &mut Transport,
    session: &mut Session,
    token: &Token,
    config: &Config,
) -> SessionOutcome {
    info!("shutdown requested; killing token and closing");
    match auth::kill_token(session, transport, token, &config.user).await {
        Ok(()) => info!("token killed"),
        Err(e) => warn!("killtoken failed during shutdown: {e:#}"),
    }
    // The Miniserver usually closes the socket itself in response to killtoken,
    // so an explicit close then fails harmlessly; log it only at debug.
    if let Err(e) = transport.close().await {
        debug!("connection already closed after killtoken: {e}");
    }
    SessionOutcome::Shutdown
}

/// Log a stage failure and map it to a normal reconnect.
fn retryable(stage: &str, err: impl std::fmt::Display) -> SessionOutcome {
    warn!("{stage} failed: {err}");
    SessionOutcome::Retry(RetryKind::Normal)
}

/// Auth failures a human must fix; retrying them only risks a login lockout.
fn is_terminal_auth(e: &AuthError) -> bool {
    matches!(
        e,
        AuthError::UnsupportedHashAlg { .. } | AuthError::GetJwt(401) | AuthError::GetKey2(401)
    )
}

/// Decide how to react to a socket closing, from the peer's close code.
fn classify_close(code: Option<u16>) -> RetryKind {
    match code {
        Some(4003) => RetryKind::Blocked,
        Some(4004 | 4005) => RetryKind::DropToken,
        // 4007 (update), 4008 (no slots), a codeless drop, or anything else.
        _ => RetryKind::Normal,
    }
}

/// The exponential-backoff ceiling for `attempt` (0-based), capped at `max`.
///
/// Doubles from `base` each attempt; the shift is clamped so a large attempt
/// count saturates to the cap rather than overflowing.
fn backoff_ceiling(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor = 1u64 << attempt.min(20);
    Duration::from_secs(base.as_secs().saturating_mul(factor)).min(max)
}

/// Apply half-to-full jitter: a random point in `[ceiling/2, ceiling]`.
///
/// Jitter decorrelates reconnect storms; keeping a floor of half the ceiling
/// stops a run of tiny delays from hammering the server.
fn jittered_backoff(ceiling: Duration, rng: &mut impl Rng) -> Duration {
    let millis = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rng.gen_range(millis / 2..=millis))
}

/// How long to wait before refreshing `token`, from `now`.
fn refresh_deadline(token: &Token, cap: Duration, now: SystemTime) -> Duration {
    capped_delay(token.refresh_at(REFRESH_FRACTION), cap, now)
}

/// Time from `now` until `at`, clamped to non-negative and capped at `cap`.
fn capped_delay(at: SystemTime, cap: Duration, now: SystemTime) -> Duration {
    at.duration_since(now).unwrap_or(Duration::ZERO).min(cap)
}

/// Spawn a task that flips the returned receiver to `true` on Ctrl-C / SIGTERM.
fn install_shutdown() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = tx.send(true);
    });
    rx
}

/// Resolve once shutdown has been requested; a `select!` branch.
async fn shutdown_requested(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        // A dropped sender (only after it sent `true`) also counts as shutdown.
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Sleep for `delay`, returning `true` early if shutdown is requested.
async fn wait_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if delay.is_zero() {
        return *shutdown.borrow_and_update();
    }
    tokio::select! {
        biased;
        _ = shutdown_requested(shutdown) => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

/// Complete on the first Ctrl-C or (on unix) SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                warn!("cannot install SIGTERM handler, using Ctrl-C only: {e}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_ceiling_is_monotonic_and_capped() {
        let base = Duration::from_secs(2);
        let max = Duration::from_secs(300);
        let mut prev = Duration::ZERO;
        for attempt in 0..24 {
            let ceiling = backoff_ceiling(attempt, base, max);
            assert!(ceiling >= prev, "ceiling must not decrease");
            assert!(ceiling <= max, "ceiling must be capped");
            prev = ceiling;
        }
        // Exact early doublings.
        assert_eq!(backoff_ceiling(0, base, max), Duration::from_secs(2));
        assert_eq!(backoff_ceiling(1, base, max), Duration::from_secs(4));
        assert_eq!(backoff_ceiling(2, base, max), Duration::from_secs(8));
        // A large attempt saturates to the cap without overflowing.
        assert_eq!(backoff_ceiling(1000, base, max), max);
    }

    #[test]
    fn jittered_backoff_stays_within_half_to_full() {
        let mut rng = rand::thread_rng();
        let ceiling = Duration::from_secs(60);
        for _ in 0..1000 {
            let delay = jittered_backoff(ceiling, &mut rng);
            assert!(delay >= ceiling / 2, "jitter floor is half the ceiling");
            assert!(delay <= ceiling, "jitter never exceeds the ceiling");
        }
        // A zero ceiling yields zero (immediate retry).
        assert_eq!(jittered_backoff(Duration::ZERO, &mut rng), Duration::ZERO);
    }

    #[test]
    fn classify_close_distinguishes_close_codes() {
        assert_eq!(classify_close(Some(4003)), RetryKind::Blocked);
        assert_eq!(classify_close(Some(4004)), RetryKind::DropToken);
        assert_eq!(classify_close(Some(4005)), RetryKind::DropToken);
        assert_eq!(classify_close(Some(4007)), RetryKind::Normal);
        assert_eq!(classify_close(Some(4008)), RetryKind::Normal);
        assert_eq!(classify_close(None), RetryKind::Normal);
        assert_eq!(classify_close(Some(1000)), RetryKind::Normal);
    }

    #[test]
    fn capped_delay_clamps_and_caps() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let cap = Duration::from_secs(24 * 60 * 60);
        // Far future beyond the cap -> capped at the cap.
        assert_eq!(
            capped_delay(now + Duration::from_secs(100 * 24 * 60 * 60), cap, now),
            cap
        );
        // Near future below the cap -> the exact remaining time.
        assert_eq!(
            capped_delay(now + Duration::from_secs(3600), cap, now),
            Duration::from_secs(3600)
        );
        // In the past -> zero (refresh immediately).
        assert_eq!(
            capped_delay(now - Duration::from_secs(10), cap, now),
            Duration::ZERO
        );
    }
}
