# loxtap configuration

The loxtap ingest daemon reads a single YAML config that describes the Miniserver
to log and the TimescaleDB store to write into. This guide walks the file
section by section. For creating the database itself, see
[doc/TIMESCALEDB-SETUP.md](TIMESCALEDB-SETUP.md).

## Where it lives and how it's loaded

The daemon takes `--config <FILE>` and parses YAML. The packaged
`loxtap.service` unit passes `/etc/loxkit/loxtap.yaml`, which should be mode
`0640`, owned `root:loxtap`, so the config (it holds passwords) is readable by the
service account but not the world.

The Debian package ships a template at `/etc/loxkit/loxtap.example.yaml`. Copy it
to the real name with that owner and mode in one step, then fill in your own
values:

```sh
sudo install -o root -g loxtap -m 0640 \
    /etc/loxkit/loxtap.example.yaml /etc/loxkit/loxtap.yaml
```

The unit is already enabled but stays skipped until the config exists, so
`sudo systemctl start loxtap` brings it up once the file is in place. From a
source build, start instead from the in-tree `loxtap/config.example.yaml`.

## The `miniserver:` section

The connection to the Miniserver you are logging.

```yaml
miniserver:
  host: miniserver.example
  port: 80
  user: user
  password: password
  server_key_pin: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
```

- **`host`**, **`port`** — the Miniserver's address; `port` defaults to `80`.
- **`user`**, **`password`** — a normal Miniserver user the daemon logs in as.
- **`server_key_pin`** — the SHA-256 fingerprint of the Miniserver's public key,
  as `sha256:<hex>`. It is verified **fail-closed**: the daemon refuses to
  continue if the presented key does not match, so a rotated or spoofed key stops
  ingestion rather than trusting a stranger.

**Obtaining the pin.** Leave the sample's placeholder pin in place and start the
daemon once. The handshake fails closed and logs a pin-mismatch error that prints
the key it was presented:

```
server key pin mismatch, refusing to continue:

    configured: sha256:0000...0000
    presented:  sha256:<hex>
```

Confirm that is your Miniserver, then copy the `presented:` fingerprint into
`server_key_pin` and restart. Alternatively, precompute it offline from the
server's public key — the value equals:

```sh
openssl pkey -pubin -outform DER < serverkey.pem | sha256sum
```

### `reconnect:`

Optional timing knobs; the defaults shown are also the built-in ones, so a
minimal config can omit the whole block.

```yaml
  reconnect:
    backoff_base_secs: 2      # first delay after a drop; doubles up to the max
    backoff_max_secs: 300     # ceiling for the exponential backoff
    keepalive_secs: 240       # keepalive cadence
```

Keep `keepalive_secs` under the Miniserver's idle cutoff (about five minutes) or
it will close the socket on you between keepalives.

## The `timescale:` section

The TimescaleDB sink.

```yaml
timescale:
  host: timescale.example
  port: 5432
  user: loxtap
  password: password
  dbname: loxtap
  # timezone: "Region/City"
```

**Defining this section selects the TimescaleDB sink.** Omit or comment it out
and loxtap falls back to a debug sink that only logs the decoded readings — handy
for a dry run, useless for storage.

- **`host`**, **`port`**, **`user`**, **`password`**, **`dbname`** — the
  connection; `port` defaults to `5432`. This is plain Postgres over a trusted
  LAN (no TLS). Use the role and database from
  [doc/TIMESCALEDB-SETUP.md](TIMESCALEDB-SETUP.md).
- **`timezone`** (optional) — an IANA zone, e.g. `"Europe/Zurich"`, pinned as the
  store's default so the daily and monthly rollups bucket in local time. The
  daemon must own the database to set this. Stored timestamps stay UTC
  regardless; only calendar bucketing is affected.

## `log_level`

The fallback tracing level when `RUST_LOG` is unset: one of `error`, `warn`,
`info`, `debug`, `trace`. `RUST_LOG`, if set, wins.

```yaml
log_level: info
```

## Client identity

Nothing to configure. The id the Miniserver tracks the daemon's token under, and
the label it shows in its device list, are derived from the host (its machine-id
and hostname).

## First run and verify

Start the daemon and follow its log:

```sh
sudo systemctl start loxtap
journalctl -u loxtap -f
```

On a healthy first run you will see, in order: the embedded migrations applying
(schema created), the crypto handshake and the pin check passing, token
authentication, then the event stream flowing as readings arrive. On a pin
mismatch the daemon exits with the message shown above — paste the presented
fingerprint and restart.
