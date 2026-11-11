# TimescaleDB setup for loxkit

loxtap stores its readings in TimescaleDB, and loxweb reads them back out of the
same store. This guide installs and configures a suitable database on Ubuntu, up
to the point where the daemon can connect. It stops there on purpose: loxtap owns
its own schema and creates the extensions and tables on first run (see the
closing note), so you only need the extension packages installed and an empty
database.

## 1. Prerequisites and why

loxtap needs three things from the database host:

- **PostgreSQL 18** — the server the extensions load into.
- **TimescaleDB, Community (TSL) edition** — the hypertable, columnstore
  compression, continuous aggregates, and retention policies loxtap relies on.
- **The `timescaledb_toolkit` extension** — its time-weighted `integral` and
  related functions back the aggregates loxtap builds for counter and gauge
  series.

> **Do not use the `universe` package.** Ubuntu's `postgresql-18-timescaledb`
> is the **Apache-only** build. It lacks compression, continuous aggregates, and
> retention, so loxtap's migrations fail against it. Install the Community edition
> from Timescale's own apt repository, as below. You confirm you got the right
> one in step 6.

## 2. Add Timescale's apt repository and install

Add Timescale's signed apt repository. The sources line uses
`$(lsb_release -c -s)` so it tracks your release automatically:

```sh
sudo install -d /etc/apt/keyrings
curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey \
  | sudo gpg --dearmor -o /etc/apt/keyrings/timescaledb.gpg
echo "deb [signed-by=/etc/apt/keyrings/timescaledb.gpg] \
https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" \
  | sudo tee /etc/apt/sources.list.d/timescaledb.list
```

If your release is too new to be published yet, substitute the nearest LTS
codename in that sources line.

Then install PostgreSQL and both extensions:

```sh
sudo apt update
sudo apt install postgresql-18 timescaledb-2-postgresql-18 \
  timescaledb-toolkit-postgresql-18
```

PostgreSQL 18 ships in recent Ubuntu releases. On an older release that does not
carry it, add the PostgreSQL PGDG apt repository first, then install as above.

## 3. Tune and preload

TimescaleDB must be loaded at server start. `timescaledb-tune` adds it to
`shared_preload_libraries` (and suggests memory settings for your host); the
toolkit needs no preload. Run it, then restart PostgreSQL:

```sh
sudo timescaledb-tune
sudo systemctl restart postgresql
```

## 4. (Optional) expose to the LAN

If the daemons run on the database host, keep PostgreSQL bound to localhost and
skip this step. To let loxtap or loxweb connect from another host on a trusted
LAN, open the server up:

- In `postgresql.conf`, set `listen_addresses` to the interface(s) you want
  reachable, e.g. `listen_addresses = '*'`.
- In `pg_hba.conf`, add a SCRAM entry for your LAN so passwords are hashed on the
  wire:

  ```
  host  loxtap  loxtap  <lan-cidr>  scram-sha-256
  ```

- Open the port in your firewall, e.g. `sudo ufw allow from <lan-cidr> to any
  port 5432`, then restart PostgreSQL.

loxtap connects as plain Postgres over the trusted LAN (no TLS). Do not expose the
port to untrusted networks.

## 5. Create the role and database

As the `postgres` superuser (`sudo -u postgres psql`), create loxtap's login role
and its database:

```sql
CREATE ROLE loxtap LOGIN PASSWORD 'changeme';
CREATE DATABASE loxtap OWNER loxtap;
```

Leave the extensions to loxtap. Both are marked trusted, so a role with `CREATE`
on the database (as its owner has) may create them without being a superuser,
and loxtap's migrations do exactly that on first run. The `loxtap` role then
owns both extensions, and owning the toolkit is what lets the daemon keep it
current after a package upgrade (see [Package upgrades](#package-upgrades)).

Optionally add a least-privilege, read-only role for loxweb. Because loxtap
creates its tables itself on first run, grant `SELECT` **after** that first run,
or — cleaner — pre-authorize the objects loxtap will create by setting default
privileges as the owner:

```sql
CREATE ROLE loxweb LOGIN PASSWORD 'changeme';
GRANT CONNECT ON DATABASE loxtap TO loxweb;
GRANT USAGE ON SCHEMA public TO loxweb;
-- after loxtap's first run creates the schema:
GRANT SELECT ON ALL TABLES IN SCHEMA public TO loxweb;
-- and for objects loxtap creates later, run as the loxtap owner:
ALTER DEFAULT PRIVILEGES FOR ROLE loxtap IN SCHEMA public
  GRANT SELECT ON TABLES TO loxweb;
```

## 6. Verify

Once loxtap has run for the first time and applied its migrations (see
[doc/LOXTAP-CONFIG.md](LOXTAP-CONFIG.md)), connect to the database
(`psql -h <db-host> -U loxtap loxtap`) and confirm both extensions are present
and you got the Community edition:

```console
loxtap=# \dx
                 List of installed extensions
        Name         | Version |   Schema   | Description
---------------------+---------+------------+-------------
 timescaledb         | ...     | public     | ...
 timescaledb_toolkit | ...     | public     | ...

loxtap=# SHOW timescaledb.license;
 timescaledb.license
---------------------
 timescale
```

`SHOW timescaledb.license;` must print `timescale`, not `apache` — that is your
proof the Community edition is loaded.

## Package upgrades

An apt upgrade of either extension package swaps its library, but each database
stays at the old extension version until `ALTER EXTENSION … UPDATE` runs in it.
The toolkit package keeps only its current library, so a lagging toolkit breaks
right away, the continuous-aggregate refreshes included. The core package
carries a few older libraries, so a lagging core lasts a while longer.

loxtap checks both on startup and hourly after that:

- **`timescaledb_toolkit`** — when the `loxtap` role owns it, loxtap updates it
  itself, so it is current again within the hour. Otherwise loxtap logs a
  warning.
- **`timescaledb`** — loxtap only warns, hourly while it lags. Its update must
  be the first command of a fresh session, and the daemons must reconnect to
  load the new library:

  ```sh
  sudo -u postgres psql -X -d loxtap -c 'ALTER EXTENSION timescaledb UPDATE'
  sudo systemctl restart loxtap loxweb
  ```

On a database where a superuser created the extensions (as older setups did),
hand the toolkit over to the `loxtap` role once. PostgreSQL has no
`ALTER EXTENSION … OWNER`, so as the `postgres` superuser, connected to the
loxtap database, update the catalog directly:

```sql
UPDATE pg_extension SET extowner = 'loxtap'::regrole
  WHERE extname = 'timescaledb_toolkit';
```

## The loxtap daemon owns the schema

Setup stops at "the packages are installed and an empty database exists."
loxtap applies its embedded migrations on first connect, creating the two
extensions, the tables, the hypertable, the compression policy, and the
continuous aggregates itself — there is no separate migration step to run.

Next, point the daemon at this database: see
[doc/LOXTAP-CONFIG.md](LOXTAP-CONFIG.md).
