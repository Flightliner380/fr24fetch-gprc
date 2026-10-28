# FR24 Fetch / Ingest / MySQL Export

This project collects FR24 live data, normalizes it, stores it in DuckDB, and exports selected rows to MySQL on a schedule or on demand.

The project is split into three main parts:
- a client that fetches FR24 data and uploads it to the server
- a FastAPI server that handles ingest, status checks, and export triggers
- a MySQL export layer that reads from DuckDB and writes to the configured table

## Repository layout

- `fr24_client.py` — fetches FR24 data and sends it to `/ingest`
- `fr24_server.py` — FastAPI server with `/health`, `/ingest`, `/export`, and `/statusdb`
- `fr24_processing.py` — FR24 conversion, normalization, airport/runway lookup, and DuckDB writes
- `mysql_export_fr24.py` — MySQL export logic, filter logic, and schema-aligned upserts
- `fr24_sync.py` — manual trigger for an immediate MySQL export
- `config.toml` — central runtime configuration file
- `nearby.py` — airport and runway lookup helpers
- `airports.json` — airport metadata for nearby-airport detection
- `icao_adsb_category.json` — ICAO aircraft category lookup
- `duckdb/` — local DuckDB storage directory
- `logs/` — runtime log output
- `AGENTS.md` — AI-operating instructions for future agents

## Requirements

Use the project virtual environment and install dependencies from the repo requirements.

```bash
cd /home/flightliner/python/fr24fetch-gprc
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

If FastAPI and UVicorn are not present:

```bash
pip install fastapi uvicorn
```

## Configuration

All runtime values are defined in `config.toml`.

Example:

```toml
[mysql]
host = "your-mysql-host"
port = 3306
user = "db_user"
password = "db_password"
database = "your_database"
autocommit = false
driver = "pymysql"

[client]
server_url = "http://127.0.0.1:8080/ingest"
timeout = 30
hostname = ""

[server]
host = "0.0.0.0"
port = 8080
duckdb_dir = "./duckdb"
duckdb_table = "fr24"
mysql_table = "your_db.fr24n"
skip_mysql_export = false
log_file = "./logs/fr24_server.log"
log_level = "INFO"
log_retention_days = 14
cors_origins = ["*"]

[export]
chunk_size = 7500
duckdb_table = "fr24"
interval_minutes = 3
export_auth_token = "CHANGE_ME"
exclude_hex = []
exclude_callsign = []
exclude_cat = ["A7", "B6"]
exclude_ac = []

[near]
max_alt_ft = 3000
```

## Security and auth

The manual export endpoints and `/statusdb` are protected with an API key.

Set a real token in `config.toml`:

```toml
[export]
export_auth_token = "super-secret-token"
```

Then call a protected endpoint using either of these headers:

```bash
curl -X POST http://127.0.0.1:8080/export \
  -H "X-API-Key: super-secret-token"
```

```bash
curl -X GET http://127.0.0.1:8080/statusdb \
  -H "Authorization: Bearer super-secret-token"
```

If the token is empty or missing, the server rejects the request.

## Export filtering

The MySQL export skips rows that match configured exact values. Supported filters:
- `exclude_hex`
- `exclude_callsign`
- `exclude_cat`
- `exclude_ac`

These are exact string/value filters applied before insert/update work. They are not wildcard matches.

Example:

```toml
exclude_hex = ["A82332", "A82331"]
exclude_callsign = ["DLH001", "UPS61"]
exclude_cat = ["A7", "B6"]
exclude_ac = ["AS50"]
```

## Data semantics

Several important rules are enforced by the pipeline:
- `gnd = 0` means airborne.
- `gnd = 1` means ground.
- MySQL export only writes airborne rows (`gnd = 0`) for the selected time window.
- DuckDB stores normalized rows in monthly `.duckdb` files.
- DuckDB upserts are keyed by `(ymd, frid, hex, cs, gnd)`.
- `/statusdb` reports airborne and ground counts separately.

## Running the server

```bash
cd /home/flightliner/python/fr24fetch-gprc
. .venv/bin/activate
python fr24_server.py --host 0.0.0.0 --port 8080
```

Or with uvicorn:

```bash
uvicorn fr24_server:app --host 0.0.0.0 --port 8080
```

### Docker (server)

Use the dedicated server image definition in [Dockerfile](/home/flightliner/python/fr24fetch-gprc/Dockerfile):

```bash
cd /home/flightliner/python/fr24fetch-gprc
mkdir -p data/duckdb data/logs
docker compose -f docker-compose.yml up --build -d
```

This compose file mounts:
- `./config.toml` (read-only) to `/app/config.toml`
- `./data/duckdb` to `/app/duckdb`
- `./data/logs` to `/app/logs`

Stop it with:

```bash
docker compose -f docker-compose.yml down
```

### Health checks

```bash
curl http://127.0.0.1:8080/health
```

### Status endpoint

```bash
curl -H "X-API-Key: super-secret-token" http://127.0.0.1:8080/statusdb
```

Response includes:
- `duckdb` status
- `mysql` status
- `airborne_count`
- `ground_count`
- `exclude_counts`
- `excluded_rows_total`

## Ingesting data

The client fetches FR24 data and posts it to the server:

```bash
cd /home/flightliner/python/fr24fetch-gprc
. .venv/bin/activate
python fr24_client.py
```

Optional force-export trigger:

```bash
python fr24_client.py --force-mysql-export
```

Manual HTTP ingest example:

```bash
curl -X POST "http://127.0.0.1:8080/ingest?force_mysql_export=true" \
  -H "X-API-Key: super-secret-token" \
  -H "Content-Type: application/json" \
  --data '{"cols":["frid","hex","cs"],"rows":[["1","A82332","DLH001"]]}'
```

The server accepts JSON, and gzip-compressed JSON bodies are supported.

## Triggering MySQL export

The export loop runs automatically according to `[export].interval_minutes`.

Manual trigger:

```bash
curl -X POST http://127.0.0.1:8080/export \
  -H "X-API-Key: super-secret-token"
```

Direct script trigger:

```bash
cd /home/flightliner/python/fr24fetch-gprc
. .venv/bin/activate
python fr24_sync.py --mysql-table your_db.fr24n --duckdb-table fr24 --chunk-size 7500
```

## Data flow

1. Client fetches FR24 payloads.
2. Client posts the payload to the ingest endpoint.
3. The server converts and enriches the payload.
4. The processed rows are stored in DuckDB.
5. The scheduled export loop reads DuckDB rows and writes filtered rows to MySQL.
6. Manual `/export` triggers an immediate export if auth is valid.

## Logging

The server writes to the configured file and stdout.
Log files roll over daily and old files are kept for the configured retention window (`log_retention_days`).

```toml
[server]
log_file = "./logs/fr24_server.log"
log_level = "INFO"
log_retention_days = 14
```

## Operational notes

- Nearby-airport and runway detection is based on airport metadata and altitude logic.
- Flights under 3000ft do not use the commercial-airport bucket for nearby detection.
- Aircraft categories are derived from the ICAO aircraft type mapping.
- DuckDB is the staging layer; MySQL is the downstream persistence/export target.
- The project is for local operational use and should not run with empty security tokens in production.

## Troubleshooting

- If the server fails to start, verify that `config.toml` is valid TOML.
- If the server exits at startup with missing data-file errors, make sure `airports.json` and `icao_adsb_category.json` are present next to `fr24_server.py`.
- If MySQL export fails, check credentials, network access, and table names.
- If no rows are exported, inspect the exclude filters and `gnd` filtering.
- If airport/runway data is missing, ensure the airport dataset and nearby logic are present and loaded.

## Validation

Before finishing a patch, run a minimal Python validation check:

```bash
cd /home/flightliner/python/fr24fetch-gprc
. .venv/bin/activate
python -m py_compile fr24_server.py fr24_processing.py mysql_export_fr24.py fr24_client.py
```

## License

This project is intended for local operational use and is not published as a general-purpose package.
