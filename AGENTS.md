# AGENTS.md

## Project purpose
This repository ingests FR24 data, stores the raw/normalized rows in DuckDB, and periodically exports selected rows to MySQL.

## Primary runtime flow
- `fr24_client.py` fetches FR24 payloads and posts them to the server ingest endpoint.
- `fr24_server.py` is the FastAPI server. It handles `/ingest`, `/export`, `/statusdb`, and the periodic export loop.
- `fr24_processing.py` normalizes FR24 rows, enriches them, and writes them to DuckDB.
- `mysql_export_fr24.py` exports filtered rows from DuckDB into MySQL.
- `config.toml` is the source of truth for runtime settings and credentials.

## Important rules for future AI agents
- Keep the client/server split intact.
- Use `config.toml` instead of hard-coded settings.
- Do not expose credentials in source code or commit secrets.
- Respect the existing API-key protection for `/export` and `/statusdb`.
- Keep the MySQL export independent from the ingest request path; do not trigger inline export during `/ingest` unless explicitly requested.
- The export loop is scheduled via `[export].interval_minutes` and a reasonable minimum delay should be respected.
- MySQL export is filtered for `gnd = 0` and for configured exact-value excludes: `exclude_hex`, `exclude_callsign`, `exclude_cat`, `exclude_ac`.
- The DuckDB uniqueness pattern for writes is based on `(ymd, frid, hex, cs, gnd)` unless a specific task changes that intentionally.
- `/statusdb` should report DuckDB counts including `airborne_count` and `ground_count`, plus exclude counts when configured.
- Keep CORS configuration in the server config and do not default to unsafe values in production.

## Validation
When changing Python files, run the smallest relevant validation command, usually:

```bash
. .venv/bin/activate
python -m py_compile fr24_server.py fr24_processing.py mysql_export_fr24.py fr24_client.py
```

## Security expectations
- `export_auth_token` must be set before exposing export or status endpoints.
- `X-API-Key` or `Authorization: Bearer <token>` is the expected auth format.
- Default insecure values should not be left active in production.

## Repo conventions
- Keep README in sync with behavior changes.
- Prefer surgical, focused changes over broad refactors.
- Do not remove the config file or replace it with Python settings modules unless a task explicitly requires it.
