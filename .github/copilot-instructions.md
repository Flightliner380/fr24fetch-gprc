# Copilot instructions for this repository

This repository implements an FR24 ingestion flow split into client and server components.

- Use `config.toml` as the only runtime configuration source.
- Keep the server in `fr24_server.py`, the conversion layer in `fr24_processing.py`, and the export logic in `mysql_export_fr24.py`.
- Respect the current authentication model: `/export` and `/statusdb` require `X-API-Key` or `Authorization: Bearer <token>`.
- The automatic MySQL export is scheduled, not inline, and should remain decoupled from upload latency.
- Use exact-value exclusion filters for MySQL export: `exclude_hex`, `exclude_callsign`, `exclude_cat`, `exclude_ac`.
- `gnd = 0` is airborne; `gnd = 1` is ground. Export filtering uses `gnd = 0`.
- DuckDB writes use the keyed uniqueness pattern `(ymd, frid, hex, cs, gnd)` unless an explicit task changes it.
- Report status separately for airborne vs ground rows and for exclude counts where relevant.
- Validate changed Python files with a focused `py_compile` or minimal test before finishing.
- Keep README aligned with current behavior and usage.
