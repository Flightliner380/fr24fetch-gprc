import argparse
import datetime as dt
import gzip
import json
import logging
import os
import socket
import threading
import time
import tomllib
from contextlib import asynccontextmanager
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine

_EXPORT_INTERVAL_SECONDS = 180

from fr24_processing import export_df_to_duckdb, fr24convert
from mysql_export_fr24 import mysql_export

LOGGER = logging.getLogger("fr24_server")
_CONFIG_PATH = Path(__file__).with_name("config.toml")
DUCKDB_ACCESS_LOCK = threading.RLock()

MYSQL_EXPORT_ENABLED = True
MYSQL_TABLE = "k53371_fp.fr24"
DUCKDB_TABLE = "fr24"
DUCKDB_DIR = "./duckdb"
EXPORT_STOP_EVENT = threading.Event()
EXPORT_THREAD: threading.Thread | None = None
LOG_RETENTION_DAYS = 14
REQUIRED_DATA_FILES = (
    "airports.json",
    "icao_adsb_category.json",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global EXPORT_THREAD
    if MYSQL_EXPORT_ENABLED:
        if EXPORT_THREAD is None or not EXPORT_THREAD.is_alive():
            EXPORT_STOP_EVENT.clear()
            EXPORT_THREAD = threading.Thread(
                target=_export_loop,
                args=(MYSQL_TABLE, DUCKDB_TABLE, EXPORT_STOP_EVENT),
                daemon=True,
                name="fr24-export-loop",
            )
            EXPORT_THREAD.start()
    try:
        yield
    finally:
        EXPORT_STOP_EVENT.set()
        if EXPORT_THREAD is not None and EXPORT_THREAD.is_alive():
            EXPORT_THREAD.join(timeout=2)
        EXPORT_THREAD = None


def _read_cors_origins() -> list[str]:
    try:
        with _CONFIG_PATH.open("rb") as config_file:
            config_data = tomllib.load(config_file)
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return ["*"]

    server_cfg = config_data.get("server", {})
    raw_origins = server_cfg.get("cors_origins", ["*"])
    if isinstance(raw_origins, str):
        return [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    if isinstance(raw_origins, list):
        return [str(origin).strip() for origin in raw_origins if str(origin).strip()]
    return ["*"]


app = FastAPI(title="FR24 ingest/export server", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_read_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _run_mysql_export_once(frdbmysql: str, duckdb_table: str) -> None:
    with DUCKDB_ACCESS_LOCK:
        LOGGER.info("Starting scheduled MySQL export table=%s source=%s", frdbmysql, duckdb_table)
        try:
            mysql_export(frdbmysql=frdbmysql, duckdb_table=duckdb_table)
        except Exception:
            LOGGER.exception("Scheduled MySQL export failed for table=%s source=%s", frdbmysql, duckdb_table)
        else:
            LOGGER.info("Scheduled MySQL export completed table=%s source=%s", frdbmysql, duckdb_table)


def _read_export_interval_seconds() -> int:
    try:
        with _CONFIG_PATH.open("rb") as config_file:
            config_data = tomllib.load(config_file)
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return _EXPORT_INTERVAL_SECONDS

    export_cfg = config_data.get("export", {})
    value = export_cfg.get("interval_minutes", 3)
    try:
        minutes = float(value)
    except (TypeError, ValueError):
        return _EXPORT_INTERVAL_SECONDS
    seconds = int(minutes * 60)
    return max(60, seconds)


def _read_export_auth_token() -> str:
    try:
        with _CONFIG_PATH.open("rb") as config_file:
            config_data = tomllib.load(config_file)
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return ""
    export_cfg = config_data.get("export", {})
    token = export_cfg.get("export_auth_token", "")
    return str(token).strip()


def _load_server_config() -> dict:
    with _CONFIG_PATH.open("rb") as file_obj:
        config_data = tomllib.load(file_obj)
    return config_data.get("server", {})


def configure_logging(log_file: str, log_level: str = "INFO", retention_days: int = LOG_RETENTION_DAYS) -> None:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    if retention_days < 1:
        raise ValueError("log_retention_days must be >= 1")

    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=True,
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def _validate_required_data_files() -> None:
    base_dir = Path(__file__).resolve().parent
    missing_files = [str(base_dir / name) for name in REQUIRED_DATA_FILES if not (base_dir / name).is_file()]
    if missing_files:
        missing_text = ", ".join(missing_files)
        LOGGER.error("Missing required data file(s): %s", missing_text)
        raise RuntimeError(f"Missing required data file(s): {missing_text}")


def _authorize_request(request: Request) -> None:
    expected_token = _read_export_auth_token()
    if not expected_token:
        raise HTTPException(status_code=401, detail="Export auth is not configured on this server")

    provided_header = (request.headers.get("x-api-key") or request.headers.get("X-API-Key") or "").strip()
    if not provided_header:
        auth_header = (request.headers.get("authorization") or request.headers.get("Authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            provided_header = auth_header[7:].strip()

    if provided_header != expected_token:
        raise HTTPException(status_code=401, detail="Missing or invalid export auth header")


async def _read_json_payload(request: Request) -> dict[str, Any]:
    body = b""
    async for chunk in request.stream():
        body += chunk
    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")
    if (request.headers.get("content-encoding") or "").lower() == "gzip":
        try:
            body = gzip.decompress(body)
        except OSError as exc:
            raise HTTPException(status_code=400, detail=f"Failed to decompress gzip body: {exc}") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")
    return payload


def _force_mysql_export_requested(request: Request, payload: dict[str, Any]) -> bool:
    force_header = (request.headers.get("x-force-mysql-export") or request.headers.get("X-Force-MySQL-Export") or "").strip().lower()
    if force_header in {"1", "true", "yes", "on"}:
        return True
    if payload.get("force_mysql_export") is True:
        return True
    return False


def _duckdb_status() -> dict[str, Any]:
    db_dir = Path(DUCKDB_DIR)
    if not db_dir.exists():
        return {"ok": False, "error": f"DuckDB directory not found: {DUCKDB_DIR}"}

    latest_row: dict[str, Any] | None = None
    db_files = sorted(db_dir.glob("fr24_*.duckdb"))

    try:
        from mysql_export_fr24 import _mysql_export_excluded_values
        excluded_values = _mysql_export_excluded_values()
    except Exception:
        excluded_values = {"hex": set(), "cs": set(), "cat": set(), "ac": set()}

    for db_path in db_files:
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            row = conn.execute(
                f"""
                SELECT
                    COALESCE(MAX(fcontact), 0) AS max_fcontact,
                    COALESCE(MAX(lcontact), 0) AS max_lcontact,
                    COALESCE(COUNT(*), 0) AS row_count,
                    COALESCE(MAX(ymd), '') AS last_ymd,
                    COALESCE(SUM(CASE WHEN gnd = 0 THEN 1 ELSE 0 END), 0) AS airborne_count,
                    COALESCE(SUM(CASE WHEN gnd = 1 THEN 1 ELSE 0 END), 0) AS ground_count
                FROM {DUCKDB_TABLE}
                """
            ).fetchone()
            if row is None:
                conn.close()
                continue

            exclude_counts = {}
            total_excluded = 0
            for field_name, values in excluded_values.items():
                if not values:
                    exclude_counts[field_name] = 0
                    continue
                normalized_values = sorted(str(value).upper() for value in values)
                placeholders = ", ".join(["?"] * len(normalized_values))
                sql = f"""
                    SELECT COUNT(*)
                    FROM {DUCKDB_TABLE}
                    WHERE ymd = ?
                      AND lcontact > ?
                      AND gnd = 0
                      AND upper({field_name}) IN ({placeholders})
                """
                count_row = conn.execute(sql, [str(row[3]), 0, *normalized_values]).fetchone()
                exclude_counts[field_name] = int(count_row[0]) if count_row else 0

            combined_filters = []
            combined_params: list[object] = [str(row[3]), 0]
            for field_name, values in excluded_values.items():
                if not values:
                    continue
                normalized_values = sorted(str(value).upper() for value in values)
                placeholders = ", ".join(["?"] * len(normalized_values))
                combined_filters.append(f"upper({field_name}) IN ({placeholders})")
                combined_params.extend(normalized_values)
            if combined_filters:
                total_sql = f"""
                    SELECT COUNT(*)
                    FROM {DUCKDB_TABLE}
                    WHERE ymd = ?
                      AND lcontact > ?
                      AND gnd = 0
                      AND ({' OR '.join(combined_filters)})
                """
                total_row = conn.execute(total_sql, combined_params).fetchone()
                total_excluded = int(total_row[0]) if total_row else 0

            current = {
                "db_file": str(db_path),
                "max_fcontact": int(row[0]),
                "max_lcontact": int(row[1]),
                "row_count": int(row[2]),
                "last_ymd": str(row[3]),
                "airborne_count": int(row[4]),
                "ground_count": int(row[5]),
                "exclude_counts": {
                    "hex": int(exclude_counts.get("hex", 0)),
                    "callsign": int(exclude_counts.get("cs", 0)),
                    "cat": int(exclude_counts.get("cat", 0)),
                    "ac": int(exclude_counts.get("ac", 0)),
                },
                "excluded_rows_total": int(total_excluded),
            }
            conn.close()
            if latest_row is None or current["max_lcontact"] > latest_row["max_lcontact"]:
                latest_row = current
        except Exception as exc:
            LOGGER.warning("DuckDB status check failed for %s: %s", db_path, exc)

    if latest_row is None:
        return {
            "ok": True,
            "database_files": len(db_files),
            "table": DUCKDB_TABLE,
            "max_fcontact": 0,
            "max_lcontact": 0,
            "row_count": 0,
            "last_ymd": "",
            "airborne_count": 0,
            "ground_count": 0,
            "exclude_counts": {"hex": 0, "callsign": 0, "cat": 0, "ac": 0},
            "excluded_rows_total": 0,
        }

    latest_row["database_files"] = len(db_files)
    latest_row["table"] = DUCKDB_TABLE
    latest_row["ok"] = True
    return latest_row


def _mysql_status() -> dict[str, Any]:
    mysql_conf = tomllib.loads(Path(_CONFIG_PATH).read_bytes().decode("utf-8")) if _CONFIG_PATH.exists() else {}
    mysql_cfg = mysql_conf.get("mysql", {})
    if not mysql_cfg:
        return {"ok": False, "error": "MySQL config not found"}

    driver = mysql_cfg.get("driver", "pymysql")
    port = mysql_cfg.get("port", 3306)
    mysql_url = (
        f"mysql+{driver}://"
        f"{mysql_cfg['user']}:{mysql_cfg['password']}"
        f"@{mysql_cfg['host']}:{port}/"
        f"{mysql_cfg['database']}"
        "?charset=utf8mb4"
    )
    try:
        engine = create_engine(
            mysql_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 3, "read_timeout": 3, "write_timeout": 3},
        )
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                f"""
                SELECT
                    COALESCE(MAX(fcontact), 0) AS max_fcontact,
                    COALESCE(MAX(lcontact), 0) AS max_lcontact,
                    COALESCE(COUNT(*), 0) AS row_count,
                    COALESCE(MAX(ymd), '') AS last_ymd
                FROM {MYSQL_TABLE}
                """
            ).fetchone()
        return {
            "ok": True,
            "table": MYSQL_TABLE,
            "max_fcontact": int(row[0]),
            "max_lcontact": int(row[1]),
            "row_count": int(row[2]),
            "last_ymd": str(row[3]),
        }
    except Exception as exc:
        LOGGER.warning("MySQL status check failed: %s", exc)
        return {"ok": False, "error": str(exc), "table": MYSQL_TABLE}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "fr24_server"}


@app.get("/statusdb")
async def statusdb(request: Request) -> JSONResponse:
    try:
        _authorize_request(request)
    except HTTPException:
        raise

    duckdb_status = _duckdb_status()
    mysql_status = _mysql_status()
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "duckdb": duckdb_status,
            "mysql": mysql_status,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        },
    )


@app.get("/export")
@app.post("/export")
async def export_handler(request: Request) -> JSONResponse:
    if not MYSQL_EXPORT_ENABLED:
        return JSONResponse(status_code=403, content={"ok": False, "error": "MySQL export is disabled on this server"})

    try:
        _authorize_request(request)
    except HTTPException:
        raise

    LOGGER.info("Manual MySQL export triggered via HTTP endpoint table=%s source=%s", MYSQL_TABLE, DUCKDB_TABLE)
    try:
        _run_mysql_export_once(MYSQL_TABLE, DUCKDB_TABLE)
    except Exception as exc:  # pragma: no cover - defensive guard
        LOGGER.exception("Manual MySQL export failed via HTTP trigger")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

    return JSONResponse(
        status_code=200,
        content={"ok": True, "mysql_export_ran": True, "mysql_table": MYSQL_TABLE, "duckdb_table": DUCKDB_TABLE},
    )


@app.post("/ingest")
async def ingest_handler(
    request: Request,
    force_mysql_export: bool = Query(False, description="Immediately export DuckDB rows to MySQL after ingest"),
) -> JSONResponse:
    try:
        payload = await _read_json_payload(request)
    except HTTPException:
        raise

    if force_mysql_export or _force_mysql_export_requested(request, payload):
        try:
            _authorize_request(request)
        except HTTPException:
            raise

    cols = payload.get("cols")
    rows = payload.get("rows")
    if not isinstance(cols, list) or not isinstance(rows, list):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Payload must include list fields: cols and rows"})

    client_hostname = str(payload.get("client_hostname", "")).strip() or "unknown"
    payload_timestamp = str(payload.get("t", "")).strip() or dt.datetime.now(dt.timezone.utc).isoformat()
    rows_received = len(rows)
    server_hostname = socket.gethostname()
    force_requested = bool(force_mysql_export or _force_mysql_export_requested(request, payload))

    LOGGER.info(
        "Ingest request client_hostname=%s payload_timestamp=%s rows=%d server_hostname=%s force_mysql_export=%s",
        client_hostname,
        payload_timestamp,
        rows_received,
        server_hostname,
        force_requested,
    )

    try:
        df_raw = pd.DataFrame(rows, columns=cols)
        df_processed = fr24convert(df_raw)
        with DUCKDB_ACCESS_LOCK:
            rows_written = export_df_to_duckdb(
                df_processed,
                table_name=DUCKDB_TABLE,
                base_dir=DUCKDB_DIR,
            )

        mysql_ran = False
        if force_requested and MYSQL_EXPORT_ENABLED:
            LOGGER.info("Forced MySQL export requested by client=%s table=%s", client_hostname, MYSQL_TABLE)
            _run_mysql_export_once(MYSQL_TABLE, DUCKDB_TABLE)
            mysql_ran = True
        elif force_requested and not MYSQL_EXPORT_ENABLED:
            LOGGER.warning("Forced MySQL export requested but server mysql_export is disabled")

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "rows_received": rows_received,
                "rows_written_duckdb": int(rows_written),
                "mysql_export_ran": mysql_ran,
                "mysql_export_queued": False,
                "mysql_export_interval_seconds": _read_export_interval_seconds(),
                "server_hostname": server_hostname,
                "payload_timestamp": payload_timestamp,
                "client_hostname": client_hostname,
                "force_mysql_export": force_requested,
            },
        )
    except Exception as exc:
        LOGGER.exception("Error while processing ingest request")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


def _export_loop(mysql_table: str, duckdb_table: str, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        interval_seconds = _read_export_interval_seconds()
        stop_event.wait(interval_seconds)
        if stop_event.is_set():
            break
        if not MYSQL_EXPORT_ENABLED:
            continue
        _run_mysql_export_once(mysql_table, duckdb_table)


def parse_args() -> argparse.Namespace:
    server_cfg = _load_server_config()
    parser = argparse.ArgumentParser(description="Receive FR24 payloads and store/export them")
    parser.add_argument("--host", default=server_cfg.get("host", "0.0.0.0"), help="Bind host")
    parser.add_argument("--port", type=int, default=int(server_cfg.get("port", 8080)), help="Bind port")
    parser.add_argument("--duckdb-dir", default=server_cfg.get("duckdb_dir", "./duckdb"), help="DuckDB folder path")
    parser.add_argument("--duckdb-table", default=server_cfg.get("duckdb_table", "fr24"), help="DuckDB table name")
    parser.add_argument("--mysql-table", default=server_cfg.get("mysql_table", "k53371_fp.fr24"), help="MySQL destination table")
    parser.add_argument("--log-file", default=server_cfg.get("log_file", "./logs/fr24_server.log"), help="Server log file path")
    parser.add_argument("--log-level", default=server_cfg.get("log_level", "INFO"), help="Server log level")
    parser.add_argument(
        "--log-retention-days",
        type=int,
        default=int(server_cfg.get("log_retention_days", LOG_RETENTION_DAYS)),
        help="Daily log retention window in days (>=1)",
    )
    parser.add_argument(
        "--skip-mysql-export",
        action="store_true",
        default=None,
        help="Store data in DuckDB only, skip MySQL export",
    )
    args = parser.parse_args()
    if args.log_retention_days < 1:
        parser.error("--log-retention-days must be >= 1")
    if args.skip_mysql_export is None:
        args.skip_mysql_export = bool(server_cfg.get("skip_mysql_export", False))
    return args


def main() -> None:
    args = parse_args()
    configure_logging(args.log_file, args.log_level, args.log_retention_days)
    _validate_required_data_files()

    global MYSQL_EXPORT_ENABLED, MYSQL_TABLE, DUCKDB_TABLE, DUCKDB_DIR
    MYSQL_EXPORT_ENABLED = not args.skip_mysql_export
    MYSQL_TABLE = args.mysql_table
    DUCKDB_TABLE = args.duckdb_table
    DUCKDB_DIR = args.duckdb_dir

    LOGGER.info(
        "FR24 server listening on http://%s:%s/ingest (mysql_export=%s)",
        args.host,
        args.port,
        "on" if MYSQL_EXPORT_ENABLED else "off",
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
