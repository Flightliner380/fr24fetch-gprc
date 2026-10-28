import argparse
import asyncio
import datetime as dt
import gzip
import json
import socket
import time
import tomllib
from pathlib import Path
from urllib import error, request

import pandas as pd
from fr24.core import FR24

_CONFIG_PATH = Path(__file__).with_name("config.toml")
APP_VERSION = "1.0.0"


def _client_user_agent() -> str:
    try:
        config_data = _load_client_config()
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return f"fr24gprc/{APP_VERSION}"
    user_agent = str(config_data.get("user_agent", "")).strip()
    return user_agent or f"fr24gprc/{APP_VERSION}"


def _utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _log(level: str, message: str) -> None:
    print(f"{_utc_timestamp()} [{level}] {message}", flush=True)


def _load_client_config() -> dict:
    with _CONFIG_PATH.open("rb") as file_obj:
        config_data = tomllib.load(file_obj)
    return config_data.get("client", {})


def build_payload(df: pd.DataFrame, client_hostname: str, force_mysql_export: bool = False) -> dict:
    payload_df = df.where(pd.notna(df), None)
    payload_timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    user_agent = _client_user_agent()
    return {
        "s": _client_user_agent(),
        "t": payload_timestamp,
        "n": int(len(payload_df)),
        "client_hostname": client_hostname,
        "force_mysql_export": bool(force_mysql_export),
        "fmt": "cols_rows",
        "cols": list(payload_df.columns),
        "rows": payload_df.values.tolist(),
    }


def send_to_server(url: str, payload: dict, timeout: int = 30, force_mysql_export: bool = False) -> bool:
    body = json.dumps(payload, default=str, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed_body = gzip.compress(body, compresslevel=9)
    _log(
        "INFO",
        f"client_version={APP_VERSION} POST {url} rows_total={len(payload.get('rows', []))} compressed_size={len(compressed_body)} bytes",
    )

    req = request.Request(
        url,
        data=compressed_body,
        headers={
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed_body)),
            "User-Agent": _client_user_agent(),
            "X-Force-MySQL-Export": "true" if force_mysql_export else "false",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            response_bytes = resp.read()
            if resp.headers.get("Content-Encoding", "").lower() == "gzip":
                response_bytes = gzip.decompress(response_bytes)

            response_text = response_bytes.decode("utf-8", errors="replace")
            status_code = getattr(resp, "status", resp.getcode())
            if status_code != 200:
                _log("ERROR", f"Server returned non-200 status={status_code} response={response_text[:500]}")
                return False

            response_json = json.loads(response_text)
            if isinstance(response_json, dict):
                _log("INFO", f"Server response payload: {response_json}")
                return bool(response_json.get("ok", False))
            _log("INFO", f"Server response payload: {response_text}")
            return False
    except error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        _log("ERROR", f"Server HTTP error status={exc.code} reason={exc.reason} body={body_text[:500]}")
        return False
    except error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        if isinstance(reason, OSError):
            _log("ERROR", f"Server offline or unreachable: {reason}")
        else:
            _log("ERROR", f"Server connection error: {reason}")
        return False
    except (TypeError, ValueError, OSError) as exc:
        _log("ERROR", f"Server communication failed: {exc}")
        return False


async def fetch_and_send(server_url: str, timeout: int, client_hostname: str, force_mysql_export: bool = False) -> int:
    started = time.perf_counter()
    _log("INFO", f"Client start UTC: {_utc_timestamp()}")

    async with FR24() as fr24:
        fetch_started = time.perf_counter()
        response = await fr24.live_feed.fetch(fields=["flight", "reg", "type", "icao_address", "route"])
        fetch_elapsed = time.perf_counter() - fetch_started

        datac = response.to_arrow()
        df = datac.df
        payload = build_payload(df, client_hostname=client_hostname, force_mysql_export=force_mysql_export)

    _log(
        "INFO",
        f"client_version={APP_VERSION} fetch_rows={len(df)} fetch_time={fetch_elapsed:.2f}s client_name={client_hostname} force_export={force_mysql_export}",
    )
    sent_ok = send_to_server(server_url, payload, timeout=timeout, force_mysql_export=force_mysql_export)
    total_elapsed = time.perf_counter() - started
    _log("INFO", f"Client total runtime: {total_elapsed:.2f}s")

    return 0 if sent_ok else 1


def parse_args() -> argparse.Namespace:
    client_cfg = _load_client_config()
    cfg_hostname = str(client_cfg.get("hostname", "")).strip() or socket.gethostname()
    parser = argparse.ArgumentParser(description="Fetch FR24 live data and send it to FR24 server")
    parser.add_argument(
        "--server-url",
        default=client_cfg.get("server_url", "http://127.0.0.1:8080/ingest"),
        help="FR24 server endpoint URL",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(client_cfg.get("timeout", 30)),
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--hostname",
        default=cfg_hostname,
        help="Hostname to include in payload metadata",
    )
    parser.add_argument(
        "--force-mysql-export",
        action="store_true",
        help="Immediately trigger MySQL export after upload",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(asyncio.run(fetch_and_send(args.server_url, args.timeout, args.hostname, args.force_mysql_export)))


if __name__ == "__main__":
    main()
