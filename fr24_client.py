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


def _load_client_config() -> dict:
    with _CONFIG_PATH.open("rb") as file_obj:
        config_data = tomllib.load(file_obj)
    return config_data.get("client", {})


def build_payload(df: pd.DataFrame, client_hostname: str, force_mysql_export: bool = False) -> dict:
    payload_df = df.where(pd.notna(df), None)
    payload_timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    return {
        "s": "FR24 gRPC fetch",
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

    req = request.Request(
        url,
        data=compressed_body,
        headers={
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed_body)),
            "User-Agent": "fr24fetch-client/1.0",
            "fr24upload": "true",
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
            print(f"Server status: {getattr(resp, 'status', resp.getcode())}")
            print(f"Server response: {response_text}")

            response_json = json.loads(response_text)
            if isinstance(response_json, dict):
                return bool(response_json.get("ok", False))
            return False
    except error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        print(f"Server HTTP error: {exc.code} {exc.reason} body={body_text}")
        return False
    except error.URLError as exc:
        print(f"Server connection error: {exc.reason}")
        return False
    except (TypeError, ValueError, OSError) as exc:
        print(f"Server communication failed: {exc}")
        return False


async def fetch_and_send(server_url: str, timeout: int, client_hostname: str, force_mysql_export: bool = False) -> int:
    started = time.perf_counter()
    print(f"Client start UTC: {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    async with FR24() as fr24:
        fetch_started = time.perf_counter()
        response = await fr24.live_feed.fetch(fields=["flight", "reg", "type", "icao_address", "route"])
        fetch_elapsed = time.perf_counter() - fetch_started

        datac = response.to_arrow()
        df = datac.df
        payload = build_payload(df, client_hostname=client_hostname, force_mysql_export=force_mysql_export)

        print(f"Fetched rows: {len(df)}")
        print(f"Fetch time: {fetch_elapsed:.2f}s")
        print(f"Client hostname: {client_hostname}")
        print(f"Force MySQL export: {force_mysql_export}")

    sent_ok = send_to_server(server_url, payload, timeout=timeout, force_mysql_export=force_mysql_export)
    total_elapsed = time.perf_counter() - started
    print(f"Client total runtime: {total_elapsed:.2f}s")

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
