import logging
import os
import time
from datetime import datetime
import datetime as dt
import duckdb
import tomllib
from pathlib import Path
from sqlalchemy import create_engine

LOGGER = logging.getLogger("fr24_mysql_export")

_PROJECT_ROOT = Path(__file__).resolve().parent
_CONFIG_PATH = _PROJECT_ROOT / "config.toml"


def _load_config() -> dict:
    with _CONFIG_PATH.open("rb") as file_obj:
        return tomllib.load(file_obj)


def _resolve_duckdb_dir(duckdb_dir: str | None = None) -> Path:
    config_data = _load_config()
    export_cfg = config_data.get("export", {})
    server_cfg = config_data.get("server", {})
    configured_dir = duckdb_dir or export_cfg.get("duckdb_dir") or server_cfg.get("duckdb_dir") or "./duckdb"
    resolved = Path(str(configured_dir)).expanduser()
    if not resolved.is_absolute():
        resolved = (_PROJECT_ROOT / resolved).resolve()
    return resolved


def _normalize_exclusion_values(raw_value) -> set[str]:
    if raw_value is None:
        return set()

    values: list[str]
    if isinstance(raw_value, str):
        values = [raw_value]
    elif isinstance(raw_value, (list, tuple, set)):
        values = [str(item) for item in raw_value]
    else:
        values = [str(raw_value)]

    normalized: set[str] = set()
    for item in values:
        if item is None:
            continue
        for chunk in str(item).replace(";", ",").split(","):
            cleaned = chunk.strip()
            if cleaned:
                normalized.add(cleaned.upper())
    return normalized


def _mysql_export_excluded_values() -> dict[str, set[str]]:
    config_data = _load_config()
    export_cfg = config_data.get("export", {})

    exclusion_map = {
        "hex": export_cfg.get("exclude_hex", export_cfg.get("exclude_hex_codes", [])),
        "cs": export_cfg.get("exclude_callsign", export_cfg.get("exclude_callsign_values", [])),
        "cat": export_cfg.get("exclude_cat", export_cfg.get("exclude_cat_values", [])),
        "ac": export_cfg.get("exclude_ac", export_cfg.get("exclude_ac_values", [])),
    }

    return {
        key: _normalize_exclusion_values(value)
        for key, value in exclusion_map.items()
    }


def _count_excluded_rows(
    conn,
    table_name: str,
    ymd: str,
    max_lcontact: int,
    excluded_values: dict[str, set[str]],
) -> tuple[dict[str, int], int]:
    counts: dict[str, int] = {}
    total_excluded = 0
    for field_name, values in excluded_values.items():
        if not values:
            counts[field_name] = 0
            continue
        normalized_values = sorted(str(value).upper() for value in values)
        placeholders = ", ".join(["?"] * len(normalized_values))
        count_sql = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE ymd = ?
              AND lcontact > ?
              AND gnd = 0
              AND upper({field_name}) IN ({placeholders})
        """
        params = [ymd, max_lcontact, *normalized_values]
        counts[field_name] = conn.execute(count_sql, params).fetchone()[0]

    if excluded_values:
        combined_filters = []
        combined_params: list[object] = [ymd, max_lcontact]
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
                FROM {table_name}
                WHERE ymd = ?
                  AND lcontact > ?
                  AND gnd = 0
                  AND ({' OR '.join(combined_filters)})
            """
            total_excluded = conn.execute(total_sql, combined_params).fetchone()[0]
    return counts, total_excluded


def mysql_export(
    frdbmysql: str = "k53371_fp.fr24",
    chunk_size: int | None = None,
    duckdb_table: str | None = None,
) -> None:

    started_at = time.perf_counter()
    config_data = _load_config()
    export_cfg = config_data.get("export", {})
    mysql_conf = config_data.get("mysql", {})

    ym = datetime.now().strftime("%Y-%m")
    ymd = datetime.now().strftime("%Y-%m-%d")
    if chunk_size is None:
        chunk_size = int(export_cfg.get("chunk_size", 7500))
    if duckdb_table is None:
        duckdb_table = str(export_cfg.get("duckdb_table", "fr24"))

    # frdbmysql = "k53371_fp.fr24_t1"

    # chunk_size = 15000

    db_dir = _resolve_duckdb_dir()
    db_name = str(db_dir / f"fr24_{ym}.duckdb")

    LOGGER.info(
        "%s Date:%s DuckDB:%s Chunk size:%s MySQL table:%s",
        dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        ymd,
        db_name,
        chunk_size,
        frdbmysql,
    )
    LOGGER.info("DuckDB source table: %s", duckdb_table)

    if not os.path.exists(db_name):
        raise FileNotFoundError(db_name)

    # ---------------------------------------------------------
    # SQLAlchemy MySQL
    # ---------------------------------------------------------

    driver = mysql_conf.get("driver", "pymysql")
    port = mysql_conf.get("port", 3306)

    mysql_url = (
        f"mysql+{driver}://"
        f"{mysql_conf['user']}:{mysql_conf['password']}"
        f"@{mysql_conf['host']}:{port}/"
        f"{mysql_conf['database']}"
        "?charset=utf8mb4"
    )

    engine = create_engine(
        mysql_url,
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    # ---------------------------------------------------------
    # Get MAX(lcontact)
    # ---------------------------------------------------------

    with engine.connect() as conn:

        query_started_at = time.perf_counter()

        result = conn.exec_driver_sql(
            f"""
            SELECT MAX(lcontact)
            FROM {frdbmysql}
            """
        )

        max_lcontact = (
            result.scalar_one_or_none() or 0
        )

    LOGGER.info("Max lcontact: %s", max_lcontact)
    LOGGER.debug("MAX(lcontact) query time: %.2fs", time.perf_counter() - query_started_at)

    # ---------------------------------------------------------
    # DuckDB
    export_started_at = time.perf_counter()
    # ---------------------------------------------------------

    db = duckdb.connect(
        database=db_name,
        read_only=True,
    )

    try:

        excluded_values = _mysql_export_excluded_values()
        per_field_counts, total_excluded_rows = _count_excluded_rows(
            db,
            duckdb_table,
            ymd,
            max_lcontact,
            excluded_values,
        )
        if any(excluded_values.values()):
            LOGGER.info(
                "Export exclude counts: hex=%d, cs=%d, cat=%d, ac=%d | total skipped rows=%d",
                per_field_counts.get("hex", 0),
                per_field_counts.get("cs", 0),
                per_field_counts.get("cat", 0),
                per_field_counts.get("ac", 0),
                total_excluded_rows,
            )
        else:
            LOGGER.info("Export exclude counts: none configured")

        where_clauses = [
            "ymd = ?",
            "lcontact > ?",
            "gnd = 0",
        ]
        where_params: list[object] = [ymd, max_lcontact]

        for field_name, values in excluded_values.items():
            if not values:
                continue
            placeholders = ", ".join(["?"] * len(values))
            column_sql = f'"{field_name}"'
            where_clauses.append(f"upper({column_sql}) NOT IN ({placeholders})")
            where_params.extend(sorted(values))

        duck_cursor = db.execute(
            f"""
            SELECT
                frid,
                ymd,
                addr,
                fcontact,
                lcontact,
                hex,
                reg,
                ac,
                cs,
                iso,
                flon,
                flat,
                llon,
                llat,
                falt,
                lalt,
                fhdg,
                lhdg,
                fspeed,
                lspeed,
                fvert,
                lvert,
                fsq,
                lsq,
                gnd,
                leg,
                flt,
                al,
                dep,
                dep_rwy,
                arr,
                arr_rwy,
                frdr,
                lrdr,
                fnear,
                lnear,
                cat
            FROM {duckdb_table}
            WHERE {" AND ".join(where_clauses)}
            ORDER BY lcontact, iso, hex
            """,
            where_params,
        )

        # ---------------------------------------------------------
        # MySQL columns
        # ---------------------------------------------------------

        full_mysql_columns = [
            "frid",
            "ymd",
            "addr",
            "fcontact",
            "lcontact",
            "hex",
            "reg",
            "ac",
            "cs",
            "iso",
            "flon",
            "flat",
            "llon",
            "llat",
            "falt",
            "lalt",
            "fhdg",
            "lhdg",
            "fspeed",
            "lspeed",
            "fvert",
            "lvert",
            "fsq",
            "lsq",
            "gnd",
            "leg",
            "flt",
            "al",
            "dep",
            "dep_rwy",
            "arr",
            "arr_rwy",
            "frdr",
            "lrdr",
            "fnear",
            "lnear",
            "cat",
        ]
        mysql_columns = full_mysql_columns

        insert_columns = ", ".join(
            f"`{x}`"
            for x in mysql_columns
        )

        # ---------------------------------------------------------
        value_template = "(" + ",".join(["%s"] * len(mysql_columns)) + ")"

        total = 0

        # ---------------------------------------------------------
        # Fetch DuckDB in chunks
        # ---------------------------------------------------------

        while True:

            rows = duck_cursor.fetchmany(
                chunk_size
            )

            if not rows:
                break

            values_sql = []
            params = []

            # -----------------------------------------------------
            # Build VALUES
            # -----------------------------------------------------

            for row in rows:

                row = list(row)
                row_map = {column: value for column, value in zip(full_mysql_columns, row)}

                row_defaults = {
                    "frid": "------",
                    "ymd": ymd,
                    "addr": "",
                    "fcontact": 0,
                    "lcontact": 0,
                    "hex": "------",
                    "reg": "",
                    "ac": "",
                    "cs": "------",
                    "iso": "",
                    "flon": 0.0,
                    "flat": 0.0,
                    "llon": 0.0,
                    "llat": 0.0,
                    "falt": 0,
                    "lalt": 0,
                    "fhdg": 0,
                    "lhdg": 0,
                    "fspeed": 0,
                    "lspeed": 0,
                    "fvert": 0,
                    "lvert": 0,
                    "fsq": 0,
                    "lsq": 0,
                    "gnd": 0,
                    "leg": 0,
                    "flt": "",
                    "al": "",
                    "dep": "",
                    "dep_rwy": "",
                    "arr": "",
                    "arr_rwy": "",
                    "frdr": "",
                    "lrdr": "",
                    "fnear": None,
                    "lnear": None,
                    "cat": "",
                }
                filtered_row = []
                for column in mysql_columns:
                    value = row_map.get(column)
                    if value is None:
                        value = row_defaults.get(column)
                    filtered_row.append(value)

                values_sql.append(
                    value_template
                )

                params.extend(filtered_row)

                # fnear
                # params.append(flat)
                # params.append(flon)

                # lnear
                # params.append(llat)
                # params.append(llon)

            # -----------------------------------------------------
            # Sanity check
            # -----------------------------------------------------

            expected = len(rows) * len(mysql_columns)

            if len(params) != expected:
                raise RuntimeError(
                    f"Parameter mismatch: "
                    f"{len(params)} != {expected}"
                )

            # -----------------------------------------------------
            # ONE MySQL INSERT
            # -----------------------------------------------------

            update_statement_map = {
                "lcontact": "lcontact = nv.lcontact",
                "llat": "llat = nv.llat",
                "llon": "llon = nv.llon",
                "lalt": "lalt = nv.lalt",
                "lhdg": "lhdg = nv.lhdg",
                "lspeed": "lspeed = nv.lspeed",
                "lvert": "lvert = nv.lvert",
                "lsq": "lsq = nv.lsq",
                "lrdr": "lrdr = nv.lrdr",
                "lnear": "lnear = nv.lnear",
                "fnear": f"fnear = IF(COALESCE({frdbmysql}.fnear, '') = '', nv.fnear, {frdbmysql}.fnear)",
                "dep": "dep = nv.dep",
                "dep_rwy": f"dep_rwy = IF(COALESCE({frdbmysql}.dep_rwy, '') = '', nv.dep_rwy, {frdbmysql}.dep_rwy)",
                "arr": "arr = nv.arr",
                "arr_rwy": "arr_rwy = nv.arr_rwy",
                "cat": f"cat = IF(COALESCE({frdbmysql}.cat, '') = '', nv.cat, {frdbmysql}.cat)",
            }
            update_statements = [
                statement
                for column_name, statement in update_statement_map.items()
                if column_name in mysql_columns
            ]

            sql = f"""
                INSERT INTO {frdbmysql}
                ({insert_columns})
                VALUES
                {",".join(values_sql)}
                AS nv

                ON DUPLICATE KEY UPDATE
                    {", ".join(update_statements)}
            """
#                    lnear = k53371_fp.nb(
#                        nv.llat,
#                        nv.llon
#                    ),


            LOGGER.debug("Bulk INSERT: %d rows", len(rows))

            # -----------------------------------------------------
            # IMPORTANT
            #
            # SQLAlchemy manages the transaction,
            # but PyMySQL executes the bulk SQL directly.
            # -----------------------------------------------------

            with engine.begin() as connection:

                raw_connection = (
                    connection.connection.driver_connection
                )

                with raw_connection.cursor() as mysql_cursor:

                    # PyMySQL converts %s -> escaped SQL values
                    final_sql = mysql_cursor.mogrify(
                        sql,
                        tuple(params),
                    )

                    # Debug:
                    # print(final_sql[:1000])

                    # Execute final SQL WITHOUT parameters.
                    mysql_cursor.execute(
                        final_sql
                    )

            total += len(rows)

            LOGGER.debug("Processed: %d", total)
            LOGGER.info(
                "%s MySQL export elapsed: %.2fs total_rows=%d",
                dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                time.perf_counter() - export_started_at,
                total,
            )

    finally:

        db.close()
        engine.dispose()

        LOGGER.info("MySQL export connections closed.")
        LOGGER.info("Total runtime: %.2fs", time.perf_counter() - started_at)


if __name__ == "__main__":
    mysql_export()