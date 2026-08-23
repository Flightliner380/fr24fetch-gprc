import os
import time
from datetime import datetime

import duckdb
import config
from sqlalchemy import create_engine


def mysql_export(frdbmysql: str = "k53371_fp.fr24", chunk_size: int = 7500) -> None:

    started_at = time.perf_counter()

    ym = datetime.now().strftime("%Y-%m")
    ymd = datetime.now().strftime("%Y-%m-%d")

    # frdbmysql = "k53371_fp.fr24_t1"

    # chunk_size = 15000

    db_name = os.path.join(
        "duckdb",
        f"fr24_{ym}.duckdb",
    )

    print(f"Date:{ymd} DuckDB: {db_name} Chunk size: {chunk_size} MySQL table: {frdbmysql}")
    
    if not os.path.exists(db_name):
        raise FileNotFoundError(db_name)

    # ---------------------------------------------------------
    # SQLAlchemy MySQL
    # ---------------------------------------------------------

    mysql_conf = config.MYSQL_CONFIG

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

    print(
        f"Max lcontact: {max_lcontact}"
    )
    print(
        f"MAX(lcontact) query time: {time.perf_counter() - query_started_at:.2f}s"
    )

    # ---------------------------------------------------------
    # DuckDB
    export_started_at = time.perf_counter()
    # ---------------------------------------------------------

    db = duckdb.connect(
        database=db_name,
        read_only=True,
    )

    try:

        duck_cursor = db.execute(
            """
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
                flt,
                al,
                dep,
                arr,
                frdr,
                lrdr
            FROM fr24
            WHERE ymd = ?
              AND lcontact > ? AND gnd = 0
            ORDER BY lcontact, iso, hex
            """,
            [
                ymd,
                max_lcontact,
            ],
        )

        # ---------------------------------------------------------
        # MySQL columns
        # ---------------------------------------------------------

        mysql_columns = [
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
            "flt",
            "al",
            "dep",
            "arr",
            "frdr",
            "lrdr",
            "fnear",
            "lnear",
        ]

        insert_columns = ", ".join(
            f"`{x}`"
            for x in mysql_columns
        )

        # ---------------------------------------------------------
        # 31 DuckDB values
        # + 4 parameters for nb()
        # = 35 parameters per row
        # ---------------------------------------------------------

        value_template = (
            "("
            "%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,"

            # fnear
            "k53371_fp.nb(%s,%s),"

            # lnear
            "k53371_fp.nb(%s,%s)"
            ")"
        )

        # Verify template
        assert value_template.count("%s") == 35

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

                # NULL -> ""
                row = [
                    "" if value is None else value
                    for value in row
                ]

                # Coordinates
                flon = row[10]
                flat = row[11]
                llon = row[12]
                llat = row[13]

                values_sql.append(
                    value_template
                )

                # 31 normal values
                params.extend(row)

                # fnear
                params.append(flat)
                params.append(flon)

                # lnear
                params.append(llat)
                params.append(llon)

            # -----------------------------------------------------
            # Sanity check
            # -----------------------------------------------------

            expected = len(rows) * 35

            if len(params) != expected:
                raise RuntimeError(
                    f"Parameter mismatch: "
                    f"{len(params)} != {expected}"
                )

            # -----------------------------------------------------
            # ONE MySQL INSERT
            # -----------------------------------------------------

            sql = f"""
                INSERT INTO {frdbmysql}
                ({insert_columns})
                VALUES
                {",".join(values_sql)}
                AS nv

                ON DUPLICATE KEY UPDATE

                    lcontact = nv.lcontact,
                    llat = nv.llat,
                    llon = nv.llon,
                    lalt = nv.lalt,
                    lhdg = nv.lhdg,
                    lspeed = nv.lspeed,
                    lvert = nv.lvert,
                    lsq = nv.lsq,
                    lrdr = nv.lrdr,

                    lnear = k53371_fp.nb(
                        nv.llat,
                        nv.llon
                    ),

                    dep = nv.dep,
                    arr = nv.arr
            """

            print(
                f"Bulk INSERT: {len(rows)} rows"
            )

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

            print(
                f"Processed: {total}"
            )
            print(
                f"Elapsed export time: {time.perf_counter() - export_started_at:.2f}s"
            )

    finally:

        db.close()
        engine.dispose()

        print(
            "Connections closed."
        )
        print(
            f"Total runtime: {time.perf_counter() - started_at:.2f}s"
        )


if __name__ == "__main__":
    mysql_export()