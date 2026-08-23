import asyncio
import time
import pandas as pd
import datetime as dt
import re
import flag
import duckdb
import os
from datetime import datetime

from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import config
from fr24.core import FR24

def int2hexcode(int): 
     if int == 0:
          return '------'
     else:
          return hex(int).replace('0x','').upper().rjust(6,'0')
def callsign(cs):
     if pd.isna(cs) or str(cs).strip() == '':
          return '------'
     else:
          return str(cs)
def int2frid(int): 
     return hex(int).replace('0x','')

def ymd(ts): 
     return dt.datetime.fromtimestamp(ts, dt.UTC).strftime('%Y-%m-%d')

def al(cs):
     x=re.findall("^((RA|UK)[0-9]{5}|V8001|T7[0-9]|B[0-9]{4,5}|^N([0-9]{1,5})([A-Z]{0,2})?)",cs)
     if len(x) > 0:
          return ''
     x=re.findall("^([A-Z]{2,10})[0-9]",cs)
     if len(x) == 1:
          return x[0]
     else:
          return ''

def normalize_callsign_series(series: pd.Series) -> pd.Series:
     s = series.fillna('').astype(str).str.strip()
     return s.mask(s == '', '------')

def al_series(series: pd.Series) -> pd.Series:
     s = series.fillna('').astype(str)
     blocked = s.str.match(r"^((RA|UK)[0-9]{5}|V8001|T7[0-9]|B[0-9]{4,5}|N([0-9]{1,5})([A-Z]{0,2})?)")
     extracted = s.str.extract(r"^([A-Z]{2,10})[0-9]", expand=False).fillna('')
     return extracted.mask(blocked, '')

def split_flt(flt):
    if pd.isna(flt):
        return flt
    
    match = re.match(r"^([A-Z]{3}|[A-Z]{2}|[A-Z][0-9]|[0-9][A-Z])(.*)", flt, re.I)
    if match:
        if len(match.group(1)) == 2:
            al = match.group(1)
            fnum = match.group(2).strip().zfill(4)
        elif len(match.group(1)) == 3:
            al = match.group(1)
            fnum = match.group(2)
        return al + fnum
    return ''


def fr24convert(df) -> pd.DataFrame:
     if df.empty:
          print("No data to process." )
          return df

     rename_map = {
          'timestamp': 'fcontact',
          'typecode': 'ac',
          'source': 'frdr',
          'latitude': 'flat',
          'longitude': 'flon',
          'track': 'fhdg',
          'altitude': 'falt',
          'ground_speed': 'fspeed',
          'vertical_speed': 'fvert',
          'squawk': 'fsq',
          'origin': 'dep',
          'destination': 'arr',
          'registration': 'reg',
     }
     existing_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
     if existing_rename_map:
          df = df.rename(columns=existing_rename_map)

     
     if 'flightid' in df.columns:
          flightid = pd.to_numeric(df['flightid'], errors='coerce').fillna(0).astype(int)
          df['frid'] = flightid.apply(int2frid)
     else:
          df['frid'] = '------'
     if 'icao_address' in df.columns:
          icao = pd.to_numeric(df['icao_address'], errors='coerce').fillna(0).astype(int)
          df['hex'] = icao.apply(int2hexcode)
     else:
          df['hex'] = '------'

     if 'callsign' in df.columns:
          df['cs'] = normalize_callsign_series(df['callsign'])
     elif 'cs' in df.columns:
          df['cs'] = normalize_callsign_series(df['cs'])
     else:
          df['cs'] = '------'
     df['al'] = al_series(df['cs'])
     hex_to_iso = {hx: flag.find_icao_range(hx) for hx in df['hex'].dropna().unique()}
     df['iso'] = df['hex'].map(hex_to_iso)
     if 'fcontact' in df.columns:
          df['ymd'] = df['fcontact'].apply(ymd)
     elif 'timestamp' in df.columns:
          df['ymd'] = df['timestamp'].apply(ymd)
     else:
          df['ymd'] = dt.date(1970, 1, 1)

     if 'on_ground' in df.columns and 'gnd' not in df.columns:
          df['gnd']=df['on_ground'].apply(lambda x: 1 if pd.notna(x) and x > 0 else 0)

     if 'flight' in df.columns and 'flt' not in df.columns:
          df['flt'] = df['flight'].apply(split_flt)
     missing_last_contact_cols = [
          'addr', 'lcontact', 'llat', 'llon', 'lalt', 'lhdg', 'lspeed', 'lvert', 'lsq', 'lrdr', 'lnear','fnear'
     ]

     for col in missing_last_contact_cols:
          if col not in df.columns:
               if col in ('lnear', 'lrdr','addr'):
                    df[col] = ''
               else:
                    df[col] = 0

     # df['fspeed'] = df['ground_speed'] if 'ground_speed' in df.columns else None
     # df['fvert'] = df['vertical_speed'] if 'vertical_speed' in df.columns else None
     
     
     pd.set_option('display.max_columns', None)
    #  print(df)
    #  print(df.columns)
     
     return df

def export_df_to_mysql(
    df: pd.DataFrame,
    table_name: str = "k53371_fp.fr24_t",
    batch_size: int = 10000,
) -> int:

    if df is None or df.empty:
        return 0

    mysql_conf = config.MYSQL_CONFIG

    columns = [
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

    # Only use required columns
    prepared = df[columns].copy()

    # Convert NaN / NaT to None
    prepared = prepared.where(pd.notna(prepared), None)

    # MySQL connection
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
    )

    columns_sql = ", ".join(columns)
    inserted = 0

    try:
          with engine.connect() as conn:
               total = len(prepared)

               for batch_start in range(0, total, batch_size):
                    batch = prepared.iloc[
                         batch_start:batch_start + batch_size
                    ]

                    print(
                         f"Inserting {batch_start + 1}-"
                         f"{min(batch_start + len(batch), total)} "
                         f"of {total}"
                    )

                    params = {}
                    value_rows = []

                    # Build one VALUES (...) per dataframe row
                    for row_number, row in enumerate(
                         batch.itertuples(index=False, name=None)
                    ):
                         values = []

                         for col_number, col in enumerate(columns):
                              parameter = f"{col}_{row_number}"

                              # fnear is calculated by MySQL
                              if col == "fnear":
                                   values.append(
                                        f"k53371_fp.nb("
                                        f":flat_{row_number}, "
                                        f":flon_{row_number})"
                                   )
                              else:
                                   values.append(f":{parameter}")
                                   params[parameter] = row[col_number]

                         value_rows.append(
                              "(" + ", ".join(values) + ")"
                         )

                    sql = f"""
                         INSERT INTO {table_name}
                         ({columns_sql})
                         VALUES
                         {", ".join(value_rows)}
                         AS nv
                         ON DUPLICATE KEY UPDATE
                              lcontact = nv.fcontact,
                              llon     = nv.flon,
                              llat     = nv.flat,
                              lalt     = nv.falt,
                              lhdg     = nv.fhdg,
                              lspeed   = nv.lspeed,
                              lvert    = nv.lvert,
                              lsq      = nv.lsq,
                              lrdr     = nv.lrdr,
                              lnear    = k53371_fp.nb(
                                   nv.flat,
                                   nv.flon
                              )
                    """

                    trans = conn.begin()
                    try:
                         conn.execute(
                              text(sql),
                              params
                         )
                         trans.commit()
                    except Exception:
                         trans.rollback()
                         raise

                    inserted += len(batch)

    except SQLAlchemyError:
        raise

    finally:
        engine.dispose()

    return inserted


def export_df_to_duckdb(
     df: pd.DataFrame,
     table_name: str = "fr24",
     base_dir: str = "./duckdb",
) -> int:
     if df is None or df.empty:
          return 0

     columns = [
          "frid", "ymd", "addr", "fcontact", "lcontact", "hex", "reg", "ac", "cs", "iso",
          "flon", "flat", "llon", "llat", "falt", "lalt", "fhdg", "lhdg", "fspeed", "lspeed",
          "fvert", "lvert", "fsq", "lsq", "gnd", "flt", "al", "dep", "arr", "frdr", "lrdr",
          "fnear", "lnear",
     ]
     defaults = {
          "frid": "------", "ymd": dt.date(1970, 1, 1), "addr": "", "fcontact": 0, "lcontact": 0,
          "hex": "------", "reg": "", "ac": "", "cs": "------", "iso": "",
          "flon": 0.0, "flat": 0.0, "llon": 0.0, "llat": 0.0,
          "falt": 0, "lalt": 0, "fhdg": 0, "lhdg": 0, "fspeed": 0, "lspeed": 0,
          "fvert": 0, "lvert": 0, "fsq": 0, "lsq": 0, "gnd": 0,
          "flt": "", "al": "", "dep": "", "arr": "", "frdr": "", "lrdr": "", "fnear": None, "lnear": None,
     }

     prepared = df.copy()
     for col in columns:
          if col not in prepared.columns:
               prepared[col] = defaults[col]
     prepared = prepared[columns]
     prepared["ymd"] = pd.to_datetime(prepared["ymd"], errors="coerce")
     prepared = prepared.dropna(subset=["ymd"])
     if prepared.empty:
          return 0

     out_dir = Path(base_dir)
     out_dir.mkdir(parents=True, exist_ok=True)

     create_sql = f"""
          CREATE TABLE IF NOT EXISTS {table_name} (
               frid VARCHAR,
               ymd DATE,
               addr VARCHAR,
               fcontact BIGINT,
               lcontact BIGINT,
               hex VARCHAR,
               reg VARCHAR,
               ac VARCHAR,
               cs VARCHAR,
               iso VARCHAR,
               flon DOUBLE,
               flat DOUBLE,
               llon DOUBLE,
               llat DOUBLE,
               falt INTEGER,
               lalt INTEGER,
               fhdg INTEGER,
               lhdg INTEGER,
               fspeed INTEGER,
               lspeed INTEGER,
               fvert INTEGER,
               lvert INTEGER,
               fsq INTEGER,
               lsq INTEGER,
               gnd INTEGER,
               flt VARCHAR,
               al VARCHAR,
               dep VARCHAR,
               arr VARCHAR,
               frdr VARCHAR,
               lrdr VARCHAR,
               fnear VARCHAR,
               lnear VARCHAR
          )
     """

     total_written = 0
     insert_columns = ", ".join(columns)
     update_l_columns_sql = ", ".join([
          "lcontact=excluded.fcontact",
          "llon=excluded.flon",
          "llat=excluded.flat",
          "lalt=excluded.falt",
          "lhdg=excluded.fhdg",
          "lspeed=excluded.fspeed",
          "lvert=excluded.fvert",
          "lsq=excluded.fsq",
          "lrdr=excluded.frdr",
          "lnear=excluded.fnear",
     ])
     insert_sql = (
          f"INSERT INTO {table_name} ({insert_columns}) "
          f"SELECT {insert_columns} FROM batch_df "
          f"ON CONFLICT (ymd,frid, hex,cs, gnd) DO UPDATE SET {update_l_columns_sql}"
     )
     print(f"Insert SQL: {insert_sql}")
     for month_period, month_df in prepared.groupby(prepared["ymd"].dt.to_period("M")):
          db_path = out_dir / f"fr24_{month_period.year}-{month_period.month:02d}.duckdb"
          conn = duckdb.connect(str(db_path))
          try:
               conn.execute(create_sql)
               conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_ymd_frid_hex_cs_gnd ON {table_name}(ymd, frid, hex, cs, gnd)"
               )
               batch_df = month_df.copy()
               batch_df["ymd"] = batch_df["ymd"].dt.date
               conn.register("batch_df", batch_df[columns])
               conn.execute(insert_sql)
               conn.unregister("batch_df")
               total_written += len(batch_df)
          finally:
               conn.close()

     print(f"DuckDB rows written: {total_written}")
     return total_written


async def fetch_grpc():
    start_time = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Start time: {start_time}")
    total_start = time.perf_counter()

    async with FR24() as fr24:
        fetch_start = time.perf_counter()
        response = await fr24.live_feed.fetch(fields=["flight", "reg", "type", "icao_address", "route"])
        fetch_elapsed = time.perf_counter() - fetch_start

        datac = response.to_arrow()
        dfm = datac.df

        convert_start = time.perf_counter()
        df=fr24convert(dfm)
        if 'gnd' in df.columns:
            df_filtered = df[df['gnd'] == 0].copy()
        else:
            df_filtered = df.copy()

        print(f"All flights: {len(df)}")
        print(f"df_filtered: {len(df_filtered)}")
        
        # export_df_to_mysql(df_filtered, table_name='k53371_fp.fr24_t1', batch_size=6000)

        export_df_to_duckdb(df, table_name='fr24', base_dir='./duckdb')
        convert_elapsed = time.perf_counter() - convert_start

    total_elapsed = time.perf_counter() - total_start
    print(f"FR24 fetch time: {fetch_elapsed:.2f}s")
    print(f"fr24convert time: {convert_elapsed:.2f}s")
    print(f"Total fetch_grpc time: {total_elapsed:.2f}s")




if __name__ == "__main__":
    asyncio.run(fetch_grpc())

     