import datetime as dt
import json
import re
import tomllib
from pathlib import Path

import duckdb
import flag
import nearby
import pandas as pd

_NEARBY_LOADED = False
_DEFAULT_CATEGORY_CACHE = None
DEFAULT_NEAR_MAX_ALT_FT = 3000
_CONFIG_PATH = Path(__file__).with_name("config.toml")


def _load_icao_category_cache() -> dict[str, str]:
    global _DEFAULT_CATEGORY_CACHE
    if _DEFAULT_CATEGORY_CACHE is not None:
        return _DEFAULT_CATEGORY_CACHE

    category_path = Path(__file__).with_name("icao_adsb_category.json")
    try:
        with category_path.open(encoding="utf-8") as file_obj:
            raw = json.load(file_obj)
    except (FileNotFoundError, json.JSONDecodeError):
        _DEFAULT_CATEGORY_CACHE = {}
        return _DEFAULT_CATEGORY_CACHE

    categories: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        category = value.get("category")
        if category:
            categories[str(key).strip().upper()] = str(category).strip().upper()
    _DEFAULT_CATEGORY_CACHE = categories
    return _DEFAULT_CATEGORY_CACHE


def _load_near_config() -> dict:
    with _CONFIG_PATH.open("rb") as file_obj:
        config_data = tomllib.load(file_obj)
    return config_data.get("near", {})


def _ensure_nearby_loaded() -> bool:
    global _NEARBY_LOADED
    if _NEARBY_LOADED:
        return True
    try:
        nearby.load()
        _NEARBY_LOADED = True
        return True
    except Exception as exc:
        print(f"Failed to load airports.json for near-airport detection: {exc}")
        return False


def _safe_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _format_near_code(airport_code: str) -> str:
    return (airport_code or "").strip().upper()[:8]


def _norm_code(value) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _near_max_alt_ft() -> float:
    near_cfg = _load_near_config()
    value = near_cfg.get("max_alt_ft", DEFAULT_NEAR_MAX_ALT_FT)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(DEFAULT_NEAR_MAX_ALT_FT)
    if parsed <= 0:
        return float(DEFAULT_NEAR_MAX_ALT_FT)
    return parsed


def _nearest_airport_codes(lat: float, lon: float, radius_nm: float = 150.0, use_commercial: bool = True) -> tuple[str, str]:
    if use_commercial:
        idx = getattr(nearby, "commercial_buckets", None)
        cell = getattr(nearby, "COMMERCIAL_CELL_SIZE", None)
    else:
        idx = getattr(nearby, "buckets", None)
        cell = getattr(nearby, "CELL_SIZE", None)

    if not idx or cell is None:
        return "", ""

    cx = int(lat // cell)
    cy = int(lon // cell)
    best_icao = ""
    best_iata = ""
    best_dist = float(radius_nm)
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for alat, alon, icao, iata in idx.get(f"{cx+dx}_{cy+dy}", []):
                dist = nearby.haversine_nm(lat, lon, alat, alon)
                if dist <= best_dist:
                    best_dist = dist
                    best_icao = _norm_code(icao)
                    best_iata = _norm_code(iata)
    return best_icao, best_iata


def int2hexcode(raw_int):
    if raw_int == 0:
        return "------"
    return hex(raw_int).replace("0x", "").upper().rjust(6, "0")


def int2frid(raw_int):
    return hex(raw_int).replace("0x", "")


def ymd(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


def normalize_callsign_series(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.mask(s == "", "------")


def al_series(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str)
    blocked = s.str.match(r"^((RA|UK)[0-9]{5}|V8001|T7[0-9]|B[0-9]{4,5}|N([0-9]{1,5})([A-Z]{0,2})?)")
    extracted = s.str.extract(r"^([A-Z]{2,10})[0-9]", expand=False).fillna("")
    return extracted.mask(blocked, "")


def split_flt(flt):
    if pd.isna(flt):
        return flt

    match = re.match(r"^([A-Z]{3}|[A-Z]{2}|[A-Z][0-9]|[0-9][A-Z])(.*)", flt, re.I)
    if match:
        if len(match.group(1)) == 2:
            airline = match.group(1)
            flight_number = match.group(2).strip().zfill(4)
        elif len(match.group(1)) == 3:
            airline = match.group(1)
            flight_number = match.group(2)
        return airline + flight_number
    return ""


def get_icao_category(icao_type: str | None, default: str = "") -> str:
    if not icao_type:
        return default

    key = str(icao_type).strip().upper()
    if not key:
        return default

    category = _load_icao_category_cache().get(key)
    return category if category else default


def fr24convert(df) -> pd.DataFrame:
    if df.empty:
        print("No data to process.")
        return df

    rename_map = {
        "timestamp": "fcontact",
        "typecode": "ac",
        "source": "frdr",
        "latitude": "flat",
        "longitude": "flon",
        "track": "fhdg",
        "altitude": "falt",
        "ground_speed": "fspeed",
        "vertical_speed": "fvert",
        "squawk": "fsq",
        "origin": "dep",
        "destination": "arr",
        "registration": "reg",
    }
    existing_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    if existing_rename_map:
        df = df.rename(columns=existing_rename_map)

    if "flightid" in df.columns:
        flightid = pd.to_numeric(df["flightid"], errors="coerce").fillna(0).astype(int)
        df["frid"] = flightid.apply(int2frid)
    else:
        df["frid"] = "------"
    if "icao_address" in df.columns:
        icao = pd.to_numeric(df["icao_address"], errors="coerce").fillna(0).astype(int)
        df["hex"] = icao.apply(int2hexcode)
    else:
        df["hex"] = "------"

    if "callsign" in df.columns:
        df["cs"] = normalize_callsign_series(df["callsign"])
    elif "cs" in df.columns:
        df["cs"] = normalize_callsign_series(df["cs"])
    else:
        df["cs"] = "------"
    df["al"] = al_series(df["cs"])
    df["lcontact"] = df["fcontact"]
    hex_to_iso = {hx: flag.find_icao_range(hx) for hx in df["hex"].dropna().unique()}
    df["iso"] = df["hex"].map(hex_to_iso)
    if "fcontact" in df.columns:
        df["ymd"] = df["fcontact"].apply(ymd)
    elif "timestamp" in df.columns:
        df["ymd"] = df["timestamp"].apply(ymd)
    else:
        df["ymd"] = dt.date(1970, 1, 1)

    if "on_ground" in df.columns and "gnd" not in df.columns:
        df["gnd"] = df["on_ground"].apply(lambda x: 1 if pd.notna(x) and x > 0 else 0)

    if "flight" in df.columns and "flt" not in df.columns:
        df["flt"] = df["flight"].apply(split_flt)
    if "typecode" in df.columns and "ac" not in df.columns:
        df["ac"] = df["typecode"].astype(str).str.upper()
    if "ac" in df.columns:
        if "cat" not in df.columns:
            df["cat"] = ""
        df["cat"] = df["cat"].fillna("").astype(str).str.strip().str.upper()
        missing_cat = df["cat"] == ""
        if missing_cat.any():
            mapped = df.loc[missing_cat, "ac"].fillna("").astype(str).str.strip().str.upper().map(
                lambda value: get_icao_category(value, default="")
            )
            df.loc[missing_cat, "cat"] = mapped
    if "leg" not in df.columns:
        df["leg"] = 0
    if "dep_rwy" not in df.columns:
        df["dep_rwy"] = ""
    if "arr_rwy" not in df.columns:
        df["arr_rwy"] = ""
    if "cat" not in df.columns:
        df["cat"] = ""
    missing_last_contact_cols = [
        "addr",
        "llat",
        "llon",
        "lalt",
        "lhdg",
        "lspeed",
        "lvert",
        "lsq",
        "lrdr",
        "lnear",
        "fnear",
    ]

    for col in missing_last_contact_cols:
        if col not in df.columns:
            if col in ("fnear", "lnear", "lrdr", "addr"):
                df[col] = ""
            else:
                df[col] = 0

    if _ensure_nearby_loaded():
        max_alt_ft = _near_max_alt_ft()
        near_values = []
        dep_rwy_values = []
        arr_rwy_values = []
        for row in df.itertuples(index=False):
            lat = _safe_float(getattr(row, "flat", None))
            lon = _safe_float(getattr(row, "flon", None))
            alt_ft = _safe_float(getattr(row, "falt", None))
            track = _safe_float(getattr(row, "fhdg", None))
            dep_code = _norm_code(getattr(row, "dep", ""))
            arr_code = _norm_code(getattr(row, "arr", ""))
            current_dep_rwy = _norm_code(getattr(row, "dep_rwy", ""))
            current_arr_rwy = _norm_code(getattr(row, "arr_rwy", ""))

            if lat is None or lon is None or alt_ft is None or alt_ft > max_alt_ft:
                near_values.append("")
                dep_rwy_values.append(current_dep_rwy)
                arr_rwy_values.append(current_arr_rwy)
                continue

            use_commercial = alt_ft > max_alt_ft
            airport_icao, airport_iata = _nearest_airport_codes(lat, lon, radius_nm=150.0, use_commercial=use_commercial)
            near_code = airport_iata or airport_icao
            runway_code = (
                nearby.nearest_runway(airport_icao or airport_iata, lat, lon, track, max_nm=6)
                if (airport_icao or airport_iata) and track is not None
                else None
            )
            airport_icao_norm = _norm_code(airport_icao)
            airport_iata_norm = _norm_code(airport_iata)
            runway_code_norm = _norm_code(runway_code)
            near_values.append(_format_near_code(near_code))
            dep_matches_near = dep_code in {airport_icao_norm, airport_iata_norm}
            arr_matches_near = arr_code in {airport_icao_norm, airport_iata_norm}
            dep_rwy_values.append(runway_code_norm if runway_code_norm and dep_matches_near else current_dep_rwy)
            arr_rwy_values.append(runway_code_norm if runway_code_norm and arr_matches_near else current_arr_rwy)

        df["fnear"] = near_values
        df["dep_rwy"] = dep_rwy_values
        df["arr_rwy"] = arr_rwy_values

    pd.set_option("display.max_columns", None)
    return df


def export_df_to_duckdb(
    df: pd.DataFrame,
    table_name: str = "fr24",
    base_dir: str = "./duckdb",
) -> int:
    if df is None or df.empty:
        return 0

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
    defaults = {
        "frid": "------",
        "ymd": dt.date(1970, 1, 1),
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
              flon DECIMAL(8,5),
              flat DECIMAL(8,5),
              llon DECIMAL(8,5),
              llat DECIMAL(8,5),
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
              leg INTEGER,
              flt VARCHAR,
              al VARCHAR,
              dep VARCHAR,
              dep_rwy VARCHAR,
              arr VARCHAR,
              arr_rwy VARCHAR,
              frdr VARCHAR,
              lrdr VARCHAR,
              fnear VARCHAR,
              lnear VARCHAR,
              cat VARCHAR
         )
    """

    total_written = 0
    insert_columns = ", ".join(columns)
    update_l_columns_sql = ", ".join(
        [
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
            "fnear=COALESCE(NULLIF(fnear, ''), excluded.fnear)",
            "dep=excluded.dep",
            "dep_rwy=COALESCE(NULLIF(dep_rwy, ''), excluded.dep_rwy)",
            "arr=excluded.arr",
            "arr_rwy=excluded.arr_rwy",
            "cat=CASE WHEN cat IS NULL OR cat = '' THEN excluded.cat ELSE cat END",
        ]
    )
    insert_sql = (
        f"INSERT INTO {table_name} ({insert_columns}) "
        f"SELECT {insert_columns} FROM batch_df "
        f"ON CONFLICT (ymd, frid, hex, cs, gnd) DO UPDATE SET {update_l_columns_sql}"
    )
    for month_period, month_df in prepared.groupby(prepared["ymd"].dt.to_period("M")):
        db_path = out_dir / f"fr24_{month_period.year}-{month_period.month:02d}.duckdb"
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(create_sql)
            existing_columns = {row[1] for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
            if "leg" not in existing_columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN leg INTEGER")
            if "dep_rwy" not in existing_columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN dep_rwy VARCHAR")
            if "arr_rwy" not in existing_columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN arr_rwy VARCHAR")
            if "cat" not in existing_columns:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN cat VARCHAR")
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_ymd_frid_hex_cs_gnd ON {table_name}(ymd, frid, hex, cs, gnd)")
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
