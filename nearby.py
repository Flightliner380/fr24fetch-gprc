import json, math
from pathlib import Path

CELL_SIZE = 0.25
COMMERCIAL_CELL_SIZE = 3.0
buckets = runways = commercial_buckets = None

def load(file=None):
    global CELL_SIZE, COMMERCIAL_CELL_SIZE, buckets, runways, commercial_buckets
    with open(file or Path(__file__).with_name("airports.json"), encoding="utf-8") as f:
        d = json.load(f)
    CELL_SIZE = d["cellSize"]
    buckets = d["buckets"]
    runways = d.get("runways", {})
    COMMERCIAL_CELL_SIZE = d.get("commercialCellSize", COMMERCIAL_CELL_SIZE)
    commercial_buckets = d.get("commercialBuckets", {})

def haversine_nm(lat1, lon1, lat2, lon2):
    r = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2-lat1)
    dl = math.radians(lon2-lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0, 1-a)))

def _valid(lat, lon):
    return (isinstance(lat, (int, float)) and math.isfinite(lat)
            and isinstance(lon, (int, float)) and math.isfinite(lon))

def is_near_airport(lat, lon, radius_nm):
    if buckets is None or not _valid(lat, lon):
        return False
    cx, cy = math.floor(lat/CELL_SIZE), math.floor(lon/CELL_SIZE)
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for alat, alon in buckets.get(f"{cx+dx}_{cy+dy}", []):
                if haversine_nm(lat, lon, alat, alon) <= radius_nm:
                    return True
    return False

def _nearest(idx, cell, lat, lon, radius_nm, prefer_iata=False):
    if not idx or not _valid(lat, lon):
        return None
    cx, cy = math.floor(lat/cell), math.floor(lon/cell)
    best, best_dist = None, radius_nm
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for alat, alon, icao, iata in idx.get(f"{cx+dx}_{cy+dy}", []):
                d = haversine_nm(lat, lon, alat, alon)
                if d <= best_dist:
                    best_dist = d
                    best = (iata or icao) if prefer_iata else (icao or iata)
    return best

def nearest_airport(lat, lon, radius_nm):
    return _nearest(buckets, CELL_SIZE, lat, lon, radius_nm)

def nearest_commercial_airport(lat, lon, radius_nm=150):
    return _nearest(commercial_buckets, COMMERCIAL_CELL_SIZE, lat, lon, radius_nm, True)

def nearest_runway(code, lat, lon, heading_deg, max_nm=6):
    if not _valid(lat, lon) or heading_deg is None or not math.isfinite(heading_deg):
        return None
    best, best_dist = None, max_nm
    for ident, hdg, rlat, rlon in (runways or {}).get(code, []):
        diff = abs(hdg-heading_deg) % 360
        diff = min(diff, 360-diff)
        if diff > 30:
            continue
        d = haversine_nm(lat, lon, rlat, rlon)
        if d <= best_dist:
            best_dist, best = d, ident
    return best

def detect_runway(lat, lon, track, alt_ft, *, max_alt_ft=6000,
                  airport_radius_nm=15, runway_radius_nm=6):
    if not isinstance(alt_ft, (int, float)) or not math.isfinite(alt_ft) or alt_ft > max_alt_ft:
        return None
    code = nearest_airport(lat, lon, airport_radius_nm)
    return nearest_runway(code, lat, lon, track, runway_radius_nm) if code else None