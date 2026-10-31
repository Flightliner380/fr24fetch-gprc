"""
Baut airports.json aus OurAirports airports_raw.csv + runways_raw.csv (siehe build-airports.sh).
Filtert auf relevante Typen, gruppiert Airports in Grid-Zellen (fuer airports.js:
isNearAirport/nearestAirport) und haengt pro Airport-Code eine Liste der Runway-Enden
(Ident, Heading, Position) an (fuer nearestRunway).
"""
import csv, json, collections, re, os

AIRPORTS_SRC = "airports_raw.csv"
RUNWAYS_SRC = "runways_raw.csv"
OUT = "airports.json"

TYPES = {"small_airport", "medium_airport", "large_airport", "seaplane_base", "heliport"}
CELL = 0.25  # Grad pro Grid-Zelle (~27km) -- muss mit airports.js (cellSize) uebereinstimmen
ICAO_RE = re.compile(r"^[A-Z0-9]{4}$")

# Separater, groberer Index nur fuer Verkehrsflughaefen (fnear/lnear, Radius ~150nm).
# Grosse Zellen (~330km), damit die 3x3-Nachbarschaft in airports.js einen 150nm-Radius
# (~278km) sicher abdeckt, ohne den bestehenden feinen Index (CELL=0.25) aufzublaehen.
# Kriterium: OurAirports-Feld scheduled_service == "yes" (Linien-/Charterverkehr laut
# OurAirports), zusaetzlich manuell erweiterbar ueber COMMERCIAL_EXTRA_ICAO (z.B. Plaetze
# mit fehlerhaft gepflegtem scheduled_service-Flag).
COMMERCIAL_CELL = 3.0
import os

AIRCRAFT_STORAGE_ICAO = set("""
LETL LERL LECH LFBT LFBF LFLX EHTW EGBP EGSY
KVCV KMZJ KROW KIGM KMHV KGYR KBYH KSGT KSBD KCEW KSFB
YBAS CYXE CYTR
""".split())

AIRCRAFT_MANUFACTURING_ICAO = set("""
LFBO EDHI ZBTJ KBFM CYMX KPAE KBFI KCHS SBYS SBSJ LEZL EDDP
KICT KSAV LFBD LSZC
""".split())

AIRCRAFT_DELIVERY_ICAO = set("""
LFBO EDHI KBFM CYMX KPAE KBFI KCHS
KICT KSAV LFBD LSZC SBSJ
""".split())

AIRCRAFT_BONEYARD_ICAO = set("""
KDMA KVCV KMZJ KROW KIGM KMHV KBYH KSGT KSBD KCEW KSFB
""".split())

AIRCRAFT_BUSINESS_JET_ICAO = set("""
KICT KSAV LFBD LFPB LSZC CYMX
""".split())

COMMERCIAL_EXTRA_ICAO = set(
    c.strip().upper()
    for c in os.environ.get("COMMERCIAL_EXTRA_ICAO", "").split(",")
    if c.strip()
) | (
    AIRCRAFT_STORAGE_ICAO
    | AIRCRAFT_MANUFACTURING_ICAO
    | AIRCRAFT_DELIVERY_ICAO
    | AIRCRAFT_BONEYARD_ICAO
    | AIRCRAFT_BUSINESS_JET_ICAO
    | {"LETL", "LFBT", "LFBF"}
)


# Runway-Oberflaechen, die bei kommerziellen Flughaefen ausgefiltert werden (Gras/Rasen-
# artige Belaege; OurAirports "surface" ist Freitext, daher Substring-Match, lowercase).
GRASS_SURFACES = ("grass", "turf", "sod")

buckets = collections.defaultdict(list)
commercial_buckets = collections.defaultdict(list)
commercial_codes = set()  # Codes von Flughaefen mit scheduled_service/COMMERCIAL_EXTRA_ICAO
ident_to_code = {}  # OurAirports "ident" -> unser Code (ICAO bevorzugt, sonst IATA)

with open(AIRPORTS_SRC, encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if row["type"] not in TYPES:
            continue
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except ValueError:
            continue

        icao = row.get("icao_code") or ""
        if not icao and ICAO_RE.match(row.get("gps_code", "")):
            icao = row["gps_code"]

        if not icao and ICAO_RE.match(row.get("ident", "")):
            icao = row["ident"]
        iata = row.get("iata_code") or ""
        # gpscode = row.get("gps_code") or ""
        # print(row.get("ident"), icao, iata, gpscode, row.get("type"))
        if not icao and not iata:
            continue

        code = icao or iata
        ident_to_code[row["ident"]] = code

        key = f"{int(lat // CELL)}_{int(lon // CELL)}"
        buckets[key].append([round(lat, 4), round(lon, 4), icao, iata])

        is_commercial = (
            row.get("scheduled_service", "").strip().lower() == "yes"
            or code in COMMERCIAL_EXTRA_ICAO
        )
        if is_commercial:
            commercial_codes.add(code)
            ckey = f"{int(lat // COMMERCIAL_CELL)}_{int(lon // COMMERCIAL_CELL)}"
            commercial_buckets[ckey].append([round(lat, 4), round(lon, 4), icao, iata])

runways = collections.defaultdict(list)
with open(RUNWAYS_SRC, encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if row.get("closed") == "1":
            continue
        code = ident_to_code.get(row["airport_ident"])
        if not code:
            continue
        surface = (row.get("surface") or "").strip().lower()
        if code in commercial_codes and any(g.lower() in surface for g in GRASS_SURFACES):
            continue
        for end in ("le", "he"):
            ident = row.get(f"{end}_ident")
            if code in commercial_codes and ident and ident.upper().startswith("H"):
                continue

            hdg = row.get(f"{end}_heading_degT")
            lat = row.get(f"{end}_latitude_deg")
            lon = row.get(f"{end}_longitude_deg")
            if not ident or not hdg or not lat or not lon:
                continue
            try:
                runways[code].append(
                    [ident, round(float(hdg)), round(float(lat), 4), round(float(lon), 4)]
                )
            except ValueError:
                continue

with open(OUT, "w") as f:
    json.dump(
        {
            "cellSize": CELL,
            "buckets": buckets,
            "runways": runways,
            "commercialCellSize": COMMERCIAL_CELL,
            "commercialBuckets": commercial_buckets,
        },
        f,
        separators=(",", ":"),
    )

n_airports = sum(len(v) for v in buckets.values())
n_runways = sum(len(v) for v in runways.values())
n_commercial = sum(len(v) for v in commercial_buckets.values())
print(
    f"Flugplaetze: {n_airports}, Runway-Enden: {n_runways}, "
    f"Verkehrsflughaefen: {n_commercial} -> {OUT}"
)
