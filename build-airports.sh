#!/usr/bin/env bash
# Lädt OurAirports-Rohdaten und baut airports.json (Grid-Index für airports.js) neu.
# Nutzung: ./build-airports.sh   (erzeugt airports.json im aktuellen Verzeichnis)
set -e
curl -sL -o airports_raw.csv "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/airports.csv"
curl -sL -o runways_raw.csv "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/runways.csv"
python3 build-airports.py
rm -f airports_raw.csv runways_raw.csv
