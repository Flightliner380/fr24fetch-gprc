#!/usr/bin/env bash

set -euo pipefail

VENV_DIR=".venv"

if ! command -v python3 >/dev/null 2>&1; then
	echo "python3 is required but not found."
	exit 1
fi

python3 -m venv "$VENV_DIR"

"$VENV_DIR/bin/python" -m pip install --upgrade pip

if [[ -d "../fr24gprc" ]]; then
	"$VENV_DIR/bin/python" -m pip install ../fr24gprc/.
fi

if [[ -f "requirements.txt" ]]; then
	"$VENV_DIR/bin/python" -m pip install -r requirements.txt
	echo "Virtual environment created and requirements installed."
else
	echo "Virtual environment created. requirements.txt not found, skipping dependency installation."
fi
