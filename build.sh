#!/bin/bash
set -e

python3 -m pip install -r requirements.txt
python3 -m build
python3 -m pip install --force-reinstall --no-deps .