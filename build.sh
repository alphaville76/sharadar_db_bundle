#!/bin/bash
python3.13 -m build
pip install --upgrade --force-reinstall dist/sharadar_db_bundle-3.0-py3-none-any.whl
