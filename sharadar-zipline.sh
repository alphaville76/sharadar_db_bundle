#!/bin/bash
# Examples:
#   sharadar-zipline.sh ingest -b sharadar
#   sharadar-zipline.sh run -b sharadar -f examples/momentum_pipeline.py -s 2020-01-01 -e 2020-02-01

python sharadar/__main__.py "$@"