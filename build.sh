#!/bin/bash
python3 -m build

# The last version is set in setup.py.
latest_wheel=$(ls -t dist/*.whl 2>/dev/null | head -n 1)
if [[ -z "${latest_wheel}" ]]; then
  echo "No wheel file found in dist/" >&2
  exit 1
fi

pip install --upgrade --force-reinstall "${latest_wheel}"
