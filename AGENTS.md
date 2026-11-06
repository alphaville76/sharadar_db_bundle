# Repository instructions

## Python environment

- For Python commands and tests, use the virtual environment at `~/zipline-reloaded-venv3.13/bin/python` (Python 3.13); do not use the system Python.
- From Windows, run commands from this repository with `wsl.exe -d FedoraLinux-44 --cd ~/sharadar_db_bundle -- ~/zipline-reloaded-venv3.13/bin/python ...`.
- Run the test suite with `-m pytest -q --tb=short`.
