import os
from zipline.utils.paths import data_root
from pathlib import Path

SHARADAR_BUNDLE_NAME = 'sharadar'
SHARADAR_BUNDLE_DIR = 'latest'

def create_data_dir(name, environ=None):
    """
    Returns a handle to data file.
    Creates containing directory, if needed.
    """
    dr = data_root(environ)

    if not os.path.exists(dr):
        os.makedirs(dr)

    return os.path.join(dr, name)

def get_data_dir():
    return os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR)

def get_cache_dir():
    cache_dir = os.path.join(create_data_dir(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR, 'cache')
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    return cache_dir

