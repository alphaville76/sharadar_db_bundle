import os
from zipline.utils.paths import data_root


def get_data_filepath(name, environ=None):
    """
    Returns a handle to data file.
    Creates containing directory, if needed.
    """
    dr = data_root(environ)

    if not os.path.exists(dr):
        os.makedirs(dr)

    return os.path.join(dr, name)

def get_output_dir():
    return os.path.join(get_data_filepath(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR)


SHARADAR_BUNDLE_NAME = 'sharadar'
SHARADAR_BUNDLE_DIR = 'latest'