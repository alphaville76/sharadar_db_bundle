import os
from zipline.data.loader import get_data_filepath


def get_output_dir():
    return os.path.join(get_data_filepath(SHARADAR_BUNDLE_NAME), SHARADAR_BUNDLE_DIR)


SHARADAR_BUNDLE_NAME = 'sharadar'
SHARADAR_BUNDLE_DIR = 'latest'