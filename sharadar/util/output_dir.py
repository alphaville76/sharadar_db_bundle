import os
from zipline.data.loader import get_data_filepath


def get_output_dir():
    return os.path.join(get_data_filepath(SEP_BUNDLE_NAME), SEP_BUNDLE_DIR)


SEP_BUNDLE_NAME = 'sharadar'
SEP_BUNDLE_DIR = 'latest'