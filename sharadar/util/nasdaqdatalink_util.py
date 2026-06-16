"""NASDAQ Data Link (formerly Quandl) API utilities.

Provides functions for downloading bulk data tables, fetching
incremental updates by date, and determining the last available
data date from the NASDAQ Data Link service.
"""
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import nasdaqdatalink
import requests
from click import progressbar
from sharadar.util.logger import log
from six.moves.urllib.parse import urlencode

ONE_MEGABYTE = 1024 * 1024
NASDAQ_DATALINK_URL = (
    'https://data.nasdaq.com/api/v3/datatables/'
)


def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers['content-length'])
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data


def format_metadata_url(api_key, table_name):
    """ Build the query URL for Quandl Prices metadata.
    """
    query_params = [('api_key', api_key), ('qopts.export', 'true')]

    return (
            NASDAQ_DATALINK_URL + table_name + ".csv?" + urlencode(query_params)
    )


def load_data_table(file, index_col=None, parse_dates=False):
    """ Load data table from zip file provided by Quandl.
    """
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        wiki_prices = file_names.pop()
        with zip_file.open(wiki_prices) as table_file:
            data_table = pd.read_csv(table_file, index_col=index_col,
                                     parse_dates=parse_dates, na_values=['NA'])

    return data_table


def fetch_entire_table(api_key, table_name, index_col=None, parse_dates=False, retries=5):
    """Download and parse an entire NASDAQ Data Link table.

    Uses the bulk export endpoint to download the complete table
    as a zip file, with retry logic for transient failures.

    Args:
        api_key: NASDAQ Data Link API key.
        table_name: Fully qualified table name (e.g., 'SHARADAR/SEP').
        index_col: Column(s) to use as DataFrame index.
        parse_dates: Column(s) to parse as dates.
        retries: Number of retry attempts. Defaults to 5.

    Returns:
        pd.DataFrame: The complete table data.

    Raises:
        ValueError: If all retry attempts fail.
    """
    log.info("Start loading the entire %s dataset..." % table_name)
    for _ in range(retries):
        try:
            source_url = format_metadata_url(api_key, table_name)
            metadata = pd.read_csv(source_url)

            # Extract link from metadata and download zip file.
            table_url = metadata.loc[0, 'file.link']

            raw_file = download_with_progress(
                table_url,
                chunk_size=ONE_MEGABYTE,
                label="Downloading data from Quandl table " + table_name
            )

            log.info("Parsing data from nasdaqdatalink table %s." % table_name)
            return load_data_table(raw_file, index_col=index_col, parse_dates=parse_dates)

        except Exception:
            log.exception("Exception raised reading Quandl data. Retrying.")

    else:
        raise ValueError("Failed to download data from '%s' after %d attempts." % (source_url, retries))


def fetch_table_by_date(api_key, table_name, start, end=None, index_col=None):
    """
    Load data from nasdaqdatalink and correct them so that they are unadjusted.
    The index must be the date
    """

    log.info(
        "Start loading Sharadar %s price data from %s to %s..." % (table_name, start, "today" if end is None else end))
    nasdaqdatalink.ApiConfig.api_key = api_key
    df = nasdaqdatalink.get_table(table_name,
                                  date={'gte': start, 'lte': end},
                                  paginate=True)
    if index_col is not None:
        # the df['date'] dtype is already datetime64[ns]
        df.set_index(index_col, inplace=True)
    return df


def fetch_sf1_table_date(api_key, start, end=None):
    """Fetch SF1 fundamentals data filtered by last-updated date.

    Args:
        api_key: NASDAQ Data Link API key.
        start: Start date for lastupdated filter.
        end: End date for lastupdated filter. Defaults to None (today).

    Returns:
        pd.DataFrame: SF1 quarterly and trailing data.
    """
    log.info("Start loading Sharadar SF1 fundamentals data from %s to %s..." % (start, "today" if end is None else end))
    nasdaqdatalink.ApiConfig.api_key = api_key
    return nasdaqdatalink.get_table('SHARADAR/SF1', dimension=['ARQ', 'ART'],
                                    lastupdated={'gte': start, 'lte': end},
                                    paginate=True)


def last_available_date():
    """Get the last available price date from NASDAQ Data Link.

    Queries the SPY ticker to determine the most recent data date.

    Returns:
        str: Last available date in 'YYYY-MM-DD' format.
    """
    return nasdaqdatalink.get_table('SHARADAR/TICKERS', ticker='SPY')['lastpricedate'][0].strftime('%Y-%m-%d')
