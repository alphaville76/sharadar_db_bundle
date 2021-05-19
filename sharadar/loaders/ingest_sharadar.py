from os import environ as env
import os
import pandas as pd
import numpy as np
import quandl
from trading_calendars import get_calendar
from sharadar.util.output_dir import get_output_dir
from sharadar.util.quandl_util import fetch_entire_table
from sharadar.util.equity_supplementary_util import lookup_sid
from sharadar.util.equity_supplementary_util import insert_asset_info, insert_fundamentals, insert_daily_metrics
from sharadar.util.quandl_util import fetch_table_by_date, fetch_sf1_table_date
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter, SQLiteDailyBarReader, SQLiteDailyAdjustmentWriter
from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter, SQLiteAssetFinder
from zipline.assets import ASSET_DB_VERSION
from zipline.utils.cli import maybe_show_progress
from pathlib import Path
from sharadar.util.logger import log, logfilename
from contextlib import closing
import sqlite3
from sharadar.loaders.constant import EXCHANGE_DF, OLDEST_DATE_SEP, METADATA_HEADERS
from sharadar.util.quandl_util import last_available_date
from sharadar.loaders.ingest_macro import create_macro_equities_df, create_macro_prices_df
import traceback

quandl.ApiConfig.api_key = env["QUANDL_API_KEY"]

def process_data_table(df):
    # 'close' prices are adjusted only for stock splits, but not for dividends.
    m = df['closeunadj'] / df['close']

    # Remove the split factor to get back the unadjusted data
    df['open'] *= m
    df['high'] *= m
    df['low'] *= m
    df['close'] = df['closeunadj']
    df['volume'] /= m

    df = df.drop(['closeunadj', 'closeadj', 'lastupdated'], axis=1)
    df = df.replace([np.inf, -np.inf, np.nan], 0)
    df = df.fillna({'volume': 0})
    return df

def must_fetch_entire_table(date):
    if pd.isnull(date):
        return True
    return pd.to_datetime(date, utc=True) <= OLDEST_DATE_SEP

def fetch_data(start, end):
    """
    Fetch the Sharadar Equity Prices (SEP) and Sharadar Fund Prices (SFP). Entire dataset or by date.
    """
    if must_fetch_entire_table(start):
        df_sep = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SEP", parse_dates=['date'])
        df_sfp = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SFP", parse_dates=['date'])
    else:
        df_sep = fetch_table_by_date(env["QUANDL_API_KEY"], 'SHARADAR/SEP', start, end)
        df_sfp = fetch_table_by_date(env["QUANDL_API_KEY"], 'SHARADAR/SFP', start, end)


    df = df_sep.append(df_sfp)
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def get_data(sharadar_metadata_df, related_tickers, start=None, end=None):
    df = fetch_data(start, end)

    log.info("Adding SIDs to all stocks...")
    df['sid'] = df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    # unknown sids are -1 instead of nan to preserve the integer type. Drop them.
    unknown_sids = df[df['sid'] == -1]
    df.drop(unknown_sids.index, inplace=True)
    df.set_index(['date', 'sid'], inplace=True)

    df = process_data_table(df)
    return df.sort_index()


def create_dividends_df(sharadar_metadata_df, related_tickers, existing_tickers, start):
    dividends_df = quandl.get_table('SHARADAR/ACTIONS', date={'gte':start}, action=['dividend', 'spinoffdividend'], paginate=True)

    # Remove dividends_df entries, whose ticker doesn't exist
    tickers_dividends = dividends_df['ticker'].unique()
    tickers_intersect = set(existing_tickers).intersection(tickers_dividends)
    dividends_df = dividends_df.loc[dividends_df['ticker'].isin(tickers_intersect)]

    dividends_df = dividends_df.rename(columns={'value': 'amount'})
    dividends_df['sid'] = dividends_df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    dividends_df.index = dividends_df['date']
    dividends_df['record_date'] = dividends_df['declared_date'] = dividends_df['pay_date'] = dividends_df[
        'ex_date'] = dividends_df.index
    dividends_df.drop(['action', 'date', 'name', 'contraticker', 'contraname', 'ticker'], axis=1, inplace=True)
    return dividends_df

def create_splits_df(sharadar_metadata_df, related_tickers, existing_tickers, start):
    splits_df = quandl.get_table('SHARADAR/ACTIONS', date={'gte':start}, action=['split'], paginate=True)

    # Remove splits_df entries, whose ticker doesn't exist
    tickers_splits = splits_df['ticker'].unique()
    tickers_intersect = set(existing_tickers).intersection(tickers_splits)
    splits_df = splits_df.loc[splits_df['ticker'].isin(tickers_intersect)]

    # The date dtype is already datetime64[ns]
    splits_df['value'] = 1.0 / splits_df['value']
    splits_df.rename(
        columns={
            'value': 'ratio',
            'date': 'effective_date',
        },
        inplace=True,
        copy=False,
    )
    splits_df['ratio'] = splits_df['ratio'].astype(float)
    splits_df['sid'] = splits_df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    splits_df.drop(['action', 'name', 'contraticker', 'contraname', 'ticker'], axis=1, inplace=True)
    return splits_df


def synch_to_calendar(sessions, start_date, end_date, df_ticker, df):
    this_cal = sessions[(sessions >= start_date) & (sessions <= end_date)]

    missing_dates = this_cal.tz_localize(None).difference(df_ticker.index.get_level_values(0).tz_localize(None)).values
    if len(missing_dates) > 0:
        sid = df_ticker.index.get_level_values('sid')[0]
        ticker = df_ticker['ticker'][0]
        log.info("Fixing missing %d interstitial dates for %s from %s to %s: %s."
                 % (len(missing_dates), ticker, this_cal[0], this_cal[-1], missing_dates))

        sids = np.full(len(this_cal), sid)
        synch_index = pd.MultiIndex.from_arrays([this_cal.tz_localize(None), sids], names=('date', 'sid'))
        df_ticker_synch = df_ticker.reindex(synch_index)

        # Forward fill missing data, volume and dividens must remain 0
        columns_ffill = ['ticker', 'open', 'high', 'low', 'close']
        df_ticker_synch[columns_ffill] = df_ticker_synch[columns_ffill].fillna(method='ffill')
        df_ticker_synch = df_ticker_synch.fillna({'volume': 0})

        # Drop remaining NaN
        df_ticker_synch.dropna(inplace=True)

        # drop the existing sub dataframe
        df.drop(df_ticker.index, inplace=True)
        # and replace if with the new one with all the dates.
        df.append(df_ticker_synch)


def _ingest(start_session, calendar=get_calendar('XNYS'), output_dir=get_output_dir(), sanity_check=True):
    os.makedirs(output_dir, exist_ok=True)

    print("logfiles:", logfilename)

    log.info("Start ingesting SEP, SFP and SF1 data into %s ..." % output_dir)

    end_session = pd.to_datetime(last_available_date())
    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)

    prices_dbpath = os.path.join(output_dir, "prices.sqlite")

    # use string format expected by quandl
    start_fetch_date = sessions[0].strftime('%Y-%m-%d')
    #end_fetch_date = None if sessions[-1].strftime('%Y-%m-%d') == last_trading_date() else sessions[-1].strftime('%Y-%m-%d')
    if os.path.exists(prices_dbpath):
        start_fetch_date = SQLiteDailyBarReader(prices_dbpath).last_available_dt.strftime('%Y-%m-%d')
        log.info("Last available date: %s" % start_fetch_date)

    log.info("Start loading sharadar metadata...")
    related_tickers, sharadar_metadata_df = create_metadata()
    prices_df = get_data(sharadar_metadata_df, related_tickers, start_fetch_date)
    if len(prices_df) > 0:
        # the first price date may differ from start_fetch_date because we query quadl by lastupdate
        log.info("Price data for %d equities from %s to %s." %
             (len(prices_df.index.get_level_values(1)), prices_df.index[0][0], prices_df.index[-1][0]))
    else:
        log.info("No price data retrieved for period from %s." % start_fetch_date)


    # iterate over all the securities and pack data and metadata for writing
    tickers = prices_df['ticker'].unique()
    log.info("Start creating data for %d equities..." % (len(tickers)))
    equities_df = create_equities_df(prices_df, tickers, sessions, sharadar_metadata_df, show_progress=True)

    # Additional MACRO data
    macro_equities_df = create_macro_equities_df(calendar)
    equities_df = equities_df.append(macro_equities_df)

    # Write equity metadata
    log.info("Start writing equities...")
    asset_dbpath = os.path.join(output_dir, ("assets-%d.sqlite" % ASSET_DB_VERSION))
    asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
    asset_db_writer.write(equities=equities_df, exchanges=EXCHANGE_DF)

    # Write PRICING data
    log.info(("Writing pricing data to '%s'..." % (prices_dbpath)))
    sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
    prices_df.sort_index(inplace=True)
    sql_daily_bar_writer.write(prices_df)

    # DIVIDENDS
    log.info("Creating dividends data...")
    dividends_df = create_dividends_df(sharadar_metadata_df, related_tickers, tickers, start_fetch_date)

    # SPLITS
    log.info("Creating splits data...")
    splits_df = create_splits_df(sharadar_metadata_df, related_tickers, tickers, start_fetch_date)

    # mergers?
    # see also https://github.com/quantopian/zipline/blob/master/zipline/data/adjustments.py

    # Write dividends and splits_df
    adjustment_dbpath = os.path.join(output_dir, "adjustments.sqlite")
    sql_daily_bar_reader = SQLiteDailyBarReader(prices_dbpath)
    asset_db_reader = SQLiteAssetFinder(asset_dbpath)
    adjustment_writer = SQLiteDailyAdjustmentWriter(adjustment_dbpath, sql_daily_bar_reader, asset_db_reader, sessions)

    log.info("Start writing %d splits and %d dividends data..." % (len(splits_df), len(dividends_df)))
    adjustment_writer.write(splits=splits_df, dividends=dividends_df)

    log.info("Adding macro data from %s ..." % (start_fetch_date))
    macro_prices_df = create_macro_prices_df(start_fetch_date, calendar)
    sql_daily_bar_writer.write(macro_prices_df)

    log.info("Start writing supplementary_mappings data...")
    # EQUITY SUPPLEMENTARY MAPPINGS are used for company name, sector, industry and fundamentals financial data.
    # They could be retrieved by AssetFinder.get_supplementary_field(sid, field_name, as_of_date)
    log.info("Start creating company info dataframe...")
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_asset_info(sharadar_metadata_df, cursor)


    start_date_fundamentals = asset_db_reader.last_available_fundamentals_dt
    log.info("Start creating Fundamentals dataframe...")
    if must_fetch_entire_table(start_date_fundamentals):
        log.info("Fetch entire table.")
        sf1_df = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])
    else:
        log.info("Start date: %s" % start_date_fundamentals)
        sf1_df = fetch_sf1_table_date(env["QUANDL_API_KEY"], start_date_fundamentals)
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_fundamentals(sharadar_metadata_df, sf1_df, cursor, show_progress=True)

    start_date_metrics = asset_db_reader.last_available_daily_metrics_dt
    log.info("Start creating daily metrics dataframe...")
    if must_fetch_entire_table(start_date_metrics):
        log.info("Fetch entire table.")
        daily_df = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/DAILY", parse_dates=['date'])
    else:
        log.info("Start date: %s" % start_date_fundamentals)
        daily_df = fetch_table_by_date(env["QUANDL_API_KEY"], 'SHARADAR/DAILY', start_date_metrics)
    with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
        insert_daily_metrics(sharadar_metadata_df, daily_df, cursor, show_progress=True)

    if sanity_check:
        if asset_db_writer.check_sanity():
            log.info("Sanity check successful!")

    okay_path = os.path.join(output_dir, "ok")
    Path(okay_path).touch()
    log.info("Ingest finished!")


def create_metadata():
    sharadar_metadata_df = quandl.get_table('SHARADAR/TICKERS', table=['SFP', 'SEP'], paginate=True)
    sharadar_metadata_df.set_index('ticker', inplace=True)
    related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
    # Add a space at the begin and end of relatedtickers, search for ' TICKER '
    related_tickers = ' ' + related_tickers.astype(str) + ' '
    return related_tickers, sharadar_metadata_df


def create_equities_df(df, tickers, sessions, sharadar_metadata_df, show_progress):
    # Prepare an empty DataFrame for equities, the index of this dataframe is the sid.
    equities_df = pd.DataFrame(columns=METADATA_HEADERS)
    with maybe_show_progress(tickers, show_progress, label='Loading custom pricing data: ') as it:
        for ticker in it:
            df_ticker = df[df['ticker'] == ticker]
            df_ticker = df_ticker.sort_index()

            sid = df_ticker.index.get_level_values('sid')[0]

            sharadar_metadata = sharadar_metadata_df[sharadar_metadata_df['permaticker'] == sid].iloc[0, :]

            asset_name = sharadar_metadata.loc['name']

            # The date when this asset was created (tzinfo=None).
            start_date = sharadar_metadata.loc['firstpricedate']

            # The last date we have trade data for this asset (tzinfo=None)..
            end_date = sharadar_metadata.loc['lastpricedate']

            # The first date we have trade data for this asset.
            first_traded = start_date

            # The date on which to close any positions in this asset.
            auto_close_date = end_date + pd.Timedelta(days=1)

            # The canonical name of the exchange, for example 'NYSE' or 'NASDAQ'
            exchange = sharadar_metadata.loc['exchange']
            if (exchange is None) or (exchange == 'None'):
                exchange = 'OTC'

            # Synch to the official exchange calendar, if necessary
            date_index = df_ticker.index.get_level_values('date')
            start_date_df = date_index[0]
            end_date_df = date_index[-1]
            synch_to_calendar(sessions, start_date_df, end_date_df, df_ticker, df)

            # Add a row to the metadata DataFrame.
            equities_df.loc[sid] = ticker, asset_name, start_date, end_date, first_traded, auto_close_date, exchange
    return equities_df

def from_quandl():
    """
    ticker,date,open,high,low,close,volume,dividends,lastupdated
    A,2008-01-02,36.67,36.8,36.12,36.3,1858900.0,0.0,2017-11-01

    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data import bundles
    from zipline.finance import metrics
    from sharadar.loaders.ingest_sharadar import from_quandl
    from sharadar.util.metric_daily import default_daily

    bundles.register("sharadar", from_quandl(), create_writers=False)
    metrics.register('default_daily', default_daily)
    """

    def ingest(environ, asset_db_writer, minute_bar_writer, daily_bar_writer, adjustment_writer, calendar,
               start_date, end_date, cache, show_progress, output_dir):
        # remove the output_dir with the timestamp, it should be empty
        try:
            os.rmdir(output_dir)
        except OSError as e:
            log.error("%s : %s" % (output_dir, e.strerror))


        try:
            _ingest(start_date, calendar)
        except Exception as e:
            log.error(traceback.format_exc())

    return ingest

