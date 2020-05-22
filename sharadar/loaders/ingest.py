from os import environ as env
import os
import pandas as pd
import numpy as np
import quandl
from sharadar.util.output_dir import get_output_dir
from sharadar.util.quandl_util import fetch_entire_table
from sharadar.util.equity_supplementary_util import lookup_sid, METADATA_HEADERS
from sharadar.util.equity_supplementary_util import insert_equity_extra_data_basic, insert_equity_extra_data_sf1
from sharadar.util.quandl_util import fetch_table_by_date, fetch_sf1_table_date
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarWriter, SQLiteDailyBarReader
from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter
from zipline.data.us_equity_pricing import SQLiteAdjustmentWriter
from zipline.assets import ASSET_DB_VERSION
from zipline.utils.cli import maybe_show_progress
from pathlib import Path
from sharadar.util.logger import log, logfilename
from contextlib import closing
import sqlite3
from sharadar.util.universe import UniverseWriter, UniverseReader
from sharadar.pipeline.filters import TradableStocksUS, TRADABLE_STOCKS_US
from sharadar.loaders.ingest_macro import create_macro_equities_df, create_macro_prices_df
import traceback

quandl.ApiConfig.api_key = env["QUANDL_API_KEY"]

EXCHANGE_DF = pd.DataFrame([
    ['NYSE', 'The New York Stock Exchange', 'US'],
    ['NASDAQ', 'National Association of Securities Dealers Automated Quotation', 'US'],
    ['OTC', 'Over The Counter', 'US'],
    ['NYSEMKT', 'American Stock Exchange', 'US'],
    ['NYSEARCA', 'Archipelago Exchange', 'US'],
    ['BATS', 'Better Alternative Trading System Exchange', 'US'],
    ['MACRO', 'Macroeconomic Data', 'US'],
    ['INDEX', 'Index Data', 'US'],
],
    columns=['exchange', 'canonical_name', 'country_code'])


def process_data_table(df):
    log.info("Adjusting for stock splits...")

    # Data are adjusted for stock splits, but not for dividends.
    m = df['closeunadj'] / df['close']

    # Remove the split factor to get back the unadjusted data
    df['open'] *= m
    df['high'] *= m
    df['low'] *= m
    df['close'] = df['closeunadj']
    df['volume'] /= m
    df['dividends'] *= m

    df = df.drop(['closeunadj', 'lastupdated'], axis=1)
    df = df.replace([np.inf, -np.inf, np.nan], 0)
    df = df.fillna({'volume': 0})
    return df


def fetch_data(end, start):
    """
    Fetch the Sharadar Equity Prices (SEP) and Sharadar Fund Prices (SFP). Entire dataset or by date.
    """
    if start is not None:
        df_sep = fetch_table_by_date(env["QUANDL_API_KEY"], 'SHARADAR/SEP', start, end)
        df_sfp = fetch_table_by_date(env["QUANDL_API_KEY"], 'SHARADAR/SFP', start, end)
    else:
        df_sep = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SEP", parse_dates=['date'])
        df_sfp = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SFP", parse_dates=['date'])

    df = df_sep.append(df_sfp)
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def get_data(sharadar_metadata_df, related_tickers, start=None, end=None):
    df = fetch_data(end, start)

    # fix where closeunadj == 0

    df.loc[df['closeunadj'] == 0, 'closeunadj'] = df['close']

    log.info("Adding SIDs to all stocks...")
    df['sid'] = df['ticker'].apply(lambda x: lookup_sid(sharadar_metadata_df, related_tickers, x))
    # unknown sids are -1 instead of nan to preserve the integer type. Drop them.
    unknown_sids = df[df['sid'] == -1]
    df.drop(unknown_sids.index, inplace=True)
    df.set_index(['date', 'sid'], inplace=True)

    df = process_data_table(df)
    return df


def create_dividends_df(df, sharadar_metadata_df):
    dividends_df = df
    dividends_df = dividends_df[dividends_df["dividends"] != 0.0]
    dividends_df = dividends_df.dropna()

    dividends_df['sid'] = dividends_df.index.get_level_values('sid')
    dividends_df = dividends_df.rename(columns={'dividends': 'amount'})
    dividends_df = dividends_df.drop(['open', 'high', 'low', 'close', 'volume', 'ticker'], axis=1)
    dividends_df.index = dividends_df.index.get_level_values('date')
    dividends_df['record_date'] = dividends_df['declared_date'] = dividends_df['pay_date'] = dividends_df[
        'ex_date'] = dividends_df.index

    # Workaround to avoid dividend warning: This is because dividends are applied to the previous trading day
    # we don't have price data before 2009-01-02
    first_day = pd.to_datetime('2009-01-02')  # it was friday
    if first_day in dividends_df.index:
        second_day = pd.to_datetime('2009-01-05')
        dividends_df.loc[first_day, ['record_date', 'declared_date', 'pay_date', 'ex_date']] = second_day
        # and finally, replace the index
        dividends_df = dividends_df.rename(index={first_day: second_day})

    return dividends_df


def create_splits_df(sharadar_metadata_df, related_tickers, existing_tickers, start):
    splits_df = quandl.get_table('SHARADAR/ACTIONS', date={'gte': start}, action=['split'], paginate=True)

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

    missing_dates = (len(this_cal) != df_ticker.shape[0])
    if missing_dates:
        sid = df_ticker.index.get_level_values('sid')[0]
        ticker = df_ticker['ticker'][0]
        log.info("Fixing missing interstitial dates for %s (%d)." % (ticker, sid))

        sids = np.full(len(this_cal), sid)
        synch_index = pd.MultiIndex.from_arrays([this_cal.tz_localize(None), sids], names=('date', 'sid'))
        df_ticker_synch = df_ticker.reindex(synch_index)

        # Forward fill missing data, volume and dividens must remain 0
        columns_ffill = ['ticker', 'open', 'high', 'low', 'close']
        df_ticker_synch[columns_ffill] = df_ticker_synch[columns_ffill].fillna(method='ffill')
        df_ticker_synch = df_ticker_synch.fillna({'volume': 0, 'dividends': 0})

        # Drop remaining NaN
        df_ticker_synch.dropna(inplace=True)

        # drop the existing sub dataframe
        df.drop(df_ticker.index, inplace=True)
        # and replace if with the new one with all the dates.
        df.append(df_ticker_synch)


def from_quandl():
    """
    ticker,date,open,high,low,close,volume,dividends,lastupdated
    A,2008-01-02,36.67,36.8,36.12,36.3,1858900.0,0.0,2017-11-01

    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data.bundles import register
    from sharadar.loaders.ingest import from_quandl

    register("sharadar", from_quandl(), create_writers=False)
    """

    def ingest(environ, asset_db_writer, minute_bar_writer, daily_bar_writer, adjustment_writer, calendar,
               start_session, end_session, cache, show_progress, output_dir):
        # remove the output_dir with the timestamp, it should be empty
        try:
            os.rmdir(output_dir)
        except OSError as e:
            log.error("%s : %s" % (output_dir, e.strerror))

        try:
            _ingest(calendar, start_session, end_session)
        except Exception as e:
            log.error(traceback.format_exc())


    def _ingest(calendar, start_session, end_session):
        # use 'latest' (SHARADAR_BUNDLE_DIR) as output dir
        output_dir = get_output_dir()
        os.makedirs(output_dir, exist_ok=True)

        print("logfiles:", logfilename)

        log.info("Start ingesting SEP, SFP and SF1 data into %s ..." % output_dir)

        # Check valid trading dates, according to the selected exchange calendar
        sessions = calendar.sessions_in_range(start_session, end_session)

        prices_dbpath = os.path.join(output_dir, "prices.sqlite")

        start_fetch_date = None
        if os.path.exists(prices_dbpath):
            start_fetch_date = SQLiteDailyBarReader(prices_dbpath).last_available_dt.strftime('%Y-%m-%d')
            log.info("Last available date: %s" % start_fetch_date)

        log.info("Start loading sharadar metadata...")
        sharadar_metadata_df = quandl.get_table('SHARADAR/TICKERS', table=['SFP', 'SEP'], paginate=True)
        sharadar_metadata_df.set_index('ticker', inplace=True)
        related_tickers = sharadar_metadata_df['relatedtickers'].dropna()
        # Add a space at the begin and end of relatedtickers, search for ' TICKER '
        related_tickers = ' ' + related_tickers.astype(str) + ' '

        prices_df = get_data(sharadar_metadata_df, related_tickers, start=start_fetch_date)

        # iterate over all the securities and pack data and metadata for writing
        tickers = prices_df['ticker'].unique()
        log.info("Start writing price data for %d equities." % (len(tickers)))

        equities_df = create_equities_df(prices_df, tickers, sessions, sharadar_metadata_df, show_progress=True)

        # Write PRICING data
        log.info(("Writing pricing data to '%s'..." % (prices_dbpath)))
        sql_daily_bar_writer = SQLiteDailyBarWriter(prices_dbpath, calendar)
        prices_df.sort_index(inplace=True)
        sql_daily_bar_writer.write(prices_df)

        # DIVIDENDS
        log.info("Creating dividends data...")
        # see also https://github.com/shlomikushchi/zipline-live2/blob/master/zipline/data/bundles/csvdir.py
        dividends_df = create_dividends_df(prices_df, sharadar_metadata_df)

        # SPLITS
        log.info("Creating splits data...")
        splits_df = create_splits_df(sharadar_metadata_df, related_tickers, tickers, start_fetch_date)

        # TODO mergers?
        # see also https://github.com/quantopian/zipline/blob/master/zipline/data/adjustments.py

        # Write dividends and splits_df
        sql_daily_bar_reader = SQLiteDailyBarReader(prices_dbpath)
        adjustment_dbpath = os.path.join(output_dir, "adjustments.sqlite")
        adjustment_writer = SQLiteAdjustmentWriter(adjustment_dbpath, sql_daily_bar_reader, sessions, overwrite=True)

        log.info("Start writing %d splits and %d dividends data..." % (len(splits_df), len(dividends_df)))
        adjustment_writer.write(splits=splits_df, dividends=dividends_df)
        adjustment_writer.close()

        # Write equity metadata
        log.info("Start writing equities and supplementary_mappings data...")
        asset_dbpath = os.path.join(output_dir, ("assets-%d.sqlite" % ASSET_DB_VERSION))
        asset_db_writer = SQLiteAssetDBWriter(asset_dbpath)
        asset_db_writer.write(equities=equities_df, exchanges=EXCHANGE_DF)

        # EQUITY SUPPLEMENTARY MAPPINGS are used for company name, sector, industry and fundamentals financial data.
        # They could be retrieved by AssetFinder.get_supplementary_field(sid, field_name, as_of_date)
        log.info("Start creating company info dataframe...")
        with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
            insert_equity_extra_data_basic(sharadar_metadata_df, cursor)

        log.info("Start creating Fundamentals dataframe...")
        if start_fetch_date is None:
            sf1_df = fetch_entire_table(env["QUANDL_API_KEY"], "SHARADAR/SF1", parse_dates=['datekey', 'reportperiod'])
        else:
            sf1_df = fetch_sf1_table_date(env["QUANDL_API_KEY"], start_fetch_date)
        with closing(sqlite3.connect(asset_dbpath)) as conn, conn, closing(conn.cursor()) as cursor:
            insert_equity_extra_data_sf1(sharadar_metadata_df, sf1_df, cursor, show_progress=True)

        # Additional MACRO data
        prices_start = prices_df.index[0][0]
        prices_end = prices_df.index[-1][0]

        log.info("Adding macro data from %s to %s ..." % (prices_start, prices_end))
        macro_equities_df = create_macro_equities_df(prices_start, prices_end)
        asset_db_writer.write(equities=macro_equities_df)

        macro_prices_df = create_macro_prices_df(prices_start, prices_end)
        sql_daily_bar_writer.write(macro_prices_df)

        # Predefined Named Universes
        create_tradable_stocks_universe(output_dir, prices_start, prices_end)

        okay_path = os.path.join(output_dir, "ok")
        Path(okay_path).touch()
        log.info("Ingest finished!")

    def create_tradable_stocks_universe(output_dir, prices_start, prices_end):
        universes_dbpath = os.path.join(output_dir, "universes.sqlite")
        universe_name = TRADABLE_STOCKS_US
        screen = TradableStocksUS()
        universe_start = prices_start.tz_localize('utc')
        universe_end = prices_end.tz_localize('utc')
        universe_last_date = UniverseReader(universes_dbpath).get_last_date(universe_name)
        if universe_last_date is not pd.NaT:
            universe_start = universe_last_date
        log.info("Start creating universe '%s' from %s to %s ..." % (universe_name, universe_start, universe_end))
        UniverseWriter(universes_dbpath).write(universe_name, screen, universe_start, universe_end)

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

                # The date when this asset was created.
                start_date = sharadar_metadata.loc['firstpricedate']

                # The last date we have trade data for this asset.
                end_date = sharadar_metadata.loc['lastpricedate']

                # The first date we have trade data for this asset.
                first_traded = start_date

                # The date on which to close any positions in this asset.
                auto_close_date = end_date + pd.Timedelta(days=1)

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

    return ingest
