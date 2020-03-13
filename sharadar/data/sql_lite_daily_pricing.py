import pandas as pd
import numpy as np
import sqlite3
import click
from contextlib import closing
from zipline.utils.calendars import get_calendar
from zipline.data.session_bars import SessionBarReader

from zipline.assets import AssetFinder, AssetDBWriter
from zipline.utils.memoize import lazyval
import math

from zipline.errors import (
    EquitiesNotFound,
    FutureContractsNotFound,
    MapAssetIdentifierIndexError,
    MultipleSymbolsFound,
    MultipleValuesFoundForField,
    MultipleValuesFoundForSid,
    NoValueForSid,
    ValueNotFoundForField,
    SidsNotFound,
    SymbolNotFound,
)

from zipline.data.bar_reader import (
    NoDataAfterDate,
    NoDataBeforeDate,
    NoDataOnDate,
)

from zipline.assets.asset_db_schema import (
    ASSET_DB_VERSION,
    asset_db_table_names,
    asset_router,
    equities as equities_table,
    equity_symbol_mappings,
    equity_supplementary_mappings as equity_supplementary_mappings_table,
    futures_contracts as futures_contracts_table,
    exchanges as exchanges_table,
    futures_root_symbols,
    metadata,
    version_info,
)

import warnings
from singleton_decorator import singleton
from memoization import cached

SCHEMA = """
CREATE TABLE IF NOT EXISTS "properties" (
"key" TEXT,
  "0" TEXT
);
CREATE INDEX "ix_properties_key" ON "properties" ("key");

CREATE TABLE IF NOT EXISTS "prices" (
  "date" TIMESTAMP,
  "sid" INTEGER,
  "open" REAL,
  "high" REAL,
  "low" REAL,
  "close" REAL,
  "volume" REAL
);
CREATE INDEX "ix_prices_date_sid" ON "prices" ("date","sid");
"""


@singleton
class SQLiteDailyBarWriter(object):
    """
    Class capable of writing daily OHLCV data to disk in a format that can
    be read efficiently by BcolzDailyOHLCVReader.

    Parameters
    ----------
    filename : str
        The location at which we should write our output.
    calendar : zipline.utils.calendar.trading_calendar
        Calendar to use to compute asset calendar offsets.
    start_session: pd.Timestamp
        Midnight UTC session label.
    end_session: pd.Timestamp
        Midnight UTC session label.

    See Also
    --------
    zipline.data.us_equity_pricing.BcolzDailyBarReader
    """


    def __init__(self, filename, calendar, start_session, end_session):
        self._filename = filename

        if start_session != end_session:
            if not calendar.is_session(start_session):
                raise ValueError(
                    "Start session %s is invalid!" % start_session
                )
            if not calendar.is_session(end_session):
                raise ValueError(
                    "End session %s is invalid!" % end_session
                )

        self._start_session = start_session
        self._end_session = end_session

        self._calendar = calendar

        # Create schema, if not exists
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='prices'")
            if c.fetchone()[0] == 0:
                c.executescript(SCHEMA)


    def _validate(self, data):
        if not isinstance(data, pd.DataFrame):
            raise ValueError("data must be an instance of DataFrame.")
        if data.index.names != ['date', 'sid']:
            raise ValueError("data indexes must be ['date', 'sid'].")

    def write(self, data, with_progress=True, drop_table=False):
        self._validate(data)

        df = data[['open', 'high', 'low', 'close', 'volume']]
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            properties = pd.Series({'calendar_name' : self._calendar.name})
            properties.to_sql('properties', con, index_label='key', if_exists="replace")

            with click.progressbar(length=len(df), label="Inserting price data...") as pbar:     
                count = 0
                for index, row in df.iterrows():
                    sql = "INSERT OR REPLACE INTO prices (date, sid, open, high, low, close, volume) VALUES ('%s',%d,%d,%d,%d,%d,%d)"
                    c.execute(sql % (index + tuple(row.values)))
                    count += 1
                    pbar.update(count)

@singleton
class SQLiteDailyBarReader(SessionBarReader):
    """
    Reader for pricing data written by SQLiteDailyBarWriter.


    See Also
    --------
    zipline.data.us_equity_pricing.BcolzDailyBarReader
    """
    def __init__(self, filename):
        self._filename = filename

    def _query(self, sql):
        with closing(sqlite3.connect(self._filename)) as con, con, closing(con.cursor()) as c:
            c.execute(sql)
            return c.fetchall()
        
    def _exist_sids(self, sids):
        sql = "SELECT COUNT(DISTINCT(sid)) FROM prices WHERE sid IN (%s)" % ",".join(map(str, sids))
        res = self._query(sql)
        return res[0][0] == len(sids)        

    def _fmt_date(self, dt):
        return pd.to_datetime(dt).strftime('%Y-%m-%d') + " 00:00:00"
    
    @cached
    def get_value(self, sid, dt, field):
        day = self._fmt_date(dt)
        sql = "SELECT %s FROM prices WHERE sid = %d and date = '%s'" % (field, sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sids([sid]):
                raise NoDataBeforeDate("No data on or before day={0} for sid={1}".format(dt, sid))
            else:
                raise KeyError(sid)
        return res[0][0]

    @cached
    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        start_day = self._fmt_date(start_dt)
        end_day = self._fmt_date(end_dt)

        if not self._exist_sids(sids):
            raise KeyError(",".join(map(str, sids)))
        raw_arrays = []
        with sqlite3.connect(self._filename) as conn:
            for field in fields:
                result = None
                for sid in sids:
                    query="SELECT date, %s as '%s' FROM prices WHERE date >= '%s' AND date <= '%s' AND sid = %s" % (field, sid, start_day, end_day, sid)
                    df = pd.read_sql_query(query, conn, index_col='date')
                    result = pd.concat([result, df], axis=1) if result is not None else df
                raw_arrays.append(result.values)

        return raw_arrays

    def get_last_traded_dt(self, sid, dt):
        day = self._fmt_date(dt)
        sql = "SELECT date FROM prices WHERE sid = %d and date = '%s'" % (sid, day)
        res = self._query(sql)
        if len(res) == 0:
            if self._exist_sids([sid]):
                return pd.NaT
            else:
                raise KeyError(sid)

        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def last_available_dt(self):
        sql = "SELECT MAX(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def trading_calendar(self):
        sql = 'SELECT "0" FROM properties WHERE key="calendar_name"'
        res = self._query(sql)
        if len(res) == 0:
            raise ValueError("No trading calendar defined.")
        return get_calendar(res[0][0])

    @property
    def first_trading_day(self):
        sql = "SELECT MIN(date) FROM prices"
        res = self._query(sql)
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0], tz='UTC')

    @property
    def sessions(self):
        cal = self.trading_calendar
        return cal.sessions_in_range(self.first_trading_day, self.last_available_dt)

class SQLiteAssetFinder(AssetFinder):
    @lazyval
    def equity_supplementary_map(self):
        raise NotImplementedError()       
        
    @lazyval
    def equity_supplementary_map_by_sid(self):
        raise NotImplementedError()

    def lookup_by_supplementary_field(self, field_name, value, as_of_date):
        raise NotImplementedError()
    
    def get_supplementary_field(self, sid, field_name, as_of_date=None):
        warnings.warn("get_supplementary_field is deprecated",DeprecationWarning)
        raise NotImplementedError()
        
    def _get_inner_select(self):
        sql = ("SELECT sid, value, "
               "ROW_NUMBER() OVER (PARTITION BY sid "
               "ORDER BY start_date DESC) AS rown "
               "FROM equity_supplementary_mappings "
               "WHERE sid IN (%s) "
               "AND field = '%s' "
               "AND start_date <= %d "
               "AND (%d - start_date) <= %d "
              )
        return sql
    
    def _get_result(self, sids, field_name, as_of_date, n, enforce_date):
        """
        'enforce_date' is relevant for fundamentals to avoid delinquent SEC files.
        """
        MAX_DELAY = 1.296e+16 # 5 months
        sql = "SELECT sid, value FROM ("+self._get_inner_select()+ ") t WHERE rown = %d;"

        date_check = as_of_date.value if enforce_date else 0
        cmd = sql % (', '.join(map(str, sids)), field_name, as_of_date.value, date_check, n*MAX_DELAY, n)
        #print(cmd)
        return self.engine.execute(cmd).fetchall()      
    
    def _get_result_ttm(self, sids, field_name, as_of_date, k):
        """
        'enforce_date' is relevant for fundamentals to avoid delinquent SEC files.
        """
        MAX_DELAY = 1.296e+16 # 5 months
        sql = "SELECT sid, SUM(value) FROM ("+self._get_inner_select()+ ") t WHERE rown >= %d and rown <= %d GROUP BY sid;"

        m = k*4
        n = m-3
        cmd = sql % (', '.join(map(str, sids)), field_name, as_of_date.value, as_of_date.value, m*MAX_DELAY*4, n, m)
        #print(cmd)
        return self.engine.execute(cmd).fetchall()  
    
    @cached
    def get_fundamentals(self, sids, field_name, as_of_date=None, n=1):
        """
        n=1 is the most recent quarter, n=2 indicate the previous quarter and so on...
        It's different from windows_lenght
        """
        result = self._get_result(sids, field_name, as_of_date, n, enforce_date=True)
        #shape: (windows lenghts=1, num of assets)
        return pd.DataFrame(result).set_index(0).reindex(sids).T.values.astype('float64')
    
    @cached
    def get_fundamentals_df_window_length(self, sids, field_name, as_of_date=None, window_length=1):
        offset = 5*math.ceil(window_length/20)*2.592e+15
        sql = "SELECT sid, start_date, value FROM equity_supplementary_mappings WHERE sid IN (%s) AND field = '%s' AND start_date <= %d AND start_date >= %d"
        cmd = sql % (', '.join(map(str, sids)), field_name, as_of_date.value, (as_of_date.value-offset))
        result = self.engine.execute(cmd).fetchall()  
        
        df = pd.DataFrame(result).set_index([0,1])
        #TODO get calendar from bundle
        calendar = get_calendar('XNYS')
        sessions = calendar.sessions_window(as_of_date, 1-window_length)
        full_index = pd.MultiIndex.from_product([sids, [x.value for x in sessions]])
        # an empty dataframe with the full index
        df_empty = pd.DataFrame(index=full_index)
        df_full = pd.concat([df, df_empty], axis=1).fillna(method='ffill').loc[full_index]
        df_full.columns=['value']
        df_full = df_full.astype('float64')

        pivot = df_full.reset_index().pivot(index='level_1', columns='level_0', values='value')[sids]
        pivot.index.name = ''
        pivot.columns.name = ''
        return pivot
        
    @cached
    def get_fundamentals_ttm(self, sids, field_name, as_of_date=None, k=1):
        """
        k=1 is the sum of the last twelve months, k=2 is the sum of the previouns twelve months and so on...
        It's different from windows_lenght
        """
        result = self._get_result_ttm(sids, field_name + '_arq', as_of_date, k)
        return pd.DataFrame(result).set_index(0).reindex(sids).T.values.astype('float64')
    
    @cached
    def get_info(self, sids, field_name, as_of_date=None):
        """
        Unlike get_fundamentals(.), it use the string 'NA' for unknown values, 
        because np.nan isn't supported in LabelArray
        """
        result = self._get_result(sids, field_name, as_of_date, n=1, enforce_date=False)
        return pd.DataFrame(result).set_index(0).reindex(sids, fill_value='NA').T.values


@singleton
class SQLiteAssetDBWriter(AssetDBWriter):

    def _write_assets(self, asset_type, assets, txn, chunk_size, mapping_data=None):
        if asset_type == 'future':
            tbl = futures_contracts_table
            if mapping_data is not None:
                raise TypeError('no mapping data expected for futures')

        elif asset_type == 'equity':
            tbl = equities_table
            if mapping_data is None:
                raise TypeError('mapping data required for equities')
            # write the symbol mapping data.
            # Overidden to avoid duplicate entries with the same sid
            mapping_data['sid'] = mapping_data.index
            self._write_df_to_table(
                equity_symbol_mappings,
                mapping_data,
                txn,
                chunk_size,
            )

        else:
            raise ValueError(
                "asset_type must be in {'future', 'equity'}, got: %s" %
                asset_type,
            )

        self._write_df_to_table(tbl, assets, txn, chunk_size)

        pd.DataFrame({
            asset_router.c.sid.name: assets.index.values,
            asset_router.c.asset_type.name: asset_type,
        }).to_sql(
            asset_router.name,
            txn.connection,
            if_exists='append',
            index=False,
            chunksize=chunk_size
        )