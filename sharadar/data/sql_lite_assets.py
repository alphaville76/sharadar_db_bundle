import math
import numpy as np
import pandas as pd
import warnings
from sharadar.util.cache import cached
from singleton_decorator import singleton
from toolz import first
from zipline.assets import AssetFinder, AssetDBWriter
from zipline.assets.continuous_futures import CHAIN_PREDICATES
from zipline.assets.asset_db_schema import (
    asset_router,
    equities as equities_table,
    equity_symbol_mappings,
    futures_contracts as futures_contracts_table,
)
from zipline.utils.calendars import get_calendar
from zipline.utils.memoize import lazyval
from pandas.tseries.offsets import DateOffset
from datetime import timedelta
from sharadar.util.logger import log

@singleton
class SQLiteAssetFinder(AssetFinder):

    def __init__(self, engine, future_chain_predicates=CHAIN_PREDICATES):
        super().__init__(engine, future_chain_predicates)
        self.is_live_trading = False

    def _retrieve_asset_dicts(self, sids, asset_tbl, querying_equities):
        rows = list(super()._retrieve_asset_dicts(sids, asset_tbl, querying_equities))
        if self.is_live_trading:
            for row in rows:
                # when we use pipeline live the end date is always yesterday so we add 5 days to still keep this condition
                # but also allowing to use pipeline live as well. 5 is a good number for weekends/holidays
                row['end_date'] = row['end_date'] + timedelta(days=5)
                row['auto_close_date'] = row['auto_close_date'] + timedelta(days=5)

        return rows

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

        if as_of_date is None:
            as_of_date = pd.Timestamp.today()

        date_check = as_of_date.value if enforce_date else 0
        cmd = sql % (', '.join(map(str, sids)), field_name, as_of_date.value, date_check, n*MAX_DELAY, n)
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

    def get_datekey(self, sids, as_of_date, n):
        """
        Get the last SEC filing date.
        """
        sql = ("SELECT sid, start_date FROM ("
               "SELECT sid, start_date, "
               "ROW_NUMBER() OVER (PARTITION BY sid "
               "ORDER BY start_date DESC) AS rown "
               "FROM equity_supplementary_mappings "
               "WHERE sid IN (%s) "
               "AND field = 'revenue_arq' "
               "AND start_date <= %d "
               ") t WHERE rown = %d;"
              )

        cmd = sql % (', '.join(map(str, sids)), as_of_date.value, n)
        result =  self.engine.execute(cmd).fetchall()
        return pd.DataFrame(result).set_index(0).reindex(sids).T.values.astype('float64')

    @cached
    def get_fundamentals(self, sids, field_name, as_of_date=None, n=1):
        """
        n=1 is the most recent quarter or last ttm, n=2 indicate the previous quarter or ttm and so on...
        It's different from the original zipline window_length
        """
        result = self._get_result(sids, field_name, as_of_date, n, enforce_date=True)
        if len(result) == 0:
            log.warn("No result: asset_finder().get_fundamentals(%s, %s, %s, n=%s)" %
                (sids, field_name, as_of_date, n)
            )
            return []
        #shape: (windows lenghts=1, num of assets)
        return pd.DataFrame(result).set_index(0).reindex(sids).T.values.astype('float64')
    
    @cached
    def get_fundamentals_df_window_length(self, sids, field_name, as_of_date=None, window_length=1):
        """
        it returns an array of this form:

            value_(t,   sid 1)	value_(t,   sid 2)	...	value_(t,   sid m)
            value_(t-1, sid 1)	value_(t-1, sid 2)	...	value_(t-1, sid m)
            value_(t-2, sid 1)	value_(t-2, sid 2)	...	value_(t-2, sid m)
            value_(t-3, sid 1)	value_(t-3, sid 2)	...	value_(t-3, sid m)
            ...                 ...                 ... ...
            value_(t-n, sid 1)	value_(t-n, sid 2)	...	value_(t-n, sid m)

        where n is the window_length (for example n=4 for the last four quarters)

        """
        if as_of_date is None:
            as_of_date = pd.Timestamp.today()
        start_date = as_of_date - DateOffset(months=(window_length + 1) * 3)

        sql = "SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY sid ORDER BY start_date DESC) row_num," \
              "sid, start_date, value FROM equity_supplementary_mappings WHERE sid IN (%s) AND field = '%s' " \
              "AND start_date >= %d AND start_date <= %d) t WHERE row_num <= %d"
        cmd = sql % (', '.join(map(str, sids)), field_name, start_date.value, as_of_date.value, window_length)

        df = pd.read_sql_query(cmd, self.engine)
        df = df.pivot(index='row_num', columns='sid', values='value')
        df = df.reindex(columns=sids)
        return df.values.astype('float64')
        
    @cached
    def get_fundamentals_ttm(self, sids, field_name, as_of_date=None, k=1):
        """
        k=1 is the sum of the last twelve months, k=2 is the sum of the previous twelve months and so on...
        It's different from windows_length
        """
        result = self._get_result_ttm(sids, field_name + '_arq', as_of_date, k)
        if len(result) == 0:
            return []
        return pd.DataFrame(result).set_index(0).reindex(sids).T.values.astype('float64')
    
    @cached
    def get_info(self, sids, field_name, as_of_date=None):
        """
        Unlike get_fundamentals(.), it use the string 'NA' for unknown values, 
        because np.nan isn't supported in LabelArray
        """
        result = self._get_result(sids, field_name, as_of_date, n=1, enforce_date=False)
        if len(result) == 0:
            return []
        return pd.DataFrame(result).set_index(0).reindex(sids, fill_value='NA').T.values

    @cached
    def get_daily_metrics(self, sids, field_name, as_of_date=pd.Timestamp.today(), n=1, calendar = get_calendar('XNYS')):
        assert n > 0
        sessions = calendar.sessions_window(as_of_date, -n+1)
        query = "SELECT start_date, sid, value FROM equity_supplementary_mappings " \
                "WHERE field ='%s' AND sid in (%s) AND start_date >= %s AND start_date <= %s" \
                % (field_name, ",".join(map(str, sids)), sessions[0].value, sessions[-1].value)

        df = pd.read_sql_query(query, self.engine)
        df = df.pivot(index='start_date', columns='sid', values='value')
        df = df.reindex(index=list(map(self._fmt_date, sessions)), columns=sids)
        return df.values.astype('float64')

    def _fmt_date(self, dt):
        return dt.value

    @property
    def last_available_fundamentals_dt(self):
        return self.last_available_dt('revenue_arq')

    @property
    def last_available_daily_metrics_dt(self):
        return self.last_available_dt('marketcap')

    def last_available_dt(self, field):
        sql = "SELECT MAX(start_date) FROM equity_supplementary_mappings WHERE field = '%s';" % field
        res = self.engine.execute(sql).fetchall()
        if len(res) == 0:
            return pd.NaT
        return pd.Timestamp(res[0][0], tz='UTC')



@singleton
class SQLiteAssetDBWriter(AssetDBWriter):

    def init_db(self, txn=None):
        super().init_db(txn)
        txn.execute("CREATE INDEX IF NOT EXISTS idx_start_date_field  ON equity_supplementary_mappings (start_date, field);")


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
            mapping_data.index = mapping_data['sid']
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

        df = pd.DataFrame({
            asset_router.c.sid.name: assets.index.values,
            asset_router.c.asset_type.name: asset_type,
        })
        self._write_df_to_table(asset_router, df, txn, chunk_size, idx=False)
        #df.to_sql(asset_router.name, txn.connection, if_exists='append', index=False, chunksize=chunk_size)

    def escape(self, name):
        # See https://stackoverflow.com/questions/6514274/how-do-you-escape-strings\
        # -for-sqlite-table-column-names-in-python
        # Ensure the string can be encoded as UTF-8.
        # Ensure the string does not include any NUL characters.
        # Replace all " with "".
        # Wrap the entire thing in double quotes.

        try:
            uname = str(name).encode("utf-8", "strict").decode("utf-8")
        except UnicodeError as err:
            raise ValueError(f"Cannot convert identifier to UTF-8: '{name}'") from err
        if not len(uname):
            raise ValueError("Empty table or column name specified")

        nul_index = uname.find("\x00")
        if nul_index >= 0:
            raise ValueError("SQLite identifier cannot contain NULs")
        return '"' + uname.replace('"', '""') + '"'

    def insert_statement(self, df, table_name, index=True, index_label=None, wld='?'):
        num_rows = df.shape[0]

        names = list(map(str, df.columns))

        if index:
            if index_label is not None:
                if not isinstance(index_label, list):
                    index_label = [index_label]
                for idx in index_label[::-1]:
                    names.insert(0, idx)
            elif df.index.name is not None:
                for idx in df.index.name[::-1]:
                    names.insert(0, idx)
            else:
                names.insert(0, "index")

        bracketed_names = [self.escape(column) for column in names]
        col_names = ",".join(bracketed_names)

        wildcards = ",".join([wld] * len(names))
        insert_statement = (
            f"INSERT OR REPLACE INTO {self.escape(table_name)} ({col_names}) VALUES ({wildcards})"
        )
        return insert_statement

    def _write_df_to_table(self, tbl, df, txn, chunk_size=None, idx=True, idx_label=None):
        engine = txn.connection
        index_label = (
            idx_label
            if idx_label is not None else
            first(tbl.primary_key.columns).name
        )
        cmd = self.insert_statement(df, tbl.name, idx, index_label)

        for index, row in df.iterrows():
            values = row.values
            if idx:
                values = np.insert(values, 0, str(index), axis=0)
            engine.execute(cmd, tuple(values))

    def check_sanity(self):
        """
        Check if there were changes in some metadata
        """

        sane = True

        field = 'category'

        expected = ['ADR Common Stock', 'ADR Common Stock Primary Class', 'ADR Common Stock Secondary Class',
                    'ADR Preferred Stock', 'ADR Stock Warrant', 'CEF',
                    'Canadian Common Stock', 'Canadian Common Stock Primary Class', 'Canadian Common Stock Secondary Class',
                    'Canadian Preferred Stock', 'Canadian Stock Warrant',
                    'Domestic Common Stock', 'Domestic Common Stock Primary Class', 'Domestic Common Stock Secondary Class',
                    'Domestic Preferred Stock', 'Domestic Stock Warrant', 'ETD', 'ETF', 'ETN', 'IDX']

        if not self._check_field(field, expected):
            sane = False

        field = 'sector'
        expected = ['Basic Materials', 'Communication Services', 'Consumer Cyclical', 'Consumer Defensive', 'Energy', 'Financial Services', 'Healthcare', 'Industrials', 'Real Estate', 'Technology', 'Utilities']
        if not self._check_field(field, expected):
            sane = False

        field = 'exchange'
        expected = ['BATS', 'INDEX', 'NASDAQ', 'NYSE', 'NYSEARCA', 'NYSEMKT', 'OTC']
        if not self._check_field(field, expected):
            sane = False

        return sane

    def _check_field(self, field, expected):
        sql = "SELECT DISTINCT(value) as r FROM equity_supplementary_mappings WHERE field = '%s' ORDER BY r;" % field
        ret = [x[0] for x in self.engine.execute(sql).fetchall()]
        ok = np.array_equal(ret, expected)
        if not ok:
            sane = False
            from sharadar.util.logger import log
            log.error("Field '%s' changed!\nActual:\n  %s\nExpected:\n  %s" % (field, ret, expected))
            return False
        else:
            return True
