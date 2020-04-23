import math
import numpy as np
import pandas as pd
import warnings
from memoization import cached
from singleton_decorator import singleton
from toolz import first
from zipline.assets import AssetFinder, AssetDBWriter
from zipline.assets.asset_db_schema import (
    asset_router,
    equities as equities_table,
    equity_symbol_mappings,
    futures_contracts as futures_contracts_table,
)
from zipline.utils.calendars import get_calendar
from zipline.utils.memoize import lazyval


@singleton
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