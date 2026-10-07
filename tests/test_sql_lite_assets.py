import pytest
import pandas as pd
from sqlalchemy.exc import OperationalError
from sharadar.data.sql_lite_assets import SQLiteAssetDBWriter, SQLiteAssetFinder


@pytest.fixture
def asset_finder(asset_db_engine):
    return SQLiteAssetFinder(asset_db_engine)


class TestSQLiteAssetFinder:
    def test_instantiation(self, asset_finder):
        assert asset_finder is not None
        assert asset_finder.is_live_trading is False

    def test_live_trading_flag(self, asset_finder):
        asset_finder.is_live_trading = True
        assert asset_finder.is_live_trading is True

    def test_retrieve_asset_dicts_empty_sids(self, asset_finder):
        result = asset_finder._retrieve_asset_dicts([], asset_finder.equities, querying_equities=True)
        assert list(result) == []

    def test_retrieve_asset_dicts_nonexistent_sids(self, asset_finder):
        result = asset_finder._retrieve_asset_dicts([1, 2, 3], asset_finder.equities, querying_equities=True)
        assert list(result) == []

    def test_get_fundamentals_nonexistent(self, asset_finder):
        result = asset_finder.get_fundamentals([9999], 'revenue_arq', pd.Timestamp('2023-01-01'))
        assert result == []

    def test_get_inner_select_returns_string(self, asset_finder):
        sql = asset_finder._get_inner_select()
        assert 'SELECT' in sql
        assert 'equity_supplementary_mappings' in sql
        assert 'ROW_NUMBER' in sql


def test_asset_db_writer_retries_when_database_is_locked(tmp_path, monkeypatch):
    writer = SQLiteAssetDBWriter(str(tmp_path / 'assets.sqlite'), lock_retry_count=2, lock_retry_delay=0)
    begin_calls = {'count': 0}

    class DummyConnection:
        def exec_driver_sql(self, sql):
            return None

        def execute(self, stmt):
            return None

    class DummyTransaction:
        def __enter__(self):
            return DummyConnection()

        def __exit__(self, exc_type, exc, tb):
            return False

    class FailingThenWorkingBegin:
        def __call__(self):
            begin_calls['count'] += 1
            if begin_calls['count'] == 1:
                raise OperationalError('database is locked', None, None)
            return DummyTransaction()

    monkeypatch.setattr(writer.engine, 'begin', FailingThenWorkingBegin())
    writer.init_db = lambda txn=None: None

    writer._real_write(None, None, None, None, None, None, 1000)

    assert begin_calls['count'] == 2


def test_asset_db_writer_leaves_no_wal_files(tmp_path):
    from sharadar.loaders.constant import EXCHANGE_DF
    from sharadar.util.sqlite_util import wal_files

    path = str(tmp_path / 'assets.sqlite')
    writer = SQLiteAssetDBWriter(path)
    writer.write(exchanges=EXCHANGE_DF)

    assert wal_files(path) == []
    finder = SQLiteAssetFinder(path)
    assert finder is not None
    assert wal_files(path) == []

def test_lifetimes_sids_are_sorted_regardless_of_insertion_order(tmp_path):
    from sharadar.loaders.constant import EXCHANGE_DF

    sids = [118691, 101361, 196191, 103968]
    equities = pd.DataFrame({
        'symbol': ['SPY', 'AAA', 'BBB', 'CCC'],
        'asset_name': ['SPY', 'AAA', 'BBB', 'CCC'],
        'start_date': pd.Timestamp('2020-01-02').as_unit('ns'),
        'end_date': pd.Timestamp('2024-12-31').as_unit('ns'),
        'first_traded': pd.Timestamp('2020-01-02').as_unit('ns'),
        'auto_close_date': pd.Timestamp('2025-01-01').as_unit('ns'),
        'exchange': ['NYSEARCA', 'NYSE', 'NASDAQ', 'NYSE'],
    }, index=pd.Index(sids, name='sid'))
    path = str(tmp_path / 'assets.sqlite')
    SQLiteAssetDBWriter(path).write(equities=equities, exchanges=EXCHANGE_DF)

    finder = SQLiteAssetFinder(path)
    lifetimes = finder.lifetimes(pd.DatetimeIndex(['2023-03-14', '2024-03-21']), False, ('US',))

    assert list(lifetimes.columns) == sorted(sids)
    assert lifetimes.columns.searchsorted(118691) == sorted(sids).index(118691)
    assert lifetimes.values.all()