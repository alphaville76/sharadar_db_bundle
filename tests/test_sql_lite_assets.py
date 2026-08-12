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
