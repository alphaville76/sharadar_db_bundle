import pytest
import pandas as pd
from sharadar.data.sql_lite_assets import SQLiteAssetFinder


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
