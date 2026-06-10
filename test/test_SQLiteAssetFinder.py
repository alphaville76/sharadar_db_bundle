from datetime import timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine

from sharadar.data.sql_lite_assets import SQLiteAssetFinder
from zipline.assets.asset_db_schema import metadata
from zipline.assets import ASSET_DB_VERSION


@pytest.fixture
def asset_db_engine():
    engine = create_engine('sqlite:///:memory:')
    metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(metadata.tables['version_info'].insert().values(version=ASSET_DB_VERSION))
        conn.commit()
    return engine


class TestSQLiteAssetFinder:

    def test_retrieve_asset_dicts_with_live_trading_adjustments(self, asset_db_engine):
        finder = SQLiteAssetFinder(asset_db_engine)
        finder.is_live_trading = True
        sids = [1, 2, 3]

        result = finder._retrieve_asset_dicts(sids, finder.equities, querying_equities=True)

        # With no data in DB, result should be empty
        assert list(result) == []

    def test_retrieve_asset_dicts_with_empty_sids_list(self, asset_db_engine):
        finder = SQLiteAssetFinder(asset_db_engine)
        sids = []

        result = finder._retrieve_asset_dicts(sids, finder.equities, querying_equities=True)

        assert list(result) == []

    def test_get_fundamentals_with_non_existent_sids(self, asset_db_engine):
        finder = SQLiteAssetFinder(asset_db_engine)
        sids = [9999]
        field_name = 'revenue_arq'
        as_of_date = pd.Timestamp('2023-01-01')

        result = finder.get_fundamentals(sids, field_name, as_of_date)

        assert result == []
