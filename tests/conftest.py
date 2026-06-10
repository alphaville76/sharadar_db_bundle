import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine
from zipline.assets.asset_db_schema import metadata as asset_metadata
from zipline.assets import ASSET_DB_VERSION


@pytest.fixture
def asset_db_engine():
    """Create an in-memory SQLite engine with zipline asset schema."""
    engine = create_engine('sqlite:///:memory:')
    asset_metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(
            asset_metadata.tables['version_info'].insert().values(version=ASSET_DB_VERSION)
        )
        conn.commit()
    return engine


@pytest.fixture
def mock_asset_finder():
    """Mock asset finder for testing pipeline factors."""
    finder = MagicMock()
    finder.get_fundamentals.return_value = np.array([1.0, 2.0, 3.0])
    finder.get_daily_metrics.return_value = np.array([[100.0, 200.0, 300.0]])
    finder.get_info.return_value = np.array(['Technology', 'Healthcare', 'Energy'])
    return finder
