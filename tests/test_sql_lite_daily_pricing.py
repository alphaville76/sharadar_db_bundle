import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
import pytest
from exchange_calendars import get_calendar

from sharadar.data.sql_lite_daily_pricing import (
    SQLiteDailyBarWriter,
    SQLiteDailyBarReader,
)


@pytest.fixture
def calendar():
    return get_calendar('XNYS', start=pd.Timestamp('2020-01-01'))


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / 'prices.sqlite')


@pytest.fixture
def sample_data():
    """Create sample OHLCV data as a MultiIndex DataFrame."""
    dates = pd.to_datetime(['2020-01-02', '2020-01-03', '2020-01-06'])
    sids = [1, 2]
    index = pd.MultiIndex.from_product([dates, sids], names=['date', 'sid'])
    data = pd.DataFrame({
        'open':   [10.0, 20.0, 11.0, 21.0, 12.0, 22.0],
        'high':   [15.0, 25.0, 16.0, 26.0, 17.0, 27.0],
        'low':    [9.0,  19.0, 10.0, 20.0, 11.0, 21.0],
        'close':  [14.0, 24.0, 15.0, 25.0, 16.0, 26.0],
        'volume': [100.0, 200.0, 110.0, 210.0, 120.0, 220.0],
    }, index=index)
    return data


@pytest.fixture
def writer(db_path, calendar):
    return SQLiteDailyBarWriter(db_path, calendar)


@pytest.fixture
def populated_db(writer, sample_data, db_path):
    writer.write(sample_data)
    return db_path


@pytest.fixture
def reader(populated_db):
    return SQLiteDailyBarReader(filename=populated_db)


class TestSQLiteDailyBarWriter:
    def test_creates_schema(self, db_path, calendar):
        SQLiteDailyBarWriter(db_path, calendar)
        with closing(sqlite3.connect(db_path)) as con:
            cur = con.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cur.fetchall()}
        assert 'prices' in tables
        assert 'properties' in tables

    def test_write_inserts_data(self, writer, sample_data, db_path):
        writer.write(sample_data)
        with closing(sqlite3.connect(db_path)) as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(sid) FROM prices")
            count = cur.fetchone()[0]
        assert count == 6

    def test_write_stores_calendar_name(self, writer, sample_data, db_path, calendar):
        writer.write(sample_data)
        with closing(sqlite3.connect(db_path)) as con:
            cur = con.cursor()
            cur.execute('SELECT "0" FROM properties WHERE key="calendar_name"')
            result = cur.fetchone()
        assert result[0] == calendar.name

    def test_write_replaces_on_conflict(self, writer, sample_data, db_path):
        writer.write(sample_data)
        # Write again with different values
        modified = sample_data.copy()
        modified['close'] = 999.0
        writer.write(modified)
        with closing(sqlite3.connect(db_path)) as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(sid) FROM prices")
            count = cur.fetchone()[0]
        # Should still be 6 rows (replaced, not duplicated)
        assert count == 6

    def test_write_validates_dataframe(self, writer):
        with pytest.raises(ValueError, match="data must be an instance of DataFrame"):
            writer.write("not a dataframe")

    def test_write_validates_index_names(self, writer):
        df = pd.DataFrame({'open': [1.0]}, index=pd.Index([0], name='wrong'))
        with pytest.raises(ValueError, match="data indexes must be"):
            writer.write(df)


class TestSQLiteDailyBarReader:
    def test_get_value(self, reader):
        val = reader.get_value(1, pd.Timestamp('2020-01-02'), 'close')
        assert val == 14.0

    def test_get_value_different_sid(self, reader):
        val = reader.get_value(2, pd.Timestamp('2020-01-03'), 'open')
        assert val == 21.0

    def test_get_value_nonexistent_sid_raises_key_error(self, reader):
        with pytest.raises(KeyError):
            reader.get_value(999, pd.Timestamp('2020-01-02'), 'close')

    def test_get_value_no_data_before_date(self, reader):
        from zipline.data.bar_reader import NoDataBeforeDate
        with pytest.raises(NoDataBeforeDate):
            reader.get_value(1, pd.Timestamp('2019-01-01'), 'close')

    def test_load_raw_arrays_single_field(self, reader):
        arrays = reader.load_raw_arrays(
            ['close'],
            pd.Timestamp('2020-01-02'),
            pd.Timestamp('2020-01-06'),
            [1, 2],
        )
        assert len(arrays) == 1
        # 3 trading days x 2 sids
        assert arrays[0].shape == (3, 2)
        # Check specific values
        assert arrays[0][0, 0] == 14.0  # sid 1, day 1
        assert arrays[0][0, 1] == 24.0  # sid 2, day 1
        assert arrays[0][2, 0] == 16.0  # sid 1, day 3
        assert arrays[0][2, 1] == 26.0  # sid 2, day 3

    def test_load_raw_arrays_multiple_fields(self, reader):
        arrays = reader.load_raw_arrays(
            ['open', 'close'],
            pd.Timestamp('2020-01-02'),
            pd.Timestamp('2020-01-06'),
            [1, 2],
        )
        assert len(arrays) == 2
        # open array
        assert arrays[0][0, 0] == 10.0
        # close array
        assert arrays[1][0, 0] == 14.0

    def test_load_raw_arrays_single_sid(self, reader):
        arrays = reader.load_raw_arrays(
            ['volume'],
            pd.Timestamp('2020-01-02'),
            pd.Timestamp('2020-01-03'),
            [2],
        )
        assert arrays[0].shape == (2, 1)
        assert arrays[0][0, 0] == 200.0
        assert arrays[0][1, 0] == 210.0

    def test_load_raw_arrays_nonexistent_sid_returns_nan(self, reader):
        arrays = reader.load_raw_arrays(
            ['close'],
            pd.Timestamp('2020-01-02'),
            pd.Timestamp('2020-01-03'),
            [999],
        )
        assert arrays[0].shape == (2, 1)
        assert np.all(np.isnan(arrays[0]))

    def test_get_last_traded_dt(self, reader):
        dt = reader.get_last_traded_dt(1, pd.Timestamp('2020-01-02'))
        assert dt == pd.Timestamp('2020-01-02')

    def test_get_last_traded_dt_no_data_returns_nat(self, reader):
        dt = reader.get_last_traded_dt(1, pd.Timestamp('2019-12-31'))
        assert dt is pd.NaT

    def test_get_last_traded_dt_nonexistent_sid_raises(self, reader):
        with pytest.raises(KeyError):
            reader.get_last_traded_dt(999, pd.Timestamp('2020-01-02'))

    def test_first_trading_day(self, reader):
        ftd = reader.first_trading_day
        assert ftd == pd.Timestamp('2020-01-02')

    def test_last_available_dt(self, reader):
        lad = reader.last_available_dt
        assert lad == pd.Timestamp('2020-01-06')

    def test_sessions(self, reader):
        sessions = reader.sessions
        assert len(sessions) == 3
        assert sessions[0] == pd.Timestamp('2020-01-02')
        assert sessions[-1] == pd.Timestamp('2020-01-06')

    def test_trading_calendar(self, reader):
        cal = reader.trading_calendar
        assert cal.name == 'XNYS'


class TestSQLiteDailyBarReaderEmpty:
    def test_last_available_dt_empty_db(self, db_path, calendar):
        # Create schema but no data
        SQLiteDailyBarWriter(db_path, calendar)
        reader = SQLiteDailyBarReader(filename=db_path)
        assert reader.last_available_dt is pd.NaT