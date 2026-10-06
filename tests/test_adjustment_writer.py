import sqlite3

import numpy as np
import pandas as pd
import pytest
from zipline.data.adjustments import (
    SQLITE_ADJUSTMENT_COLUMN_DTYPES,
    SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES,
)

from sharadar.data.sql_lite_daily_pricing import SQLiteDailyAdjustmentWriter
from sharadar.util.fix_dividends import fix_shifted_rows

EX_DATE = 1790899200  # 2026-10-02


@pytest.fixture
def adjustments_path(tmp_path):
    path = str(tmp_path / "adjustments.sqlite")
    SQLiteDailyAdjustmentWriter(path, None, None, None)
    return path


def rows(path, table):
    with sqlite3.connect(path) as con:
        return con.execute('SELECT * FROM "%s" ORDER BY sid' % table).fetchall()


def test_write_dividend_ratios_by_column_name(adjustments_path):
    writer = SQLiteDailyAdjustmentWriter(adjustments_path, None, None, None)
    # same column order as calc_dividend_ratios: differs from the table order
    frame = pd.DataFrame({
        'sid': np.array([199994, 645225], dtype='int64'),
        'effective_date': np.array([EX_DATE, EX_DATE], dtype='int64'),
        'ratio': np.array([0.9974, 0.9869], dtype='float64'),
    })
    writer._write('dividends', SQLITE_ADJUSTMENT_COLUMN_DTYPES, frame)

    assert rows(adjustments_path, 'dividends') == [(0, EX_DATE, 0.9974, 199994), (1, EX_DATE, 0.9869, 645225)]


def test_write_dividend_payouts_with_timestamp_index(adjustments_path):
    writer = SQLiteDailyAdjustmentWriter(adjustments_path, None, None, None)
    frame = pd.DataFrame({
        'sid': np.array([199994], dtype='int64'),
        'ex_date': np.array([EX_DATE], dtype='int64'),
        'declared_date': np.array([EX_DATE], dtype='int64'),
        'record_date': np.array([EX_DATE], dtype='int64'),
        'pay_date': np.array([EX_DATE], dtype='int64'),
        'amount': np.array([0.16], dtype='float64'),
    }, index=[pd.Timestamp('2026-10-02')])
    writer._write('dividend_payouts', SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES, frame)

    assert rows(adjustments_path, 'dividend_payouts') == [
        ('2026-10-02 00:00:00', 0.16, 199994, EX_DATE, EX_DATE, EX_DATE, EX_DATE)]


def test_fix_dividends_repairs_shifted_rows(adjustments_path):
    with sqlite3.connect(adjustments_path) as con:
        # shifted row as written by the old positional insert, plus a correct row
        con.execute("INSERT INTO dividends VALUES (0, 199994, %f, 0.9974)" % EX_DATE)
        con.execute("INSERT INTO dividends VALUES (1, %d, 0.9869, 645225)" % EX_DATE)

    assert fix_shifted_rows(adjustments_path, dry_run=True) == 1
    assert rows(adjustments_path, 'dividends')[0] == (0, 199994, float(EX_DATE), 0.9974)
    assert fix_shifted_rows(adjustments_path) == 1
    assert rows(adjustments_path, 'dividends') == [(0, EX_DATE, 0.9974, 199994), (1, EX_DATE, 0.9869, 645225)]
    assert fix_shifted_rows(adjustments_path) == 0


class StubPricesReader:
    """Raw closes: sid 1 = 100 -> 50 (2:1 split on 2026-01-07), sid 2 always 0 (no price)."""
    sessions = pd.DatetimeIndex(['2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08'])

    def load_raw_arrays(self, fields, start, end, sids):
        close = {1: [100.0, 100.0, 50.0, 50.0], 2: [0.0, 0.0, 0.0, 0.0]}
        return [np.array([close[s] for s in sids]).T]


class StubAssetFinder:
    def retrieve_asset(self, sid):
        return type('Asset', (), {'start_date': pd.Timestamp('2020-01-01')})()


def test_calc_dividend_ratios_uses_raw_previous_close(adjustments_path):
    writer = SQLiteDailyAdjustmentWriter(adjustments_path, StubPricesReader(), StubAssetFinder(), None)
    dividends = pd.DataFrame({
        'sid': np.array([1, 1, 2], dtype='int64'),
        'ex_date': pd.to_datetime(['2026-01-06', '2026-01-08', '2026-01-06']),
        'amount': [1.0, 1.0, 1.0],
    })
    ratios = writer.calc_dividend_ratios(dividends)

    # raw close of the previous session; sid 2 has no valid close and is skipped
    assert ratios['sid'].tolist() == [1, 1]
    assert ratios['effective_date'].tolist() == list(pd.to_datetime(['2026-01-06', '2026-01-08']))
    assert ratios['ratio'].tolist() == [1 - 1 / 100.0, 1 - 1 / 50.0]

class StubPricesReaderByPairs(StubPricesReader):
    def load_raw_arrays(self, fields, start, end, sids):
        raise AssertionError("the full price history must not be loaded")

    def load_values_at(self, field, sids, dates):
        close = StubPricesReader().load_raw_arrays([field], None, None, [1, 2])[0]
        return np.array([close[self.sessions.get_loc(d), s - 1] for s, d in zip(sids, dates)])


def test_calc_dividend_ratios_loads_only_previous_closes(adjustments_path):
    writer = SQLiteDailyAdjustmentWriter(adjustments_path, StubPricesReaderByPairs(), StubAssetFinder(), None)
    dividends = pd.DataFrame({
        'sid': np.array([1, 1, 2], dtype='int64'),
        'ex_date': pd.to_datetime(['2026-01-06', '2026-01-08', '2026-01-06']),
        'amount': [1.0, 1.0, 1.0],
    })
    ratios = writer.calc_dividend_ratios(dividends)

    assert ratios['sid'].tolist() == [1, 1]
    assert ratios['ratio'].tolist() == [1 - 1 / 100.0, 1 - 1 / 50.0]
