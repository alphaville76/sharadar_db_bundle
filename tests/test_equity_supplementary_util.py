import sqlite3

import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch
from sharadar.util.equity_supplementary_util import value_changed, lookup_sid, lookup_related_tickers
from sharadar.util.equity_supplementary_util import map_tickers_to_sids, insert_fundamentals, insert_daily_metrics


class TestValueChanged:
    def test_no_existing_record_returns_false(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        result = value_changed(cursor, 1, 'sector', 'Technology')
        assert result is False

    def test_same_value_returns_false(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ('Technology',)
        result = value_changed(cursor, 1, 'sector', 'Technology')
        assert result is False

    def test_different_value_returns_true(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = ('Healthcare',)
        result = value_changed(cursor, 1, 'sector', 'Technology')
        assert result is True


class TestLookupSid:
    def test_ticker_found_directly(self):
        metadata_df = pd.DataFrame({
            'permaticker': [100, 200, 300]
        }, index=['AAPL', 'GOOG', 'MSFT'])
        related = pd.Series(dtype=str)
        result = lookup_sid(metadata_df, related, 'AAPL')
        assert result == 100

    def test_ticker_not_found_falls_back_to_related(self):
        metadata_df = pd.DataFrame({
            'permaticker': [100, 200],
            'category': ['Domestic', 'ADR'],
            'relatedtickers': [' XYZ ', ' ABC ']
        }, index=['AAPL', 'GOOG'])
        related = pd.Series([' XYZ ', ' UNKNOWN '], index=['AAPL', 'GOOG'])
        result = lookup_sid(metadata_df, related, 'XYZ')
        assert result == 100


class TestLookupRelatedTickers:
    def test_finds_related_domestic(self):
        metadata_df = pd.DataFrame({
            'permaticker': [100, 200],
            'category': ['Domestic', 'ADR'],
        }, index=['AAPL', 'GOOG'])
        related = pd.Series([' TICKER1 ', ' TICKER1 '], index=['AAPL', 'GOOG'])
        result = lookup_related_tickers(metadata_df, related, 'TICKER1')
        assert result == 100

    def test_no_match_returns_negative_one(self):
        metadata_df = pd.DataFrame({
            'permaticker': [100],
            'category': ['ADR'],
        }, index=['GOOG'])
        related = pd.Series([' NOMATCH '], index=['GOOG'])
        result = lookup_related_tickers(metadata_df, related, 'TICKER1')
        assert result == -1

class TestMapTickersToSids:
    def test_maps_each_ticker_once(self):
        metadata_df = pd.DataFrame({'permaticker': [100, 200], 'category': ['Domestic', 'Domestic']},
                                   index=['AAPL', 'GOOG'])
        tickers = pd.Series(['GOOG', 'AAPL', 'GOOG', 'NONE', 'NONE'], index=[5, 6, 7, 8, 9])
        with patch('sharadar.util.equity_supplementary_util.lookup_sid', wraps=lookup_sid) as lookup:
            result = map_tickers_to_sids(metadata_df, pd.Series(dtype=str), tickers)
        assert result.tolist() == [200, 100, 200, -1, -1]
        assert result.index.tolist() == [5, 6, 7, 8, 9]
        assert lookup.call_count == 3

    def test_duplicate_ticker_uses_first_row(self):
        metadata_df = pd.DataFrame({'permaticker': [100, 101]}, index=['AAPL', 'AAPL'])
        assert lookup_sid(metadata_df, pd.Series(dtype=str), 'AAPL') == 100


def _mappings_cursor():
    conn = sqlite3.connect(':memory:')
    conn.execute("CREATE TABLE equity_supplementary_mappings (sid INTEGER, field TEXT, start_date INTEGER, "
                 "end_date INTEGER, value TEXT, PRIMARY KEY (sid, field, start_date))")
    return conn, conn.cursor()


def _rows(conn):
    return conn.execute("SELECT sid, field, start_date, value FROM equity_supplementary_mappings "
                        "ORDER BY sid, field, start_date").fetchall()


METADATA = pd.DataFrame({'permaticker': [100, 200], 'relatedtickers': [None, 'OLD'],
                         'category': ['Domestic', 'Domestic']}, index=['AAA', 'BBB'])


class TestInsertFundamentals:
    def test_inserts_non_null_values_per_dimension(self):
        conn, cursor = _mappings_cursor()
        datekey = pd.Timestamp('2020-02-10')
        sf1 = pd.DataFrame({
            'ticker': ['AAA', 'AAA', 'OLD', 'UNKNOWN'],
            'dimension': ['ARQ', 'ART', 'ARQ', 'ARQ'],
            'calendardate': datekey, 'datekey': datekey, 'lastupdated': datekey,
            'reportperiod': pd.Timestamp('2019-12-31'), 'fiscalperiod': '2019-Q4', 'pe': 1.0,
            'revenue': [10.5, np.nan, 3.0, 4.0],
            'shares': [7, 8, 9, 10],
            'note': ['x', 'None', None, 'y'],
        })

        insert_fundamentals(METADATA, sf1, cursor, show_progress=False)

        start = (datekey + pd.Timedelta(days=1)).value
        assert _rows(conn) == [
            (100, 'note_arq', start, 'x'),
            (100, 'reportperiod_arq', start, '2019-12-31 00:00:00'),
            (100, 'reportperiod_art', start, '2019-12-31 00:00:00'),
            (100, 'revenue_arq', start, '10.5'),
            (100, 'shares_arq', start, '7'),
            (100, 'shares_art', start, '8'),
            (200, 'reportperiod_arq', start, '2019-12-31 00:00:00'),
            (200, 'revenue_arq', start, '3.0'),
            (200, 'shares_arq', start, '9'),
        ]


class TestInsertDailyMetrics:
    def test_inserts_non_null_metrics(self):
        conn, cursor = _mappings_cursor()
        dates = pd.to_datetime(['2020-01-02', '2020-01-03'])
        daily = pd.DataFrame({'ticker': ['AAA', 'BBB'], 'date': dates, 'lastupdated': dates,
                              'marketcap': [1.5, np.nan], 'pe': [10.0, 20.25]})

        insert_daily_metrics(METADATA, daily, cursor, show_progress=False)

        assert _rows(conn) == [
            (100, 'marketcap', dates[0].value, '1.5'),
            (100, 'pe', dates[0].value, '10.0'),
            (200, 'pe', dates[1].value, '20.25'),
        ]
