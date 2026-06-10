import pandas as pd
from unittest.mock import MagicMock
from sharadar.util.equity_supplementary_util import value_changed, lookup_sid, lookup_related_tickers


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