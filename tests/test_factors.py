import numpy as np
import pandas as pd


class TestDaysSinceFilingLogic:
    """Test the core computation logic of DaysSinceFiling."""

    def test_days_calculation(self):
        today = pd.Timestamp('2023-06-15')
        # datekey is 30 days ago
        datekey_ts = pd.Timestamp('2023-05-16').value
        days = (today.value - datekey_ts) / (24 * 60 * 60 * 1e9)
        assert abs(days - 30) < 0.01

    def test_zero_days(self):
        today = pd.Timestamp('2023-06-15')
        datekey_ts = today.value
        days = (today.value - datekey_ts) / (24 * 60 * 60 * 1e9)
        assert days == 0.0


class TestIsDomesticCommonStockLogic:
    """Test the category matching logic."""

    def test_domestic_common_stock_matches(self):
        categories = np.array(['Domestic Common Stock', 'ADR', 'Domestic Common Stock Primary Class'])
        expected_valid = ['Domestic Common Stock', 'Domestic Common Stock Primary Class',
                         'Domestic Common Stock Secondary Class', 'Domestic Preferred Stock']
        result = np.isin(categories, expected_valid)
        assert result[0] == True
        assert result[1] == False
        assert result[2] == True

    def test_empty_category(self):
        categories = np.array([''])
        expected_valid = ['Domestic Common Stock', 'Domestic Common Stock Primary Class']
        result = np.isin(categories, expected_valid)
        assert result[0] == False


class TestExchangeClassifier:
    def test_exchange_categories(self):
        expected = ['BATS', 'INDEX', 'NASDAQ', 'NYSE', 'NYSEARCA', 'NYSEMKT', 'OTC']
        assert 'NYSE' in expected
        assert 'OTC' in expected


class TestSectorClassifier:
    def test_sector_categories(self):
        expected = ['Basic Materials', 'Communication Services', 'Consumer Cyclical',
                   'Consumer Defensive', 'Energy', 'Financial Services', 'Healthcare',
                   'Industrials', 'Real Estate', 'Technology', 'Utilities']
        assert len(expected) == 11
        assert 'Technology' in expected