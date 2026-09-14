"""Tests for sharadar.loaders.ingest_sharadar.synch_to_calendar.

Regression test for the bug where the concatenated (forward-filled)
DataFrame produced by `pd.concat` was discarded instead of being
returned/reassigned, silently dropping interstitial trading dates from
the ingested price data.
"""
import numpy as np
import pandas as pd

from sharadar.loaders.ingest_sharadar import synch_to_calendar


def _make_df(dates, sid, ticker, closes, volumes):
    # Columns mirror the schema produced by get_data()/process_data_table():
    # 'ticker', 'open', 'high', 'low', 'close', 'volume' (no 'dividends').
    index = pd.MultiIndex.from_arrays([pd.DatetimeIndex(dates), np.full(len(dates), sid)],
                                       names=('date', 'sid'))
    return pd.DataFrame({
        'ticker': ticker,
        'open': closes,
        'high': closes,
        'low': closes,
        'close': closes,
        'volume': volumes,
    }, index=index)


class TestSynchToCalendar:
    def test_fills_missing_interstitial_dates(self):
        sessions = pd.DatetimeIndex(
            ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05']
        )
        # ticker data is missing 2020-01-03 (interstitial date)
        ticker_dates = ['2020-01-01', '2020-01-02', '2020-01-04', '2020-01-05']
        df_ticker = _make_df(ticker_dates, sid=1, ticker='AAA',
                              closes=[10.0, 11.0, 13.0, 14.0], volumes=[100, 200, 400, 500])

        # df also contains an unrelated ticker (different sid) that must be preserved untouched.
        other_dates = ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05']
        df_other = _make_df(other_dates, sid=2, ticker='BBB',
                             closes=[1.0, 2.0, 3.0, 4.0, 5.0], volumes=[10, 20, 30, 40, 50])

        df = pd.concat([df_ticker, df_other])

        start_date = df_ticker.index.get_level_values('date')[0]
        end_date = df_ticker.index.get_level_values('date')[-1]

        result = synch_to_calendar(sessions, start_date, end_date, df_ticker, df)

        # The missing interstitial date must now be present for sid=1.
        assert ('2020-01-03', 1) in result.index
        filled_row = result.loc[('2020-01-03', 1)]
        assert filled_row['close'] == 11.0  # forward-filled from 2020-01-02
        assert filled_row['volume'] == 0  # volume must remain 0 for a synthesized row

        # All original sid=1 dates must still be present.
        for date in ticker_dates:
            assert (date, 1) in result.index

        # The unrelated ticker's data must be untouched.
        for date in other_dates:
            assert (date, 2) in result.index
        assert result.loc[('2020-01-03', 2)]['close'] == 3.0

        # Total row count: 5 dates for sid=1 (after fill) + 5 dates for sid=2.
        assert len(result) == 10

    def test_no_missing_dates_returns_df_unchanged(self):
        sessions = pd.DatetimeIndex(['2020-01-01', '2020-01-02', '2020-01-03'])
        ticker_dates = ['2020-01-01', '2020-01-02', '2020-01-03']
        df_ticker = _make_df(ticker_dates, sid=1, ticker='AAA',
                              closes=[10.0, 11.0, 12.0], volumes=[100, 200, 300])
        df = df_ticker.copy()

        start_date = df_ticker.index.get_level_values('date')[0]
        end_date = df_ticker.index.get_level_values('date')[-1]

        result = synch_to_calendar(sessions, start_date, end_date, df_ticker, df)

        assert result is df
        assert len(result) == 3
