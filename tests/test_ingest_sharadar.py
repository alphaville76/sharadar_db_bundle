"""Tests for calendar repair during Sharadar ingestion.

Regression test for the bug where the concatenated (forward-filled)
DataFrame produced by `pd.concat` was discarded instead of being
returned/reassigned, silently dropping interstitial trading dates from
the ingested price data.
"""
import numpy as np
import pandas as pd
from unittest.mock import patch

from sharadar.loaders.ingest_sharadar import create_equities_df, synch_to_calendar


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

    def test_consecutive_gaps_stay_within_ticker_lifetime(self):
        sessions = pd.date_range('2020-01-01', periods=7)
        df = _make_df(['2020-01-02', '2020-01-05'], sid=1, ticker='AAA',
                      closes=[10.0, 15.0], volumes=[100, 500])
        original = df.copy(deep=True)

        result = synch_to_calendar(sessions, sessions[1], sessions[4], df, df)

        assert len(result) == 4
        for date in sessions[2:4]:
            row = result.loc[(date, 1)]
            assert row[['open', 'high', 'low', 'close']].eq(10.0).all()
            assert row['ticker'] == 'AAA'
            assert row['volume'] == 0
        assert (sessions[0], 1) not in result.index
        assert (sessions[5], 1) not in result.index
        pd.testing.assert_frame_equal(df, original)

    def test_existing_nan_handling_is_preserved(self):
        sessions = pd.date_range('2020-01-01', periods=4)
        df = _make_df([sessions[0], sessions[2], sessions[3]], sid=1, ticker='AAA',
                      closes=[np.nan, 12.0, np.nan], volumes=[100, np.nan, 300])

        result = synch_to_calendar(sessions, sessions[0], sessions[-1], df, df)

        assert list(result.index.get_level_values('date')) == list(sessions[2:])
        assert result.loc[(sessions[2], 1), 'volume'] == 0
        assert result.loc[(sessions[3], 1), 'close'] == 12.0


def _make_metadata(sids):
    return pd.DataFrame({
        'permaticker': sids,
        'name': [f'Asset {sid}' for sid in sids],
        'firstpricedate': pd.Timestamp('2020-01-01'),
        'lastpricedate': pd.Timestamp('2020-01-07'),
        'exchange': 'NYSE',
    })


class TestCreateEquitiesDf:
    def test_batches_repairs_and_preserves_unprocessed_tickers(self):
        sessions = pd.date_range('2020-01-01', periods=7)
        aaa = _make_df(sessions[[1, 4]], sid=1, ticker='AAA',
                       closes=[10.0, 14.0], volumes=[100, 400])
        bbb = _make_df(sessions, sid=2, ticker='BBB',
                       closes=np.arange(7.0), volumes=np.arange(7) * 10)
        ccc = _make_df(sessions[[2, 4, 5]], sid=3, ticker='CCC',
                       closes=[22.0, 24.0, 25.0], volumes=[200, 400, 500])
        ddd = _make_df(sessions[1:4], sid=4, ticker='DDD',
                       closes=[31.0, 32.0, 33.0], volumes=[100, 200, 300])
        df = pd.concat([aaa, bbb, ccc, ddd]).sort_index(ascending=False)
        original = df.copy(deep=True)
        metadata = _make_metadata([1, 3, 4, 1])
        metadata.loc[3, 'name'] = 'Later duplicate metadata'
        expected = df
        for ticker in ['AAA', 'DDD', 'CCC']:
            prices = df[df['ticker'] == ticker].sort_index()
            dates = prices.index.get_level_values('date')
            expected = synch_to_calendar(sessions, dates[0], dates[-1], prices, expected)

        with patch('sharadar.loaders.ingest_sharadar.pd.concat', wraps=pd.concat) as concat:
            equities, result = create_equities_df(
                df, ['AAA', 'DDD', 'CCC'], sessions, metadata, show_progress=False,
            )

        assert concat.call_count == 1
        pd.testing.assert_frame_equal(result.sort_index(), expected.sort_index())
        pd.testing.assert_frame_equal(df, original)
        assert list(equities.index) == [1, 4, 3]
        assert equities.loc[1, 'asset_name'] == 'Asset 1'
        assert equities.loc[3, 'auto_close_date'] == pd.Timestamp('2020-01-08')
        assert equities.loc[4, 'symbol'] == 'DDD'

    def test_no_repairs_returns_original_prices(self):
        sessions = pd.date_range('2020-01-01', periods=3)
        df = _make_df(sessions, sid=1, ticker='AAA',
                      closes=[10.0, 11.0, 12.0], volumes=[100, 200, 300])

        with patch('sharadar.loaders.ingest_sharadar.pd.concat', wraps=pd.concat) as concat:
            equities, result = create_equities_df(
                df, ['AAA'], sessions, _make_metadata([1]), show_progress=False,
            )

        assert concat.call_count == 0
        assert result is df
        assert equities.loc[1, 'symbol'] == 'AAA'
