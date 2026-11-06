"""Offline integration checks for the pinned scientific and storage stack."""
import io

import numpy as np
import pandas as pd
import pytest


@pytest.mark.parametrize('values', [
    np.array([[0.1, np.nan], [np.inf, -0.2]], dtype='float64'),
    np.array([[1, 2], [3, 4]], dtype='int64'),
    np.array([[True, False]], dtype='bool'),
    np.array([['Technology', None], ['Energy', 'Finance']], dtype=object),
    np.empty((0, 2), dtype='float64'),
])
def test_pipeline_term_cache_roundtrip(tmp_path, values):
    from sharadar.pipeline.engine import _atomic_np_save

    path = tmp_path / 'term.npy'
    _atomic_np_save(path, np.array([-1]))
    _atomic_np_save(path, values)

    restored = np.load(path, allow_pickle=True, fix_imports=True)
    np.testing.assert_array_equal(restored, values)
    assert restored.dtype == values.dtype
    assert list(tmp_path.iterdir()) == [path]


def test_storage_roundtrips(tmp_path):
    import bcolz
    import blosc2
    import h5py

    values = np.arange(60, dtype='float64').reshape(20, 3)
    frame = pd.DataFrame(values, columns=['open', 'close', 'volume'])
    hdf_path = tmp_path / 'prices.h5'
    frame.to_hdf(hdf_path, key='prices')
    pd.testing.assert_frame_equal(pd.read_hdf(hdf_path, key='prices'), frame)

    with h5py.File(tmp_path / 'arrays.h5', 'w') as store:
        store['prices'] = values
    with h5py.File(tmp_path / 'arrays.h5', 'r') as store:
        np.testing.assert_array_equal(store['prices'][:], values)

    array_path = str(tmp_path / 'bcolz')
    array = bcolz.carray(values, rootdir=array_path, mode='w')
    array.flush()
    np.testing.assert_array_equal(bcolz.open(array_path)[:], values)
    np.testing.assert_array_equal(blosc2.unpack_array(blosc2.pack_array(values)), values)


def test_pyfolio_returns_report():
    from matplotlib.figure import Figure
    import matplotlib.pyplot as plt
    import pyfolio as pf

    index = pd.bdate_range('2020-01-01', periods=504, tz='UTC')
    rng = np.random.default_rng(42)
    returns = pd.Series(rng.normal(0.0005, 0.01, len(index)), index=index)
    benchmark = pd.Series(rng.normal(0.0003, 0.01, len(index)), index=index)

    with plt.ioff():
        figure = pf.create_returns_tear_sheet(
            returns, benchmark_rets=benchmark, return_fig=True,
        )
    try:
        assert isinstance(figure, Figure)
        assert figure.axes
        image = io.BytesIO()
        figure.savefig(image, format='png')
        assert image.getvalue().startswith(b'\x89PNG')
    finally:
        plt.close(figure)


def test_zipline_daily_backtest(tmp_path, monkeypatch):
    from zipline import run_algorithm
    from zipline.api import order_target, symbol
    from zipline.data.bundles import ingest, register, unregister
    from zipline.utils.calendar_utils import get_calendar

    monkeypatch.setenv('ZIPLINE_ROOT', str(tmp_path))
    calendar = get_calendar('XNYS')
    sessions = calendar.sessions_in_range('2023-01-03', '2023-03-31')
    prices = np.linspace(100.0, 110.0, len(sessions))
    bars = pd.DataFrame({
        'open': prices,
        'high': prices + 1,
        'low': prices - 1,
        'close': prices,
        'volume': np.full(len(sessions), 100000),
    }, index=sessions)
    equities = pd.DataFrame({
        'symbol': ['TEST'],
        'asset_name': ['Test equity'],
        'start_date': [sessions[0]],
        'end_date': [sessions[-1]],
        'exchange': ['XNYS'],
    }, index=[1])

    def ingest_test_bundle(environ, asset_db_writer, minute_bar_writer,
                           daily_bar_writer, adjustment_writer, calendar,
                           start_session, end_session, cache, show_progress,
                           output_dir):
        asset_db_writer.write(equities=equities)
        daily_bar_writer.write([(1, bars)])
        adjustment_writer.write()

    def initialize(context):
        context.asset = symbol('TEST')

    def handle_data(context, data):
        order_target(context.asset, 10)

    bundle_name = 'dependency-compatibility-test'
    register(
        bundle_name, ingest_test_bundle, calendar_name='XNYS',
        start_session=sessions[0], end_session=sessions[-1],
    )
    try:
        ingest(bundle_name)
        result = run_algorithm(
            start=sessions[1], end=sessions[-1], initialize=initialize,
            handle_data=handle_data, capital_base=10000, bundle=bundle_name,
            trading_calendar=calendar, default_extension=False,
            benchmark_returns=pd.Series(0.0, index=sessions),
        )
    finally:
        unregister(bundle_name)

    assert len(result) == len(sessions) - 1
    assert np.isfinite(result['portfolio_value']).all()
    assert result.iloc[-1]['positions'][0]['amount'] == 10
    assert result.iloc[-1]['portfolio_value'] > 10000
