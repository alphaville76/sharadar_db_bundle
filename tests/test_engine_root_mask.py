import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
from zipline.pipeline import SimplePipelineEngine

from sharadar.pipeline import engine as engine_module
from sharadar.pipeline.engine import BundlePipelineEngine

DOMAIN = SimpleNamespace(calendar_name='XNYS', country_code='US')
DATES = pd.DatetimeIndex(['2024-03-13', '2024-03-14', '2024-03-15'])
FILENAME = 'root-2024-03-14_2024-03-15_XNYS_US_1.pkl'


def _engine():
    return BundlePipelineEngine.__new__(BundlePipelineEngine)


def _mask(sids):
    return pd.DataFrame(np.ones((len(DATES), len(sids)), dtype=bool), index=DATES, columns=sids)


def test_unsorted_cached_root_mask_is_recomputed_with_its_terms(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_module, 'get_cache_dir', lambda: str(tmp_path))
    _mask([118691, 101361, 196191]).to_pickle(tmp_path / FILENAME)
    stale_term = tmp_path / 'term-2024-03-13_2024-03-15_screen_beta.npy'
    other_term = tmp_path / 'term-2024-03-12_2024-03-15_screen_beta.npy'
    stale_term.touch()
    other_term.touch()
    monkeypatch.setattr(SimplePipelineEngine, '_compute_root_mask',
                        lambda self, *args: _mask([101361, 118691, 196191]))

    root_mask = _engine()._compute_root_mask(DOMAIN, DATES[1], DATES[-1], 1)

    assert list(root_mask.columns) == [101361, 118691, 196191]
    assert list(pd.read_pickle(tmp_path / FILENAME).columns) == [101361, 118691, 196191]
    assert not stale_term.exists()
    assert other_term.exists()


def test_sorted_cached_root_mask_is_reused(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_module, 'get_cache_dir', lambda: str(tmp_path))
    _mask([101361, 118691]).to_pickle(tmp_path / FILENAME)
    term = tmp_path / 'term-2024-03-13_2024-03-15_screen_beta.npy'
    term.touch()

    def fail(*args):
        raise AssertionError('root mask must be loaded from cache')

    monkeypatch.setattr(SimplePipelineEngine, '_compute_root_mask', fail)

    root_mask = _engine()._compute_root_mask(DOMAIN, DATES[1], DATES[-1], 1)

    assert list(root_mask.columns) == [101361, 118691]
    assert os.path.exists(term)
