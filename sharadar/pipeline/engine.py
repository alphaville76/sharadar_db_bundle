import datetime
import time

import os
import pandas as pd
from memoization import cached
from sharadar.data.sql_lite_assets import SQLiteAssetFinder
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader
from sharadar.loaders.ingest import SEP_BUNDLE_NAME, SEP_BUNDLE_DIR
from sharadar.util.logger import log
from six import iteritems
from toolz import juxt, groupby
from zipline.data.bundles.core import BundleData, asset_db_path, adjustment_db_path
from zipline.data.us_equity_pricing import SQLiteAdjustmentReader
from zipline.pipeline import SimplePipelineEngine, USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.term import LoadableTerm
from zipline.utils import paths as pth
from zipline.utils.date_utils import compute_date_range_chunks
from zipline.utils.pandas_utils import categorical_df_concat
from toolz.curried.operator import getitem

class BundlePipelineEngine(SimplePipelineEngine):

    def run_pipeline(self, pipeline, start_date, end_date, chunksize=21):
        ranges = compute_date_range_chunks(
            self._calendar,
            start_date,
            end_date,
            chunksize,
        )

        start_ix, end_ix = self._calendar.slice_locs(start_date, end_date)
        n_ranges = max(len(self._calendar[start_ix:end_ix])/chunksize, 1)
        log.info("Compute pipeline values in chunks of %d days. Number of chunks: %d." % (chunksize, n_ranges))

        chunks = []
        for s, e in ranges:
            log.info("Compute values for pipeline from %s to %s (period %d of %d)." \
                     % (str(s.date()), str(e.date()), len(chunks)+1, n_ranges))
            chunks.append(self._run_pipeline(pipeline, s, e))

        if len(chunks) == 1:
            # OPTIMIZATION: Don't make an extra copy in `categorical_df_concat`
            # if we don't have to.
            return chunks[0]

        return categorical_df_concat(chunks, inplace=True)

    def _run_pipeline(self, pipeline, start_date=None, end_date=None):
        if start_date is None:
            start_date = self._get_loader

        if end_date is None:
            end_date = pd.to_datetime('today', utc=True)

        for factor in pipeline.columns.values():
            self._set_asset_finder(factor)
        if pipeline.screen is not None:
            for factor in pipeline.screen.inputs:
                self._set_asset_finder(factor)

        return super().run_pipeline(pipeline, start_date, end_date)

    def _set_asset_finder(self, factor):
        if isinstance(factor, WithAssetFinder):
            factor.set_asset_finder(self._finder)
        for factor_input in factor.inputs:
            if isinstance(factor_input, WithAssetFinder):
                factor_input.set_asset_finder(self._finder)

    def run_chunked_pipeline(self, pipeline, start_date, end_date, chunksize):
        raise NotImplementedError("because run_pipeline is always chuncked.")

    def compute_chunk(self, graph, dates, assets, initial_workspace):
        """
        Identical to the method in the super class; overidden only for the additiona logging.
        """
        self._validate_compute_chunk_params(dates, assets, initial_workspace)
        get_loader = self.get_loader

        # Copy the supplied initial workspace so we don't mutate it in place.
        workspace = initial_workspace.copy()
        refcounts = graph.initial_refcounts(workspace)
        execution_order = graph.execution_order(refcounts)

        # If loadable terms share the same loader and extra_rows, load them all
        # together.
        loadable_terms = graph.loadable_terms
        loader_group_key = juxt(get_loader, getitem(graph.extra_rows))
        loader_groups = groupby(
            loader_group_key,
            # Only produce loader groups for the terms we expect to load.  This
            # ensures that we can run pipelines for graphs where we don't have
            # a loader registered for an atomic term if all the dependencies of
            # that term were supplied in the initial workspace.
            (t for t in execution_order if t in loadable_terms),
        )

        l = len(refcounts)
        i = 0
        for term in graph.execution_order(refcounts):
            start_time = time.time()
            i += 1
            log.info("Computing term %d of %d [%s]" % (i,l, str(term)))


            # `term` may have been supplied in `initial_workspace`, and in the
            # future we may pre-compute loadable terms coming from the same
            # dataset.  In either case, we will already have an entry for this
            # term, which we shouldn't re-compute.
            if term in workspace:
                log.info("Term already in workspace: no computation needed")
                continue

            # Asset labels are always the same, but date labels vary by how
            # many extra rows are needed.
            mask, mask_dates = graph.mask_and_dates_for_term(
                term,
                self._root_mask_term,
                workspace,
                dates,
            )

            if isinstance(term, LoadableTerm):
                to_load = sorted(
                    loader_groups[loader_group_key(term)],
                    key=lambda t: t.dataset
                )
                loader = get_loader(term)
                loaded = loader.load_adjusted_array(
                    to_load, mask_dates, assets, mask,
                )
                assert set(loaded) == set(to_load), (
                    'loader did not return an AdjustedArray for each column\n'
                    'expected: %r\n'
                    'got:      %r' % (sorted(to_load), sorted(loaded))
                )
                workspace.update(loaded)
            else:
                workspace[term] = term._compute(
                    self._inputs_for_term(term, workspace, graph),
                    mask_dates,
                    assets,
                    mask,
                )
                if term.ndim == 2:
                    assert workspace[term].shape == mask.shape
                else:
                    assert workspace[term].shape == (mask.shape[0], 1)

                # Decref dependencies of ``term``, and clear any terms whose
                # refcounts hit 0.
                for garbage_term in graph.decref_dependencies(term, refcounts):
                    del workspace[garbage_term]

            log.info("Elapsed time: %s" % datetime.timedelta(seconds=(time.time() - start_time)))

        out = {}
        graph_extra_rows = graph.extra_rows
        for name, term in iteritems(graph.outputs):
            # Truncate off extra rows from outputs.
            out[name] = workspace[term][graph_extra_rows[term]:]
        return out


class WithAssetFinder:
    _asset_finder = None

    def get_asset_finder(self):
        if self._asset_finder is None:
            self._asset_finder = _asset_finder()

        return self._asset_finder

    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder


def daily_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        (bundle_name, timestr, 'prices.sqlite'),
        environ=environ,
    )


def load_sep_bundle(name=SEP_BUNDLE_NAME, timestr=SEP_BUNDLE_DIR, environ=os.environ):
    return BundleData(
        asset_finder = SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ),),
        equity_minute_bar_reader = None,
        equity_daily_bar_reader = SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ),),
        adjustment_reader = SQLiteAdjustmentReader(adjustment_db_path(name, timestr, environ=environ),),
    )


@cached
def _asset_finder(name=SEP_BUNDLE_NAME, timestr=SEP_BUNDLE_DIR, environ=os.environ):
    return SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ))


@cached
def symbol(ticker, as_of_date=None):
    return _asset_finder().lookup_symbol(ticker, as_of_date)


@cached
def symbols(ticker, as_of_date=None):
    return _asset_finder().lookup_symbols(ticker, as_of_date)


@cached
def sid(sid):
    return sids((sid,))[0]


@cached
def sids(sids):
    return _asset_finder().retrieve_all(sids)


def make_pipeline_engine(bundle=load_sep_bundle(), start=None, end=None):
    """Creates a pipeline engine for the dates in (start, end).
    Using this allows usage very similar to run_pipeline in Quantopian's env."""
    if start is None:
        start = bundle.equity_daily_bar_reader.first_trading_day

    if end is None:
        end = pd.to_datetime('today', utc=True)

    pipeline_loader = USEquityPricingLoader(bundle.equity_daily_bar_reader, bundle.adjustment_reader)


    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        raise ValueError("No PipelineLoader registered for column %s." % column)

    # set up pipeline
    cal = bundle.equity_daily_bar_reader.trading_calendar.all_sessions
    cal2 = cal[(cal >= start) & (cal <= end)]

    spe = BundlePipelineEngine(get_loader=choose_loader,
                               calendar=cal2,
                               asset_finder=bundle.asset_finder)
    return spe