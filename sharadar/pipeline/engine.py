import datetime
import time

import os
import pandas as pd
from memoization import cached
from sharadar.data.sql_lite_assets import SQLiteAssetFinder
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader
from sharadar.util.output_dir import SHARADAR_BUNDLE_NAME, SHARADAR_BUNDLE_DIR
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
from zipline.errors import NoFurtherDataError


class BundlePipelineEngine(SimplePipelineEngine):

    def run_pipeline(self, pipeline, start_date, end_date, chunksize=120):
        if chunksize < 0:
            log.info("Compute pipeline values without chunks.")
            return self._run_pipeline(pipeline, start_date, end_date)

        ranges = compute_date_range_chunks(
            self._calendar,
            start_date,
            end_date,
            chunksize,
        )

        start_ix, end_ix = self._calendar.slice_locs(start_date, end_date)
        log.info("Compute pipeline values in chunks of %d days." % (chunksize))

        chunks = []
        for s, e in ranges:
            log.info("Compute values for pipeline from %s to %s (period %d)." \
                     % (str(s.date()), str(e.date()), len(chunks)+1))
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

        try:
            return super().run_pipeline(pipeline, start_date, end_date)
        except NoFurtherDataError as e:
            new_start_date = self._calendar[self._extra_rows + 1]
            log.warning("Starting computing universe from %s instead of %s because of insufficient data." % (str(new_start_date.date()), str(start_date.date())))
            return self.run_pipeline(pipeline, new_start_date, end_date)


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

    def _compute_root_mask(self, start_date, end_date, extra_rows):
        """
        Compute a lifetimes matrix from our AssetFinder, then drop columns that
        didn't exist at all during the query dates.

        Parameters
        ----------
        start_date : pd.Timestamp
            Base start date for the matrix.
        end_date : pd.Timestamp
            End date for the matrix.
        extra_rows : int
            Number of extra rows to compute before `start_date`.
            Extra rows are needed by terms like moving averages that require a
            trailing window of data.

        Returns
        -------
        lifetimes : pd.DataFrame
            Frame of dtype `bool` containing dates from `extra_rows` days
            before `start_date`, continuing through to `end_date`.  The
            returned frame contains as columns all assets in our AssetFinder
            that existed for at least one day between `start_date` and
            `end_date`.
        """
        calendar = self._calendar
        finder = self._finder
        start_idx, end_idx = self._calendar.slice_locs(start_date, end_date)
        if start_idx < extra_rows:
            self._extra_rows = extra_rows
            raise NoFurtherDataError.from_lookback_window(
                initial_message="Insufficient data to compute Pipeline:",
                first_date=calendar[0],
                lookback_start=start_date,
                lookback_length=extra_rows,
            )

        # Build lifetimes matrix reaching back to `extra_rows` days before
        # `start_date.`
        lifetimes = finder.lifetimes(
            calendar[start_idx - extra_rows:end_idx],
            include_start_date=False,
            country_codes={'??', 'US'},
        )

        if lifetimes.index[extra_rows] != start_date:
            raise ValueError(
                ('The first date (%s) of the lifetimes matrix does not match the'
                ' start date of the pipeline. Did you forget to align the'
                ' start_date (%s) to the trading calendar?' % (str(lifetimes.index[extra_rows]), str(start_date)))
            )
        if lifetimes.index[-1] != end_date:
            raise ValueError(
                ('The last date (%s) of the lifetimes matrix does not match the'
                ' start date of the pipeline. Did you forget to align the'
                ' end_date (%s) to the trading calendar?' % (str(lifetimes.index[-1]), str(end_date)))
            )

        if not lifetimes.columns.unique:
            columns = lifetimes.columns
            duplicated = columns[columns.duplicated()].unique()
            raise AssertionError("Duplicated sids: %d" % duplicated)

        # Filter out columns that didn't exist from the farthest look back
        # window through the end of the requested dates.
        existed = lifetimes.any()
        ret = lifetimes.loc[:, existed]
        shape = ret.shape

        if shape[0] * shape[1] == 0:
            raise ValueError(
                "Found only empty asset-days between {} and {}.\n"
                "This probably means that either your asset db is out of date"
                " or that you're trying to run a Pipeline during a period with"
                " no market days.".format(start_date, end_date),
            )

        return ret



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


def load_sharadar_bundle(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return BundleData(
        asset_finder = SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ),),
        equity_minute_bar_reader = None,
        equity_daily_bar_reader = SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ),),
        adjustment_reader = SQLiteAdjustmentReader(adjustment_db_path(name, timestr, environ=environ),),
    )


@cached
def _asset_finder(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
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


def make_pipeline_engine(bundle=None, start=None, end=None):
    """Creates a pipeline engine for the dates in (start, end).
    Using this allows usage very similar to run_pipeline in Quantopian's env."""
    if bundle is None:
        bundle = load_sharadar_bundle()

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