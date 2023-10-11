import datetime
import hashlib
import os
from os.path import exists

import click
import numpy as np
import pandas as pd
from sharadar.data.sql_lite_assets import SQLiteAssetFinder
from sharadar.data.sql_lite_daily_pricing import SQLiteDailyBarReader
from sharadar.util.logger import log
from sharadar.util.output_dir import SHARADAR_BUNDLE_NAME, SHARADAR_BUNDLE_DIR, get_cache_dir
from toolz import groupby
from zipline.data.adjustments import SQLiteAdjustmentReader
from zipline.data.bundles.core import BundleData, asset_db_path, adjustment_db_path
from zipline.data.data_portal import DataPortal
from zipline.lib.adjusted_array import AdjustedArray
from zipline.lib.labelarray import LabelArray
from zipline.pipeline import SimplePipelineEngine, CustomClassifier, TermGraph
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.domain import US_EQUITIES
from zipline.pipeline.hooks.progress import ProgressHooks
from zipline.pipeline.loaders.equity_pricing_loader import USEquityPricingLoader
from zipline.pipeline.term import LoadableTerm, Term
from zipline.utils import paths as pth
from zipline.utils.date_utils import compute_date_range_chunks
from functools import partial

class BundlePipelineEngine(SimplePipelineEngine):
    def __init__(self, get_loader, asset_finder, default_domain=US_EQUITIES, populate_initial_workspace=None,
                 default_hooks=None):
        super().__init__(get_loader, asset_finder, default_domain, populate_initial_workspace, default_hooks)

    def _compute_root_mask(self, domain, start_date, end_date, extra_rows):

        root_mask_filename = "root-%s_%s_%s_%s_%d.pkl" % (
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            domain.calendar_name,
            domain.country_code,
            extra_rows
        )

        root_mask_filepath = get_cache_dir() + '/' + root_mask_filename
        if exists(root_mask_filepath):
            log.info("Load root mask file: " + root_mask_filename)
            root_mask = pd.read_pickle(root_mask_filepath)
        else:
            root_mask = super()._compute_root_mask(domain, start_date, end_date, extra_rows)
            log.info("Save root mask file: " + root_mask_filename)
            root_mask.to_pickle(get_cache_dir() + '/' + root_mask_filename)

        return root_mask

    def run_pipeline(self, pipeline, start_date, end_date=None, chunksize=120, hooks=None):
        if end_date is None:
            end_date = start_date

        if hooks is None:
            hooks = [ProgressHooks.with_static_publisher(CliProgressPublisher())]

        if chunksize <= 1:
            log.info("Compute pipeline values without chunks.")
            return super().run_pipeline(pipeline, start_date, end_date, hooks)

        return self.run_chunked_pipeline(pipeline, start_date, end_date, chunksize, hooks)

    def run_chunked_pipeline(
            self, pipeline, start_date, end_date, chunksize, hooks=None
    ):
        """Compute values for ``pipeline`` from ``start_date`` to ``end_date``, in
        date chunks of size ``chunksize``.

        Chunked execution reduces memory consumption, and may reduce
        computation time depending on the contents of your pipeline.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to run.
        start_date : pd.Timestamp
            The start date to run the pipeline for.
        end_date : pd.Timestamp
            The end date to run the pipeline for.
        chunksize : int
            The number of days to execute at a time.
        hooks : list[implements(PipelineHooks)], optional
            Hooks for instrumenting Pipeline execution.

        Returns
        -------
        result : pd.DataFrame
            A frame of computed results.

            The ``result`` columns correspond to the entries of
            `pipeline.columns`, which should be a dictionary mapping strings to
            instances of :class:`zipline.pipeline.Term`.

            For each date between ``start_date`` and ``end_date``, ``result``
            will contain a row for each asset that passed `pipeline.screen`.
            A screen of ``None`` indicates that a row should be returned for
            each asset that existed each day.

        See Also
        --------
        :meth:`zipline.pipeline.engine.PipelineEngine.run_pipeline`
        """
        domain = self.resolve_domain(pipeline)
        ranges = compute_date_range_chunks(
            domain.sessions(),
            start_date,
            end_date,
            chunksize,
        )
        hooks = self._resolve_hooks(hooks)

        run_pipeline = partial(self._run_pipeline_impl, pipeline, hooks=hooks)
        with hooks.running_pipeline(pipeline, start_date, end_date):
            chunks = [run_pipeline(s, e) for s, e in ranges]

        if len(chunks) == 1:
            # OPTIMIZATION: Don't make an extra copy in `categorical_df_concat`
            # if we don't have to.
            return chunks[0]

        # Filter out empty chunks. Empty dataframes lose dtype information,
        # which makes concatenation fail.
        df_list = [c for c in chunks if len(c)]

        # Assert each dataframe has the same columns/dtypes
        df = df_list[0]
        if not all([(df.dtypes.equals(df_i.dtypes)) for df_i in df_list[1:]]):
            raise ValueError("Input DataFrames must have the same columns/dtypes.")

        categorical_columns = df.columns[df.dtypes == "category"]

        for col in categorical_columns:
            new_categories = _sort_set_none_first(
                _union_all(frame[col].cat.categories for frame in df_list)
            )

            for df in df_list:
                # https://stackoverflow.com/questions/70344193/pandas-dataframe-set-categories-the-inplace-parameter-in-pandas-categorical
                df[col] = df[col].cat.set_categories(new_categories)

        return pd.concat(df_list)

    def compute_chunk(
            self, graph, dates, sids, workspace, refcounts, execution_order, hooks
    ):
        """
        Compute the Pipeline terms in the graph for the requested start and end
        dates.

        This is where we do the actual work of running a pipeline.

        Parameters
        ----------
        graph : zipline.pipeline.graph.ExecutionPlan
            Dependency graph of the terms to be executed.
        dates : pd.DatetimeIndex
            Row labels for our root mask.
        sids : pd.Int64Index
            Column labels for our root mask.
        workspace : dict
            Map from term -> output.
            Must contain at least entry for `self._root_mask_term` whose shape
            is `(len(dates), len(assets))`, but may contain additional
            pre-computed terms for testing or optimization purposes.
        refcounts : dict[Term, int]
            Dictionary mapping terms to number of dependent terms. When a
            term's refcount hits 0, it can be safely discarded from
            ``workspace``. See TermGraph.decref_dependencies for more info.
        execution_order : list[Term]
            Order in which to execute terms.
        hooks : implements(PipelineHooks)
            Hooks to instrument pipeline execution.

        Returns
        -------
        results : dict
            Dictionary mapping requested results to outputs.
        """
        self._validate_compute_chunk_params(graph, dates, sids, workspace)

        cached_out = {}
        for name, term in graph.outputs.items():
            # Check if a term is in the cache (addition to super class)
            term_filename = create_term_filename(dates, graph, term)
            term_filepath = get_cache_dir() + '/' + term_filename
            if exists(term_filepath):
                term_data = np.load(term_filepath, allow_pickle=True, fix_imports=True)
                log.info("load " + term_filename + " from cache")
                if isinstance(term, CustomClassifier):
                    label_array = LabelArray(term_data, term.missing_value, categories=term.categories)
                    cached_out[name] = label_array
                    workspace[term] = label_array
                elif isinstance(term, AdjustedArray):
                    adjusted_array = AdjustedArray(term_data, term.adjustments, term.missing_value)
                    cached_out[name] = adjusted_array
                    workspace[term] = adjusted_array
                else:
                    cached_out[name] = term_data
                    workspace[term] = term_data

        if len(cached_out) == len(graph.outputs):
            return cached_out

        get_loader = self._get_loader

        # Copy the supplied initial workspace so we don't mutate it in place.
        workspace = workspace.copy()
        domain = graph.domain

        # Many loaders can fetch data more efficiently if we ask them to
        # retrieve all their inputs at once. For example, a loader backed by a
        # SQL database can fetch multiple columns from the database in a single
        # query.
        #
        # To enable these loaders to fetch their data efficiently, we group
        # together requests for LoadableTerms if they are provided by the same
        # loader and they require the same number of extra rows.
        #
        # The extra rows condition is a simplification: we don't currently have
        # a mechanism for asking a loader to fetch different windows of data
        # for different terms, so we only batch requests together when they're
        # going to produce data for the same set of dates.
        def loader_group_key(term):
            loader = get_loader(term)
            extra_rows = graph.extra_rows[term]
            return loader, extra_rows

        # Only produce loader groups for the terms we expect to load.  This
        # ensures that we can run pipelines for graphs where we don't have a
        # loader registered for an atomic term if all the dependencies of that
        # term were supplied in the initial workspace.
        will_be_loaded = graph.loadable_terms - workspace.keys()
        loader_groups = groupby(
            loader_group_key,
            (t for t in execution_order if t in will_be_loaded),
        )

        for term in execution_order:
            # `term` may have been supplied in `initial_workspace`, or we may
            # have loaded `term` as part of a batch with another term coming
            # from the same loader (see note on loader_group_key above). In
            # either case, we already have the term computed, so don't
            # recompute.

            if term in workspace:
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
                loader = get_loader(term)
                to_load = sorted(
                    loader_groups[loader_group_key(term)], key=lambda t: t.dataset
                )
                self._ensure_can_load(loader, to_load)
                with hooks.loading_terms(to_load):
                    loaded = loader.load_adjusted_array(
                        domain,
                        to_load,
                        mask_dates,
                        sids,
                        mask,
                    )
                assert set(loaded) == set(to_load), (
                        "loader did not return an AdjustedArray for each column\n"
                        "expected: %r\n"
                        "got:      %r"
                        % (
                            sorted(to_load, key=repr),
                            sorted(loaded, key=repr),
                        )
                )
                workspace.update(loaded)
            else:
                with hooks.computing_term(term):
                    workspace[term] = term._compute(
                        self._inputs_for_term(
                            term,
                            workspace,
                            graph,
                            domain,
                            refcounts,
                        ),
                        mask_dates,
                        sids,
                        mask,
                    )
                mask_shape = mask.shape if term.ndim == 2 else (mask.shape[0], 1)
                if workspace[term].shape != mask_shape:
                    raise ValueError("The shape %s of term '%s' does not match with the shape (%s) of the mask" %
                                     (str(workspace[term].shape), str(term), str(mask_shape)))

                # Decref dependencies of ``term``, and clear any terms
                # whose refcounts hit 0.
                for garbage in graph.decref_dependencies(term, refcounts):
                    del workspace[garbage]

        # At this point, all the output terms are in the workspace.
        out = {}
        graph_extra_rows = graph.extra_rows
        for name, term in graph.outputs.items():
            # Truncate off extra rows from outputs.
            term_values = workspace[term][graph_extra_rows[term]:]
            out[name] = term_values

            # Save all terms to cache (addition to super class)
            term_filename = create_term_filename(dates, graph, term)
            term_filepath = get_cache_dir() + '/' + term_filename
            if not exists(term_filepath):
                log.info("save " + term_filename + " to cache")
                if isinstance(term_values, LabelArray):
                    np.save(term_filepath, term_values.as_string_array(), allow_pickle=True, fix_imports=True)
                elif isinstance(term_values, AdjustedArray):
                    np.save(term_filepath, term_values.data, allow_pickle=True, fix_imports=True)
                elif type(term_values) == np.ndarray:
                    np.save(term_filepath, term_values, allow_pickle=True, fix_imports=True)
                else:
                    log.warn("Cannot save unknown type: %s" % str(type(term_values)))

        return out


def create_term_filename(dates, graph, term):
    return "term-%s_%s_%s_%s.npy" % (
        dates[0].strftime("%Y-%m-%d"),
        dates[-1].strftime("%Y-%m-%d"),
        graph.screen_name,
        find_term_name(graph, term)
    )


def find_term_name(graph: TermGraph, term: Term):
    for term_name, term_value in graph.outputs.items():
        if term_value == term:
            return term_name
    raise ValueError("Term not in graph")


class BundleLoader:
    _asset_finder = None
    _bar_reader = None

    def asset_finder(self):
        if self._asset_finder is None:
            self._asset_finder = _asset_finder()

        return self._asset_finder

    def bar_reader(self):
        if self._bar_reader is None:
            self._bar_reader = _bar_reader()

        return self._bar_reader


def daily_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        (bundle_name, timestr, 'prices.sqlite'),
        environ=environ,
    )


# @cached
def load_sharadar_bundle(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return BundleData(
        asset_finder=SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ), ),
        equity_minute_bar_reader=None,
        equity_daily_bar_reader=SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ), ),
        adjustment_reader=SQLiteAdjustmentReader(adjustment_db_path(name, timestr, environ=environ), ),
    )


# @cached
def _asset_finder(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return SQLiteAssetFinder(asset_db_path(name, timestr, environ=environ))


# @cached
def _bar_reader(name=SHARADAR_BUNDLE_NAME, timestr=SHARADAR_BUNDLE_DIR, environ=os.environ):
    return SQLiteDailyBarReader(daily_equity_path(name, timestr, environ=environ))


# @cached
def symbol(ticker, as_of_date=None):
    return _asset_finder().lookup_symbol(ticker, as_of_date)


# @cached
def symbols(tickers, as_of_date=None):
    return _asset_finder().lookup_symbols(tickers, as_of_date)


# @cached
def sector(ticker, as_of_date=None):
    return _asset_finder().get_info(symbol(ticker).sid, 'sector')


# @cached
def sectors(tickers, as_of_date=None):
    sids = [x.sid for x in symbols(tickers)]
    return _asset_finder().get_info(sids, 'sector')


# @cached
def sid(sid):
    return sids((sid,))[0]


# @cached
def sids(sids):
    return _asset_finder().retrieve_all(sids)


def make_pipeline_engine(bundle=None, start=None, end=None, live=False):
    """Creates a pipeline engine for the dates in (start, end).
    Using this allows usage very similar to run_pipeline in Quantopian's env."""
    if bundle is None:
        bundle = load_sharadar_bundle()

    if start is None:
        start = bundle.equity_daily_bar_reader.first_trading_day

    if end is None:
        end = pd.Timestamp.today()

    # pipeline_loader = USEquityPricingLoader(bundle.equity_daily_bar_reader, bundle.adjustment_reader, SimpleFXRateReader())
    pipeline_loader = USEquityPricingLoader.without_fx(bundle.equity_daily_bar_reader, bundle.adjustment_reader)

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        raise ValueError("No PipelineLoader registered for column %s." % column)

    bundle.asset_finder.is_live_trading = live
    spe = BundlePipelineEngine(get_loader=choose_loader, asset_finder=bundle.asset_finder)
    return spe


def trading_date(date):
    """
    Given a date, return the same date if a trading session or the next valid one
    """
    if isinstance(date, str):
        date = pd.Timestamp(date)
    if date.tz is not None:
        date = date.tz_localize(None)
    date = date.normalize()
    cal = _bar_reader().trading_calendar
    if not cal.is_session(date):
        date = cal.next_close(date)
        date = date.tz_localize(None).normalize()
    return date


def to_sids(assets):
    if hasattr(assets, '__iter__'):
        return [x.sid for x in assets]
    return [assets.sid]


def prices(assets, start, end, field='close', offset=0):
    """
    Get price data for assets between start and end.
    """
    start = trading_date(start)
    end = trading_date(end)

    bundle = load_sharadar_bundle()
    trading_calendar = bundle.equity_daily_bar_reader.trading_calendar

    if offset > 0:
        start = trading_calendar.sessions_window(start, -offset)[0]

    bar_count = trading_calendar.sessions_distance(start, end)

    data_portal = DataPortal(bundle.asset_finder,
                             trading_calendar=trading_calendar,
                             first_trading_day=start,
                             equity_daily_reader=bundle.equity_daily_bar_reader,
                             adjustment_reader=bundle.adjustment_reader)

    df = data_portal.get_history_window(assets=assets, end_dt=end, bar_count=bar_count,
                                        frequency='1d',
                                        field=field,
                                        data_frequency='daily')

    return df if len(assets) > 1 else df.squeeze()


def history(assets, as_of_date, n, field='close'):
    as_of_date = trading_date(as_of_date)
    trading_calendar = load_sharadar_bundle().equity_daily_bar_reader.trading_calendar
    sessions = trading_calendar.sessions_window(as_of_date, -n + 1)
    return prices(assets, sessions[0], sessions[-1], field)


def returns(assets, start, end, periods=1, field='close'):
    """
    Fetch returns for one or more assets in a date range.
    """
    df = prices(assets, start, end, field, periods)
    df = df.sort_index().pct_change(1).iloc[1:]
    return df


class LogProgressPublisher(object):

    def publish(self, model):
        try:
            start = str(model.current_chunk_bounds[0].date())
            end = str(model.current_chunk_bounds[1].date())
            completed = model.percent_complete
            work = model.current_work
            if start == end:
                log.info("Percent completed: %3.0f%% (%s): %s" % (completed, start, work))
            else:
                log.info("Percent completed: %3.0f%% (%s - %s): %s" % (completed, start, end, work))
        except:
            log.error("Cannot publish progress state.")


class CliProgressPublisher(object):

    def __init__(self):
        self.pbar = click.progressbar(length=100, label="Analyzing Pipeline...")

    def publish(self, model):
        start = str(model.current_chunk_bounds[0].date())
        end = str(model.current_chunk_bounds[1].date())
        completed = model.percent_complete

        self.pbar.update(completed)
        self.pbar.label = "Pipeline from %s to %s" % (start, end)
        if model.state == "success":
            log.info("Pipeline from %s to %s completed in %s." % (
                start, end, str(datetime.timedelta(seconds=int(model.execution_time)))))


def _union_all(iterables):
    """Union entries in ``iterables`` into a set."""
    return set().union(*iterables)


def _sort_set_none_first(set_):
    """Sort a set, sorting ``None`` before other elements, if present."""
    if None in set_:
        set_.remove(None)
        out = [None]
        out.extend(sorted(set_))
        set_.add(None)
        return out
    else:
        return sorted(set_)
