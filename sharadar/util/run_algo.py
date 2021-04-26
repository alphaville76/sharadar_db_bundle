"""
Created on the basis of ~/zipline/lib/python3.6/site-packages/zipline-trader/zipline/utils/run_algo.py
"""
import sys

import click
import pandas as pd
import warnings
from functools import partial
from sharadar.pipeline.engine import *
from sharadar.util.logger import log

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter
    PYGMENTS = True
except ImportError:
    PYGMENTS = False
import six
from toolz import concatv
from trading_calendars import get_calendar

from zipline.data.data_portal import DataPortal
from sharadar.live.data_portal_live import DataPortalLive
from zipline.finance import metrics
from zipline.finance.trading import SimulationParameters

import zipline.utils.paths as pth
from zipline.extensions import load
from zipline.algorithm import TradingAlgorithm
from sharadar.live.algorithm_live import LiveTradingAlgorithm
from zipline.finance.blotter import Blotter
from sharadar.util.serialization_utils import store_context
from trading_calendars import register_calendar_alias
from trading_calendars.errors import CalendarNameCollision
from sharadar.pipeline.engine import returns

class _RunAlgoError(click.ClickException, ValueError):
    """Signal an error that should have a different message if invoked from
    the cli.

    Parameters
    ----------
    pyfunc_msg : str
        The message that will be shown when called as a python function.
    cmdline_msg : str, optional
        The message that will be shown on the command line. If not provided,
        this will be the same as ``pyfunc_msg`
    """
    exit_code = 1

    def __init__(self, pyfunc_msg, cmdline_msg=None):
        if cmdline_msg is None:
            cmdline_msg = pyfunc_msg

        super(_RunAlgoError, self).__init__(cmdline_msg)
        self.pyfunc_msg = pyfunc_msg

    def __str__(self):
        return self.pyfunc_msg


def _run(handle_data,
         initialize,
         before_trading_start,
         analyze,
         algofile,
         algotext,
         defines,
         data_frequency,
         capital_base,
         bundle,
         bundle_timestamp,
         start,
         end,
         output,
         trading_calendar,
         print_algo,
         metrics_set,
         local_namespace,
         environ,
         blotter,
         benchmark_symbol,
         broker,
         state_filename):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.

    additions useful for live trading:
    broker - wrapper to connect to a real broker
    state_filename - saving the context of the algo to be able to restart
    """
    log.info("Using bundle '%s'." % bundle)

    if trading_calendar is None:
        trading_calendar = get_calendar('XNYS')

    bundle_data = load_sharadar_bundle(bundle)
    now = pd.Timestamp.utcnow()
    if start is None:
        start = bundle_data.equity_daily_bar_reader.first_trading_day if not broker else now

    if not trading_calendar.is_session(start.date()):
        start = trading_calendar.next_open(start)

    if end is None:
        end = bundle_data.equity_daily_bar_reader.last_available_dt if not broker else start

    # date parameter validation
    if trading_calendar.session_distance(start, end) < 0:
        raise _RunAlgoError(
            'There are no trading days between %s and %s' % (
                start.date(),
                end.date(),
            ),
        )

    if broker:
        log.info("Live Trading on %s." % start.date())
        # No benchmark needed for live trading
        benchmark_symbol=None
    else:
        log.info("Backtest from %s to %s." % (start.date(), end.date()))

    if benchmark_symbol:
        benchmark = symbol(benchmark_symbol)
        benchmark_sid = benchmark.sid
        benchmark_returns = returns([benchmark], start, end)
    else:
        benchmark_sid = None
        benchmark_returns = pd.Series(index=pd.date_range(start, end, tz='utc'),data=0.0)


    # emission_rate is a string representing the smallest frequency at which metrics should be reported.
    # emission_rate will be either minute or daily. When emission_rate is daily, end_of_bar will not be called at all.
    emission_rate = 'daily'

    if algotext is not None:
        if local_namespace:
            # noinspection PyUnresolvedReferences
            ip = get_ipython()  # noqa
            namespace = ip.user_ns
        else:
            namespace = {}

        for assign in defines:
            try:
                name, value = assign.split('=', 2)
            except ValueError:
                raise ValueError(
                    'invalid define %r, should be of the form name=value' %
                    assign,
                )
            try:
                # evaluate in the same namespace so names may refer to
                # eachother
                namespace[name] = eval(value, namespace)
            except Exception as e:
                raise ValueError(
                    'failed to execute definition for name %r: %s' % (name, e),
                )
    elif defines:
        raise _RunAlgoError(
            'cannot pass define without `algotext`',
            "cannot pass '-D' / '--define' without '-t' / '--algotext'",
        )
    else:
        namespace = {}
        if algofile is not None:
            algotext = algofile.read()

    if print_algo:
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            click.echo(algotext)

    first_trading_day = \
        bundle_data.equity_daily_bar_reader.first_trading_day

    if isinstance(metrics_set, six.string_types):
        try:
            metrics_set = metrics.load(metrics_set)
        except ValueError as e:
            raise _RunAlgoError(str(e))

    if isinstance(blotter, six.string_types):
        try:
            blotter = load(Blotter, blotter)
        except ValueError as e:
            raise _RunAlgoError(str(e))

    # Special defaults for live trading
    if broker:
        data_frequency = 'minute'

        # No benchmark
        benchmark_sid = None
        benchmark_returns = pd.Series(index=pd.date_range(start, end, tz='utc'), data=0.0)

        broker.daily_bar_reader = bundle_data.equity_daily_bar_reader

        if start.date() < now.date():
            backtest_start = start
            backtest_end = bundle_data.equity_daily_bar_reader.last_available_dt

            if not os.path.exists(state_filename):
                log.info("Backtest from %s to %s." % (backtest_start.date(), backtest_end.date()))
                backtest_data = DataPortal(
                    bundle_data.asset_finder,
                    trading_calendar=trading_calendar,
                    first_trading_day=first_trading_day,
                    equity_minute_reader=bundle_data.equity_minute_bar_reader,
                    equity_daily_reader=bundle_data.equity_daily_bar_reader,
                    adjustment_reader=bundle_data.adjustment_reader,
                )
                backtest = create_algo_class(TradingAlgorithm, backtest_start, backtest_end, algofile, algotext, analyze, before_trading_start,
                          benchmark_returns, benchmark_sid, blotter, bundle_data, capital_base, backtest_data, 'daily',
                          emission_rate, handle_data, initialize, metrics_set, namespace, trading_calendar)

                ctx_blacklist = ['trading_client']
                ctx_whitelist = ['perf_tracker']
                ctx_excludes = ctx_blacklist + [e for e in backtest.__dict__.keys() if e not in ctx_whitelist]
                backtest.run()
                checksum = getattr(algofile, 'name', '<algorithm>')
                store_context(state_filename, context=backtest, checksum=checksum, exclude_list=ctx_excludes)
            else:
                log.warn("State file already exists. Do not run the backtest.")

            # Set start and end to now for live trading
            start = pd.Timestamp.utcnow()
            if not trading_calendar.is_session(start.date()):
                start = trading_calendar.next_open(start)
            end = start


    # usare store_context prima di passare da TradingAlgorithm a LiveTradingAlgorithm
    TradingAlgorithmClass = (partial(LiveTradingAlgorithm,
                                     broker=broker,
                                     state_filename=state_filename)
                             if broker else TradingAlgorithm)

    DataPortalClass = (partial(DataPortalLive, broker) if broker else DataPortal)
    data = DataPortalClass(
        bundle_data.asset_finder,
        trading_calendar=trading_calendar,
        first_trading_day=first_trading_day,
        equity_minute_reader=bundle_data.equity_minute_bar_reader,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader,
    )
    algo = create_algo_class(TradingAlgorithmClass, start, end, algofile, algotext, analyze, before_trading_start,
                      benchmark_returns, benchmark_sid, blotter, bundle_data, capital_base, data, data_frequency,
                      emission_rate, handle_data, initialize, metrics_set, namespace, trading_calendar)

    perf = algo.run()

    if output == '-':
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf


def create_algo_class(TradingAlgorithmClass, start, end, algofile, algotext, analyze, before_trading_start,
                      benchmark_returns, benchmark_sid, blotter, bundle_data, capital_base, data, data_frequency,
                      emission_rate, handle_data, initialize, metrics_set, namespace, trading_calendar):
    algo = TradingAlgorithmClass(
        namespace=namespace,
        data_portal=data,
        get_pipeline_loader=None,
        trading_calendar=trading_calendar,
        sim_params=SimulationParameters(
            start_session=start,
            end_session=end,
            trading_calendar=trading_calendar,
            capital_base=capital_base,
            emission_rate=emission_rate,
            data_frequency=data_frequency
        ),
        metrics_set=metrics_set,
        blotter=blotter,
        benchmark_returns=benchmark_returns,
        benchmark_sid=benchmark_sid,
        **{
            'initialize': initialize,
            'handle_data': handle_data,
            'before_trading_start': before_trading_start,
            'analyze': analyze,
        } if algotext is None else {
            'algo_filename': getattr(algofile, 'name', '<algorithm>'),
            'script': algotext,
        }
    )
    algo.engine = make_pipeline_engine(bundle_data, live=isinstance(algo, LiveTradingAlgorithm))
    return algo


# All of the loaded extensions. We don't want to load an extension twice.
_loaded_extensions = set()

def load_extensions(default, extensions, strict, environ, reload=False):
    """Load all of the given extensions. This should be called by run_algo
    or the cli.

    Parameters
    ----------
    default : bool
        Load the default exension (~/.zipline/extension.py)?
    extension : iterable[str]
        The paths to the extensions to load. If the path ends in ``.py`` it is
        treated as a script and executed. If it does not end in ``.py`` it is
        treated as a module to be imported.
    strict : bool
        Should failure to load an extension raise. If this is false it will
        still warn.
    environ : mapping
        The environment to use to find the default extension path.
    reload : bool, optional
        Reload any extensions that have already been loaded.
    """
    if default:
        default_extension_path = pth.default_extension(environ=environ)
        pth.ensure_file(default_extension_path)
        # put the default extension first so other extensions can depend on
        # the order they are loaded
        extensions = concatv([default_extension_path], extensions)

    for ext in extensions:
        if ext in _loaded_extensions and not reload:
            continue
        try:
            # load all of the zipline extensionss
            if ext.endswith('.py'):
                with open(ext) as f:
                    ns = {}
                    six.exec_(compile(f.read(), ext, 'exec'), ns, ns)
            else:
                __import__(ext)
        except Exception as e:
            if strict:
                # if `strict` we should raise the actual exception and fail
                raise
            # without `strict` we should just log the failure
            warnings.warn(
                'Failed to load extension: %r\n%s' % (ext, e),
                stacklevel=2
            )
        else:
            _loaded_extensions.add(ext)


def run_algorithm(initialize,
                  start=None,
                  end=None,
                  capital_base=1e6,
                  handle_data=None,
                  before_trading_start=None,
                  analyze=None,
                  data_frequency='daily',
                  bundle='sharadar',
                  bundle_timestamp=None,
                  trading_calendar=None,
                  metrics_set='default_daily',
                  benchmark_symbol='SPY',
                  default_extension=True,
                  extensions=(),
                  strict_extensions=True,
                  environ=os.environ,
                  blotter='default',
                  broker=None,
                  state_filename=None
                  ):
    """
    Run a trading algorithm.

    Parameters
    ----------
    start : datetime
        The start date of the backtest.
    end : datetime
        The end date of the backtest..
    initialize : callable[context -> None]
        The initialize function to use for the algorithm. This is called once
        at the very begining of the backtest and should be used to set up
        any state needed by the algorithm.
    capital_base : float
        The starting capital for the backtest.
    handle_data : callable[(context, BarData) -> None], optional
        The handle_data function to use for the algorithm. This is called
        every minute when ``data_frequency == 'minute'`` or every day
        when ``data_frequency == 'daily'``.
    before_trading_start : callable[(context, BarData) -> None], optional
        The before_trading_start function for the algorithm. This is called
        once before each trading day (after initialize on the first day).
    analyze : callable[(context, pd.DataFrame) -> None], optional
        The analyze function to use for the algorithm. This function is called
        once at the end of the backtest and is passed the context and the
        performance data.
    data_frequency : {'daily', 'minute'}, optional
        The data frequency to run the algorithm at. For live trading the default is 'minute', otherwise 'daily'
    bundle : str, optional
        The name of the data bundle to use to load the data to run the backtest
        with. This defaults to 'quantopian-quandl'.
    bundle_timestamp : datetime, optional
        The datetime to lookup the bundle data for. This defaults to the
        current time.
    trading_calendar : TradingCalendar, optional
        The trading calendar to use for your backtest.
    metrics_set : iterable[Metric] or str, optional
        The set of metrics to compute in the simulation. If a string is passed,
        resolve the set with :func:`zipline.finance.metrics.load`.
    benchmark_symbol: The symbol of the benchmark. For live trading the default None, otherwise 'SPY'.
    default_extension : bool, optional
        Should the default zipline extension be loaded. This is found at
        ``$ZIPLINE_ROOT/extension.py``
    extensions : iterable[str], optional
        The names of any other extensions to load. Each element may either be
        a dotted module path like ``a.b.c`` or a path to a python file ending
        in ``.py`` like ``a/b/c.py``.
    strict_extensions : bool, optional
        Should the run fail if any extensions fail to load. If this is false,
        a warning will be raised instead.
    environ : mapping[str -> str], optional
        The os environment to use. Many extensions use this to get parameters.
        This defaults to ``os.environ``.
    blotter : str or zipline.finance.blotter.Blotter, optional
        Blotter to use with this algorithm. If passed as a string, we look for
        a blotter construction function registered with
        ``zipline.extensions.register`` and call it with no parameters.
        Default is a :class:`zipline.finance.blotter.SimulationBlotter` that
        never cancels orders.
    broker : instance of zipline.gens.brokers.broker.Broker
    state_filename : path to pickle file storing the algorithm "context" (similar to self)

    Returns
    -------
    perf : pd.DataFrame
        The daily performance of the algorithm.

    See Also
    --------
    zipline.data.bundles.bundles : The available data bundles.
    """
    load_extensions(default_extension, extensions, strict_extensions, environ)

    try:
        register_calendar_alias('NYSEMKT', 'XNYS')
        register_calendar_alias('OTC', 'XNYS')
    except CalendarNameCollision as e:
        log.info(e)

    return _run(
        handle_data=handle_data,
        initialize=initialize,
        before_trading_start=before_trading_start,
        analyze=analyze,
        algofile=None,
        algotext=None,
        defines=(),
        data_frequency=data_frequency,
        capital_base=capital_base,
        bundle=bundle,
        bundle_timestamp=bundle_timestamp,
        start=start,
        end=end,
        output=os.devnull,
        trading_calendar=trading_calendar,
        print_algo=False,
        metrics_set=metrics_set,
        local_namespace=False,
        environ=environ,
        blotter=blotter,
        benchmark_symbol=benchmark_symbol,
        broker=broker,
        state_filename=state_filename
    )
