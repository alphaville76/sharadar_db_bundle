import pandas as pd
import matplotlib.pyplot as plt
import pyfolio as pf
from pyfolio.tears import utils
from pyfolio.utils import format_asset
from pyfolio import round_trips as rt
from pyfolio.plotting import STAT_FUNCS_PCT
from collections import OrderedDict
import numpy as np
import base64
import io
import os
from sharadar.util.logger import log
import time
import gc
import psutil
import datetime
from sharadar.pipeline.engine import symbol, returns

DATETIME_FMT = '%Y-%m-%d_%H%M'


def _to_img(figure):
    pic_IObytes = io.BytesIO()
    figure.savefig(pic_IObytes, format='png')
    pic_IObytes.seek(0)
    pic_hash = base64.b64encode(pic_IObytes.read())
    return '<img width="60%" height="60%" src="data:image/png;base64, ' + pic_hash.decode("utf-8") + '" />'


def analyze(perf, filename, doc=None, duration=None, param=None, info=None, show_image=True):
    num_positions = perf.positions.shape[0]
    if num_positions == 0:
        raise ValueError("No positions found")

    gc.collect()
    mem = psutil.virtual_memory()
    log.info("Memory used %.2f Gb von %.2f Gb (%d%%)" % (mem.used / 1e9, mem.total / 1e9, mem.percent))

    now = datetime.datetime.now()

    serialise(perf, filename, now)

    create_report(perf, filename, now, doc, duration, param, info, show_image)


def serialise(perf, filename, now):
    suffix = '_' + now.strftime(DATETIME_FMT) + '_perf.dump'
    perf_dump_file = change_extension(filename, suffix)
    log.info("Serialise performance date in %s" % perf_dump_file)
    # joblib.dump(perf, perf_dump_file)
    perf.to_pickle(perf_dump_file)


def create_report(perf, filename, now, doc=None, duration=None, param=None, info=None, show_image=True):
    rets, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(perf)
    date_rows = OrderedDict()
    if len(rets.index) > 0:
        date_rows['Start date'] = rets.index[0].strftime('%Y-%m-%d')
        date_rows['End date'] = rets.index[-1].strftime('%Y-%m-%d')
        date_rows['Total months'] = int(len(rets) / 21)

    perf_stats_series = pf.timeseries.perf_stats(rets, positions=positions, transactions=transactions)

    benchmark_rets = returns(symbol('SPY'), rets.index[0], rets.index[-1])
    benchmark_perf_stats = pf.timeseries.perf_stats(benchmark_rets)

    perf_stats_df = pd.DataFrame(perf_stats_series, columns=['Backtest'])
    perf_stats_df['Benchmark'] = benchmark_perf_stats
    perf_stats_df['Spread'] = perf_stats_df['Backtest'] - perf_stats_df['Benchmark']
    format_perf_stats(perf_stats_df)

    drawdown_df = pf.timeseries.gen_drawdown_table(rets, top=5)
    rets_interesting = pf.timeseries.extract_interesting_date_ranges(rets)
    positions = utils.check_intraday('infer', rets, positions, transactions)
    transactions_closed = rt.add_closing_transactions(positions, transactions)
    trades = rt.extract_round_trips(
        transactions_closed,
        portfolio_value=positions.sum(axis='columns') / (1 + rets)
    )

    if show_image:
        fig0 = create_log_returns_chart(rets, benchmark_rets)
        fig1 = pf.create_returns_tear_sheet(rets, positions, transactions, benchmark_rets=benchmark_rets, return_fig=True)
        fig2 = pf.create_position_tear_sheet(rets, positions, return_fig=True)
        fig3 = pf.create_txn_tear_sheet(rets, positions, transactions, return_fig=True)
        fig4 = pf.create_interesting_times_tear_sheet(rets, return_fig=True)
        fig5 = None
        try:
            fig5 = pf.create_round_trip_tear_sheet(rets, positions, transactions, return_fig=True)
        except:
            pass

    report_suffix = "_%s_%.2f_report.htm" % (now.strftime(DATETIME_FMT), 100. * perf_stats_series['Annual return'])
    reportfile = change_extension(filename, report_suffix)
    with open(reportfile, 'w') as f:
        print("""<!DOCTYPE html>
<html>
   <head>
      <title>Performance Report</title>
      <style >
         body {
         font-family: Arial, Helvetica, sans-serif;
         }
         table {
         border-collapse: collapse;
         }
         tbody tr:nth-child(odd) {
         background-color: lightgrey;
         }
         tbody tr:nth-child(even) {
         background-color: white;
         }
         tr th {
         border: none;
         text-align: right;
         padding: 2px 5px 2px;
         }
         tr td {
         border: none;
         text-align: right;
         padding: 2px 5px 2px;
         }
      </style>
      
      <script type="text/javascript">
        function showElement() {
            element = document.getElementById('code'); 
            element.style.visibility = 'visible'; 
        } 
      
        function hideElement() { 
            element = document.getElementById('code'); 
            element.style.visibility = 'hidden'; 
        } 
      </script> 
   </head>
   <body>""", file=f)
        print("<h1>Performance report for " + os.path.basename(filename) + "</h1>", file=f)
        print("<p>Created on %s</p>" % (now), file=f)
        if duration is not None:
            print("<p>Backtest executed in %s</p>" % (time.strftime("%H:%M:%S", time.gmtime(duration))), file=f)
        if doc is not None:
            print('<h3>Description</h3>', file=f)
            print('<p style="white-space: pre">%s</p>' % doc.strip(), file=f)
        if param is not None and len(param) > 0:
            print('<h3>Parameters</h3>', file=f)
            print('<pre>%s</pre><br/>' % str(param), file=f)
        if info is not None and len(info) > 0:
            print('<h3>Info</h3>', file=f)
            print('<pre>%s</pre><br/>' % str(info), file=f)
        print(to_html_table(
            perf_stats_df,
            float_format='{0:.2f}'.format,
            header_rows=date_rows
        ), file=f)
        print("<br/>", file=f)
        if show_image:
            print("<h3>Log Returns</h3>", file=f)
            print(_to_img(fig0), file=f)
            print("<br/>", file=f)
        print(to_html_table(
            drawdown_df.sort_values('Net drawdown in %', ascending=False),
            name='Worst drawdown periods',
            float_format='{0:.2f}'.format,
        ), file=f)
        print("<br/>", file=f)
        print(to_html_table(pd.DataFrame(rets_interesting)
                            .describe().transpose()
                            .loc[:, ['mean', 'min', 'max']] * 100,
                            name='Stress Events',
                            float_format='{0:.2f}%'.format), file=f)
        print("<br/>", file=f)
        if len(trades) >= 5:
            stats = rt.gen_round_trip_stats(trades)
            print(to_html_table(stats['summary'], float_format='{:.2f}'.format, name='Summary stats'), file=f)
            print("<br/>", file=f)
            print(to_html_table(stats['pnl'], float_format='${:.2f}'.format, name='PnL stats'), file=f)
            print("<br/>", file=f)
            print(to_html_table(stats['duration'], float_format='{:.2f}'.format, name='Duration stats'), file=f)
            print("<br/>", file=f)
            print(to_html_table(stats['returns'] * 100, float_format='{:.2f}%'.format, name='Return stats'), file=f)
            print("<br/>", file=f)
            stats['symbols'].columns = stats['symbols'].columns.map(format_asset)
            print(to_html_table(stats['symbols'] * 100, float_format='{:.2f}%'.format, name='Symbol stats'), file=f)

        if show_image:
            print("<h3>Returns</h3>", file=f)
            print(_to_img(fig1), file=f)

            print("<h3>Positions</h3>", file=f)
            print(_to_img(fig2), file=f)

            print("<h3>Transactions</h3>", file=f)
            print(_to_img(fig3), file=f)

            print("<h3>Interesting Times</h3>", file=f)
            print(_to_img(fig4), file=f)

            if fig5 is not None:
                print("<h3>Trades</h3>", file=f)
                print(_to_img(fig5), file=f)

        print('<br/>', file=f)
        print('<button onclick="showElement()">Show Code</button> <button onclick="hideElement()">Hide Code</button>', file=f)
        print('<pre id="code" style="visibility: hidden">', file=f)
        print(open(filename, "r").read(), file=f)
        print('</pre>', file=f)

        print("</body>\n</html>", file=f)


def format_perf_stats(perf_stats_df):
    for column in perf_stats_df.columns:
        for stat, value in perf_stats_df[column].iteritems():
            if stat in STAT_FUNCS_PCT:
                perf_stats_df.loc[stat, column] = str(np.round(value * 100, 2)) + '%'


def create_log_returns_chart(rets, benchmark_rets):
    cum_log_returns = np.log1p(rets).cumsum()
    cum_log_benchmark_rets = np.log1p(benchmark_rets).cumsum()

    fig, ax = plt.subplots()
    cum_log_returns.plot(ax=ax, figsize=(20, 10))
    cum_log_benchmark_rets.plot(ax=ax)
    ax.grid(True)
    ax.axhline(y=0, linestyle='--', color='black')
    ax.legend(['Backtest', 'Benchmark'])
    plt.title("Log returns")
    return fig


def change_extension(filename, new_ext):
    path, ext = os.path.splitext(filename)
    return path + new_ext


def to_html_table(table,
                name=None,
                float_format=None,
                formatters=None,
                header_rows=None):
    """
    Pretty print a pandas DataFrame.

    Uses HTML output if running inside Jupyter Notebook, otherwise
    formatted text output.

    Parameters
    ----------
    table : pandas.Series or pandas.DataFrame
        Table to pretty-print.
    name : str, optional
        Table name to display in upper left corner.
    float_format : function, optional
        Formatter to use for displaying table elements, passed as the
        `float_format` arg to pd.Dataframe.to_html.
        E.g. `'{0:.2%}'.format` for displaying 100 as '100.00%'.
    formatters : list or dict, optional
        Formatters to use by column, passed as the `formatters` arg to
        pd.Dataframe.to_html.
    header_rows : dict, optional
        Extra rows to display at the top of the table.
    """

    if isinstance(table, pd.Series):
        table = pd.DataFrame(table)

    if name is not None:
        table.columns.name = name

    html = table.to_html(float_format=float_format, formatters=formatters)

    if header_rows is not None:
        # Count the number of columns for the text to span
        n_cols = html.split('<thead>')[1].split('</thead>')[0].count('<th>')

        # Generate the HTML for the extra rows
        rows = ''
        for name, value in header_rows.items():
            rows += ('\n    <tr style="text-align: right;"><th>%s</th>' +
                     '<td colspan=%d>%s</td></tr>') % (name, n_cols, value)

        # Inject the new HTML
        html = html.replace('<thead>', '<thead>' + rows)

    return html


if __name__ == "__main__":
    algo_file = '../../algo/haugen20/haugen20.py'
    perf_dump_file = '../../algo/haugen20/haugen20_202006300552_perf.dump'
    perf = pd.read_pickle(perf_dump_file)
    now = datetime.datetime.now()
    create_report(perf, algo_file, now)
