import pandas as pd
import matplotlib.pyplot as plt
import pyfolio as pf
from pyfolio.plotting import STAT_FUNCS_PCT
from collections import OrderedDict
import numpy as np
import base64
import io
import os

def _to_img(figure):
    pic_IObytes = io.BytesIO()
    figure.savefig(pic_IObytes, format='png')
    pic_IObytes.seek(0)
    pic_hash = base64.b64encode(pic_IObytes.read())
    return '<img width="60%" height="60%" src="data:image/png;base64, ' + pic_hash.decode("utf-8") + '" />'

def analyze(context, perf, filename):
    returns, positions, transactions = pf.utils.extract_rets_pos_txn_from_zipline(perf)
    #pf.create_full_tear_sheet(returns, positions, transactions, round_trips=True)

    date_rows = OrderedDict()
    if len(returns.index) > 0:
        date_rows['Start date'] = returns.index[0].strftime('%Y-%m-%d')
        date_rows['End date'] = returns.index[-1].strftime('%Y-%m-%d')
        date_rows['Total months'] = int(len(returns) / 21)

    perf_stats = pf.timeseries.perf_stats(returns, positions=positions, transactions=transactions)
    perf_stats = pd.DataFrame(perf_stats, columns=['Backtest'])
    for column in perf_stats.columns:
        for stat, value in perf_stats[column].iteritems():
            if stat in STAT_FUNCS_PCT:
                perf_stats.loc[stat, column] = str(np.round(value * 100, 3)) + '%'

    drawdown_df = pf.timeseries.gen_drawdown_table(returns, top=5)

    rets_interesting = pf.timeseries.extract_interesting_date_ranges(returns)

    fig1 = pf.create_returns_tear_sheet(returns, positions, transactions, return_fig=True)
    fig2 = pf.create_position_tear_sheet(returns, positions, return_fig=True)
    fig3 = pf.create_txn_tear_sheet(returns, positions, transactions, return_fig=True)
    fig4 = pf.create_interesting_times_tear_sheet(returns, return_fig=True)

    reportfile = change_extension(filename, '_report.htm')
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
   </head>
   <body>""", file=f)
        print("<h3/>Performance report for " + os.path.basename(filename) + "<h3/>", file=f)

        print("<br/>", file=f)
        print(to_html_table(
            perf_stats,
            float_format='{0:.2f}'.format,
            header_rows=date_rows
        ), file=f)
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

        print("<h3/>Returns<h3/>", file=f)
        print(_to_img(fig1), file=f)

        print("<h3/>Positions<h3/>", file=f)
        print(_to_img(fig2), file=f)

        print("<h3/>Transactions<h3/>", file=f)
        print(_to_img(fig3), file=f)

        print("<h3/>Interesting Times<h3/>", file=f)
        print(_to_img(fig4), file=f)
        print("   </body>\n</html>", file=f)


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
