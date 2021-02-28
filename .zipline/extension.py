from zipline.data import bundles
from zipline.finance import metrics
from sharadar.loaders.ingest_sharadar import from_quandl
from sharadar.util.metric_daily import default_daily

bundles.register("sharadar", from_quandl(), create_writers=False)
metrics.register('default_daily', default_daily)
