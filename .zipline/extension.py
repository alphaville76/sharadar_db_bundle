from zipline.data import bundles
from zipline.finance import metrics
from sharadar.loaders.ingest_sharadar import from_nasdaqdatalink
from sharadar.util.metric_daily import default_daily

bundles.register("sharadar", from_nasdaqdatalink(), create_writers=False)
metrics.register('default_daily', default_daily)
