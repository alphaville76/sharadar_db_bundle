from sharadar.loaders.ingest_sharadar import create_metadata

related_tickers, sharadar_metadata_df = create_metadata()

sharadar_metadata = sharadar_metadata_df[sharadar_metadata_df['permaticker'] == 199623].iloc[0, :]

start_date = sharadar_metadata.loc['firstpricedate']
print(start_date)


