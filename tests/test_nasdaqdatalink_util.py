import pytest
import pandas as pd
import zipfile
from sharadar.util.nasdaqdatalink_util import format_metadata_url, load_data_table

class TestFormatMetadataUrl:
    def test_basic_url(self):
        url = format_metadata_url('my_api_key', 'SHARADAR/SEP')
        assert 'https://data.nasdaq.com/api/v3/datatables/SHARADAR/SEP.csv' in url
        assert 'api_key=my_api_key' in url
        assert 'qopts.export=true' in url

    def test_different_table(self):
        url = format_metadata_url('key123', 'SHARADAR/SF1')
        assert 'SHARADAR/SF1.csv' in url
        assert 'api_key=key123' in url

class TestLoadDataTable:
    def test_loads_csv_from_zip(self, tmp_path):
        csv_content = "date,close\n2023-01-01,100.0\n2023-01-02,101.0\n"
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", csv_content)
        result = load_data_table(str(zip_path))
        assert len(result) == 2
        assert 'date' in result.columns
        assert 'close' in result.columns

    def test_with_index_col(self, tmp_path):
        csv_content = "date,close\n2023-01-01,100.0\n2023-01-02,101.0\n"
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", csv_content)
        result = load_data_table(str(zip_path), index_col='date')
        assert result.index.name == 'date'

    def test_with_parse_dates(self, tmp_path):
        csv_content = "date,close\n2023-01-01,100.0\n2023-01-02,101.0\n"
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", csv_content)
        result = load_data_table(str(zip_path), index_col='date', parse_dates=True)
        assert pd.api.types.is_datetime64_any_dtype(result.index)

    def test_na_values_handled(self, tmp_path):
        csv_content = "date,close\n2023-01-01,NA\n2023-01-02,101.0\n"
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data.csv", csv_content)
        result = load_data_table(str(zip_path))
        assert pd.isna(result.iloc[0]['close'])

    def test_multiple_files_in_zip_raises(self, tmp_path):
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("data1.csv", "a,b\n1,2\n")
            zf.writestr("data2.csv", "a,b\n3,4\n")
        with pytest.raises(AssertionError):
            load_data_table(str(zip_path))
