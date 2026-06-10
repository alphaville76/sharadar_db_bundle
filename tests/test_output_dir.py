import os
import pytest
from unittest.mock import patch, MagicMock


class TestOutputDir:
    @patch('sharadar.util.output_dir.data_root')
    def test_get_data_dir(self, mock_data_root):
        mock_data_root.return_value = '/tmp/test_data'
        from sharadar.util.output_dir import get_data_dir
        # Should return a string path
        result = get_data_dir()
        assert isinstance(result, str)

    @patch('sharadar.util.output_dir.data_root')
    def test_get_cache_dir(self, mock_data_root):
        mock_data_root.return_value = '/tmp/test_data'
        from sharadar.util.output_dir import get_cache_dir
        result = get_cache_dir()
        assert isinstance(result, str)
