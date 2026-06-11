import os
from unittest.mock import patch


from sharadar.util.output_dir import (
    SHARADAR_BUNDLE_DIR,
    SHARADAR_BUNDLE_NAME,
    create_data_dir,
    get_data_dir,
)


class TestCreateDataDir:
    @patch("sharadar.util.output_dir.os.makedirs")
    @patch("sharadar.util.output_dir.os.path.exists", return_value=False)
    @patch("sharadar.util.output_dir.data_root")
    def test_creates_directory_when_missing(self, mock_data_root, mock_exists, mock_makedirs, tmp_path):
        mock_data_root.return_value = str(tmp_path / "root")

        create_data_dir("bundle_name")

        mock_makedirs.assert_called_once_with(str(tmp_path / "root"))

    @patch("sharadar.util.output_dir.os.makedirs")
    @patch("sharadar.util.output_dir.os.path.exists", return_value=True)
    @patch("sharadar.util.output_dir.data_root")
    def test_does_not_create_directory_when_exists(self, mock_data_root, mock_exists, mock_makedirs, tmp_path):
        mock_data_root.return_value = str(tmp_path / "root")

        create_data_dir("bundle_name")

        mock_makedirs.assert_not_called()

    @patch("sharadar.util.output_dir.os.makedirs")
    @patch("sharadar.util.output_dir.os.path.exists", return_value=True)
    @patch("sharadar.util.output_dir.data_root")
    def test_returns_correct_path(self, mock_data_root, mock_exists, mock_makedirs, tmp_path):
        mock_data_root.return_value = str(tmp_path / "root")

        result = create_data_dir("my_bundle")

        assert result == os.path.join(str(tmp_path / "root"), "my_bundle")


class TestGetDataDir:
    @patch("sharadar.util.output_dir.create_data_dir")
    def test_path_includes_sharadar_and_latest(self, mock_create_data_dir):
        mock_create_data_dir.return_value = "/fake/path/sharadar"

        result = get_data_dir()

        mock_create_data_dir.assert_called_once_with(SHARADAR_BUNDLE_NAME)
        assert "sharadar" in result
        assert result.endswith(SHARADAR_BUNDLE_DIR)

