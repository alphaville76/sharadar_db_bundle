from unittest.mock import MagicMock
from sharadar.pipeline.pipeline import Pipeline


class TestPipeline:
    def test_pipeline_with_named_screen(self):
        mock_filter = MagicMock()
        pipe = Pipeline(
            columns={'close': MagicMock()},
            screen=('my_screen', mock_filter)
        )
        assert pipe is not None

    def test_pipeline_without_named_screen(self):
        mock_filter = MagicMock()
        pipe = Pipeline(
            columns={'close': MagicMock()},
            screen=mock_filter
        )
        assert pipe is not None
