from unittest.mock import MagicMock, patch
from zipline.pipeline import Filter
from sharadar.pipeline.pipeline import Pipeline


class TestPipeline:
    def test_pipeline_with_named_screen(self):
        mock_filter = MagicMock(spec=Filter)
        with patch.object(Pipeline.__bases__[0], '__init__', return_value=None):
            pipe = Pipeline(
                columns={'close': MagicMock()},
                screen=('my_screen', mock_filter)
            )
        assert pipe.screen_name == 'my_screen'

    def test_pipeline_without_screen(self):
        with patch.object(Pipeline.__bases__[0], '__init__', return_value=None):
            pipe = Pipeline(
                columns={'close': MagicMock()},
                screen=None
            )
        assert pipe.screen_name == 'screen_default'