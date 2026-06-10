import pytest
from unittest.mock import MagicMock, patch
from zipline.pipeline import Filter
from zipline.pipeline.domain import GENERIC

from sharadar.pipeline.pipeline import ExecutionPlan, Pipeline


class TestExecutionPlan:
    def test_screen_name_property(self):
        _domain = GENERIC
        _terms = {}
        _start_date = "2023-01-01"
        _end_date = "2023-12-31"
        screen_name = "my_screen"

        with patch.object(ExecutionPlan.__bases__[0], '__init__', return_value=None):
            plan = ExecutionPlan.__new__(ExecutionPlan)
            plan._screen_name = screen_name

        assert plan.screen_name == "my_screen"


class TestPipeline:
    def test_valid_screen_tuple_works(self):
        mock_filter = MagicMock(spec=Filter)
        with patch.object(Pipeline.__bases__[0], '__init__', return_value=None):
            pipe = Pipeline(columns={}, screen=("my_screen", mock_filter), domain=GENERIC)
        assert pipe.screen_name == "my_screen"

    def test_invalid_screen_raises_type_error(self):
        with pytest.raises(TypeError):
            Pipeline(columns={}, screen=(123, "not_a_filter"), domain=GENERIC)

    def test_screen_name_stored(self):
        mock_filter = MagicMock(spec=Filter)
        with patch.object(Pipeline.__bases__[0], '__init__', return_value=None):
            pipe = Pipeline(columns={}, screen=("custom_name", mock_filter), domain=GENERIC)
        assert pipe.screen_name == "custom_name"