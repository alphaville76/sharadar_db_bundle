import os
from unittest.mock import Mock, patch

import pytest
from logbook import Logger, StreamHandler

from sharadar.util.logger import NewRelicLogHandler


@pytest.fixture
def agent():
    agent = Mock()
    with patch.dict(os.environ, {'NEW_RELIC_LICENSE_KEY': 'test-license'}, clear=True):
        with patch('sharadar.util.logger.import_module', return_value=agent):
            yield agent


def test_algorithm_name_overrides_environment_and_selects_application(agent):
    os.environ['NEW_RELIC_APP_NAME'] = 'AnotherAlgorithm'
    handler = NewRelicLogHandler(app_name='MyAlgorithm')

    assert handler.app_name == 'MyAlgorithm'
    assert os.environ['NEW_RELIC_APP_NAME'] == 'MyAlgorithm'
    assert os.environ['NEW_RELIC_APPLICATION_LOGGING_ENABLED'] == 'true'
    assert os.environ['NEW_RELIC_APPLICATION_LOGGING_FORWARDING_ENABLED'] == 'true'
    agent.initialize.assert_called_once_with()
    agent.application.assert_called_once_with(name='MyAlgorithm')


def test_forwarding_preserves_console_output_and_levels(agent):
    import io

    output = io.StringIO()
    logger = Logger('MyAlgorithm')
    handler = NewRelicLogHandler(app_name='MyAlgorithm')
    logger.handlers.append(StreamHandler(output))
    logger.handlers.insert(0, handler)

    logger.info('Test {}', 'message')
    logger.warning('Test warning')
    logger.error('Test error')

    assert [call.kwargs['level'] for call in agent.record_log_event.call_args_list] == [
        'INFO', 'WARNING', 'ERROR',
    ]
    assert agent.record_log_event.call_args_list[0].args == ('Test message',)
    assert all(
        call.kwargs['application'] is agent.application.return_value
        for call in agent.record_log_event.call_args_list
    )
    assert 'Test message' in output.getvalue()
    assert 'Test warning' in output.getvalue()
    assert 'Test error' in output.getvalue()
    handler.shutdown()
    agent.shutdown_agent.assert_called_once_with(timeout=10)


def test_environment_forwarding_options_are_preserved(agent):
    os.environ['NEW_RELIC_APPLICATION_LOGGING_FORWARDING_ENABLED'] = 'false'
    NewRelicLogHandler(app_name='MyAlgorithm')
    assert os.environ['NEW_RELIC_APPLICATION_LOGGING_FORWARDING_ENABLED'] == 'false'


def test_no_license_warns_without_importing_agent():
    with patch.dict(os.environ, {}, clear=True):
        with patch('sharadar.util.logger.import_module') as import_agent:
            with pytest.warns(RuntimeWarning, match='NEW_RELIC_LICENSE_KEY'):
                handler = NewRelicLogHandler(app_name='MyAlgorithm')
            handler.emit(Mock())
            handler.shutdown()
            import_agent.assert_not_called()


@pytest.mark.parametrize('app_name', ['', '   ', None])
def test_invalid_application_name_is_rejected(app_name):
    with pytest.raises(ValueError, match='non-empty string'):
        NewRelicLogHandler(app_name=app_name)


def test_missing_package_is_reported(agent):
    with patch('sharadar.util.logger.import_module', side_effect=ImportError):
        with pytest.raises(RuntimeError, match='could not be imported'):
            NewRelicLogHandler(app_name='MyAlgorithm')


def test_old_agent_is_reported(agent):
    del agent.record_log_event
    with pytest.raises(RuntimeError, match='8.5.0'):
        NewRelicLogHandler(app_name='MyAlgorithm')


def test_shutdown_runs_when_algorithm_fails(agent):
    handler = NewRelicLogHandler(app_name='MyAlgorithm')
    with pytest.raises(RuntimeError, match='algorithm failed'):
        try:
            raise RuntimeError('algorithm failed')
        finally:
            handler.shutdown(timeout=15)
    agent.shutdown_agent.assert_called_once_with(timeout=15)
