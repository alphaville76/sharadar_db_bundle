import os
import tempfile
from unittest.mock import patch


class TestLoggerHelpers:
    def test_get_log_dir_with_home(self):
        from sharadar.util.logger import _get_log_dir
        with patch.dict(os.environ, {'HOME': '/home/testuser'}):
            result = _get_log_dir()
            assert result == '/home/testuser/log'

    def test_get_log_dir_with_userprofile(self):
        from sharadar.util.logger import _get_log_dir
        with patch.dict(os.environ, {'USERPROFILE': 'C:\\Users\\test'}, clear=True):
            result = _get_log_dir()
            assert 'log' in result

    def test_get_log_dir_fallback_to_current(self):
        from sharadar.util.logger import _get_log_dir
        with patch.dict(os.environ, {}, clear=True):
            result = _get_log_dir()
            assert result == './log'


class TestAlgoLogger:
    def test_algo_logger_creation(self):
        from sharadar.util.logger import AlgoLogger
        import pandas as pd
        
        with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as f:
            filepath = f.name
        
        try:
            def mock_time():
                return pd.Timestamp('2023-10-16 10:00:00')
            
            logger = AlgoLogger(filepath, arena='backtest', record_time=mock_time)
            assert logger.arena == 'backtest'
            assert logger.record_time == mock_time
        finally:
            # Clean up log file
            import glob
            for f in glob.glob(filepath.replace('.py', '*.log')):
                os.unlink(f)
            if os.path.exists(filepath):
                os.unlink(filepath)

    def test_algo_logger_default_record_time(self):
        from sharadar.util.logger import AlgoLogger
        
        with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as f:
            filepath = f.name
        
        try:
            # Should not raise even with default record_time
            logger = AlgoLogger(filepath, arena='backtest')
            assert logger.arena == 'backtest'
        finally:
            import glob
            for f in glob.glob(filepath.replace('.py', '*.log')):
                os.unlink(f)
            if os.path.exists(filepath):
                os.unlink(filepath)

    def test_default_live_clock_uses_current_local_time(self, tmp_path):
        import datetime
        from logbook import TestHandler
        from sharadar.util.logger import AlgoLogger

        logger = AlgoLogger(str(tmp_path / 'live.py'), arena='live')
        handlers = list(logger.handlers)
        logger.handlers.clear()
        capture = TestHandler()
        logger.handlers.append(capture)
        try:
            with patch('sharadar.util.logger.get_datetime', side_effect=AssertionError), \
                    patch('sharadar.util.logger.send_mail'):
                logger.info('Live message')
            timestamp = capture.records[-1].time
            assert isinstance(timestamp, datetime.datetime)
            # same local wall clock as the other sharadar logs
            assert timestamp.tzinfo is None
            assert abs(datetime.datetime.now() - timestamp) < datetime.timedelta(seconds=5)
        finally:
            for handler in handlers:
                handler.close()
            capture.close()

    def test_default_backtest_clock_uses_zipline_time(self, tmp_path):
        import datetime
        from types import SimpleNamespace
        from logbook import TestHandler
        from sharadar.util.logger import AlgoLogger

        expected = datetime.datetime(2026, 11, 6, 14, 53, tzinfo=datetime.timezone.utc)
        logger = AlgoLogger(str(tmp_path / 'backtest.py'), arena='backtest')
        handlers = list(logger.handlers)
        logger.handlers.clear()
        capture = TestHandler()
        logger.handlers.append(capture)
        try:
            with patch('sharadar.util.logger.get_algo_instance', return_value=SimpleNamespace(datetime=expected)), \
                    patch('sharadar.util.logger.get_datetime', return_value=expected):
                logger.info('Backtest message')
            assert capture.records[-1].time == expected
        finally:
            for handler in handlers:
                handler.close()
            capture.close()

    def test_backtest_clock_outside_and_during_initialize(self, tmp_path):
        import datetime
        from types import SimpleNamespace
        from logbook import TestHandler
        from sharadar.util.logger import AlgoLogger

        logger = AlgoLogger(str(tmp_path / 'backtest.py'), arena='backtest')
        handlers = list(logger.handlers)
        logger.handlers.clear()
        capture = TestHandler()
        logger.handlers.append(capture)
        session = datetime.datetime(2000, 1, 3)
        try:
            with patch('sharadar.util.logger.get_datetime', side_effect=AssertionError):
                with patch('sharadar.util.logger.get_algo_instance', return_value=None):
                    logger.info('Before simulation')
                assert capture.records[-1].time.utcoffset() == datetime.timedelta(0)
                with patch('sharadar.util.logger.get_algo_instance',
                           return_value=SimpleNamespace(datetime=session)):
                    logger.info('In initialize')
                assert capture.records[-1].time == session.replace(tzinfo=datetime.timezone.utc)
        finally:
            for handler in handlers:
                handler.close()
            capture.close()

    def test_previous_name_remains_compatible(self):
        from sharadar.util.logger import AlgoLogger, BacktestLogger

        assert BacktestLogger is AlgoLogger

    def test_live_logging_uses_clock_and_sends_mail(self, tmp_path):
        import datetime
        from logbook import TestHandler
        from sharadar.util.logger import AlgoLogger

        now = datetime.datetime(2026, 11, 6, 14, 53)
        logger = AlgoLogger(
            str(tmp_path / 'algorithm.py'),
            arena='live',
            logname='LiveAlgorithm',
            record_time=lambda: now,
        )
        handlers = list(logger.handlers)
        logger.handlers.clear()
        capture = TestHandler()
        logger.handlers.append(capture)
        try:
            with patch('sharadar.util.logger.send_mail') as send_mail:
                logger.debug('Debug message')
                send_mail.assert_not_called()
                logger.info('Live message')
                send_mail.assert_called_once_with('LiveAlgorithm INFO', 'Live message')
            assert capture.records[-1].time == now
            assert capture.records[-1].message == 'Live message'
        finally:
            for handler in handlers:
                handler.close()
