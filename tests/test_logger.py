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
