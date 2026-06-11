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


class TestBacktestLogger:
    def test_backtest_logger_creation(self):
        from sharadar.util.logger import BacktestLogger
        import pandas as pd
        
        with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as f:
            filepath = f.name
        
        try:
            def mock_time():
                return pd.Timestamp('2023-10-16 10:00:00')
            
            logger = BacktestLogger(filepath, arena='backtest', record_time=mock_time)
            assert logger.arena == 'backtest'
            assert logger.record_time == mock_time
        finally:
            # Clean up log file
            import glob
            for f in glob.glob(filepath.replace('.py', '*.log')):
                os.unlink(f)
            if os.path.exists(filepath):
                os.unlink(filepath)

    def test_backtest_logger_default_record_time(self):
        from sharadar.util.logger import BacktestLogger
        
        with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as f:
            filepath = f.name
        
        try:
            # Should not raise even with default record_time
            logger = BacktestLogger(filepath, arena='backtest')
            assert logger.arena == 'backtest'
        finally:
            import glob
            for f in glob.glob(filepath.replace('.py', '*.log')):
                os.unlink(f)
            if os.path.exists(filepath):
                os.unlink(filepath)
