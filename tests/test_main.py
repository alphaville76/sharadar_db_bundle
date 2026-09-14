"""Unit tests for sharadar/__main__.py (the zipline-compatible CLI)."""
import errno
import os

import pandas as pd
import pytest
from click.testing import CliRunner
from unittest.mock import MagicMock, patch

from sharadar import __main__ as main_module


@pytest.fixture
def runner():
    return CliRunner()


class TestExtractOptionObject:
    def test_extract_option_object_returns_click_option(self):
        import click

        option = click.option('--foo', default=1, help='a foo option')
        opt_obj = main_module.extract_option_object(option)
        assert isinstance(opt_obj, click.Option)
        assert opt_obj.name == 'foo'
        assert opt_obj.default == 1


class TestIpythonOnly:
    def test_ipython_only_when_not_ipython_injects_none(self):
        import click

        with patch.object(main_module, '__IPYTHON__', False):
            @main_module.ipython_only(click.option('--local-namespace', is_flag=True, default=None))
            def f(local_namespace=None):
                return local_namespace

            # the decorator should force local_namespace to None regardless of
            # what is passed in, since we are not running under ipython
            assert f(local_namespace=True) is None
            assert f() is None

    def test_ipython_only_when_ipython_returns_option_unchanged(self):
        import click

        with patch.object(main_module, '__IPYTHON__', True):
            option = click.option('--local-namespace', is_flag=True, default=None)
            decorated = main_module.ipython_only(option)
            # when running under ipython the option decorator is returned as-is
            assert decorated is option


class TestMainGroup:
    def test_main_invokes_load_extensions_and_create_args(self, runner):
        with patch.object(main_module, 'load_extensions') as mock_load_ext, \
             patch.object(main_module, 'create_args') as mock_create_args, \
             patch.object(main_module.bundles_module, 'bundles', {}):
            result = runner.invoke(main_module.main, ['-x', 'foo=bar', 'bundles'])

        assert result.exit_code == 0
        mock_create_args.assert_called_once()
        args, kwargs = mock_create_args.call_args
        assert args[0] == ('foo=bar',)

        mock_load_ext.assert_called_once()
        load_args, load_kwargs = mock_load_ext.call_args
        # default=True (default-extension flag), extension=(), strict=True (default)
        assert load_args[0] is True
        assert load_args[1] == ()
        assert load_args[2] is True
        assert load_args[3] is os.environ

    def test_main_non_strict_extensions_flag(self, runner):
        with patch.object(main_module, 'load_extensions') as mock_load_ext, \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', {}):
            result = runner.invoke(
                main_module.main,
                ['--non-strict-extensions', '--no-default-extension', 'bundles'],
            )

        assert result.exit_code == 0
        load_args, _ = mock_load_ext.call_args
        assert load_args[0] is False  # default-extension disabled
        assert load_args[2] is False  # non-strict-extensions


class TestBundlesCommand:
    def test_bundles_lists_sorted_bundles_with_ingestions(self, runner):
        fake_bundles = {'sharadar': object(), 'quandl': object()}
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', fake_bundles), \
             patch.object(
                 main_module.bundles_module,
                 'ingestions_for_bundle',
                 side_effect=lambda b: [pd.Timestamp('2020-01-01')],
             ):
            result = runner.invoke(main_module.main, ['bundles'])

        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        # sorted alphabetically: quandl before sharadar
        assert lines[0].startswith('quandl')
        assert lines[1].startswith('sharadar')

    def test_bundles_skips_hidden_bundles(self, runner):
        fake_bundles = {'.test-bundle': object(), 'sharadar': object()}
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', fake_bundles), \
             patch.object(main_module.bundles_module, 'ingestions_for_bundle', return_value=[]):
            result = runner.invoke(main_module.main, ['bundles'])

        assert result.exit_code == 0
        assert '.test-bundle' not in result.output
        assert 'sharadar' in result.output

    def test_bundles_no_ingestions_prints_placeholder(self, runner):
        fake_bundles = {'sharadar': object()}
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', fake_bundles), \
             patch.object(main_module.bundles_module, 'ingestions_for_bundle', return_value=[]):
            result = runner.invoke(main_module.main, ['bundles'])

        assert result.exit_code == 0
        assert '<no ingestions>' in result.output

    def test_bundles_enoent_oserror_treated_as_no_ingestions(self, runner):
        fake_bundles = {'sharadar': object()}
        enoent_error = OSError(errno.ENOENT, 'not found')
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', fake_bundles), \
             patch.object(main_module.bundles_module, 'ingestions_for_bundle', side_effect=enoent_error):
            result = runner.invoke(main_module.main, ['bundles'])

        assert result.exit_code == 0
        assert '<no ingestions>' in result.output

    def test_bundles_reraises_non_enoent_oserror(self, runner):
        fake_bundles = {'sharadar': object()}
        other_error = OSError(errno.EACCES, 'permission denied')
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'bundles', fake_bundles), \
             patch.object(main_module.bundles_module, 'ingestions_for_bundle', side_effect=other_error):
            result = runner.invoke(main_module.main, ['bundles'])

        assert result.exit_code != 0
        assert isinstance(result.exception, OSError)


class TestIngestCommand:
    def test_ingest_calls_bundles_module_ingest_with_defaults(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'ingest') as mock_ingest:
            result = runner.invoke(main_module.main, ['ingest'])

        assert result.exit_code == 0
        mock_ingest.assert_called_once()
        args, kwargs = mock_ingest.call_args
        assert args[0] == 'sharadar'
        assert args[1] is os.environ
        assert isinstance(args[2], pd.Timestamp)
        assert args[3] == ()
        assert args[4] is True  # show-progress default

    def test_ingest_passes_custom_bundle_and_flags(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'ingest') as mock_ingest:
            result = runner.invoke(
                main_module.main,
                ['ingest', '-b', 'my-bundle', '--assets-version', '3', '--no-show-progress'],
            )

        assert result.exit_code == 0
        args, kwargs = mock_ingest.call_args
        assert args[0] == 'my-bundle'
        assert args[3] == (3,)
        assert args[4] is False


class TestCleanCommand:
    def test_clean_calls_bundles_module_clean(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.bundles_module, 'clean') as mock_clean:
            result = runner.invoke(
                main_module.main,
                ['clean', '-b', 'my-bundle', '-k', '5'],
            )

        assert result.exit_code == 0
        mock_clean.assert_called_once_with('my-bundle', None, None, 5)


class TestRunCommandValidation:
    def test_run_requires_start_and_end_without_broker(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(main_module.main, ['run', '-t', 'algo'])

        assert result.exit_code != 0
        assert "must specify dates with '-s' / '--start' and '-e' / '--end'" in result.output

    def test_run_requires_start_when_only_end_given(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                ['run', '-t', 'algo', '-e', '2020-02-01'],
            )

        assert result.exit_code != 0
        assert "must specify a start date" in result.output

    def test_run_requires_end_when_only_start_given(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                ['run', '-t', 'algo', '-s', '2020-01-01'],
            )

        assert result.exit_code != 0
        assert "must specify an end date" in result.output

    def test_run_broker_requires_broker_uri(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                ['run', '-t', 'algo', '--broker', 'IB'],
            )

        assert result.exit_code != 0
        assert "must specify broker-uri" in result.output

    def test_run_broker_requires_state_file(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                ['run', '-t', 'algo', '--broker', 'IB', '--broker-uri', 'uri://x'],
            )

        assert result.exit_code != 0
        assert "must specify state-file" in result.output

    def test_run_broker_requires_realtime_bar_target(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '--broker', 'IB',
                    '--broker-uri', 'uri://x', '--state-file', 'state.pkl',
                ],
            )

        assert result.exit_code != 0
        assert "must specify realtime-bar-target" in result.output

    def test_run_unsupported_broker_module_fails(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '--broker', 'nosuchbroker',
                    '--broker-uri', 'uri://x', '--state-file', 'state.pkl',
                    '--realtime-bar-target', '/tmp',
                ],
            )

        assert result.exit_code != 0
        assert "unsupported broker: can't import module" in result.output

    def test_run_broker_missing_class_fails(self, runner):
        fake_module = MagicMock(spec=[])  # no BrokerClass attribute
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module, 'import_module', return_value=fake_module):
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '--broker', 'ib',
                    '--broker-uri', 'uri://x', '--state-file', 'state.pkl',
                    '--realtime-bar-target', '/tmp',
                ],
            )

        assert result.exit_code != 0
        assert "unsupported broker: can't import class" in result.output

    def test_run_requires_exactly_one_of_algofile_or_algotext(self, runner):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'):
            result = runner.invoke(
                main_module.main,
                ['run', '-s', '2020-01-01', '-e', '2020-02-01'],
            )

        assert result.exit_code != 0
        assert "must specify exactly one of '-f' / '--algofile' or" in result.output

    def test_run_list_brokers_lists_available_broker_modules(self, runner):
        fake_modules = [
            (None, 'broker', False),
            (None, 'ib_broker', False),
        ]
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module.pkgutil, 'iter_modules', return_value=fake_modules):
            result = runner.invoke(main_module.main, ['run', '--list-brokers'])

        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert lines[0] == 'Supported brokers:'
        # the base 'broker' module should be filtered out
        assert lines[1:] == ['ib_broker']


class TestRunCommandExecution:
    def _fake_perf(self):
        perf = MagicMock()
        perf.__str__.return_value = 'FAKE-PERF'
        return perf

    def test_run_success_writes_perf_to_stdout(self, runner):
        fake_perf = self._fake_perf()
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module, 'get_calendar') as mock_get_cal, \
             patch.object(main_module, 'BenchmarkSpec') as mock_bench_spec, \
             patch.object(main_module, '_run', return_value=fake_perf) as mock_run:
            result = runner.invoke(
                main_module.main,
                ['run', '-t', 'algo', '-s', '2020-01-01', '-e', '2020-02-01'],
            )

        assert result.exit_code == 0, result.output
        assert 'FAKE-PERF' in result.output
        mock_run.assert_called_once()
        mock_get_cal.assert_called_once()
        mock_bench_spec.from_cli_params.assert_called_once()

    def test_run_success_writes_perf_to_file(self, runner, tmp_path):
        fake_perf = self._fake_perf()
        output_file = tmp_path / 'perf.pkl'
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module, 'get_calendar'), \
             patch.object(main_module, 'BenchmarkSpec'), \
             patch.object(main_module, '_run', return_value=fake_perf):
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '-s', '2020-01-01', '-e', '2020-02-01',
                    '-o', str(output_file),
                ],
            )

        assert result.exit_code == 0, result.output
        fake_perf.to_pickle.assert_called_once_with(str(output_file))

    def test_run_success_devnull_output_skips_write(self, runner):
        fake_perf = self._fake_perf()
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module, 'get_calendar'), \
             patch.object(main_module, 'BenchmarkSpec'), \
             patch.object(main_module, '_run', return_value=fake_perf):
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '-s', '2020-01-01', '-e', '2020-02-01',
                    '-o', os.devnull,
                ],
            )

        assert result.exit_code == 0, result.output
        fake_perf.to_pickle.assert_not_called()

    def test_run_with_valid_broker_instantiates_broker_class(self, runner):
        fake_perf = self._fake_perf()
        fake_broker_instance = MagicMock()
        fake_broker_class = MagicMock(return_value=fake_broker_instance)
        fake_module = MagicMock()
        fake_module.IBBroker = fake_broker_class
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module, 'create_args'), \
             patch.object(main_module, 'get_calendar'), \
             patch.object(main_module, 'BenchmarkSpec'), \
             patch.object(main_module, 'import_module', return_value=fake_module), \
             patch.object(main_module, '_run', return_value=fake_perf) as mock_run:
            result = runner.invoke(
                main_module.main,
                [
                    'run', '-t', 'algo', '--broker', 'ib',
                    '--broker-uri', 'uri://x', '--state-file', 'state.pkl',
                    '--realtime-bar-target', '/tmp',
                ],
            )

        assert result.exit_code == 0, result.output
        fake_broker_class.assert_called_once_with('uri://x')
        _, run_kwargs = mock_run.call_args
        assert run_kwargs['broker'] is fake_broker_instance
        assert run_kwargs['state_filename'] == 'state.pkl'


class TestZiplineMagic:
    def test_zipline_magic_cell_mode_success(self):
        with patch.object(main_module, 'load_extensions') as mock_load_ext, \
             patch.object(main_module.run, 'main', return_value='ok') as mock_run_main:
            result = main_module.zipline_magic('--bundle sharadar', cell='print(1)')

        assert result == 'ok'
        mock_load_ext.assert_called_once()
        call_args, call_kwargs = mock_run_main.call_args
        argv = call_args[0]
        assert '--algotext' in argv
        assert 'print(1)' in argv
        assert call_kwargs['standalone_mode'] is False

    def test_zipline_magic_line_mode_uses_local_namespace(self):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module.run, 'main', return_value='ok') as mock_run_main:
            result = main_module.zipline_magic('--bundle sharadar', cell=None)

        assert result == 'ok'
        argv = mock_run_main.call_args[0][0]
        assert '--local-namespace' in argv

    def test_zipline_magic_reraises_nonzero_system_exit_as_value_error(self):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module.run, 'main', side_effect=SystemExit(1)):
            with pytest.raises(ValueError, match='main returned non-zero status code'):
                main_module.zipline_magic('', cell='print(1)')

    def test_zipline_magic_swallows_zero_system_exit(self):
        with patch.object(main_module, 'load_extensions'), \
             patch.object(main_module.run, 'main', side_effect=SystemExit(0)):
            result = main_module.zipline_magic('', cell='print(1)')

        assert result is None
