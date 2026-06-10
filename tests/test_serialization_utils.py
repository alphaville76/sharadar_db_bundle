import os
import tempfile
from sharadar.util.serialization_utils import load_context, store_context


class TestSerializationUtils:
    def test_store_and_load_context(self):
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
            path = f.name

        try:
            class Context:
                pass

            ctx = Context()
            ctx.whitelist = ['PARAM']
            ctx.PARAM = {'loss_limit': -0.05, 'max_positions': 10}

            store_context(path, ctx)
            assert os.path.exists(path)

            # Load it back
            ctx2 = Context()
            ctx2.PARAM = {}
            load_context(path, ctx2)
            assert ctx2.PARAM == {'loss_limit': -0.05, 'max_positions': 10}
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_store_without_whitelist_does_nothing(self):
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
            path = f.name

        try:
            class Context:
                pass

            ctx = Context()
            ctx.PARAM = {'key': 'value'}

            store_context(path, ctx)
            # File should exist but be empty (nothing written)
            assert os.path.getsize(path) == 0
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_load_nonexistent_file_raises(self):
        class Context:
            pass
        ctx = Context()
        import pytest
        with pytest.raises(FileNotFoundError):
            load_context('/nonexistent/path.pkl', ctx)