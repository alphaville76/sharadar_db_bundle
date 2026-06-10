import os
import tempfile
import pytest
from sharadar.util.serialization_utils import load_context, store_context


class TestSerializationUtils:
    def test_store_and_load_context(self):
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
            path = f.name

        try:
            # Create a mock context
            class Context:
                pass
            
            ctx = Context()
            ctx.PARAM = {'loss_limit': -0.05, 'max_positions': 10}
            
            store_context(path, ctx)
            assert os.path.exists(path)
            
            # Load it back
            ctx2 = Context()
            load_context(path, ctx2)
            assert ctx2.PARAM == {'loss_limit': -0.05, 'max_positions': 10}
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_load_nonexistent_file(self):
        class Context:
            pass
        ctx = Context()
        # Should not raise
        load_context('/nonexistent/path.pkl', ctx)
