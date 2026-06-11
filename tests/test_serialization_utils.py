import os
import pytest
import tempfile
import warnings
from sharadar.util.serialization_utils import load_context, store_context


class MockContext:
    def __init__(self):
        self.whitelist = ['field1', 'field2']
        self.field1 = 'value1'
        self.field2 = {'key': 'value'}


class TestStoreContext:
    def test_store_and_load_context(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pkl') as f:
            filepath = f.name
        
        try:
            context = MockContext()
            store_context(filepath, context)
            
            # Load into new context
            new_context = type('Context', (), {})()
            load_context(filepath, new_context)
            
            assert new_context.field1 == 'value1'
            assert new_context.field2 == {'key': 'value'}
        finally:
            os.unlink(filepath)

    def test_store_without_whitelist(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pkl') as f:
            filepath = f.name
        
        try:
            context = type('Context', (), {'field1': 'value1'})()
            # No whitelist attribute
            
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')
                store_context(filepath, context)
                assert len(w) == 1
                assert 'whitelist' in str(w[0].message)
        finally:
            if os.path.exists(filepath):
                os.unlink(filepath)

    def test_store_with_missing_field_in_whitelist(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pkl') as f:
            filepath = f.name
        
        try:
            context = type('Context', (), {
                'whitelist': ['field1', 'missing_field'],
                'field1': 'value1'
            })()
            
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')
                store_context(filepath, context)
                assert len(w) == 1
                assert 'missing_field' in str(w[0].message)
            
            # Should still save field1
            new_context = type('Context', (), {})()
            load_context(filepath, new_context)
            assert new_context.field1 == 'value1'
            assert not hasattr(new_context, 'missing_field')
        finally:
            os.unlink(filepath)

    def test_load_overwrites_existing_attributes(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pkl') as f:
            filepath = f.name
        
        try:
            context = MockContext()
            store_context(filepath, context)
            
            new_context = type('Context', (), {'field1': 'old_value'})()
            load_context(filepath, new_context)
            
            assert new_context.field1 == 'value1'  # overwritten
        finally:
            os.unlink(filepath)


class TestLoadContext:
    def test_load_nonexistent_file_raises(self):
        context = type('Context', (), {})()
        with pytest.raises(FileNotFoundError):
            load_context('/nonexistent/path.pkl', context)
