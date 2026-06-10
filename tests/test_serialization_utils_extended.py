import pickle
from unittest.mock import MagicMock


from sharadar.util.serialization_utils import load_context, store_context


class TestStoreContext:
    def test_store_with_whitelist(self, tmp_path):
        state_file = tmp_path / "state.pkl"
        context = MagicMock()
        context.whitelist = ["field1"]
        context.field1 = "value1"

        store_context(str(state_file), context)

        assert state_file.exists()
        with open(state_file, "rb") as f:
            data = pickle.load(f)
        assert data == {"field1": "value1"}

    def test_no_whitelist_returns_early(self, tmp_path):
        state_file = tmp_path / "state.pkl"
        context = MagicMock(spec=[])  # no attributes

        store_context(str(state_file), context)

        assert not state_file.exists()

    def test_multiple_fields(self, tmp_path):
        state_file = tmp_path / "state.pkl"
        context = MagicMock()
        context.whitelist = ["alpha", "beta", "gamma"]
        context.alpha = 1
        context.beta = [2, 3]
        context.gamma = {"key": "val"}

        store_context(str(state_file), context)

        with open(state_file, "rb") as f:
            data = pickle.load(f)
        assert data == {"alpha": 1, "beta": [2, 3], "gamma": {"key": "val"}}


class TestLoadContext:
    def test_load_sets_attributes(self, tmp_path):
        state_file = tmp_path / "state.pkl"
        state = {"x": 10, "y": "hello"}
        with open(state_file, "wb") as f:
            pickle.dump(state, f)

        context = MagicMock()
        load_context(str(state_file), context)

        assert context.x == 10
        assert context.y == "hello"

    def test_round_trip(self, tmp_path):
        state_file = tmp_path / "state.pkl"

        # Store
        store_ctx = MagicMock()
        store_ctx.whitelist = ["name", "count"]
        store_ctx.name = "test"
        store_ctx.count = 42
        store_context(str(state_file), store_ctx)

        # Load
        class Obj:
            pass

        load_ctx = Obj()
        load_context(str(state_file), load_ctx)

        assert load_ctx.name == "test"
        assert load_ctx.count == 42
