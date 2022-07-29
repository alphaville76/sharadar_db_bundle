import pickle
import json

# Persistence blacklist/whitelist and excludes gives a way to include/
# exclude (so do not persist on disk if initiated or excluded from the serialization
# function that reinstate or save the context variable to its last state).
# trading client can never be serialized, the initialized function and
# perf tracker remember the context variables and the past performance
# and need to be whitelisted
_context_blacklist = ['trading_client']
_context_whitelist = ['initialized', 'perf_tracker']


def load_context(state_file_path, context):
    with open(state_file_path, 'rb') as f:
        loaded_state = pickle.load(f)

        for k, v in loaded_state.items():
            setattr(context, k, v)


def store_context(state_file_path, context):
    exclude_list = _context_blacklist + [e for e in context.__dict__.keys() if e not in _context_whitelist]
    fields_to_store = list(set(context.__dict__.keys()) - set(exclude_list))

    state = {}
    for field in fields_to_store:
        state[field] = getattr(context, field)

    with open(state_file_path, 'wb') as f:
        pickle.dump(state, f)


if __name__ == '__main__':
    import sys

    context_file_path = sys.argv[1]
    with open(context_file_path, 'rb') as f:
        context = pickle.load(f)

    import pprint

    pp = pprint.PrettyPrinter()
    pp.pprint(context)
