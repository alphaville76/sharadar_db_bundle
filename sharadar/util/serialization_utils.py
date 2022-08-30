import pickle
import json

def load_context(state_file_path, context):
    with open(state_file_path, 'rb') as f:
        loaded_state = pickle.load(f)

        for k, v in loaded_state.items():
            setattr(context, k, v)


def store_context(state_file_path, context):
    state = {}
    if not hasattr(context, 'whitelist'):
        return

    for field in context.whitelist:
        state[field] = getattr(context, field)

    with open(state_file_path, 'wb') as f:
        pickle.dump(state, f)


if __name__ == '__main__':
    import sys

    print("Enter file path:")
    #context_file_path = str(input())
    context_file_path = '/home/c.cerbo/zipline-reloaded-venv3.9/lib/python3.9/site-packages/sharadar_db_bundle/algo/cluster/clusters13g.state'
    with open(context_file_path, 'rb') as f:
        context = pickle.load(f)

    import pprint

    pp = pprint.PrettyPrinter()
    pp.pprint(context)
