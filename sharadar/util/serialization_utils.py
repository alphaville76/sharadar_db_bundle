import pickle
import warnings


def load_context(state_file_path, context):
    '''
    Load context state from a pickle file.
    
    Parameters
    ----------
    state_file_path : str
        Path to the pickle file containing saved state.
    context : object
        Context object to restore state to.
    '''
    with open(state_file_path, 'rb') as f:
        loaded_state = pickle.load(f)

        for k, v in loaded_state.items():
            setattr(context, k, v)


def store_context(state_file_path, context):
    '''
    Store context state to a pickle file.
    Only stores fields listed in context.whitelist.
    
    Parameters
    ----------
    state_file_path : str
        Path where to save the pickle file.
    context : object
        Context object with whitelist attribute listing fields to save.
    '''
    state = {}
    if not hasattr(context, 'whitelist'):
        warnings.warn('Context has no whitelist attribute, nothing will be saved.')
        return

    for field in context.whitelist:
        if hasattr(context, field):
            state[field] = getattr(context, field)
        else:
            warnings.warn(f'Field "{field}" in whitelist not found in context, skipping.')

    with open(state_file_path, 'wb') as f:
        pickle.dump(state, f)


if __name__ == '__main__':
    print('Enter file path:')
    context_file_path = str(input())
    with open(context_file_path, 'rb') as f:
        context = pickle.load(f)

    import pprint
    pp = pprint.PrettyPrinter()
    pp.pprint(context)
