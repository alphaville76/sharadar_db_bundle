def load_ipython_extension(ipython):
    from .__main__ import zipline_magic
    ipython.register_magic_function(zipline_magic, 'line_cell', 'zipline')