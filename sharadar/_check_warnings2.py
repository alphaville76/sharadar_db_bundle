import warnings, sys, os, importlib, traceback

warnings.simplefilter('always')

os.chdir('..')
sys.path.insert(0, '.')

all_warnings = []

# Walk through all python files and try to import them
for root, dirs, files in os.walk('sharadar'):
    for f in files:
        if not f.endswith('.py') or f.startswith('_'):
            continue
        filepath = os.path.join(root, f)
        module_path = filepath.replace(os.sep, '.').replace('/', '.')[:-3]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            try:
                importlib.import_module(module_path)
            except Exception as e:
                pass
            for warning in w:
                # Only report warnings from our code
                if 'sharadar' in str(warning.filename):
                    all_warnings.append(f"{warning.filename}:{warning.lineno}: {warning.category.__name__}: {warning.message}")

for w in sorted(set(all_warnings)):
    print(w)
print(f"\nTotal unique warnings: {len(set(all_warnings))}")
