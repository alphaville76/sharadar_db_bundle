import ast, sys, os, re, warnings, tokenize, io

results = []

def check_invalid_escapes(filepath, source):
    """Check for invalid escape sequences in string literals."""
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except tokenize.TokenError:
        return
    for tok in tokens:
        if tok.type == tokenize.STRING:
            s = tok.string
            # Skip raw strings and bytes
            prefix = ''
            for i, c in enumerate(s):
                if c in ('"', "'"):
                    prefix = s[:i].lower()
                    break
            if 'r' in prefix:
                continue
            # Check for invalid escapes in the string value
            # Simple heuristic: look for backslash followed by non-valid escape char
            content = s[len(prefix):]
            # Remove quotes
            if content.startswith('"""') or content.startswith("'''"):
                quote = content[:3]
                inner = content[3:-3]
            elif content.startswith('"') or content.startswith("'"):
                quote = content[0]
                inner = content[1:-1]
            else:
                continue
            i = 0
            while i < len(inner):
                if inner[i] == '\\' and i + 1 < len(inner):
                    next_char = inner[i+1]
                    valid_escapes = set('\\\'\"abfnrtvx01234567NuU\n')
                    if next_char not in valid_escapes:
                        results.append(f"{filepath}:{tok.start[0]}: DeprecationWarning: invalid escape sequence '\\{next_char}'")
                        break
                    i += 2
                else:
                    i += 1

for root, dirs, files in os.walk('.'):
    for f in files:
        if not f.endswith('.py') or f == '_check_warnings.py':
            continue
        filepath = os.path.join(root, f)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as fh:
            source = fh.read()
        
        # Check invalid escape sequences
        check_invalid_escapes(filepath, source)
        
        # Check 'is' with literals
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                for op, comparator in zip(node.ops, node.comparators):
                    if isinstance(op, (ast.Is, ast.IsNot)):
                        if isinstance(comparator, ast.Constant):
                            if comparator.value not in (None, True, False):
                                op_name = "is" if isinstance(op, ast.Is) else "is not"
                                results.append(f"{filepath}:{node.lineno}: SyntaxWarning: '{op_name}' with literal {repr(comparator.value)}")
                        if isinstance(node.left, ast.Constant):
                            if node.left.value not in (None, True, False):
                                op_name = "is" if isinstance(op, ast.Is) else "is not"
                                results.append(f"{filepath}:{node.lineno}: SyntaxWarning: '{op_name}' with literal (left side) {repr(node.left.value)}")
        
        # Check deprecated imports
        lines = source.split('\n')
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith('#'):
                continue
            if re.search(r'from collections import.*(?:Mapping|MutableMapping|Sequence|MutableSequence|Iterable|Iterator|Callable|Set|MutableSet)', stripped):
                results.append(f"{filepath}:{i}: DeprecationWarning: import from collections.abc instead of collections")

for r in sorted(results):
    print(r)
print(f"\nTotal: {len(results)} warnings")
