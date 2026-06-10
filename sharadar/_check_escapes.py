import tokenize, io, os, re

results = []
for root, dirs, files in os.walk('.'):
    for f in files:
        if not f.endswith('.py') or f.startswith('_check'):
            continue
        filepath = os.path.join(root, f)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as fh:
            source = fh.read()
        try:
            tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
        except tokenize.TokenError:
            continue
        for tok in tokens:
            if tok.type == tokenize.STRING:
                s = tok.string
                prefix = ''
                for i, c in enumerate(s):
                    if c in ('"', "'"):
                        prefix = s[:i].lower()
                        break
                if 'r' in prefix or 'b' in prefix:
                    continue
                content = s[len(prefix):]
                if content.startswith('"""') or content.startswith("'''"):
                    inner = content[3:-3]
                elif content.startswith('"') or content.startswith("'"):
                    inner = content[1:-1]
                else:
                    continue
                i = 0
                while i < len(inner):
                    if inner[i] == '\\' and i + 1 < len(inner):
                        next_char = inner[i+1]
                        valid = set("\\'\"\nabfnrtvx01234567NuU ")
                        if next_char not in valid:
                            results.append(f"{filepath}:{tok.start[0]}: invalid escape sequence '\\{next_char}'")
                            break
                        if next_char == 'x':
                            i += 4
                        elif next_char in '01234567':
                            i += 2
                        elif next_char in 'uU':
                            i += 2
                        else:
                            i += 2
                    else:
                        i += 1

for r in sorted(results):
    print(r)
print(f"\nTotal: {len(results)} invalid escape sequences")
