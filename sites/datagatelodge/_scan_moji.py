"""Scan site files: distinct non-ASCII sequences + counts, to build the
mojibake repair map."""
import collections
import pathlib

SITE = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise\site")

for f in sorted(SITE.glob("*.html")) + sorted(SITE.glob("*.js")) \
        + sorted(SITE.glob("*.css")):
    if f.name.startswith("_"):
        continue
    text = f.read_text(encoding="utf-8", errors="replace")
    runs = collections.Counter()
    i = 0
    while i < len(text):
        if ord(text[i]) > 127:
            j = i
            while j < len(text) and ord(text[j]) > 127:
                j += 1
            runs[text[i:j]] += 1
            i = j
        else:
            i += 1
    if runs:
        print(f"\n== {f.name}")
        for run, n in runs.most_common(30):
            print(f"  {n:4}x {run!r}")
