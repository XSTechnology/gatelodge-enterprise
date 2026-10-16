"""Repair mojibake in site HTML files: each non-ASCII run that
round-trips (cp1252-encode -> utf8-decode) is the original character.
JS/CSS are already clean; only HTML files are affected."""
import pathlib
import re

SITE = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise\site")

# runs the generic cp1252->utf8 round-trip cannot handle (cp1252
# undefined bytes that passed through as control chars)
PRE = {"â€\x9d": "\u201d",      # right double quote
       "â†\x90": "\u2190"}      # left arrow

RUN = re.compile(r"[^\x00-\x7f]+")

total_runs = 0
fixed_runs = 0
left_runs = []

for f in sorted(SITE.glob("*.html")):
    if f.name.startswith("_") or f.name.startswith("google"):
        continue
    text = f.read_text(encoding="utf-8")
    for bad, good in PRE.items():
        text = text.replace(bad, good)

    def repair(m):
        global total_runs, fixed_runs
        run = m.group(0)
        if run == "\ufeff":
            return run                     # BOM stays
        total_runs += 1
        try:
            fixed = run.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            left_runs.append((f.name, run[:20]))
            return run
        fixed_runs += 1
        return fixed

    new = RUN.sub(repair, text)
    if new != text:
        f.write_text(new, encoding="utf-8", newline="")
        print(f"fixed: {f.name}")
    else:
        print(f"clean: {f.name}")

print(f"\nruns: {total_runs}, repaired: {fixed_runs}, left: {len(left_runs)}")
for name, run in left_runs:
    print("  LEFT:", name, repr(run))
