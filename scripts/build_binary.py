"""Build the closed-source Gatelodge binary (Nuitka standalone).

Both editions ship as COMPILED binaries — machine code, no distributed
source or bytecode (v1.5 closed distribution; source is disclosed only
under contract / external-audit conditions):

    community  (this repo)    gateway/__main__.py — full governance
                              core, 30-day trial, license-gated Pro
                              extras. Enterprise code is NOT PRESENT
                              in this artifact (separate repos).

    enterprise (the fork)     enterprise_main.py — gateway_enterprise
                              (tier D, SCIM, session budgets) + the
                              enterprise license gate. Refuses to boot
                              without an enterprise license.

Usage:
    uv run scripts/build_binary.py                # auto-detect edition
    uv run scripts/build_binary.py --edition community
    uv run scripts/build_binary.py --debug        # fast diagnostics build

Output: dist/gatelodge-<edition>/ — a portable folder (zip to ship).
The data/ tree (config, license.json, models) is RUNTIME state: it
lives NEXT TO the exe and is never shipped. Standalone (not onefile)
on purpose: onefile extracts to a per-boot temp dir, which would
orphan data/ between boots; config.py anchors PROJECT_ROOT to the
exe's directory under __compiled__ anyway, but the folder build keeps
startup fast and the tree inspectable.

Nuitka needs a C toolchain — MSVC if installed, else it downloads
MinGW64 (--assume-yes-for-downloads). First build: ~5-15 min.
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
DIST = ROOT / "dist"


def _edition_from_repo() -> str:
    return "enterprise" if (ROOT / "enterprise_main.py").is_file() \
        else "community"


def _version() -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    return m.group(1) if m else "0.0.0"


def build(edition: str, debug: bool) -> int:
    if edition not in ("community", "enterprise"):
        raise SystemExit(f"unknown edition: {edition}")
    entry = "run_gateway.py" if edition == "community" \
        else "enterprise_main.py"
    if not (ROOT / entry).is_file():
        raise SystemExit(f"entry point missing for {edition}: {entry}")

    version = _version()
    exe = f"gatelodge-{edition}{'.exe' if sys.platform == 'win32' else ''}"
    print(f"building Gatelodge {edition} v{version} ({entry}) -> dist/")

    cmd = [sys.executable, "-m", "nuitka",
           "--standalone",
           "--assume-yes-for-downloads",
           f"--output-dir={BUILD}",
           f"--output-filename={exe}",
           # the console shell is a data file, not code — ships readable
           # by design (operators can audit what runs in their browser)
           "--include-data-files=gateway/console.html=gateway/console.html",
           # dynamic imports Nuitka cannot see statically:
           # uvicorn's loop/protocol auto-loading, keyring's backends
           "--include-package=uvicorn",
           "--include-package=keyring",
           "--include-package=keyring.backends",
           # Windows version metadata
           f"--company-name=Gatelodge",
           f"--product-name=Gatelodge {edition.title()}",
           f"--product-version={version}",
           f"--file-version={version}",
           f"--file-description=Gatelodge {edition} edition "
           f"(closed distribution)",
           ]
    if debug:
        cmd.append("--debug")           # much faster, unoptimized
    cmd.append(entry)

    print(" ".join(cmd))
    rc = subprocess.call(cmd, cwd=ROOT)
    if rc != 0:
        return rc

    # Nuitka names the dist folder after the entry script — normalize.
    src = BUILD / (Path(entry).stem + ".dist")
    dst = DIST / f"gatelodge-{edition}"
    if dst.exists():
        shutil.rmtree(dst)
    if not src.is_dir():
        raise SystemExit(f"nuitka produced no dist folder at {src}")
    DIST.mkdir(exist_ok=True)
    shutil.move(str(src), str(dst))
    exe_path = dst / exe
    print(f"\nbuilt: {exe_path}")
    print(f"ship:  zip {dst.name}/ (the data/ tree is runtime state — "
          f"the operator provides config + license next to the exe)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Gatelodge closed-binary build")
    p.add_argument("--edition", choices=["community", "enterprise"],
                   default=_edition_from_repo())
    p.add_argument("--debug", action="store_true",
                   help="Nuitka --debug (fast, unoptimized diagnostics)")
    a = p.parse_args()
    return build(a.edition, a.debug)


if __name__ == "__main__":
    raise SystemExit(main())
