"""python run_gateway.py — the community edition's executable entry.

Identical to ``python -m gateway``: imports the package entry point
and calls main(). This file exists for the COMPILED distribution
(scripts/build_binary.py): Nuitka compiles THIS script as the exe,
because gateway/__main__.py uses relative imports (``from .config
import …``) that only resolve inside the package — a compiled
__main__.py would have no parent package and crash at boot. The
launcher keeps the package imported AS a package.
"""
from gateway.__main__ import main

if __name__ == "__main__":
    main()
