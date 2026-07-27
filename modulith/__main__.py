"""``python -m modulith`` — the console script's entry point, reachable by module.

Identical to running the ``modulith`` command installed by
``[project.scripts]``; both call :func:`modulith.cli.main`. This form needs no
directory on ``PATH``, which is what makes it work when the console script is
shadowed, when the virtualenv was never activated
(``/srv/app/venv/bin/python -m modulith``), or inside a sandbox that only
permits invoking an interpreter by path.
"""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    main()
