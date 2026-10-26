"""Journal des tables traduites, dans work/journal.csv (separateur ;).

Une ligne par couple (script, section). Une mise a jour ne touche qu'aux
colonnes qu'on lui passe : la colonne "resolution", qu'on remplit a la
main, n'est donc jamais ecrasee.
"""

from __future__ import annotations

import csv
from pathlib import Path

FIELDS = [
    "date",
    "script",
    "section",
    "status",
    "attempts",
    "seconds",
    "compare",
    "problem",
    "resolution",
]


def update(path: Path, row: dict) -> None:
    rows = []
    if path.exists():
        with path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f, delimiter=";"))
    for r in rows:
        if r.get("script") == row["script"] and r.get("section") == row["section"]:
            r.update({k: v for k, v in row.items() if k != "resolution"})
            break
    else:
        rows.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=FIELDS, delimiter=";", restval="", extrasaction="ignore"
        )
        w.writeheader()
        w.writerows(rows)
