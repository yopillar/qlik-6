"""Assemble un script traduit dans src/<script>/ : les .sql + job.py.

Chaque section validée (ok ou deterministic) devient src/<script>/NN_<table>.sql,
dans l'ordre du script. job.py (gabarit : tools/job_template.py) expose les
fichiers lus comme vues, exécute les requêtes dans l'ordre et écrit les
sorties. Mécanique pure : aucune décision n'est laissée au modèle.

Usage :
    python tools/assemble.py work/ventes             # -> src/ventes/
    python tools/assemble.py work/ventes --partial   # ignore les tables non validées
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_template import JOB  # noqa: E402


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--partial", action="store_true",
                   help="assembler même si des tables ne sont pas validées")
    args = p.parse_args(argv)

    manifest = json.loads((args.workdir / "manifest.json").read_text(
        encoding="utf-8"))
    secs = [s for s in manifest["sections"] if s["status"] != "skip"]
    pending = [s["id"] for s in secs
               if s["status"] not in ("ok", "deterministic")]
    if pending and not args.partial:
        print(f"{len(pending)} table(s) non validée(s) : {pending[:8]}…\n"
              "Assemblage annulé (--partial pour forcer).")
        return 1

    stem = Path(manifest["script"]).stem
    out = Path("src") / stem
    if out.exists():
        for old in out.glob("*.sql"):
            old.unlink()
    out.mkdir(parents=True, exist_ok=True)

    inputs, steps, k = {}, [], 0
    for s in secs:
        if s["id"] in pending:
            continue
        k += 1
        name = f"{k:02d}_{s['id'].split('_', 1)[1]}.sql"
        shutil.copyfile(args.workdir / s["id"] / "query.sql", out / name)
        for view, src in s.get("views", {}).items():
            if "path" in src:
                inputs[view] = (src["path"], src.get("format", "parquet"))
        output = s.get("outputs", {}).get(s["table"])
        fmt = s.get("output_formats", {}).get(s["table"], "parquet")
        steps.append(f"    ({name!r}, {s['view']!r}, {output!r}, {fmt!r}),")

    (out / "job.py").write_text(JOB.format(
        stem=stem, script=manifest["script"],
        inputs="\n".join(f"    {v!r}: {p!r}," for v, p in sorted(inputs.items())),
        steps="\n".join(steps)), encoding="utf-8")
    print(f"{out}/ : {len(steps)} requête(s) + job.py"
          + (f", {len(pending)} ignorée(s)" if pending else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
