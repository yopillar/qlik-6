"""Génère src/<nom-projet>/job.py, le job unique du projet.

Ce job enchaîne tous les scripts assemblés, dans l'ordre de leurs
dépendances. L'ordre est calculé à partir de work/stores.json (rempli par
prepare.py --collect), sans passer par le modèle : si le script A écrit un
fichier que B lit, A passe avant B. C'est ce que faisait l'enchaînement des
tâches de rechargement côté Qlik. Les entrées et les étapes de chaque script
sont lues dans work/<script>/assembly.json, écrit par assemble.py.

Usage :
    python tools/orchestrate.py          # écrit src/<nom-projet>/job.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

STORES = Path("work/stores.json")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_template import JOB, project_dir, project_name  # noqa: E402


def order_scripts(io: dict) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Trie les scripts pour que chacun passe après ceux qu'il lit."""
    # Même rapprochement que dans prepare.py : même chemin, ou même nom de
    # fichier sans tenir compte de l'extension ni de la casse.
    def stem(path):
        return re.sub(r"\.(qvd|txt|csv|parquet|xlsx?)$", "",
                      re.split(r"[\\/]", path)[-1], flags=re.I).lower()
    writers = {}
    for script, d in io.items():
        for w in d["writes"]:
            writers.setdefault(w, set()).add(script)
            writers.setdefault(stem(w), set()).add(script)
    deps = {s: sorted({w for r in d["reads"]
                       for key in (r, stem(r))
                       for w in writers.get(key, ()) if w != s})
            for s, d in io.items()}
    order, done, cycles = [], set(), []
    while len(order) < len(deps):
        ready = sorted(s for s in deps if s not in done
                       and set(deps[s]) <= done)
        if not ready:      # cycle : on en sort par ordre alphabétique
            blocked = sorted(s for s in deps if s not in done)
            cycles.append(blocked[0])
            ready = blocked[:1]
        for s in ready:
            order.append((s, deps[s]))
            done.add(s)
    return order, cycles


def main(argv=None) -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    if not STORES.exists():
        print(f"{STORES} absent : lance d'abord migrate.py (ou "
              "prepare.py --collect script_qvd/*.qvs).")
        return 1
    io = json.loads(STORES.read_text(encoding="utf-8"))
    order, cycles = order_scripts(io)
    stem = {s: Path(s).stem for s in io}
    assembled = {s: Path("work") / stem[s] / "assembly.json" for s in io}
    ready = [(s, d) for s, d in order if assembled[s].exists()]
    missing = [s for s, _ in order if not assembled[s].exists()]

    blocks = []
    for s, d in ready:
        a = json.loads(assembled[s].read_text(encoding="utf-8"))
        inputs = "\n".join(f"            {v!r}: {io_name!r},"
                           for v, io_name in a["inputs"].items())
        steps = "\n".join(f"            {tuple(st)!r}," for st in a["steps"])
        blocks.append(f"    {stem[s]!r}: {{\n"
                      f"        \"depends_on\": {[stem[x] for x in d]!r},\n"
                      f"        \"inputs\": {{\n{inputs}\n        }},\n"
                      f"        \"steps\": [\n{steps}\n        ],\n    }},")
    out = project_dir()
    out.mkdir(parents=True, exist_ok=True)
    # src/<nom-projet>/ est un package Python, comme dp_test. On ne crée le
    # __init__.py que s'il manque, pour ne pas écraser ce qu'on y aurait mis.
    if not (out / "__init__.py").exists():
        (out / "__init__.py").write_text(
            f'"""Projet {project_name()} migré depuis Qlik."""\n',
            encoding="utf-8")
    code = JOB.format(project=project_name(), scripts="\n".join(blocks))
    # on s'assure que le job compile avant de l'écrire
    compile(code, str(out / "job.py"), "exec")
    (out / "job.py").write_text(code, encoding="utf-8")

    print(f"{out}/job.py : {len(ready)} script(s) dans l'ordre :")
    for s, d in ready:
        print(f"  {stem[s]}" + (f"   (après {', '.join(stem[x] for x in d)})"
                                if d else ""))
    blocked = [stem[s] for s, d in ready if any(x in missing for x in d)]
    if missing:
        print(f"Pas encore assemblés : {', '.join(stem[s] for s in missing)}")
    if blocked:
        print(f"⚠ Dépendent d'un script non assemblé (liront des données "
              f"absentes ou anciennes) : {', '.join(blocked)}")
    if cycles:
        print(f"⚠ Dépendance circulaire autour de : "
              f"{', '.join(stem[s] for s in cycles)} (ordre forcé)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
