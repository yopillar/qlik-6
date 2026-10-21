"""Génère src/run_all.py : le job principal qui lance tous les scripts migrés
dans l'ordre de leurs dépendances.

L'ordre est déduit sans modèle de work/stores.json (recensé par
prepare.py --collect) : si le script A écrit un fichier que le script B
lit, A passe avant B. C'est l'équivalent de l'enchaînement des tâches de
rechargement dans Qlik.

Usage :
    python tools/orchestrate.py              # -> src/run_all.py
    python src/run_all.py                    # tout lancer
    python src/run_all.py --from ventes      # reprendre à partir d'un script
    python src/run_all.py --only ventes      # un seul script
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

STORES = Path("work/stores.json")
SRC = Path("src")

TEMPLATE = '''"""Job principal — généré par tools/orchestrate.py, ne pas éditer.

Lance les scripts migrés dans l'ordre de leurs dépendances (un script qui
lit le fichier écrit par un autre passe après lui), dans une seule session
Spark. Un script en échec n'arrête pas les autres ; ceux qui en dépendent ne
sont pas lancés. Code retour 1 s'il y a eu un échec.
"""

import argparse
import importlib.util
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

SRC = Path(__file__).resolve().parent
logger = logging.getLogger("run_all")

# Ordre d'exécution : (script, scripts dont il dépend)
ORDER = [
{order}
]
{missing}

def _load(name):
    """Importe src/<name>/job.py."""
    spec = importlib.util.spec_from_file_location(name, SRC / name / "job.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Lance les scripts migrés.")
    p.add_argument("--from", dest="start", help="reprendre à ce script")
    p.add_argument("--only", help="ne lancer que ce script")
    args = p.parse_args(argv)

    names = [n for n, _ in ORDER]
    for opt in (args.start, args.only):
        if opt and opt not in names:
            logger.error("Script inconnu : %s (connus : %s)", opt, names)
            return 1
    if args.only:
        names = [args.only]
    elif args.start:
        names = names[names.index(args.start):]

    spark = SparkSession.builder.appName("run_all").getOrCreate()
    t_all = time.perf_counter()
    deps = dict(ORDER)
    failed, skipped = [], []
    for name in names:
        blocked = [d for d in deps.get(name, []) if d in failed + skipped]
        if blocked:
            # il lirait des données absentes ou anciennes : on ne le lance pas
            logger.error("%s non lancé : dépend de %s en échec", name,
                         ", ".join(blocked))
            skipped.append(name)
            continue
        logger.info("=== %s", name)
        t0 = time.perf_counter()
        try:
            steps_failed = _load(name).main(spark)
        except Exception:
            logger.exception("Échec de %s", name)
            steps_failed = ["job"]
        if steps_failed:
            failed.append(name)
            logger.error("%s : %d étape(s) en échec", name, len(steps_failed))
        else:
            logger.info("%s terminé en %.0fs", name, time.perf_counter() - t0)
    if failed or skipped:
        logger.error("En échec : %s ; non lancés : %s. Reprendre avec --from "
                     "<script> une fois corrigé.", ", ".join(failed) or "aucun",
                     ", ".join(skipped) or "aucun")
        return 1
    logger.info("Terminé : %d script(s) en %.0fs", len(names),
                time.perf_counter() - t_all)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)-7s | %(message)s")
    sys.exit(main())
'''


def order_scripts(io: dict) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Tri topologique des scripts selon leurs fichiers lus/écrits."""
    # même règle que prepare.py : même chemin, ou même nom de fichier
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
        if not ready:      # cycle : on débloque par ordre alphabétique
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
    ready = [(s, d) for s, d in order if (SRC / stem[s] / "job.py").exists()]
    missing = [s for s, _ in order if not (SRC / stem[s] / "job.py").exists()]

    lines = []
    for s, d in ready:
        lines.append(f"    ({stem[s]!r}, {[stem[x] for x in d]!r}),")
    miss_txt = ""
    if missing:
        miss_txt = ("\n# Pas encore migrés (absents de src/) : "
                    + ", ".join(stem[s] for s in missing) + "\n")
    SRC.mkdir(exist_ok=True)
    (SRC / "run_all.py").write_text(TEMPLATE.format(
        order="\n".join(lines), missing=miss_txt), encoding="utf-8")

    print(f"src/run_all.py : {len(ready)} script(s) dans l'ordre :")
    for s, d in ready:
        print(f"  {stem[s]}" + (f"   (après {', '.join(stem[x] for x in d)})"
                                if d else ""))
    blocked = [stem[s] for s, d in ready
               if any(x in missing for x in d)]
    if missing:
        print(f"Pas encore migrés : {', '.join(stem[s] for s in missing)}")
    if blocked:
        print(f"⚠ Dépendent d'un script non migré (liront des données "
              f"absentes ou anciennes) : {', '.join(blocked)}")
    if cycles:
        print(f"⚠ Dépendance circulaire autour de : "
              f"{', '.join(stem[s] for s in cycles)} (ordre forcé)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
