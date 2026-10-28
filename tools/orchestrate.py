"""Genere le job du projet : src/<nom-projet>/job.py et un fichier par
script dans src/<nom-projet>/scripts/.

Le job enchaine tous les scripts assembles, dans l'ordre de leurs
dependances. L'ordre est calcule a partir de work/stores.json (rempli par
prepare.py --collect), sans passer par le modele : si le script A ecrit un
fichier que B lit, A passe avant B. C'est ce que faisait l'enchainement des
taches de rechargement cote Qlik. Les entrees et les etapes de chaque script
sont lues dans work/<script>/assembly.json, ecrit par assemble.py.

Usage :
    python tools/orchestrate.py          # ecrit src/<nom-projet>/
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

STORES = Path("work/stores.json")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from job_template import (  # noqa: E402
    module_name,
    project_dir,
    project_name,
    render_job,
    render_script,
)


def order_scripts(io: dict) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Trie les scripts pour que chacun passe apres ceux qu'il lit."""

    # Meme rapprochement que dans prepare.py : meme chemin, ou meme nom de
    # fichier sans tenir compte de l'extension ni de la casse.
    def stem(path: str) -> str:
        """Nom de fichier sans dossier, extension ni casse."""
        return re.sub(
            r"\.(qvd|txt|csv|parquet|xlsx?)$",
            "",
            re.split(r"[\\/]", path)[-1],
            flags=re.I,
        ).lower()

    writers: dict[str, set[str]] = {}
    for script, d in io.items():
        for w in d["writes"]:
            writers.setdefault(w, set()).add(script)
            writers.setdefault(stem(w), set()).add(script)
    deps = {
        s: sorted(
            {
                w
                for r in d["reads"]
                for key in (r, stem(r))
                for w in writers.get(key, ())
                if w != s
            }
        )
        for s, d in io.items()
    }
    order: list[tuple[str, list[str]]] = []
    done: set[str] = set()
    cycles: list[str] = []
    while len(order) < len(deps):
        ready = sorted(s for s in deps if s not in done and set(deps[s]) <= done)
        if not ready:  # cycle : on en sort par ordre alphabetique
            blocked = sorted(s for s in deps if s not in done)
            cycles.append(blocked[0])
            ready = blocked[:1]
        for s in ready:
            order.append((s, deps[s]))
            done.add(s)
    return order, cycles


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    if not STORES.exists():
        print(
            f"{STORES} absent : lance d'abord migrate.py (ou "
            "prepare.py --collect script_qvd/)."
        )
        return 1
    io = json.loads(STORES.read_text(encoding="utf-8"))
    order, cycles = order_scripts(io)
    stem = {s: Path(s).stem for s in io}
    assembled = {s: Path("work") / stem[s] / "assembly.json" for s in io}
    ready = [(s, d) for s, d in order if assembled[s].exists()]
    missing = [s for s, _ in order if not assembled[s].exists()]

    out = project_dir()
    modules = out / "scripts"
    modules.mkdir(parents=True, exist_ok=True)
    # src/<nom-projet>/ et scripts/ sont des packages Python, comme dp_test.
    # On ne cree un __init__.py que s'il manque, pour ne rien ecraser.
    for package, doc in (
        (out, f"Projet {project_name()} migre depuis Qlik."),
        (modules, "Scripts Qlik du projet, un module par script."),
    ):
        if not (package / "__init__.py").exists():
            (package / "__init__.py").write_text(f'"""{doc}"""\n', encoding="utf-8")
    module = {s: module_name(stem[s]) for s in io}
    code: dict[Path, str] = {}
    for s, _ in ready:
        a = json.loads(assembled[s].read_text(encoding="utf-8"))
        code[modules / f"{module[s]}.py"] = render_script(
            Path(s).name, a["inputs"], a["steps"]
        )
    code[out / "job.py"] = render_job(
        project_name(), [(module[s], [module[x] for x in d]) for s, d in ready]
    )
    # on s'assure que tout compile avant d'ecrire quoi que ce soit
    for path, text in code.items():
        compile(text, str(path), "exec")
    # un script qui n'est plus assemble ne doit pas rester dans scripts/
    for old in modules.glob("*.py"):
        if old.name != "__init__.py" and old not in code:
            old.unlink()
    for path, text in code.items():
        path.write_text(text, encoding="utf-8")

    print(f"{out}/job.py : {len(ready)} script(s) dans l'ordre :")
    for s, d in ready:
        print(
            f"  {stem[s]}"
            + (f"   (apres {', '.join(stem[x] for x in d)})" if d else "")
        )
    blocked = [stem[s] for s, d in ready if any(x in missing for x in d)]
    if missing:
        print(f"Pas encore assembles : {', '.join(stem[s] for s in missing)}")
    if blocked:
        print(
            f"[!] Dependent d'un script non assemble (liront des donnees "
            f"absentes ou anciennes) : {', '.join(blocked)}"
        )
    if cycles:
        print(
            f"[!] Dependance circulaire autour de : "
            f"{', '.join(stem[s] for s in cycles)} (ordre force)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
