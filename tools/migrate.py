"""Migration complète en UNE commande.

    python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # 1re fois
    python tools/migrate.py script_qvd/ventes.qvs             # le reste
    python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier

Enchaîne pour chaque script :
  1. prepare.py   découpage et traduction Qlik -> SQL sans modèle ;
                  les parties non traduites sont marquées TODO(model)
  2. translate.py le modèle complète ces parties, une table par appel,
                  contrôle, 1 relance
  3. assemble.py  src/<script>/ (.sql + job.py), dès que tout est validé
  4. verify.py    si qlik_outputs/ existe : lance le job, compare à Qlik
Puis orchestrate.py génère src/run_all.py, le job principal qui lance tous
les scripts migrés dans l'ordre de leurs dépendances.

Relançable à volonté : ce qui est déjà traduit et inchangé n'est pas refait.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
WORK = Path("work")


def run(script: str, *args: str) -> int:
    return subprocess.run([sys.executable, str(TOOLS / script), *args]).returncode


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un .qvs ou le dossier script_qvd/")
    p.add_argument("--pilot", type=int, help="nombre de tables, puis arrêt")
    p.add_argument("--jobs", type=int, default=1,
                   help="tables traduites en parallèle")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--opencode-bin", default="opencode")
    args = p.parse_args(argv)

    scripts = sorted(args.path.glob("*.qvs")) if args.path.is_dir() \
        else [args.path]
    if not scripts:
        print(f"Aucun .qvs dans {args.path}")
        return 1
    compare = Path("qlik_outputs").is_dir() and any(Path("qlik_outputs").iterdir())

    # recensement de TOUS les scripts du dossier : un fichier écrit par un
    # script et lu par un autre doit être lu dans data/output
    folder = args.path if args.path.is_dir() else args.path.parent
    run("prepare.py", "--collect", *map(str, sorted(folder.glob("*.qvs"))))

    summary, stopped = [], False
    for script in scripts:
        print(f"\n{'#' * 70}\n# {script.name}\n{'#' * 70}")
        if run("prepare.py", str(script)):
            summary.append((script.name, "échec de la préparation", []))
            continue
        workdir = WORK / script.stem
        extra = ["--opencode-bin", args.opencode_bin, "--jobs", str(args.jobs)]
        if args.model:
            extra += ["--model", args.model]
        if args.pilot:
            extra += ["--limit", str(args.pilot)]
        if run("translate.py", str(workdir), *extra):
            # modèle injoignable ou qui ne répond pas : inutile de continuer
            # avec les autres scripts, mais run_all.py est quand même généré
            summary.append((script.name, "traduction arrêtée : le modèle ne "
                            "répond pas (voir le test ci-dessus)", []))
            stopped = True
            break

        manifest = json.loads((workdir / "manifest.json").read_text(
            encoding="utf-8"))
        todo = sum(s["status"] == "todo" for s in manifest["sections"])
        to_fix = [s["id"] for s in manifest["sections"]
                  if s["status"] in ("escalade", "erreur_modele")]
        if todo or to_fix:
            summary.append((script.name, f"{todo} à traduire, {len(to_fix)} "
                            "à corriger à la main", to_fix))
        else:
            run("assemble.py", str(workdir))
            state = "assemblé dans src/"
            if compare:
                rc = run("verify.py", str(workdir))
                verdict = {0: "données identiques à Qlik",
                           1: "écarts avec Qlik : voir comparaison.md"}.get(
                    rc, "le job a échoué : voir la trace (pyspark-commons "
                        "installé ? données dans data/input ?)")
                state += f", {verdict}"
            summary.append((script.name, state, []))
        if args.pilot:
            break

    print()
    run("orchestrate.py")      # src/run_all.py : le job principal

    print(f"\n{'=' * 70}\nRÉSUMÉ")
    for name, state, to_fix in summary:
        print(f"- {name} : {state}")
        for sid in to_fix[:15]:
            print(f"    à corriger : {WORK / Path(name).stem / sid}/query.sql")
    ready = sum("assemblé" in state for _, state, _ in summary)
    print(f"\nJob principal : python src/run_all.py "
          f"({'contient les scripts assemblés' if ready else 'aucun script assemblé pour l’instant'})")
    print(f"Journal : {WORK / 'journal.csv'}")
    if not compare:
        print("Pas de dossier qlik_outputs/ : comparaison des données non faite.")
    if args.pilot:
        print("Pilote terminé : relis les query.sql, puis relance sans --pilot "
              "(le script n'est assemblé, et ajouté à run_all.py, qu'une fois "
              "toutes ses tables validées).")
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
