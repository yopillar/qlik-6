"""Lance toute la migration d'un script, ou d'un dossier de scripts.

    python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois
    python tools/migrate.py script_qvd/ventes.qvs             # ensuite
    python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier

Avant tout, fill_conf.py ajoute dans conf/io.yml et dans les schémas ce
qui manque pour les scripts du dossier (jamais de modification de
l'existant, --no-conf pour s'en passer). Un fichier écrit par un script et
relu par un autre n'est déclaré en entrée qu'une fois son auteur traduit,
avec les types exacts de la requête Spark qui l'écrit : fill_conf.py est
donc relancé avant chaque script. Puis, pour chaque script :
  1. prepare.py    découpe le script et traduit en SQL tout ce qu'on sait
                   traduire sans le modèle (le reste est marqué TODO(model))
  2. translate.py  fait compléter les TODO par le modèle, une table par
                   appel, avec un contrôle et une relance si ça ne passe pas
  3. assemble.py   copie les requêtes dans src/<nom-projet>/sql/ quand tout
                   est validé, puis orchestrate.py régénère job.py
  4. verify.py     compare avec Qlik, s'il y a des sorties dans
                   data/qlik_outputs/

Au bout, src/<nom-projet>/job.py contient tous les scripts assemblés, dans
l'ordre de leurs dépendances.

On peut relancer autant de fois qu'on veut : ce qui est déjà traduit et n'a
pas bougé n'est pas refait.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
from conf import WORK, find_scripts  # noqa: E402
from job_template import project_dir  # noqa: E402
from orchestrate import STORES, order_scripts  # noqa: E402


def run(script: str, *args: str) -> int:
    return subprocess.run([sys.executable, str(TOOLS / script), *args]).returncode


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un script Qlik ou le dossier script_qvd/")
    p.add_argument("--pilot", type=int, help="nombre de tables, puis arrêt")
    p.add_argument("--jobs", type=int, default=1,
                   help="tables traduites en parallèle")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--opencode-bin", default="opencode")
    p.add_argument("--skip-selftest", action="store_true",
                   help="ne pas tester le modèle avant de traduire")
    p.add_argument("--no-conf", action="store_true",
                   help="ne pas compléter conf/io.yml ni les schémas")
    p.add_argument("--var", action="append", default=[], metavar="NOM=VALEUR",
                   help="variable des locations de conf/io.yml, pour la "
                        "comparaison (ex. --var sorties=data/output)")
    args = p.parse_args(argv)

    scripts = find_scripts(args.path)
    if not scripts:
        print(f"Aucun script Qlik dans {args.path} : on prend les fichiers "
              ".qvs, .txt ou sans extension.")
        return 1
    qlik_dir = Path("data") / "qlik_outputs"
    compare = qlik_dir.is_dir() and any(qlik_dir.iterdir())

    # On recense tous les scripts du dossier, même si on n'en migre qu'un :
    # quand un script lit un fichier écrit par un autre, il doit le lire dans
    # data/output.
    folder = args.path if args.path.is_dir() else args.path.parent
    run("prepare.py", "--collect", *map(str, find_scripts(folder)))
    # On traite les scripts dans l'ordre de leurs dépendances, comme le job :
    # verify.py lance un script seul, et il doit trouver les fichiers écrits
    # par ceux dont il dépend.
    if STORES.exists():
        rank = {s: i for i, (s, _) in enumerate(order_scripts(
            json.loads(STORES.read_text(encoding="utf-8")))[0])}
        scripts.sort(key=lambda s: rank.get(s.name, len(rank)))
    # On complète conf/io.yml et les schémas avant de traduire, puisque la
    # traduction s'en sert (colonnes des LOAD *, types des dates). Seulement
    # des ajouts : ce qui est déjà déclaré n'est jamais modifié.
    conf_state = "non touchée (--no-conf)"
    if not args.no_conf:
        if not Path("conf").is_dir():
            # Tous les chemins du kit partent du dossier courant : lancé
            # ailleurs qu'à la racine du projet, il écrirait un conf/ et un
            # src/ à côté des vrais.
            print(f"⚠ pas de dossier conf/ dans {Path.cwd()} : migrate.py se "
                  "lance depuis la racine du projet. Un conf/io.yml va être "
                  "créé ici.")
        rc = run("fill_conf.py", str(folder), *[f"--var={v}" for v in args.var])
        conf_state = (f"complétée ({Path('conf/io.yml').resolve()})" if rc == 0
                      else "PAS complétée, voir le message de fill_conf.py "
                           "plus haut")

    summary, stopped = [], False
    for script in scripts:
        print(f"\n{'#' * 70}\n# {script.name}\n{'#' * 70}")
        if not args.no_conf:
            # Les fichiers écrits par un script déjà traduit sont déclarés
            # maintenant, avec les types exacts de sa requête Spark.
            run("fill_conf.py", str(folder), "--quiet",
                *[f"--var={v}" for v in args.var])
        if run("prepare.py", str(script)):
            summary.append((script.name, "échec de la préparation", []))
            continue
        workdir = WORK / script.stem
        extra = ["--opencode-bin", args.opencode_bin, "--jobs", str(args.jobs)]
        if args.skip_selftest:
            extra.append("--skip-selftest")
        if args.model:
            extra += ["--model", args.model]
        if args.pilot:
            extra += ["--limit", str(args.pilot)]
        if run("translate.py", str(workdir), *extra):
            # Le modèle ne répond pas, pas la peine d'enchaîner sur les scripts
            # suivants. On régénère quand même le job en sortant de la boucle.
            summary.append((script.name, "traduction arrêtée : le modèle ne "
                            "répond pas (voir le test ci-dessus)", []))
            stopped = True
            break

        # Spark parse toutes les requêtes avant l'assemblage. Une traduction
        # bancale se voit ici, et pas au premier lancement du job.
        run("check.py", str(workdir), "--parse", "--mark")
        manifest = json.loads((workdir / "manifest.json").read_text(
            encoding="utf-8"))
        todo = sum(s["status"] == "todo" for s in manifest["sections"])
        to_fix = [s["id"] for s in manifest["sections"]
                  if s["status"] in ("escalade", "erreur_modele")]
        if todo or to_fix:
            summary.append((script.name, f"{todo} à traduire, {len(to_fix)} "
                            "à corriger à la main", to_fix))
        elif run("assemble.py", str(workdir)):
            summary.append((script.name, "non assemblé : entrées ou sorties "
                            "absentes de conf/io.yml (voir ci-dessus)", []))
        else:
            run("orchestrate.py")      # ajoute ce script au job
            state = "assemblé dans src/"
            if compare:
                rc = run("verify.py", str(workdir),
                         *[f"--var={v}" for v in args.var])
                verdict = {0: "données identiques à Qlik",
                           1: "écarts avec Qlik : voir comparaison.md",
                           3: "pas d'export Qlik dans data/qlik_outputs/ "
                              "pour comparer"}.get(
                    rc, "des étapes du job ont échoué (voir la trace), les "
                        "sorties produites sont identiques à Qlik")
                state += f", {verdict}"
            summary.append((script.name, state, []))
        if args.pilot:
            break

    print()
    run("orchestrate.py")      # régénère src/<nom-projet>/job.py

    print(f"\n{'=' * 70}\nRÉSUMÉ")
    print(f"Conf : {conf_state}")
    for name, state, to_fix in summary:
        print(f"- {name} : {state}")
        for sid in to_fix[:15]:
            print(f"    à corriger : {WORK / Path(name).stem / sid}/query.sql")
    ready = sum("assemblé" in state for _, state, _ in summary)
    print(f"\nJob du projet : {(project_dir() / 'job.py').resolve()} "
          f"({'contient les scripts assemblés' if ready else 'aucun script assemblé pour l’instant'})")
    print(f"Journal : {WORK / 'journal.csv'}")
    if not compare:
        print("Pas de dossier data/qlik_outputs/ : comparaison des données "
              "non faite.")
    if args.pilot:
        print("Pilote terminé : relis les query.sql, puis relance sans --pilot "
              "(le script n'est assemblé, et ajouté au job, qu'une fois "
              "toutes ses tables validées).")
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
