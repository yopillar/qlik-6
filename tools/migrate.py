"""Lance toute la migration d'un script, ou d'un dossier de scripts.

    python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois
    python tools/migrate.py script_qvd/ventes.qvs             # ensuite
    python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier

Avant tout, fill_conf.py ajoute dans conf/io.yml et dans les schemas ce
qui manque pour les scripts du dossier (jamais de modification de
l'existant, --no-conf pour s'en passer). Un fichier ecrit par un script et
relu par un autre n'est declare en entree qu'une fois son auteur traduit,
avec les types exacts de la requete Spark qui l'ecrit : fill_conf.py est
donc relance avant chaque script. Puis, pour chaque script :
  1. prepare.py    decoupe le script et traduit en SQL tout ce qu'on sait
                   traduire sans le modele (le reste est marque TODO(model))
  2. translate.py  fait completer les TODO par le modele, une table par
                   appel, avec un controle et une relance si ca ne passe pas
  3. assemble.py   copie les requetes dans src/<nom-projet>/sql/ quand tout
                   est valide, puis orchestrate.py regenere job.py
  4. verify.py     compare avec Qlik, s'il y a des sorties dans
                   data/qlik_outputs/

Au bout, src/<nom-projet>/job.py contient tous les scripts assembles, dans
l'ordre de leurs dependances.

On peut relancer autant de fois qu'on veut : ce qui est deja traduit et n'a
pas bouge n'est pas refait.
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
    """Lance un outil du kit et renvoie son code retour."""
    return subprocess.run([sys.executable, str(TOOLS / script), *args]).returncode


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un script Qlik ou le dossier script_qvd/")
    p.add_argument("--pilot", type=int, help="nombre de tables, puis arret")
    p.add_argument("--jobs", type=int, default=1, help="tables traduites en parallele")
    p.add_argument("--model", help="provider/model")
    p.add_argument("--opencode-bin", default="opencode")
    p.add_argument(
        "--skip-selftest",
        action="store_true",
        help="ne pas tester le modele avant de traduire",
    )
    p.add_argument(
        "--no-conf",
        action="store_true",
        help="ne pas completer conf/io.yml ni les schemas",
    )
    p.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="NOM=VALEUR",
        help="variable des locations de conf/io.yml, pour la "
        "comparaison (ex. --var sorties=data/output)",
    )
    args = p.parse_args(argv)

    # avant tout : scripts en minuscules, # remplace par _ dans les fichiers
    if run("normalize.py"):
        return 1
    if not args.path.exists():
        # le script vient peut-etre d'etre renomme en minuscules
        args.path = args.path.with_name(args.path.name.lower())
    scripts = find_scripts(args.path)
    if not scripts:
        print(
            f"Aucun script Qlik dans {args.path} : on prend les fichiers "
            ".qvs, .txt ou sans extension."
        )
        return 1
    qlik_dir = Path("data") / "qlik_outputs"
    compare = qlik_dir.is_dir() and any(qlik_dir.iterdir())

    # On recense tous les scripts du dossier, meme si on n'en migre qu'un :
    # quand un script lit un fichier ecrit par un autre, il doit le lire dans
    # data/output.
    folder = args.path if args.path.is_dir() else args.path.parent
    run("prepare.py", "--collect", *map(str, find_scripts(folder)))
    # On traite les scripts dans l'ordre de leurs dependances, comme le job :
    # verify.py lance un script seul, et il doit trouver les fichiers ecrits
    # par ceux dont il depend.
    if STORES.exists():
        rank = {
            s: i
            for i, (s, _) in enumerate(
                order_scripts(json.loads(STORES.read_text(encoding="utf-8")))[0]
            )
        }
        scripts.sort(key=lambda s: rank.get(s.name, len(rank)))
    # On complete conf/io.yml et les schemas avant de traduire, puisque la
    # traduction s'en sert (colonnes des LOAD *, types des dates). Seulement
    # des ajouts : ce qui est deja declare n'est jamais modifie.
    conf_state = "non touchee (--no-conf)"
    if not args.no_conf:
        if not Path("conf").is_dir():
            # Tous les chemins du kit partent du dossier courant : lance
            # ailleurs qu'a la racine du projet, il ecrirait un conf/ et un
            # src/ a cote des vrais.
            print(
                f"[!] pas de dossier conf/ dans {Path.cwd()} : migrate.py se "
                "lance depuis la racine du projet. Un conf/io.yml va etre "
                "cree ici."
            )
        rc = run("fill_conf.py", str(folder), *[f"--var={v}" for v in args.var])
        conf_state = (
            f"completee ({Path('conf/io.yml').resolve()})"
            if rc == 0
            else "PAS completee, voir le message de fill_conf.py plus haut"
        )

    summary: list[tuple[str, str, list[str]]] = []
    stopped = False
    for script in scripts:
        print(f"\n{'#' * 70}\n# {script.name}\n{'#' * 70}")
        if not args.no_conf:
            # Les fichiers ecrits par un script deja traduit sont declares
            # maintenant, avec les types exacts de sa requete Spark.
            run(
                "fill_conf.py",
                str(folder),
                "--quiet",
                *[f"--var={v}" for v in args.var],
            )
        if run("prepare.py", str(script)):
            summary.append((script.name, "echec de la preparation", []))
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
            # Le modele ne repond pas, pas la peine d'enchainer sur les scripts
            # suivants. On regenere quand meme le job en sortant de la boucle.
            summary.append(
                (
                    script.name,
                    "traduction arretee : le modele ne "
                    "repond pas (voir le test ci-dessus)",
                    [],
                )
            )
            stopped = True
            break

        # Spark parse toutes les requetes avant l'assemblage. Une traduction
        # bancale se voit ici, et pas au premier lancement du job.
        run("check.py", str(workdir), "--parse", "--mark")
        manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
        todo = sum(s["status"] == "todo" for s in manifest["sections"])
        to_fix = [
            s["id"]
            for s in manifest["sections"]
            if s["status"] in ("escalade", "erreur_modele")
        ]
        if todo or to_fix:
            summary.append(
                (
                    script.name,
                    f"{todo} a traduire, {len(to_fix)} a corriger a la main",
                    to_fix,
                )
            )
        elif run("assemble.py", str(workdir)):
            summary.append(
                (
                    script.name,
                    "non assemble : entrees ou sorties "
                    "absentes de conf/io.yml (voir ci-dessus)",
                    [],
                )
            )
        else:
            run("orchestrate.py")  # ajoute ce script au job
            state = "assemble dans src/"
            if compare:
                rc = run("verify.py", str(workdir), *[f"--var={v}" for v in args.var])
                verdict = {
                    0: "donnees identiques a Qlik",
                    1: "ecarts avec Qlik : voir comparaison.md",
                    3: "pas d'export Qlik dans data/qlik_outputs/ pour comparer",
                }.get(
                    rc,
                    "des etapes du job ont echoue (voir la trace), les "
                    "sorties produites sont identiques a Qlik",
                )
                state += f", {verdict}"
            summary.append((script.name, state, []))
        if args.pilot:
            break

    print()
    run("orchestrate.py")  # regenere src/<nom-projet>/job.py

    print(f"\n{'=' * 70}\nRESUME")
    print(f"Conf : {conf_state}")
    for name, state, to_fix in summary:
        print(f"- {name} : {state}")
        for sid in to_fix[:15]:
            print(f"    a corriger : {WORK / Path(name).stem / sid}/query.sql")
    ready = sum("assemble" in state for _, state, _ in summary)
    print(
        f"\nJob du projet : {(project_dir() / 'job.py').resolve()} "
        f"({'contient les scripts assembles' if ready else 'aucun script assemble pour le moment'})"
    )
    print(f"Journal : {WORK / 'journal.csv'}")
    if not compare:
        print("Pas de dossier data/qlik_outputs/ : comparaison des donnees non faite.")
    if args.pilot:
        print(
            "Pilote termine : relis les query.sql, puis relance sans --pilot "
            "(le script n'est assemble, et ajoute au job, qu'une fois "
            "toutes ses tables validees)."
        )
    return 1 if stopped else 0


if __name__ == "__main__":
    sys.exit(main())
