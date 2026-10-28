"""Lance le job et compare chaque sortie avec celle de Qlik.

C'est la verification sur les donnees. Pour chaque STORE du script (les
OUTPUTS des sections), on compare avec compare.py :
    data/qlik_outputs/<nom>.qvd|.csv|.parquet   (export Qlik, a fournir)
    data/output/<nom>/                          (ecrit par le job du projet)
Le resultat va dans work/<script>/comparaison.md, dans le manifest (champ
"compare") et dans work/journal.csv.

Code retour : 0 si tout est identique, 1 s'il y a des ecarts, 2 si le job
n'a pas pu tourner, 3 si aucune sortie n'avait d'export Qlik pour comparer.
Un ecart dans l'ordre des lignes est signale ("identique, ordre
different") mais ne compte comme ecart qu'avec --strict-order.

Usage :
    python tools/verify.py work/ventes            # lance le job puis compare
    python tools/verify.py work/ventes --no-run   # compare seulement
    python tools/verify.py work/ventes --var sorties=data/output
    python tools/verify.py work/ventes --strict-order

Les locations de conf/io.yml contiennent des variables (${sorties}...) que
le socle resout a l'execution. Pour retrouver les fichiers, on les prend
dans --var, sinon dans l'environnement.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import journal  # noqa: E402
from compare import compare_files  # noqa: E402
from conf import resolve_location, work_dir  # noqa: E402
from job_template import module_name, project_dir  # noqa: E402

QLIK_DIR = Path("data") / "qlik_outputs"
EXTENSIONS = (".qvd", ".csv", ".txt", ".parquet")


def find_reference(name: str) -> Path | None:
    """Cherche dans data/qlik_outputs/ le fichier du meme nom, sans tenir
    compte de la casse ni de l'extension."""
    if not QLIK_DIR.exists():
        return None
    for f in QLIK_DIR.iterdir():
        if f.stem.lower() == name.lower() and (
            f.suffix.lower() in EXTENSIONS or f.is_dir()
        ):
            return f
    return None


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument(
        "--no-run",
        action="store_true",
        help="ne pas relancer le job (sorties deja produites)",
    )
    p.add_argument(
        "--ignore",
        nargs="*",
        default=[],
        help="colonnes a exclure (ex. date de chargement)",
    )
    p.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="NOM=VALEUR",
        help="valeur d'une variable des locations de conf/io.yml "
        "(ex. --var sorties=data/output), sinon "
        "l'environnement",
    )
    p.add_argument(
        "--strict-order",
        action="store_true",
        help="un ecart dans l'ordre des lignes compte comme un ecart",
    )
    args = p.parse_args(argv)
    args.workdir = work_dir(args.workdir)
    variables = dict(v.split("=", 1) for v in args.var if "=" in v)

    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    stem = Path(manifest["script"]).stem
    job = project_dir() / "job.py"
    # formats du script, pour relire les dates d'un export Qlik en CSV
    date_formats = [manifest.get("date_format"), manifest.get("timestamp_format")]

    job_failed = False
    if not args.no_run:
        if not job.exists():
            print(
                f"{job} absent : lance d'abord assemble.py puis "
                "orchestrate.py (ou migrate.py)."
            )
            return 2
        print(f"Execution de {job} ...")
        res = subprocess.run([sys.executable, str(job), "--only", module_name(stem)])
        if res.returncode:
            # Le script s'est arrete a l'etape en echec, mais il a pu ecrire
            # les sorties d'avant : on compare quand meme ce qui existe.
            print(
                "[!] Une ou plusieurs etapes du job ont echoue (voir la trace "
                "ci-dessus) : comparaison de ce qui a ete produit."
            )
            job_failed = True

    verified: dict = {}
    report, n_ok, n_ko, n_none = [f"# Comparaison des sorties de `{stem}`", ""], 0, 0, 0
    for sec in manifest["sections"]:
        for table, location in sec.get("outputs", {}).items():
            out_path = resolve_location(location, variables)
            # L'export Qlik peut porter le nom de la sortie dans io.yml ou
            # celui du fichier de sa location (ils different parfois).
            names = list(
                dict.fromkeys(
                    filter(
                        None, (sec.get("output_io", {}).get(table), Path(out_path).name)
                    )
                )
            )
            ref = next(filter(None, map(find_reference, names)), None)
            if ref is None:
                verdict, detail = (
                    "pas de reference",
                    f"aucun fichier {' ou '.join(names)}.* dans {QLIK_DIR}/",
                )
                n_none += 1
            elif "${" in out_path:
                verdict, detail = (
                    "sortie introuvable",
                    (
                        f"{location} : variable inconnue, la passer avec "
                        "--var nom=chemin"
                    ),
                )
                n_ko += 1
            elif not Path(out_path).exists():
                verdict, detail = "sortie absente", f"{out_path} n'existe pas"
                n_ko += 1
            else:
                # Les cles AutoNumber / Hash ne peuvent pas avoir les memes
                # valeurs que dans Qlik, on les sort de la comparaison.
                # _qlik_order n'est pas comparee comme une colonne : elle sert
                # a verifier l'ordre des lignes.
                tech = sec.get("technical_columns", [])
                ok, detail, order = compare_files(
                    ref,
                    Path(out_path),
                    ignore=list(args.ignore) + tech,
                    date_formats=date_formats,
                    strict_order=args.strict_order,
                )
                if tech:
                    detail = (
                        f"Colonnes techniques exclues (AutoNumber/Hash) : "
                        f"{', '.join(tech)}\n\n"
                    ) + detail
                verdict = "identique" if ok else "different"
                if ok and order == "different":
                    verdict = "identique, ordre different"
                n_ok += ok
                n_ko += not ok
            sec["compare"] = verdict
            # le contenu suffit, que l'ordre des lignes soit le bon ou non
            if verdict.startswith("identique"):
                verified.update(sec.get("hole_answers", {}))
            print(f"  {sec['id']:<35} {table:<25} {verdict}")
            report += [f"## {sec['id']} - {table} : {verdict}", "", detail, ""]
            journal.update(
                args.workdir.parent / "journal.csv",
                {
                    "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "script": manifest["script"],
                    "section": sec["id"],
                    "status": sec["status"],
                    "compare": verdict,
                },
            )

    if verified:
        # Ces expressions venaient du modele, mais la comparaison vient de leur
        # donner raison : on les met en cache pour les reutiliser telles
        # quelles dans les autres scripts, sans rappeler le modele.
        cache_path = args.workdir.parent / "expressions.json"
        cache = (
            json.loads(cache_path.read_text(encoding="utf-8"))
            if cache_path.exists()
            else {}
        )
        for key, sql in verified.items():
            if key in cache and cache[key]["sql"] == sql:
                cache[key]["verified"] = True
        cache_path.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    (args.workdir / "comparaison.md").write_text("\n".join(report), encoding="utf-8")
    mpath.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(
        f"\n{n_ok} identique(s), {n_ko} differente(s), {n_none} sans "
        f"reference -> {args.workdir / 'comparaison.md'}"
    )
    if job_failed and not n_ko:
        return 2
    if not n_ok and not n_ko:
        return 3  # rien a comparer : aucun export Qlik trouve
    return 1 if n_ko else 0


if __name__ == "__main__":
    sys.exit(main())
