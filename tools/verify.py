"""Lance le job et compare chaque sortie avec celle de Qlik.

C'est la vérification sur les données. Pour chaque STORE du script (les
OUTPUTS des sections), on compare avec compare.py :
    data/qlik_outputs/<nom>.qvd|.csv|.parquet   (export Qlik, à fournir)
    data/output/<nom>/                          (écrit par le job du projet)
Le résultat va dans work/<script>/comparaison.md, dans le manifest (champ
« compare ») et dans work/journal.csv.

Code retour : 0 si tout est identique, 1 s'il y a des écarts, 2 si le job
n'a pas pu tourner.

Usage :
    python tools/verify.py work/ventes            # lance le job puis compare
    python tools/verify.py work/ventes --no-run   # compare seulement
    python tools/verify.py work/ventes --var sorties=data/output

Les locations de conf/io.yml contiennent des variables (${sorties}…) que
le socle résout à l'exécution. Pour retrouver les fichiers, on les prend
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
from conf import resolve_location  # noqa: E402
from job_template import project_dir  # noqa: E402

QLIK_DIR = Path("data") / "qlik_outputs"
EXTENSIONS = (".qvd", ".csv", ".txt", ".parquet")


def find_reference(name: str) -> Path | None:
    """Cherche dans data/qlik_outputs/ le fichier du même nom, sans tenir
    compte de la casse ni de l'extension."""
    if not QLIK_DIR.exists():
        return None
    for f in QLIK_DIR.iterdir():
        if f.stem.lower() == name.lower() and (f.suffix.lower() in EXTENSIONS
                                               or f.is_dir()):
            return f
    return None


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--no-run", action="store_true",
                   help="ne pas relancer le job (sorties déjà produites)")
    p.add_argument("--ignore", nargs="*", default=[],
                   help="colonnes à exclure (ex. date de chargement)")
    p.add_argument("--var", action="append", default=[], metavar="NOM=VALEUR",
                   help="valeur d'une variable des locations de conf/io.yml "
                        "(ex. --var sorties=data/output), sinon "
                        "l'environnement")
    args = p.parse_args(argv)
    variables = dict(v.split("=", 1) for v in args.var if "=" in v)

    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    stem = Path(manifest["script"]).stem
    job = project_dir() / "job.py"

    job_failed = False
    if not args.no_run:
        if not job.exists():
            print(f"{job} absent : lance d'abord assemble.py puis "
                  "orchestrate.py (ou migrate.py).")
            return 2
        print(f"Exécution de {job} …")
        res = subprocess.run([sys.executable, str(job), "--only", stem])
        if res.returncode:
            # Le job isole ses étapes, donc il a pu écrire une partie des
            # sorties : on compare quand même ce qui existe.
            print("⚠ Une ou plusieurs étapes du job ont échoué (voir la trace "
                  "ci-dessus) : comparaison de ce qui a été produit.")
            job_failed = True

    verified: dict = {}
    report, n_ok, n_ko, n_none = [f"# Comparaison des sorties de `{stem}`", ""], 0, 0, 0
    for sec in manifest["sections"]:
        for table, location in sec.get("outputs", {}).items():
            out_path = resolve_location(location, variables)
            # L'export Qlik peut porter le nom de la sortie dans io.yml ou
            # celui du fichier de sa location (ils diffèrent parfois).
            names = list(dict.fromkeys(filter(None, (
                sec.get("output_io", {}).get(table), Path(out_path).name))))
            ref = next(filter(None, map(find_reference, names)), None)
            if ref is None:
                verdict, detail = "pas de référence", \
                    f"aucun fichier {' ou '.join(names)}.* dans {QLIK_DIR}/"
                n_none += 1
            elif "${" in out_path:
                verdict, detail = "sortie introuvable", (
                    f"{location} : variable inconnue, la passer avec "
                    "--var nom=chemin")
                n_ko += 1
            elif not Path(out_path).exists():
                verdict, detail = "sortie absente", f"{out_path} n'existe pas"
                n_ko += 1
            else:
                # Les clés AutoNumber / Hash ne peuvent pas avoir les mêmes
                # valeurs que dans Qlik, on les sort de la comparaison.
                # _qlik_order aussi, Qlik n'a pas cette colonne. Conséquence :
                # l'ordre des lignes n'est pas vérifié ici.
                tech = sec.get("technical_columns", [])
                ok, detail = compare_files(ref, Path(out_path),
                                           ignore=list(args.ignore) + tech
                                           + ["_qlik_order"])
                if tech:
                    detail = (f"Colonnes techniques exclues (AutoNumber/Hash) : "
                              f"{', '.join(tech)}\n\n") + detail
                verdict = "identique" if ok else "différent"
                n_ok += ok
                n_ko += not ok
            sec["compare"] = verdict
            if verdict == "identique":
                verified.update(sec.get("hole_answers", {}))
            print(f"  {sec['id']:<35} {table:<25} {verdict}")
            report += [f"## {sec['id']} — {table} : {verdict}", "", detail, ""]
            journal.update(args.workdir.parent / "journal.csv", {
                "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "script": manifest["script"], "section": sec["id"],
                "status": sec["status"], "compare": verdict})

    if verified:
        # Ces expressions venaient du modèle, mais la comparaison vient de leur
        # donner raison : on les met en cache pour les réutiliser telles
        # quelles dans les autres scripts, sans rappeler le modèle.
        cache_path = args.workdir.parent / "expressions.json"
        cache = json.loads(cache_path.read_text(encoding="utf-8")) \
            if cache_path.exists() else {}
        for key, sql in verified.items():
            if key in cache and cache[key]["sql"] == sql:
                cache[key]["verified"] = True
        cache_path.write_text(json.dumps(cache, indent=2, ensure_ascii=False),
                              encoding="utf-8")
    (args.workdir / "comparaison.md").write_text("\n".join(report),
                                                 encoding="utf-8")
    mpath.write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                     encoding="utf-8")
    print(f"\n{n_ok} identique(s), {n_ko} différente(s), {n_none} sans "
          f"référence -> {args.workdir / 'comparaison.md'}")
    if job_failed and not n_ko:
        return 2
    return 1 if n_ko else 0


if __name__ == "__main__":
    sys.exit(main())
