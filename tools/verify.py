"""Preuve par les données : lance le job, compare chaque sortie à Qlik.

Pour chaque STORE du script (OUTPUTS des sections) :
    qlik_outputs/<nom>.qvd|.csv|.parquet   (fourni par toi, exporté de Qlik)
    data/output/<nom>/                     (écrit par src/<script>/job.py)
sont comparés par compare.py. Le résultat est écrit dans
work/<script>/comparaison.md, dans le manifest (champ « compare ») et dans
work/journal.csv.

Code retour : 0 identique, 1 écarts, 2 le job n'a pas pu tourner.

Usage :
    python tools/verify.py work/ventes            # lance le job puis compare
    python tools/verify.py work/ventes --no-run   # compare seulement
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

QLIK_DIR = Path("qlik_outputs")
EXTENSIONS = (".qvd", ".csv", ".txt", ".parquet")


def find_reference(name: str) -> Path | None:
    """Fichier Qlik de même nom (insensible à la casse et à l'extension)."""
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
    args = p.parse_args(argv)

    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    stem = Path(manifest["script"]).stem
    job = Path("src") / stem / "job.py"

    if not args.no_run:
        if not job.exists():
            print(f"{job} absent : lance d'abord l'assemblage.")
            return 2
        print(f"Exécution de {job} …")
        res = subprocess.run([sys.executable, str(job)])
        if res.returncode:
            print("Le job a échoué : voir la trace ci-dessus.")
            return 2

    report, n_ok, n_ko, n_none = [f"# Comparaison des sorties de `{stem}`", ""], 0, 0, 0
    for sec in manifest["sections"]:
        for table, out_path in sec.get("outputs", {}).items():
            name = Path(out_path).name
            ref = find_reference(name)
            if ref is None:
                verdict, detail = "pas de référence", \
                    f"aucun fichier {name}.* dans {QLIK_DIR}/"
                n_none += 1
            elif not Path(out_path).exists():
                verdict, detail = "sortie absente", f"{out_path} n'existe pas"
                n_ko += 1
            else:
                # clés AutoNumber / Hash : valeurs différentes de Qlik par
                # construction, exclues de la comparaison
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
            print(f"  {sec['id']:<35} {table:<25} {verdict}")
            report += [f"## {sec['id']} — {table} : {verdict}", "", detail, ""]
            journal.update(args.workdir.parent / "journal.csv", {
                "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "script": manifest["script"], "section": sec["id"],
                "status": sec["status"], "compare": verdict})

    (args.workdir / "comparaison.md").write_text("\n".join(report),
                                                 encoding="utf-8")
    mpath.write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                     encoding="utf-8")
    print(f"\n{n_ok} identique(s), {n_ko} différente(s), {n_none} sans "
          f"référence -> {args.workdir / 'comparaison.md'}")
    return 1 if n_ko else 0


if __name__ == "__main__":
    sys.exit(main())
