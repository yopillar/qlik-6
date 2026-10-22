"""Compare une sortie Qlik (QVD ou CSV) avec la sortie Spark (Parquet ou CSV).

C'est le seul vrai test d'une traduction : check.py ne regarde que le code,
ici on compare les données. verify.py l'appelle pour chaque STORE, mais on
peut aussi le lancer à la main.

On vérifie, dans cet ordre :
    1. les colonnes (manquantes ou en trop, sans tenir compte de la casse)
    2. le nombre de lignes
    3. le contenu, ligne à ligne, sans regarder l'ordre des lignes mais en
       comptant les doublons

Avant de comparer, on normalise : espaces de début et de fin retirés,
nombres arrondis (--decimals), dates en ISO, vide et null confondus.

Usage :
    python tools/compare.py data/qlik_outputs/commandes.qvd data/output/commandes/
    python tools/compare.py data/qlik_outputs/commandes.csv data/output/commandes/
Code retour : 0 si identique, 1 sinon (utilisable en CI).

Pour lire les QVD il faut le paquet `pyqvd` (pas encore vérifié qu'il est
sur le Nexus). Sinon, exporter la table en CSV depuis Qlik et comparer ce CSV.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

MAX_EXAMPLES = 20


def guess_sep(path: Path) -> str:
    """Devine le séparateur d'un CSV parmi , ; tabulation et | (virgule par
    défaut). On ne laisse pas faire pandas : sur un fichier d'une seule
    colonne, il lui arrive de prendre une lettre pour séparateur."""
    with path.open(encoding="utf-8-sig", errors="replace") as f:
        head = f.readline()
    counts = {c: head.count(c) for c in (",", ";", "\t", "|")}
    best = max(counts, key=counts.get)
    return best if counts[best] else ","


def read_any(path: Path, sep: str) -> pd.DataFrame:
    """Lit un QVD, un CSV ou un Parquet (fichier seul ou dossier de part-*)."""
    suffix = path.suffix.lower()
    if suffix == ".qvd":
        try:
            from pyqvd import QvdTable
        except ImportError as exc:
            raise SystemExit(
                "Lecture QVD impossible : paquet 'pyqvd' absent. Installe-le "
                "ou exporte la table en CSV depuis Qlik.") from exc
        return QvdTable.from_qvd(str(path)).to_pandas()
    if suffix in (".csv", ".txt"):
        # pas de séparateur fourni : on le devine
        return pd.read_csv(path, sep=sep or guess_sep(path), dtype=str,
                           keep_default_na=False, encoding="utf-8-sig")
    if path.is_dir():
        if any(path.glob("*.parquet")):
            return pd.read_parquet(path)
        parts = sorted(path.glob("part-*.csv"))
        if parts:
            return pd.concat(pd.read_csv(p, sep=sep or guess_sep(p), dtype=str,
                                         keep_default_na=False)
                             for p in parts)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise SystemExit(f"Format non reconnu : {path}")


def normalize(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
    """Ramène chaque valeur à une chaîne comparable d'un fichier à l'autre."""
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            s = s.dt.strftime("%Y-%m-%d %H:%M:%S").str.replace(
                " 00:00:00", "", regex=False)
        else:
            s = s.astype("string").str.strip()
            num = pd.to_numeric(s, errors="coerce")
            is_num = num.notna() & s.notna()
            s = s.where(~is_num, num.round(decimals).map(
                lambda x: f"{x:.{decimals}f}".rstrip("0").rstrip(".")))
        out[col.lower()] = s.fillna("").replace({"nan": "", "None": "",
                                                  "NaT": ""})
    return out


def compare(qlik: pd.DataFrame, spark: pd.DataFrame, decimals: int,
            ignore: list[str]) -> tuple[bool, list[str]]:
    report, ok = [], True
    ignore = {c.lower() for c in ignore}
    q = normalize(qlik, decimals).drop(columns=list(ignore), errors="ignore")
    s = normalize(spark, decimals).drop(columns=list(ignore), errors="ignore")

    missing = sorted(set(q.columns) - set(s.columns))
    extra = sorted(set(s.columns) - set(q.columns))
    report.append("## 1. Colonnes\n")
    if missing or extra:
        ok = False
        report.append(f"- Manquantes côté PySpark : {missing or 'aucune'}")
        report.append(f"- En trop côté PySpark : {extra or 'aucune'}")
    else:
        report.append(f"- Identiques ({len(q.columns)} colonnes)")

    report.append("\n## 2. Nombre de lignes\n")
    report.append(f"- Qlik : {len(q)} | PySpark : {len(s)}")
    if len(q) != len(s):
        ok = False
        report.append(f"- **Écart : {len(s) - len(q):+d}**")

    common = sorted(set(q.columns) & set(s.columns))
    report.append("\n## 3. Contenu (colonnes communes)\n")
    if not common:
        report.append("- Aucune colonne commune, comparaison impossible")
        return False, report
    # On numérote les doublons, sinon deux lignes identiques côté Qlik contre
    # une seule côté Spark passeraient inaperçues.
    q2, s2 = q[common].copy(), s[common].copy()
    q2["_n"] = q2.groupby(common, dropna=False).cumcount()
    s2["_n"] = s2.groupby(common, dropna=False).cumcount()
    diff = q2.merge(s2, how="outer", on=common + ["_n"], indicator=True)
    only_q = diff[diff["_merge"] == "left_only"].drop(columns=["_n", "_merge"])
    only_s = diff[diff["_merge"] == "right_only"].drop(columns=["_n", "_merge"])
    if only_q.empty and only_s.empty:
        report.append("- Contenu identique")
    else:
        ok = False
        report.append(f"- Lignes uniquement dans Qlik : {len(only_q)}")
        report.append(f"- Lignes uniquement dans PySpark : {len(only_s)}")
        for title, frame in (("Qlik", only_q), ("PySpark", only_s)):
            if not frame.empty:
                report.append(f"\n### Exemples présents seulement dans {title}\n")
                report.append(frame.head(MAX_EXAMPLES).to_markdown(index=False)
                              if _has_tabulate() else
                              frame.head(MAX_EXAMPLES).to_string(index=False))
    return ok, report


def _has_tabulate() -> bool:
    """Pour to_markdown(), pandas a besoin du paquet tabulate."""
    return importlib.util.find_spec("tabulate") is not None


def compare_files(qlik: Path, spark: Path, sep=None, decimals=6,
                  ignore=()) -> tuple[bool, str]:
    """Compare deux fichiers (ou dossiers) et renvoie (identique, rapport en
    markdown)."""
    ok, lines = compare(read_any(qlik, sep), read_any(spark, sep),
                        decimals, list(ignore))
    text = (f"# Comparaison {qlik.name} vs {spark.name}\n\n"
            + "\n".join(lines)
            + f"\n\nRÉSULTAT: {'IDENTIQUE' if ok else 'DIFFÉRENT'}\n")
    return ok, text


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("qlik", type=Path, help="sortie Qlik : .qvd ou .csv")
    p.add_argument("spark", type=Path, help="sortie PySpark : dossier ou fichier")
    p.add_argument("--sep", default=None,
                   help="séparateur CSV (défaut : détection automatique)")
    p.add_argument("--decimals", type=int, default=6,
                   help="arrondi des nombres avant comparaison")
    p.add_argument("--ignore", nargs="*", default=[],
                   help="colonnes à exclure (ex. horodatage de chargement)")
    p.add_argument("--report", type=Path, help="écrire le rapport markdown ici")
    args = p.parse_args(argv)

    ok, text = compare_files(args.qlik, args.spark, args.sep, args.decimals,
                             args.ignore)
    if args.report:
        args.report.write_text(text, encoding="utf-8")
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
