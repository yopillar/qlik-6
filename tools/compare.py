"""Compare la sortie Qlik (QVD ou CSV) à la sortie PySpark (Parquet ou CSV).

C'est la seule preuve réelle qu'une traduction est juste : check.py
contrôle la forme du code, ce script compare les DONNÉES.
Appelé par verify.py pour chaque STORE ; utilisable seul.

Contrôles, dans l'ordre :
    1. colonnes (manquantes / en trop, comparaison insensible à la casse)
    2. nombre de lignes
    3. contenu : comparaison des lignes en multi-ensemble (l'ordre des lignes
       n'a pas de sens, les doublons comptent)

Normalisation appliquée avant comparaison : espaces en bord de chaîne,
nombres arrondis (--decimals), dates ramenées en ISO, vides/null unifiés.

Usage :
    python tools/compare.py qlik_outputs/commandes.qvd data/output/commandes/
    python tools/compare.py qlik_outputs/commandes.csv data/output/commandes/
Code retour : 0 si identique, 1 sinon (utilisable en CI).

Lecture des QVD : nécessite le paquet `pyqvd` (à vérifier sur le Nexus).
À défaut, exporter la table en CSV depuis Qlik et comparer le CSV.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

MAX_EXAMPLES = 20


def read_any(path: Path, sep: str) -> pd.DataFrame:
    """Lit un QVD, un CSV ou un Parquet (fichier ou dossier de part-*)."""
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
        # séparateur détecté automatiquement si non fourni
        return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False,
                           encoding="utf-8-sig", engine="python")
    if path.is_dir():
        if any(path.glob("*.parquet")):
            return pd.read_parquet(path)
        parts = sorted(path.glob("part-*.csv"))
        if parts:
            return pd.concat(pd.read_csv(p, sep=sep, dtype=str,
                                         keep_default_na=False,
                                         engine="python")
                             for p in parts)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise SystemExit(f"Format non reconnu : {path}")


def normalize(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
    """Ramène chaque valeur à une chaîne canonique comparable."""
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
    # multi-ensemble : on numérote les doublons pour qu'ils comptent
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
    """to_markdown() de pandas a besoin du paquet tabulate."""
    return importlib.util.find_spec("tabulate") is not None


def compare_files(qlik: Path, spark: Path, sep=None, decimals=6,
                  ignore=()) -> tuple[bool, str]:
    """Compare deux fichiers/dossiers et renvoie (identique, rapport markdown)."""
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
