"""Compare une sortie Qlik (QVD ou CSV) avec la sortie Spark (Parquet ou CSV).

C'est le seul vrai test d'une traduction : check.py ne regarde que le code,
ici on compare les donnees. verify.py l'appelle pour chaque STORE, mais on
peut aussi le lancer a la main.

On verifie, dans cet ordre :
    1. les colonnes (manquantes ou en trop, sans tenir compte de la casse)
    2. le nombre de lignes
    3. le contenu, ligne a ligne, sans regarder l'ordre des lignes mais en
       comptant les doublons
    4. l'ordre des lignes, si le contenu est identique et que la sortie Spark
       a la colonne _qlik_order : on trie la sortie Spark sur cette colonne
       et on la compare position par position avec l'export Qlik, qui est
       dans l'ordre de chargement. Un ecart d'ordre est signale, mais ne rend
       la comparaison differente qu'avec --strict-order : la regle d'ordre
       apres un JOIN n'est pas encore confirmee.

Avant de comparer, on normalise : espaces de debut et de fin retires,
nombres arrondis (--decimals), dates en ISO, vide et null confondus. Un
export CSV de Qlik ecrit les dates dans le DateFormat du script
(29/12/2024) : avec --date-format 'DD/MM/YYYY', une colonne de texte dont
toutes les valeurs ont ce format est lue comme une date.

Usage :
    python tools/compare.py data/qlik_outputs/commandes.qvd data/output/commandes/
    python tools/compare.py ... --strict-order    # l'ordre des lignes compte
    python tools/compare.py data/qlik_outputs/commandes.csv data/output/commandes/
Code retour : 0 si identique, 1 sinon (utilisable en CI).

Pour lire les QVD il faut le paquet `pyqvd` (pas encore verifie qu'il est
sur le Nexus). Sinon, exporter la table en CSV depuis Qlik et comparer ce CSV.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

MAX_EXAMPLES = 20
ORDER = "_qlik_order"


def guess_sep(path: Path) -> str:
    """Devine le separateur d'un CSV parmi , ; tabulation et | (virgule par
    defaut). On ne laisse pas faire pandas : sur un fichier d'une seule
    colonne, il lui arrive de prendre une lettre pour separateur."""
    with path.open(encoding="utf-8-sig", errors="replace") as f:
        head = f.readline()
    counts = {c: head.count(c) for c in (",", ";", "\t", "|")}
    best = max(counts, key=lambda c: counts[c])
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
                "ou exporte la table en CSV depuis Qlik."
            ) from exc
        return QvdTable.from_qvd(str(path)).to_pandas()
    if suffix in (".csv", ".txt"):
        # pas de separateur fourni : on le devine
        return pd.read_csv(
            path,
            sep=sep or guess_sep(path),
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    if path.is_dir():
        if any(path.glob("*.parquet")):
            return pd.read_parquet(path)
        parts = sorted(path.glob("part-*.csv"))
        if parts:
            return pd.concat(
                pd.read_csv(
                    p, sep=sep or guess_sep(p), dtype=str, keep_default_na=False
                )
                for p in parts
            )
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise SystemExit(f"Format non reconnu : {path}")


# Format de date Qlik -> strptime. Les plus longs d'abord : YYYY avant YY,
# MMM avant MM. None : noms de mois ou de jours, millisecondes, AM/PM, qu'on
# ne cherche pas a relire.
QLIK_TO_STRPTIME = [
    ("MMMM", None),
    ("MMM", None),
    ("WWWW", None),
    ("WWW", None),
    ("TT", None),
    ("f", None),
    ("YYYY", "%Y"),
    ("YY", "%y"),
    ("MM", "%m"),
    ("M", "%m"),
    ("DD", "%d"),
    ("D", "%d"),
    ("hh", "%H"),
    ("h", "%H"),
    ("mm", "%M"),
    ("m", "%M"),
    ("ss", "%S"),
    ("s", "%S"),
]


def strptime_format(fmt: str) -> str | None:
    """Traduit un format de date Qlik pour strptime, ou None s'il contient
    autre chose : on ne devine pas."""
    out, i = [], 0
    while i < len(fmt):
        for q, s in QLIK_TO_STRPTIME:
            if fmt.startswith(q, i):
                if s is None:
                    return None
                out.append(s)
                i += len(q)
                break
        else:
            if fmt[i].isalpha():
                return None
            out.append(fmt[i].replace("%", "%%"))
            i += 1
    return "".join(out)


def as_dates(s: pd.Series, formats: list[str]) -> pd.Series:
    """La colonne convertie en dates si toutes ses valeurs non vides ont
    l'un des formats, sinon telle quelle. Tout ou rien : une colonne de
    codes dont quelques valeurs ressemblent a des dates reste du texte."""
    text = s.astype("string").str.strip()
    filled = text.notna() & (text != "")
    if not filled.any() or pd.api.types.is_numeric_dtype(s):
        return s
    for fmt in formats:
        parsed = pd.to_datetime(text.where(filled), format=fmt, errors="coerce")
        if parsed[filled].notna().all():
            return parsed
    return s


def normalize(
    df: pd.DataFrame, decimals: int, date_formats: list[str] | None = None
) -> pd.DataFrame:
    """Ramene chaque valeur a une chaine comparable d'un fichier a l'autre."""
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        s = df[col]
        if date_formats and not pd.api.types.is_datetime64_any_dtype(s):
            s = as_dates(s, date_formats)
        if pd.api.types.is_datetime64_any_dtype(s):
            s = s.dt.strftime("%Y-%m-%d %H:%M:%S").str.replace(
                " 00:00:00", "", regex=False
            )
        else:
            s = s.astype("string").str.strip()
            num = pd.to_numeric(s, errors="coerce")
            is_num = num.notna() & s.notna()
            s = s.where(
                ~is_num,
                num.round(decimals).map(
                    lambda x: f"{x:.{decimals}f}".rstrip("0").rstrip(".")
                ),
            )
        out[col.lower()] = s.fillna("").replace({"nan": "", "None": "", "NaT": ""})
    return out


def load_order(spark: pd.DataFrame) -> list[int] | None:
    """Positions des lignes Spark triees sur _qlik_order, ou None si la
    colonne manque. Le tri est stable : a egalite, l'ordre du fichier."""
    if ORDER not in spark.columns:
        return None
    keys = [tuple(int(x) for x in v) if v is not None else () for v in spark[ORDER]]
    return sorted(range(len(keys)), key=keys.__getitem__)


def compare_order(
    q: pd.DataFrame, s: pd.DataFrame, order: list[int], common: list[str]
) -> list[str]:
    """Les ecarts d'ordre, position par position : une ligne par ecart, au
    plus MAX_EXAMPLES. q et s ont le meme contenu, a l'ordre pres."""
    qv = q[common].to_numpy()
    sv = s[common].to_numpy()[order]
    bad = [i for i in range(len(qv)) if list(qv[i]) != list(sv[i])]
    lines = []
    if bad:
        lines.append(
            f"- {len(bad)} ligne(s) pas a leur place, la premiere en "
            f"position {bad[0] + 1}"
        )
        for i in bad[:MAX_EXAMPLES]:
            lines.append(
                f"  - position {i + 1} : Qlik {list(qv[i])} | PySpark {list(sv[i])}"
            )
    return lines


def compare(
    qlik: pd.DataFrame,
    spark: pd.DataFrame,
    decimals: int,
    ignore: list[str],
    date_formats: list[str] | None = None,
    strict_order: bool = False,
) -> tuple[bool, list[str], str]:
    """Renvoie (identique, rapport, verdict sur l'ordre). Le verdict sur
    l'ordre vaut "identique", "different" ou "non verifie"."""
    report, ok = [], True
    order = load_order(spark)
    ignored = {c.lower() for c in ignore}
    # Seul l'export Qlik a des dates en texte : cote Spark, elles sont typees.
    q = normalize(qlik, decimals, date_formats).drop(
        columns=list(ignored), errors="ignore"
    )
    s = normalize(spark, decimals).drop(columns=list(ignored), errors="ignore")

    missing = sorted(set(q.columns) - set(s.columns))
    extra = sorted(set(s.columns) - set(q.columns))
    report.append("## 1. Colonnes\n")
    if missing or extra:
        ok = False
        report.append(f"- Manquantes cote PySpark : {missing or 'aucune'}")
        report.append(f"- En trop cote PySpark : {extra or 'aucune'}")
    else:
        report.append(f"- Identiques ({len(q.columns)} colonnes)")

    report.append("\n## 2. Nombre de lignes\n")
    report.append(f"- Qlik : {len(q)} | PySpark : {len(s)}")
    if len(q) != len(s):
        ok = False
        report.append(f"- **Ecart : {len(s) - len(q):+d}**")

    common = sorted(set(q.columns) & set(s.columns))
    report.append("\n## 3. Contenu (colonnes communes)\n")
    if not common:
        report.append("- Aucune colonne commune, comparaison impossible")
        return False, report, "non verifie"
    # On numerote les doublons, sinon deux lignes identiques cote Qlik contre
    # une seule cote Spark passeraient inapercues.
    q2, s2 = q[common].copy(), s[common].copy()
    q2["_n"] = q2.groupby(common, dropna=False).cumcount()
    s2["_n"] = s2.groupby(common, dropna=False).cumcount()
    diff = q2.merge(s2, how="outer", on=common + ["_n"], indicator=True)
    only_q = diff[diff["_merge"] == "left_only"].drop(columns=["_n", "_merge"])
    only_s = diff[diff["_merge"] == "right_only"].drop(columns=["_n", "_merge"])
    if only_q.empty and only_s.empty:
        report.append("- Contenu identique")
        report.append("\n## 4. Ordre des lignes\n")
        if order is None:
            report.append(f"- Non verifie : pas de colonne {ORDER} cote PySpark")
            return ok, report, "non verifie"
        gaps = compare_order(q, s, order, common)
        if not gaps:
            report.append(f"- Identique (PySpark trie sur {ORDER})")
            return ok, report, "identique"
        report += gaps
        if strict_order:
            report.append("- **Ordre exige (--strict-order) : different**")
            return False, report, "different"
        return ok, report, "different"
    else:
        ok = False
        report.append(f"- Lignes uniquement dans Qlik : {len(only_q)}")
        report.append(f"- Lignes uniquement dans PySpark : {len(only_s)}")
        for title, frame in (("Qlik", only_q), ("PySpark", only_s)):
            if not frame.empty:
                report.append(f"\n### Exemples presents seulement dans {title}\n")
                report.append(
                    frame.head(MAX_EXAMPLES).to_markdown(index=False)
                    if _has_tabulate()
                    else frame.head(MAX_EXAMPLES).to_string(index=False)
                )
    return ok, report, "non verifie"


def _has_tabulate() -> bool:
    """Pour to_markdown(), pandas a besoin du paquet tabulate."""
    return importlib.util.find_spec("tabulate") is not None


def compare_files(
    qlik: Path,
    spark: Path,
    sep=None,
    decimals=6,
    ignore=(),
    date_formats=(),
    strict_order=False,
) -> tuple[bool, str, str]:
    """Compare deux fichiers (ou dossiers) et renvoie (identique, rapport en
    markdown, verdict sur l'ordre). date_formats : formats Qlik
    (DateFormat, TimestampFormat) des dates ecrites en texte dans l'export
    Qlik."""
    formats = [f for f in map(strptime_format, filter(None, date_formats)) if f]
    # _qlik_order sert au controle de l'ordre, pas au contenu
    ok, lines, order = compare(
        read_any(qlik, sep),
        read_any(spark, sep),
        decimals,
        list(ignore) + [ORDER],
        formats,
        strict_order,
    )
    text = (
        f"# Comparaison {qlik.name} vs {spark.name}\n\n"
        + "\n".join(lines)
        + f"\n\nRESULTAT: {'IDENTIQUE' if ok else 'DIFFERENT'}"
        + (" (ordre des lignes different)" if ok and order == "different" else "")
        + "\n"
    )
    return ok, text, order


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("qlik", type=Path, help="sortie Qlik : .qvd ou .csv")
    p.add_argument("spark", type=Path, help="sortie PySpark : dossier ou fichier")
    p.add_argument(
        "--sep", default=None, help="separateur CSV (defaut : detection automatique)"
    )
    p.add_argument(
        "--decimals", type=int, default=6, help="arrondi des nombres avant comparaison"
    )
    p.add_argument(
        "--ignore",
        nargs="*",
        default=[],
        help="colonnes a exclure (ex. horodatage de chargement)",
    )
    p.add_argument(
        "--date-format",
        action="append",
        default=[],
        help="format Qlik des dates en texte dans l'export Qlik "
        "(ex. DD/MM/YYYY), repetable",
    )
    p.add_argument(
        "--strict-order",
        action="store_true",
        help="un ecart dans l'ordre des lignes rend la comparaison differente",
    )
    p.add_argument("--report", type=Path, help="ecrire le rapport markdown ici")
    args = p.parse_args(argv)

    ok, text, _ = compare_files(
        args.qlik,
        args.spark,
        args.sep,
        args.decimals,
        args.ignore,
        args.date_format,
        args.strict_order,
    )
    if args.report:
        args.report.write_text(text, encoding="utf-8")
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
