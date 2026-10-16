"""Contrôles automatiques d'une requête traduite (sans modèle, sans Spark).

Vérifie work/<script>/<table>/query.sql contre les faits du manifest :
  - plus aucun bloc « TODO(model) » ;
  - une seule requête, aucune instruction interdite (CREATE, INSERT…) ;
  - seules les vues et CTE existantes sont lues ;
  - chaque colonne de sortie attendue apparaît ;
  - chaque instruction Qlik est référencée par un commentaire « l.<n> » ;
  - avec --parse (ou depuis translate.py) : la syntaxe est validée par le
    parseur de Spark, sans exécuter la requête.

Ces contrôles ne prouvent pas que le résultat est juste (c'est le rôle de
verify.py sur les données), mais ils rattrapent en une seconde les oublis
typiques d'un modèle faible.

Usage :
    python tools/check.py work/ventes                  # toutes les tables
    python tools/check.py work/ventes --only 012_lot   # valide une table corrigée
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

FORBIDDEN = ("CREATE", "INSERT", "DELETE", "UPDATE", "MERGE", "DROP",
             "ALTER", "TRUNCATE", "CACHE", "REFRESH")
# au-delà de ce taux d'instructions non référencées, c'est une erreur
MAX_UNREFERENCED = 0.3


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


TOKENS = re.compile(r"'(?:\\.|[^'\\])*'|`(?:``|[^`])*`")


def strip_sql_strings(sql: str, idents: bool = False) -> str:
    """Vide les chaînes '…' (et les noms `…` si idents). Les deux sont lus
    ensemble : un nom `Payment's type` ne doit pas ouvrir une chaîne."""
    def repl(m):
        tok = m.group()
        if tok.startswith("'"):
            return "''"
        return "``" if idents else tok
    return TOKENS.sub(repl, sql)


def get_parser():
    """Parseur SQL de Spark (None si pyspark n'est pas installé)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = (SparkSession.builder.master("local[1]")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark._jsparkSession.sessionState().sqlParser()


def check_section(section: dict, query_path: Path,
                  parser=None) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    if not query_path.exists() or not query_path.read_text(encoding="utf-8").strip():
        return ["query.sql absent ou vide"], []
    raw = query_path.read_text(encoding="utf-8")
    if "TODO(model)" in raw:
        left = re.findall(r"TODO\(model\):\s*(l\.\d+)", raw)
        errors.append(f"{raw.count('TODO(model)')} bloc(s) TODO(model) "
                      f"restant(s) : {', '.join(left) or '?'}")
    code = strip_sql_strings(strip_sql_comments(raw))
    code_no_idents = strip_sql_strings(strip_sql_comments(raw), idents=True)
    if code.strip().rstrip(";").count(";"):
        errors.append("plusieurs instructions : une seule requête attendue")
    for kw in FORBIDDEN:
        if re.search(rf"\b{kw}\b", code, re.I):
            errors.append(f"instruction interdite : {kw}")
    if re.search(r"```", raw):
        errors.append("balises ``` dans le fichier : SQL seul attendu")
    # habitude Qlik : 'l''été'. En Spark SQL, '' colle deux chaînes et
    # l'apostrophe disparaît (« lété ») : écrire "l'été" ou 'l\'été'
    if re.search(r"\w''\w", strip_sql_comments(raw)):
        errors.append("apostrophe doublée '' dans une chaîne : en Spark SQL elle "
                      "disparaît ; écrire \"l'été\" (guillemets doubles)")

    # vues et CTE lues
    ctes = {m.lower() for m in re.findall(r"(\w+)\s+AS\s*\(", code_no_idents,
                                          re.I)}
    allowed = ctes | {v.lower() for v in section.get("views", {})}
    unknown = []
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)", code_no_idents,
                         re.I):
        name = m.group(1).lower()
        if name not in allowed and name not in ("values", "lateral") \
                and m.group(1) not in unknown:
            unknown.append(m.group(1))
    if unknown:
        errors.append(f"table ou vue inconnue : {', '.join(unknown)} (vues "
                      f"autorisées : {', '.join(sorted(section.get('views', {}))) or 'aucune'}"
                      " + les CTE de la requête)")

    # colonnes de sortie attendues
    cols = section.get("columns") or []
    if cols and "*" not in cols and "?" not in cols:
        missing = [c for c in cols if not re.search(
            r"(?<![\w])" + re.escape(c) + r"(?![\w])", code)]
        if missing:
            errors.append(f"colonnes de sortie absentes : {missing}")

    # couverture des instructions Qlik
    refs = {int(n) for n in re.findall(r"--\s*l\.(\d+)", raw)}
    lines = section.get("statement_lines", [])
    unref = [n for n in lines if n not in refs]
    if lines and len(unref) / len(lines) > MAX_UNREFERENCED:
        errors.append(f"{len(unref)}/{len(lines)} instructions Qlik sans "
                      f"commentaire « -- l.<n> » : {unref[:10]}")
    elif unref:
        warnings.append(f"instructions sans commentaire « -- l.<n> » : {unref}")
    if parser is not None and not errors:
        try:
            parser.parsePlan(raw)
        except Exception as exc:          # erreur de syntaxe Spark SQL
            errors.append("syntaxe SQL : " + str(exc).splitlines()[0][:300])
    if "TODO" in raw.replace("TODO(model)", ""):
        warnings.append("TODO laissé dans la requête : à relire")
    return sorted(set(errors), key=errors.index), warnings


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--only", help="id d'une table")
    p.add_argument("--parse", action="store_true",
                   help="valider aussi la syntaxe avec le parseur Spark")
    args = p.parse_args(argv)
    parser = get_parser() if args.parse else None
    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    failed, changed = 0, False
    for sec in manifest["sections"]:
        if sec["status"] == "skip" or (args.only and sec["id"] != args.only):
            continue
        errors, warnings = check_section(sec, args.workdir / sec["id"]
                                         / "query.sql", parser)
        if not errors and sec["status"] in ("escalade", "erreur_modele",
                                            "todo") and args.only:
            sec["status"] = "ok"         # corrigée à la main puis validée
            changed = True
        print(f"{'✗' if errors else '✓'} {sec['id']} ({sec['status']})")
        for e in errors:
            print(f"    ERREUR  {e}")
        for w in warnings:
            print(f"    info    {w}")
        failed += bool(errors)
    if changed:
        mpath.write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                         encoding="utf-8")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
