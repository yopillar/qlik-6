"""Contrôles automatiques d'une requête traduite (sans modèle, sans Spark).

Vérifie work/<script>/<table>/query.sql contre les faits du manifest :
  - plus aucun bloc « TODO(model) » ;
  - une seule requête, aucune instruction interdite (CREATE, INSERT…) ;
  - seules les vues et CTE existantes sont lues ;
  - chaque colonne de sortie attendue apparaît ;
  - chaque instruction Qlik est référencée par un commentaire « l.<n> » ;
  - avec --parse (ou depuis translate.py) : syntaxe validée par le parseur
    de Spark, puis requête analysée contre des vues vides quand toutes leurs
    colonnes sont connues (colonnes inexistantes, fonctions inconnues…).

Ces contrôles ne prouvent pas que le résultat est juste (c'est le rôle de
verify.py sur les données), mais ils rattrapent en une seconde les oublis
typiques d'un modèle faible.

Usage :
    python tools/check.py work/ventes                  # toutes les tables
    python tools/check.py work/ventes --only 012_lot   # valide une table corrigée
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import journal  # noqa: E402

FORBIDDEN = ("CREATE", "INSERT", "DELETE", "UPDATE", "MERGE", "DROP",
             "ALTER", "TRUNCATE", "CACHE", "REFRESH")
# au-delà de ce taux d'instructions non référencées, c'est une erreur
MAX_UNREFERENCED = 0.3


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


TOKENS = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:``|[^`])*`")


def strip_sql_strings(sql: str, idents: bool = False) -> str:
    """Vide les chaînes '…' (et les noms `…` si idents). Les deux sont lus
    ensemble : un nom `Payment's type` ne doit pas ouvrir une chaîne."""
    def repl(m):
        tok = m.group()
        if tok[0] in "'\"":
            return "''"
        return "``" if idents else tok
    return TOKENS.sub(repl, sql)


SPARK_TYPES = {"int": "INT", "integer": "INT", "bigint": "BIGINT", "long": "BIGINT",
               "smallint": "SMALLINT", "double": "DOUBLE", "float": "DOUBLE",
               "decimal": "DECIMAL(38,10)", "numeric": "DECIMAL(38,10)",
               "number": "DOUBLE", "string": "STRING", "str": "STRING",
               "text": "STRING", "varchar": "STRING", "date": "DATE",
               "timestamp": "TIMESTAMP", "datetime": "TIMESTAMP",
               "boolean": "BOOLEAN", "bool": "BOOLEAN"}


def spark_type(t) -> str:
    """Type de conf/schema -> type Spark (inconnu : STRING)."""
    if not t:
        return "STRING"
    t = str(t).lower().split("(")[0].strip()
    return SPARK_TYPES.get(t, "STRING")


# erreurs d'analyse qui dépendent des types : non fiables tant que les types
# des colonnes ne sont pas tous connus (une colonne sans type est STRING)
TYPE_ERRORS = ("DATATYPE_MISMATCH", "CAST_WITHOUT_SUGGESTION",
               "UNEXPECTED_INPUT_TYPE", "BINARY_OP_DIFF_TYPES")
# erreurs qui dépendent de la connaissance des colonnes amont (déduites du
# script quand il n'y a pas de schéma) : à signaler sans bloquer
KNOWLEDGE_ERRORS = ("UNRESOLVED_COLUMN",)


class SparkChecker:
    """Parseur et analyseur de Spark, sans données : la requête est analysée
    contre des vues vides aux colonnes connues (noms de colonnes et de vues,
    fonctions, types de base). Rien n'est exécuté."""

    def __init__(self, spark):
        import threading
        self.spark = spark
        self.parser = spark._jsparkSession.sessionState().sqlParser()
        self.lock = threading.Lock()       # vues partagées entre threads

    def parse(self, sql: str) -> str | None:
        try:
            self.parser.parsePlan(sql)
            return None
        except Exception as exc:
            return "syntaxe SQL : " + str(exc).splitlines()[0][:300]

    def analyze(self, section: dict, sql: str) -> str | None:
        views = section.get("views", {})
        if not views or any(not v.get("columns") for v in views.values()):
            return None                    # colonnes inconnues : pas d'analyse
        with self.lock:
            try:
                for name, v in views.items():
                    types = v.get("types") or {}
                    cols = [f"CAST(NULL AS {spark_type(types.get(c))}) AS "
                            f"`{c.replace('`', '``')}`"
                            for c in dict.fromkeys(v["columns"])
                            if c != "_qlik_order"]
                    cols.append("CAST(NULL AS ARRAY<BIGINT>) AS _qlik_order")
                    self.spark.sql(f"SELECT {', '.join(cols)} WHERE false") \
                        .createOrReplaceTempView(name)
                self.spark.sql(sql).schema       # analyse seulement
                return None
            except Exception as exc:
                msg = str(exc).splitlines()[0]
                all_typed = all(set(v["columns"]) - {"_qlik_order"}
                                <= set(v.get("types") or {}) for v in views.values())
                if any(k in msg for k in TYPE_ERRORS) and not all_typed:
                    return None                  # types inconnus : non fiable
                return "analyse Spark : " + msg[:300]


def get_parser():
    """Vérificateur Spark (None si pyspark n'est pas installé)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = (SparkSession.builder.master("local[1]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.ansi.enabled", "false")      # comme Spark 3.5
             .config("spark.sql.caseSensitive", "true")      # comme Qlik
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return SparkChecker(spark)


def check_section(section: dict, query_path: Path,
                  parser=None) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    if not query_path.exists() or not query_path.read_text(encoding="utf-8").strip():
        return ["query.sql absent ou vide"], []
    raw = query_path.read_text(encoding="utf-8")
    if "TODO(model)" in raw:
        left = re.findall(r"TODO\(model\):\s*(l\.\d+|h\d+)", raw)
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
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)(\s*\()?",
                         code_no_idents, re.I):
        if m.group(2):
            continue          # fonction-table (range(…), explode…), pas une vue
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
    # les blocs TODO(model) restants n'empêchent pas d'analyser le reste
    if parser is not None and not [e for e in errors if "TODO(model)" not in e]:
        problem = parser.parse(raw) or parser.analyze(section, raw)
        if problem:
            errors.append(problem)
    if "TODO" in raw.replace("TODO(model)", ""):
        warnings.append("TODO laissé dans la requête : à relire")
    return sorted(set(errors), key=errors.index), warnings


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--only", help="id d'une table")
    p.add_argument("--parse", action="store_true",
                   help="valider aussi la syntaxe avec le parseur Spark")
    p.add_argument("--mark", action="store_true",
                   help="passer en escalade les tables validées que le contrôle "
                        "refuse (utilisé par migrate.py avant l'assemblage)")
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
        sure = [e for e in errors if not any(k in e for k in KNOWLEDGE_ERRORS)]
        if errors and not sure and args.mark:
            print(f"⚠ {sec['id']} : {errors[0][:160]} (colonnes amont déduites "
                  "du script : à confirmer, non bloquant)")
            continue
        if sure and args.mark and sec["status"] in ("deterministic", "ok"):
            # traduction acceptée mais refusée par l'analyse Spark : souvent un
            # cas que le traducteur gère mal ; on bloque avant l'exécution
            sec["status"] = "escalade"
            sec.setdefault("todos", []).append("contrôle : " + errors[0][:200])
            changed = True
            journal.update(args.workdir.parent / "journal.csv", {
                "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "script": manifest["script"], "section": sec["id"],
                "status": "escalade",
                "problem": "contrôle refusé (traduction à vérifier) : "
                           + " | ".join(errors)[:400]})
        if args.mark and not errors:
            continue
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
