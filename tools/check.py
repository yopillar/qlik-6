"""Controles automatiques d'une requete traduite, sans modele et sans donnees.

On verifie work/<script>/<table>/query.sql par rapport a ce que le manifest
sait de la table :
  - il ne reste plus de bloc "TODO(model)"
  - il n'y a qu'une requete, et pas d'instruction interdite (CREATE, INSERT...)
  - elle ne lit que des vues ou des CTE qui existent
  - toutes les colonnes de sortie attendues y figurent
  - chaque instruction Qlik est citee dans un commentaire "l.<n>"
  - avec --parse (toujours le cas depuis translate.py), Spark parse la
    requete, puis l'analyse sur des vues vides quand on connait toutes leurs
    colonnes (colonne qui n'existe pas, fonction inconnue...)

Ca ne prouve pas que le resultat est juste, c'est le travail de verify.py
sur les donnees. Mais ca rattrape en une seconde les oublis classiques d'un
petit modele.

Usage :
    python tools/check.py work/ventes                  # toutes les tables
    python tools/check.py work/ventes --only 012_lot   # valide une table corrigee
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
from conf import work_dir  # noqa: E402

FORBIDDEN = (
    "CREATE",
    "INSERT",
    "DELETE",
    "UPDATE",
    "MERGE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CACHE",
    "REFRESH",
)
# Au-dela de 30 % d'instructions Qlik non citees, on considere que la requete a
# saute une partie du script.
MAX_UNREFERENCED = 0.3


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


TOKENS = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"|`(?:``|[^`])*`")


def strip_sql_strings(sql: str, idents: bool = False) -> str:
    """Vide les chaines '...' (et les noms `...` si idents=True). On traite les
    deux dans la meme regex, sinon l'apostrophe de `Payment's type` serait
    prise pour le debut d'une chaine."""

    def repl(m):
        tok = m.group()
        if tok[0] in "'\"":
            return "''"
        return "``" if idents else tok

    return TOKENS.sub(repl, sql)


SPARK_TYPES = {
    "int": "INT",
    "integer": "INT",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "smallint": "SMALLINT",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "decimal": "DECIMAL(38,10)",
    "numeric": "DECIMAL(38,10)",
    "number": "DOUBLE",
    "string": "STRING",
    "str": "STRING",
    "text": "STRING",
    "varchar": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}


def spark_type(t) -> str:
    """Traduit un type de conf/schema en type Spark (STRING si on ne le
    connait pas)."""
    if not t:
        return "STRING"
    t = str(t).lower().split("(")[0].strip()
    return SPARK_TYPES.get(t, "STRING")


# Erreurs liees aux types. Tant que toutes les colonnes ne sont pas typees (une
# colonne sans type est declaree STRING), on ne peut pas s'y fier.
TYPE_ERRORS = (
    "DATATYPE_MISMATCH",
    "CAST_WITHOUT_SUGGESTION",
    "UNEXPECTED_INPUT_TYPE",
    "BINARY_OP_DIFF_TYPES",
)
# Colonne introuvable : quand il n'y a pas de schema, les colonnes amont sont
# deduites du script et peuvent etre incompletes. On le signale, sans bloquer.
KNOWLEDGE_ERRORS = ("UNRESOLVED_COLUMN",)


class SparkChecker:
    """Parseur et analyseur de Spark, sans aucune donnee.

    La requete est analysee sur des vues vides qui ont les bonnes colonnes,
    ce qui suffit a verifier les noms de colonnes et de vues, les fonctions
    et les types de base. Rien n'est execute."""

    def __init__(self, spark):
        import threading

        self.spark = spark
        self.parser = spark._jsparkSession.sessionState().sqlParser()
        # les vues sont partagees entre threads
        self.lock = threading.Lock()

    def parse(self, sql: str) -> str | None:
        try:
            self.parser.parsePlan(sql)
            return None
        except Exception as exc:
            return "syntaxe SQL : " + str(exc).splitlines()[0][:300]

    def analyze(self, section: dict, sql: str) -> str | None:
        views = section.get("views", {})
        if not views or any(not v.get("columns") for v in views.values()):
            # on ne connait pas toutes les colonnes
            return None
        with self.lock:
            try:
                for name, v in views.items():
                    types = v.get("types") or {}
                    cols = [
                        f"CAST(NULL AS {spark_type(types.get(c))}) AS "
                        f"`{c.replace('`', '``')}`"
                        for c in dict.fromkeys(v["columns"])
                        if c != "_qlik_order"
                    ]
                    cols.append("CAST(NULL AS ARRAY<BIGINT>) AS _qlik_order")
                    self.spark.sql(
                        f"SELECT {', '.join(cols)} WHERE false"
                    ).createOrReplaceTempView(name)
                schema = self.spark.sql(sql).schema  # rien n'est execute
                # Types des colonnes produites, tels que Spark les ecrira :
                # fill_conf.py s'en sert pour le schema d'un fichier relu par
                # un autre script. Surs seulement si toutes les colonnes lues
                # sont typees (sinon une colonne recopiee vaut STRING par
                # defaut).
                section["output_types"] = {
                    f.name: f.dataType.simpleString().upper()
                    for f in schema.fields
                    if f.name != "_qlik_order"
                }
                section["output_types_sure"] = all(
                    set(v["columns"]) - {"_qlik_order"} <= set(v.get("types") or {})
                    for v in views.values()
                )
                return None
            except Exception as exc:
                msg = str(exc).splitlines()[0]
                all_typed = all(
                    set(v["columns"]) - {"_qlik_order"} <= set(v.get("types") or {})
                    for v in views.values()
                )
                if any(k in msg for k in TYPE_ERRORS) and not all_typed:
                    # types incomplets, on ne conclut pas
                    return None
                return "analyse Spark : " + msg[:300]


def get_parser():
    """Renvoie le verificateur Spark, ou None si pyspark n'est pas installe.

    Pas besoin du socle ici : on ne lit ni n'ecrit rien, une session locale
    minimale suffit pour parser et analyser."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # meme comportement que Spark 3.5
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Qlik est sensible a la casse
    spark.conf.set("spark.sql.caseSensitive", "true")
    spark.sparkContext.setLogLevel("ERROR")
    return SparkChecker(spark)


def is_table_from(code: str, pos: int) -> bool:
    """Dit si le FROM / JOIN trouve a pos introduit vraiment une table.

    En Spark SQL, FROM sert aussi a l'interieur de certaines fonctions :
    extract(YEAROFWEEK FROM col), trim(BOTH FROM col), substring(s FROM 2)...
    Le nom qui suit est alors une colonne, pas une vue. On regarde la
    parenthese ouvrante la plus proche : s'il n'y a pas de SELECT entre elle
    et le FROM, on est dans les arguments d'une fonction. Un FROM au premier
    niveau, ou un JOIN, introduit toujours une table."""
    if not re.match(r"FROM\b", code[pos:], re.I):
        return True
    depth = 0
    for k in range(pos - 1, -1, -1):
        if code[k] == ")":
            depth += 1
        elif code[k] == "(":
            if depth == 0:
                return bool(re.search(r"\bSELECT\b", code[k:pos], re.I))
            depth -= 1
    return True


def check_section(
    section: dict, query_path: Path, parser=None
) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    if not query_path.exists() or not query_path.read_text(encoding="utf-8").strip():
        return ["query.sql absent ou vide"], []
    raw = query_path.read_text(encoding="utf-8")
    if "TODO(model)" in raw:
        left = re.findall(r"TODO\(model\):\s*(l\.\d+|h\d+)", raw)
        errors.append(
            f"{raw.count('TODO(model)')} bloc(s) TODO(model) "
            f"restant(s) : {', '.join(left) or '?'}"
        )
    code = strip_sql_strings(strip_sql_comments(raw))
    code_no_idents = strip_sql_strings(strip_sql_comments(raw), idents=True)
    if code.strip().rstrip(";").count(";"):
        errors.append("plusieurs instructions : une seule requete attendue")
    for kw in FORBIDDEN:
        if re.search(rf"\b{kw}\b", code, re.I):
            errors.append(f"instruction interdite : {kw}")
    if re.search(r"```", raw):
        errors.append("balises ``` dans le fichier : SQL seul attendu")
    # Reflexe Qlik : 'l''ete'. En Spark SQL, '' accole deux chaines et
    # l'apostrophe disparait (on obtient "lete"). Il faut ecrire "l'ete" ou
    # 'l\'ete'.
    if re.search(r"\w''\w", strip_sql_comments(raw)):
        errors.append(
            "apostrophe doublee '' dans une chaine : en Spark SQL elle "
            'disparait ; ecrire "l\'ete" (guillemets doubles)'
        )

    # Noms de fonctions de date qui ressemblent a Spark sans en etre : le
    # modele les ecrit par analogie avec AddMonths ou d'autres SQL.
    for bad in re.findall(
        r"\b(add_month|month_add|addmonths?|dateadd|"
        r"date_add_months|add_years?|addyears)\s*\(",
        code_no_idents,
        re.I,
    ):
        errors.append(
            f"fonction {bad}() inconnue de Spark : AddMonths(d, n) "
            "s'ecrit add_months(d, n), AddYears(d, n) "
            "add_months(d, 12 * n)"
        )

    # vues et CTE lues par la requete
    ctes = {m.lower() for m in re.findall(r"(\w+)\s+AS\s*\(", code_no_idents, re.I)}
    allowed = ctes | {v.lower() for v in section.get("views", {})}
    unknown = []
    # Hors mode ANSI, "FROM t WHERE (x)" est lu comme l'alias WHERE avec une
    # colonne renommee en x : le filtre disparait sans erreur.
    if re.search(
        r"\bFROM\s+[\w`.]+\s+WHERE\s*\(\s*[\w`.]+\s*\)",
        code_no_idents.replace("``", "x"),
        re.I,
    ):
        errors.append(
            "'FROM table WHERE (colonne)' : Spark lit WHERE comme "
            "un alias de table et ignore le filtre. Ecrire "
            "'WHERE colonne' ou 'WHERE ((colonne))'"
        )
    for m in re.finditer(
        r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)(\s*\()?", code_no_idents, re.I
    ):
        if m.group(2):
            continue  # fonction-table (range(...), explode...)
        if not is_table_from(code_no_idents, m.start()):
            continue  # FROM d'une fonction : extract(... FROM col)...
        name = m.group(1).lower()
        if (
            name not in allowed
            and name not in ("values", "lateral")
            and m.group(1) not in unknown
        ):
            unknown.append(m.group(1))
    if unknown:
        errors.append(
            f"table ou vue inconnue : {', '.join(unknown)} (vues "
            f"autorisees : {', '.join(sorted(section.get('views', {}))) or 'aucune'}"
            " + les CTE de la requete)"
        )

    # colonnes de sortie
    cols = section.get("columns") or []
    if cols and "*" not in cols and "?" not in cols:
        missing = [
            c
            for c in cols
            if not re.search(r"(?<![\w])" + re.escape(c) + r"(?![\w])", code)
        ]
        if missing:
            errors.append(f"colonnes de sortie absentes : {missing}")

    # chaque instruction Qlik doit etre citee en "-- l.<n>"
    refs = {int(n) for n in re.findall(r"--\s*l\.(\d+)", raw)}
    lines = section.get("statement_lines", [])
    unref = [n for n in lines if n not in refs]
    if lines and len(unref) / len(lines) > MAX_UNREFERENCED:
        errors.append(
            f"{len(unref)}/{len(lines)} instructions Qlik sans "
            f"commentaire '-- l.<n>' : {unref[:10]}"
        )
    elif unref:
        warnings.append(f"instructions sans commentaire '-- l.<n>' : {unref}")
    # On lance l'analyse Spark meme s'il reste des TODO(model) : autant
    # remonter tout de suite les autres erreurs.
    if parser is not None and not [e for e in errors if "TODO(model)" not in e]:
        problem = parser.parse(raw) or parser.analyze(section, raw)
        if problem:
            errors.append(problem)
    if "TODO" in raw.replace("TODO(model)", ""):
        warnings.append("TODO laisse dans la requete : a relire")
    return sorted(set(errors), key=errors.index), warnings


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("workdir", type=Path, help="work/<script>")
    p.add_argument("--only", help="id d'une table")
    p.add_argument(
        "--parse",
        action="store_true",
        help="valider aussi la syntaxe avec le parseur Spark",
    )
    p.add_argument(
        "--mark",
        action="store_true",
        help="passer en escalade les tables validees que le controle "
        "refuse (utilise par migrate.py avant l'assemblage)",
    )
    args = p.parse_args(argv)
    args.workdir = work_dir(args.workdir)
    parser = get_parser() if args.parse else None
    mpath = args.workdir / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    failed, changed = 0, False
    for sec in manifest["sections"]:
        if sec["status"] == "skip" or (args.only and sec["id"] != args.only):
            continue
        errors, warnings = check_section(
            sec, args.workdir / sec["id"] / "query.sql", parser
        )
        if (
            not errors
            and sec["status"] in ("escalade", "erreur_modele", "todo")
            and args.only
        ):
            sec["status"] = "ok"  # corrigee a la main, et validee
            changed = True
        sure = [e for e in errors if not any(k in e for k in KNOWLEDGE_ERRORS)]
        if errors and not sure and args.mark:
            print(
                f"[!] {sec['id']} : {errors[0][:160]} (colonnes amont deduites "
                "du script : a confirmer, non bloquant)"
            )
            continue
        if sure and args.mark and sec["status"] in ("deterministic", "ok"):
            # La requete avait ete acceptee mais Spark la refuse. C'est souvent
            # un cas que le traducteur gere mal : on la repasse en escalade
            # avant qu'elle n'arrive dans le job.
            sec["status"] = "escalade"
            sec.setdefault("todos", []).append("controle : " + errors[0][:200])
            changed = True
            journal.update(
                args.workdir.parent / "journal.csv",
                {
                    "date": dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "script": manifest["script"],
                    "section": sec["id"],
                    "status": "escalade",
                    "problem": "controle refuse (traduction a verifier) : "
                    + " | ".join(errors)[:400],
                },
            )
        if args.mark and not errors:
            continue
        print(f"{'KO' if errors else 'OK'} {sec['id']} ({sec['status']})")
        for e in errors:
            print(f"    ERREUR  {e}")
        for w in warnings:
            print(f"    info    {w}")
        failed += bool(errors)
    if changed or parser:
        # avec --parse, les types de sortie calcules par Spark sont gardes
        mpath.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
