"""Traduction déterministe Qlik -> Spark SQL (sans modèle).

Deux niveaux :
  - expressions : If, &, arithmétique, comparaisons, fonctions Qlik courantes
    -> expressions Spark SQL natives (aucune UDF) ;
  - instructions : LOAD (champs, WHERE, GROUP BY, DISTINCT, FIRST, LOAD
    empilés), FROM fichier / RESIDENT / INLINE / SQL SELECT simple, JOIN,
    KEEP, CONCATENATE, DROP FIELD, ApplyMap -> une CTE chacune.

Ce qui ne se traduit pas avec certitude lève Untranslatable : la CTE devient
un bloc « TODO(model) » que le modèle complète, le reste de la requête est
déjà écrit.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field


# Colonne technique portée par TOUTES les tables : l'ordre de chargement Qlik.
# Créée à la lecture (job.py), propagée par chaque étape selon les règles de
# Qlik, écrite dans les sorties : le script suivant hérite de l'ordre exact.
# Type array<bigint>, comparé élément par élément : une concaténation préfixe
# 0 (lignes existantes) ou 1 (lignes ajoutées) sans renuméroter la table.
ORD = "_qlik_order"


class Untranslatable(Exception):
    """L'élément Qlik ne peut pas être traduit de façon sûre."""


# ==========================================================================
# Noms SQL
# ==========================================================================

SQL_RESERVED = {
    "ALL", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CROSS",
    "CURRENT", "DATE", "DESC", "DISTINCT", "ELSE", "END", "EXCEPT", "FALSE",
    "FOR", "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INTERSECT",
    "INTERVAL", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "NATURAL",
    "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "RIGHT", "SELECT", "SEMI",
    "TABLE", "THEN", "TIME", "TIMESTAMP", "TO", "TRUE", "UNION", "USING",
    "VALUES", "WHEN", "WHERE", "WITH", "YEAR", "MONTH", "DAY", "USER", "ROW",
    "ROWS", "RANGE", "WINDOW", "OVER", "PARTITION", "ANTI", "LATERAL",
}


def ident(name: str) -> str:
    """Nom de colonne SQL : tel quel si simple, sinon entre backticks."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) and \
            name.upper() not in SQL_RESERVED:
        return name
    return "`" + name.replace("`", "``") + "`"


def view_name(name: str) -> str:
    """Nom de vue / CTE sûr (ASCII). Les noms non ASCII reçoivent un suffixe
    court pour rester uniques ; le nom Qlik est rappelé en commentaire."""
    v = re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_")
    if not v or not re.fullmatch(r"[A-Za-z0-9_]+", name):
        h = hashlib.sha1(name.encode()).hexdigest()[:6]
        v = f"{v or 't'}_{h}"
    if v[0].isdigit():
        v = "t_" + v
    if v.upper() in SQL_RESERVED:
        v = v + "_t"
    return v


def sql_string(value: str) -> str:
    """Littéral Spark SQL. Une apostrophe Qlik ('l''été') ne se double PAS en
    Spark (deux chaînes collées : « lété ») : on passe en guillemets doubles,
    lisibles et sans échappement ; antislash en dernier recours."""
    v = value.replace("\\", "\\\\")
    if "'" in value and '"' not in value:
        return '"' + v + '"'
    return "'" + v.replace("'", "\\'") + "'"


# ==========================================================================
# Expressions
# ==========================================================================

TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<str>'(?:[^']|'')*')
  | (?P<num>\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)
  | (?P<field>\[[^\]]*\]|"[^"]*")
  | (?P<var>\$\()
  | (?P<op><=|>=|<>|=|<|>|\+|-|\*|/|&|\(|\)|,)
  | (?P<word>[^\W\d][\w.#%@$]*|[%@#][\w.#%@$]*)
""", re.X)

KEYWORDS = {"AND", "OR", "NOT", "XOR", "LIKE", "PRECEDES", "FOLLOWS",
            "DISTINCT", "BITAND", "BITOR", "BITXOR", "BITNOT"}


def tokenize_expr(text: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(text):
        m = TOKEN.match(text, pos)
        if not m:
            raise Untranslatable(f"caractère non reconnu « {text[pos:pos + 10]} »")
        pos = m.end()
        kind = m.lastgroup
        val = m.group()
        if kind == "ws":
            continue
        if kind == "var":
            raise Untranslatable("variable $(…) non résolue dans l'expression")
        if kind == "word" and val.upper() in KEYWORDS:
            kind, val = "kw", val.upper()
        out.append((kind, val))
    return out


@dataclass
class ExprCtx:
    """Contexte de traduction d'une expression."""
    qualifier: str = ""                       # alias de la source (« s »)
    applymap: object = None                   # callback (map, key, default)
    date_format: str | None = None            # DateFormat Qlik du script
    timestamp_format: str | None = None
    recno: str | None = None                  # RecNo()/RowNo() d'un AUTOGENERATE
    iterno: str | None = None                 # IterNo() d'un AUTOGENERATE … WHILE
    rowno: str | None = None                  # RowNo() si différent de RecNo()
    order_by: str | None = None               # ORDER BY du LOAD (RowNo/RecNo)
    in_where: bool = False
    exists: object = None                     # callback Exists(champ, valeur)
    order_key: str | None = None              # clé d'ordre (FirstValue…)
    output_field: object = None               # champ du LOAD en cours (Peek)
    peek_table: object = None                 # Peek('f', n, 'T')
    noofrows: object = None                   # NoOfRows('T')
    fieldvalue: str | None = None             # champ lu par FieldValue()
    technical: bool = False                   # AutoNumber/Hash utilisés


class Bool(str):
    """Expression SQL booléenne (comparaison, AND, IsNull…)."""


class DateSql(str):
    """Expression SQL de type DATE (Date(), MakeDate(), Today()…)."""


def as_value(sql: str) -> str:
    """Booléen utilisé comme valeur : Qlik donne -1 (vrai) ou 0 (faux)."""
    return f"CASE WHEN {sql} THEN -1 ELSE 0 END" if isinstance(sql, Bool) else sql


def as_cond(sql: str) -> str:
    """Valeur utilisée comme condition : en Qlik, tout nombre non nul est vrai."""
    return sql if isinstance(sql, Bool) else Bool(f"(({sql}) <> 0)")


class ExprParser:
    """Analyse descendante : or < and < not < comparaison < & < + - < * / <
    unaire < primaire. Chaque règle renvoie directement du SQL."""

    def __init__(self, text: str, ctx: ExprCtx):
        self.t = tokenize_expr(text)
        self.i = 0
        self.ctx = ctx

    # -- outils
    def peek(self, k=0):
        j = self.i + k
        return self.t[j] if j < len(self.t) else (None, None)

    def take(self):
        tok = self.peek()
        self.i += 1
        return tok

    def expect(self, val):
        kind, v = self.take()
        if v != val:
            raise Untranslatable(f"« {val} » attendu, trouvé « {v} »")

    def parse(self) -> str:
        sql = self.p_or()
        if self.i != len(self.t):
            raise Untranslatable(f"reste non compris : « {self.peek()[1]} »")
        return sql

    # -- règles
    def p_or(self):
        left = self.p_and()
        while self.peek() == ("kw", "OR") or self.peek() == ("kw", "XOR"):
            op = self.take()[1]
            right = self.p_and()
            left = Bool(f"({as_cond(left)} OR {as_cond(right)})" if op == "OR"
                        else f"({as_cond(left)} <> {as_cond(right)})")
        return left

    def p_and(self):
        left = self.p_not()
        while self.peek() == ("kw", "AND"):
            self.take()
            left = Bool(f"({as_cond(left)} AND {as_cond(self.p_not())})")
        return left

    def p_not(self):
        if self.peek() == ("kw", "NOT"):
            self.take()
            return Bool(f"(NOT {as_cond(self.p_not())})")
        return self.p_cmp()

    def p_cmp(self):
        left = self.p_concat()
        kind, v = self.peek()
        if kind == "op" and v in ("=", "<>", "<", ">", "<=", ">="):
            self.take()
            return Bool(f"({as_value(left)} {v} {as_value(self.p_concat())})")
        if (kind, v) == ("kw", "LIKE"):
            self.take()
            pk, pv = self.take()
            if pk != "str":
                raise Untranslatable("LIKE avec un motif non littéral")
            pat = pv[1:-1].replace("''", "'")
            pat = pat.replace("%", "\\%").replace("_", "\\_") \
                .replace("*", "%").replace("?", "_")
            # LIKE Qlik : jokers * ? et insensible à la casse
            return Bool(f"(lower({as_value(left)}) LIKE lower({sql_string(pat)}))")
        if kind == "kw" and v in ("PRECEDES", "FOLLOWS"):
            raise Untranslatable(f"opérateur {v}")
        return left

    def p_concat(self):
        parts = [self.p_add()]
        while self.peek() == ("op", "&"):
            self.take()
            parts.append(self.p_add())
        if len(parts) == 1:
            return parts[0]
        # & Qlik : un null compte comme une chaîne vide
        return "concat_ws('', " + ", ".join(f"CAST({as_value(p)} AS STRING)"
                                            for p in parts) + ")"

    def p_add(self):
        left = self.p_mul()
        while self.peek() in (("op", "+"), ("op", "-")):
            op = self.take()[1]
            right = self.p_mul()
            if op == "-" and isinstance(left, DateSql) and isinstance(right, DateSql):
                # Qlik : une date est un nombre de jours, date - date = jours
                left = f"datediff({left}, {right})"
            elif isinstance(left, DateSql) and not isinstance(right, DateSql):
                left = DateSql(f"({left} {op} {as_value(right)})")   # date ± jours
            else:
                left = f"({as_value(left)} {op} {as_value(right)})"
        return left

    def p_mul(self):
        left = self.p_unary()
        while self.peek() in (("op", "*"), ("op", "/")):
            op = self.take()[1]
            left = f"({as_value(left)} {op} {as_value(self.p_unary())})"
        return left

    def p_unary(self):
        if self.peek() == ("op", "-"):
            self.take()
            return f"(- {as_value(self.p_unary())})"
        if self.peek() == ("op", "+"):
            self.take()
        return self.p_primary()

    def p_primary(self):
        kind, v = self.take()
        if kind == "num":
            return v
        if kind == "str":
            return sql_string(v[1:-1].replace("''", "'"))
        if kind == "field":
            return self.field(v[1:-1])
        if (kind, v) == ("op", "("):
            inner = self.p_or()
            self.expect(")")
            return inner
        if kind == "word":
            if self.peek() == ("op", "("):
                return self.call(v)
            return self.field(v)
        raise Untranslatable(f"élément inattendu « {v} »")

    def field(self, name: str) -> str:
        q = f"{self.ctx.qualifier}." if self.ctx.qualifier else ""
        return q + ident(name)

    def call(self, name: str) -> str:
        self.expect("(")
        args, raw, distinct = [], [], False
        if self.peek() == ("kw", "DISTINCT"):
            self.take()
            distinct = True
        if self.peek() != ("op", ")"):
            while True:
                start = self.i
                args.append(self.p_or())
                raw.append(self.t[start:self.i])
                if self.peek() == ("op", ","):
                    self.take()
                    continue
                break
        self.expect(")")
        n = name.lower()
        if n == "if" and args:          # condition, puis valeurs
            args = [as_cond(args[0])] + [as_value(a) for a in args[1:]]
        else:
            args = [as_value(a) for a in args]
        return translate_function(name, args, raw, distinct, self.ctx)


def literal(raw_tokens) -> str | None:
    """Valeur d'un argument s'il est une chaîne littérale seule."""
    if len(raw_tokens) == 1 and raw_tokens[0][0] == "str":
        return raw_tokens[0][1][1:-1].replace("''", "'")
    return None


def number(raw_tokens) -> str | None:
    if len(raw_tokens) == 1 and raw_tokens[0][0] == "num":
        return raw_tokens[0][1]
    return None


def signed_int(raw_tokens) -> int | None:
    """Entier littéral, éventuellement négatif (-1)."""
    if len(raw_tokens) == 1 and raw_tokens[0][0] == "num":
        return int(float(raw_tokens[0][1]))
    if len(raw_tokens) == 2 and raw_tokens[0] == ("op", "-") \
            and raw_tokens[1][0] == "num":
        return -int(float(raw_tokens[1][1]))
    return None


def regex_class(chars: str) -> str:
    """Classe de caractères Java regex, échappée."""
    return "".join("\\" + c if c in "\\]^-[" else c for c in chars)


def regex_escape(text: str) -> str:
    return "".join("\\" + c if c in ".$|()[]{}^?*+\\" else c for c in text)


QLIK_DATE_TOKENS = [
    ("YYYY", "yyyy"), ("YY", "yy"), ("MMMM", "MMMM"), ("MMM", "MMM"),
    ("MM", "MM"), ("M", "M"), ("DD", "dd"), ("D", "d"), ("WWWW", "EEEE"),
    ("WWW", "EEE"), ("hh", "HH"), ("h", "H"), ("mm", "mm"), ("m", "m"),
    ("ss", "ss"), ("s", "s"), ("fff", "SSS"), ("ff", "SS"), ("f", "S"),
    ("TT", "a"),
]


def qlik_format_to_spark(fmt: str) -> str:
    """Format de date Qlik -> motif Spark (lettres inconnues : refus)."""
    out, i = [], 0
    twelve = "TT" in fmt
    while i < len(fmt):
        for q, s in QLIK_DATE_TOKENS:
            if fmt.startswith(q, i):
                if q in ("hh", "h") and twelve:
                    s = s.lower()
                out.append(s)
                i += len(q)
                break
        else:
            c = fmt[i]
            if c.isalpha():
                raise Untranslatable(f"format de date Qlik « {fmt} » non géré")
            out.append(c if c not in "'" else "''")
            i += 1
    return "".join(out)


def translate_function(name, a, raw, distinct, ctx: ExprCtx) -> str:
    sql = _translate_function(name, a, raw, distinct, ctx)
    if name.lower() in ("date", "date#", "makedate", "today", "addmonths",
                        "addyears", "monthstart", "yearstart", "monthend"):
        return DateSql(sql)
    return sql


def _translate_function(name, a, raw, distinct, ctx: ExprCtx) -> str:
    n = name.lower()
    k = len(a)

    def need(*counts):
        if k not in counts:
            raise Untranslatable(f"{name}() avec {k} argument(s)")

    simple = {"upper": "upper", "lower": "lower", "trim": "trim",
              "ltrim": "ltrim", "rtrim": "rtrim", "len": "length",
              "capitalize": "initcap", "fabs": "abs", "sqrt": "sqrt",
              "exp": "exp", "log": "ln", "log10": "log10", "chr": "char",
              "ord": "ascii", "year": "year", "day": "dayofmonth",
              "hour": "hour", "minute": "minute", "second": "second",
              "monthend": "last_day", "sign": "sign"}
    if n in simple:
        need(1)
        return f"{simple[n]}({a[0]})"
    if n == "if":
        need(2, 3)
        return f"CASE WHEN {a[0]} THEN {a[1]} ELSE {a[2] if k == 3 else 'NULL'} END"
    if n == "alt":
        return f"COALESCE({', '.join(a)})"
    if n in ("left", "right"):
        need(2)
        return f"{n}({a[0]}, {a[1]})"
    if n == "mid":
        need(2, 3)
        return f"substring({', '.join(a)})"
    if n == "index":
        need(2, 3)
        if k == 2:
            return f"instr({a[0]}, {a[1]})"
        sub, occ = literal(raw[1]), number(raw[2]) or (
            "-" + raw[2][1][1] if len(raw[2]) == 2 and raw[2][0] == ("op", "-")
            and raw[2][1][0] == "num" else None)
        if sub is None or occ is None:
            raise Untranslatable("Index() avec 3 arguments non littéraux")
        occ = int(float(occ))
        count = f"(size(split({a[0]}, {sql_string(regex_escape(sub))})) - 1)"
        if occ == 0:
            return "0"
        if occ > 0:          # position de la n-ième occurrence
            pos = f"length(substring_index({a[0]}, {a[1]}, {occ})) + 1"
        else:                # n-ième occurrence en partant de la fin
            pos = (f"length({a[0]}) - length(substring_index({a[0]}, {a[1]}, "
                   f"{occ})) - length({a[1]}) + 1")
        return f"CASE WHEN {count} >= {abs(occ)} THEN {pos} ELSE 0 END"
    if n == "replace":
        need(3)
        return f"replace({a[0]}, {a[1]}, {a[2]})"
    if n == "repeat":
        need(2)
        return f"repeat({a[0]}, {a[1]})"
    if n == "subfield":
        need(3)
        sep = literal(raw[1])
        if sep is None:
            raise Untranslatable("SubField avec séparateur non littéral")
        return f"element_at(split({a[0]}, {sql_string(regex_escape(sep))}), {a[2]})"
    if n in ("purgechar", "keepchar"):
        need(2)
        chars = literal(raw[1])
        if chars is None or chars == "":
            raise Untranslatable(f"{name} avec liste non littérale")
        neg = "^" if n == "keepchar" else ""
        return (f"regexp_replace({a[0]}, "
                f"{sql_string('[' + neg + regex_class(chars) + ']')}, '')")
    if n == "text":
        need(1)
        return f"CAST({a[0]} AS STRING)"
    if n == "num#":
        need(1)
        return f"CAST({a[0]} AS DOUBLE)"
    if n == "num":
        need(1, 2, 3, 4)
        return a[0]                               # format d'affichage
    if n in ("date#", "timestamp#"):
        fmt = literal(raw[1]) if k >= 2 else (
            ctx.date_format if n == "date#" else ctx.timestamp_format)
        if k > 2 or fmt is None:
            raise Untranslatable(f"{name} sans format littéral")
        ts = (f"try_to_timestamp(CAST({a[0]} AS STRING), "
              f"{sql_string(qlik_format_to_spark(fmt))})")
        return f"CAST({ts} AS DATE)" if n == "date#" else ts
    if n == "date":
        need(1, 2)
        return f"CAST({a[0]} AS DATE)"            # format d'affichage ignoré
    if n == "timestamp":
        need(1, 2)
        return f"CAST({a[0]} AS TIMESTAMP)"
    if n in ("today", "now"):
        return "current_date()" if n == "today" else "current_timestamp()"
    if n == "month":
        need(1)
        return f"month({a[0]})"                   # ⚠ Qlik : valeur duale
    if n == "weekday":
        need(1)
        return f"((dayofweek({a[0]}) + 5) % 7)"
    if n == "week":
        need(1)
        return f"weekofyear({a[0]})"
    if n == "weekyear":
        need(1)
        return f"extract(YEAROFWEEK FROM {a[0]})"     # année de la semaine ISO
    if n == "isnum":
        need(1)
        return Bool(f"(try_cast(CAST({a[0]} AS STRING) AS DOUBLE) IS NOT NULL)")
    if n == "istext":
        need(1)
        return Bool(f"({a[0]} IS NOT NULL AND try_cast(CAST({a[0]} AS STRING) "
                    "AS DOUBLE) IS NULL)")
    if n == "exists":
        need(1, 2)
        field_name = literal(raw[0]) if raw[0] and raw[0][0][0] == "str" else (
            raw[0][0][1].strip("[]\"") if len(raw[0]) == 1 else None)
        if field_name is None or ctx.exists is None:
            raise Untranslatable("Exists() avec un nom de champ non littéral")
        value = a[1] if k == 2 else f"{ctx.qualifier + '.' if ctx.qualifier else ''}" \
            f"{ident(field_name)}"
        value_tokens = [v.lower() for _, v in raw[1]] if k == 2 else None
        return Bool(ctx.exists(field_name, value, value_tokens))
    if n == "makedate":
        need(1, 2, 3)
        m = a[1] if k > 1 else "1"
        d = a[2] if k > 2 else "1"
        return f"make_date({a[0]}, {m}, {d})"
    if n == "addmonths":
        need(2)
        return f"add_months({a[0]}, {a[1]})"
    if n == "addyears":
        need(2)
        return f"add_months({a[0]}, 12 * ({a[1]}))"
    if n in ("monthstart", "yearstart"):
        need(1)
        return f"trunc({a[0]}, {'MM' if n == 'monthstart' else 'YYYY'!r})"
    if n in ("floor", "ceil"):
        need(1, 2)
        if k == 1:
            return f"{n}({a[0]})"
        return f"({n}({a[0]} / {a[1]}) * {a[1]})"     # 2e arg Qlik = pas
    if n == "rangesum":
        # RangeSum ignore les valeurs nulles (comptées 0)
        return "(" + " + ".join(f"COALESCE({x}, 0)" for x in a) + ")"
    if n in ("rangemax", "rangemin"):
        return f"{'greatest' if n == 'rangemax' else 'least'}({', '.join(a)})"
    if n == "round":
        need(1, 2)
        if k == 1:
            return f"round({a[0]})"
        return f"(round({a[0]} / {a[1]}) * {a[1]})"   # 2e arg Qlik = pas
    if n == "mod":
        need(2)
        return f"pmod({a[0]}, {a[1]})"
    if n == "div":
        need(2)
        return f"CAST({a[0]} / {a[1]} AS BIGINT)"
    if n == "pow":
        need(2)
        return f"power({a[0]}, {a[1]})"
    if n == "dual":
        need(2)
        return a[0]       # valeur duale : Parquet n'a qu'un type, on garde le texte
    if n == "null":
        return "NULL"
    if n == "isnull":
        need(1)
        return Bool(f"({a[0]} IS NULL)")
    if n in ("match", "mixmatch", "wildmatch"):
        if k < 2:
            raise Untranslatable(f"{name} sans valeur")
        whens = []
        for pos, (sql, r) in enumerate(zip(a[1:], raw[1:]), 1):
            if n == "match":
                whens.append(f"WHEN {a[0]} = {sql} THEN {pos}")
            elif n == "mixmatch":
                whens.append(f"WHEN lower({a[0]}) = lower({sql}) THEN {pos}")
            else:
                pat = literal(r)
                if pat is None:
                    raise Untranslatable("WildMatch avec motif non littéral")
                like = pat.replace("%", "\\%").replace("_", "\\_") \
                    .replace("*", "%").replace("?", "_")
                whens.append(f"WHEN lower({a[0]}) LIKE lower({sql_string(like)})"
                             f" THEN {pos}")
        return "CASE " + " ".join(whens) + " ELSE 0 END"
    if n == "pick":
        if k < 2:
            raise Untranslatable("Pick sans valeur")
        whens = " ".join(f"WHEN {pos} THEN {v}" for pos, v in enumerate(a[1:], 1))
        return f"CASE {a[0]} {whens} END"
    if n in ("sum", "min", "max", "avg", "count"):
        need(1)
        return f"{n}({'DISTINCT ' if distinct else ''}{a[0]})"
    if n in ("maxstring", "minstring"):
        need(1)
        return f"{n[:3]}({a[0]})"
    if n == "only":
        need(1)
        return f"CASE WHEN count(DISTINCT {a[0]}) = 1 THEN min({a[0]}) END"
    if n == "concat":
        need(1, 2)
        sep = a[1] if k == 2 else "''"
        return (f"concat_ws({sep}, array_sort(collect_list("
                f"{'DISTINCT ' if distinct else ''}CAST({a[0]} AS STRING))))")
    if n == "previous":
        need(1, 2)
        if ctx.in_where or not ctx.order_by:
            raise Untranslatable("Previous() dans un WHERE")
        lag = int(number(raw[1]) or 1) if k == 2 else 1
        return f"lag({a[0]}, {lag}) OVER (ORDER BY {ctx.order_by})"
    if n == "peek":
        need(1, 2, 3)
        fname = literal(raw[0])
        row = signed_int(raw[1]) if k >= 2 else -1
        if fname is None or row is None:
            raise Untranslatable("Peek() avec champ ou ligne non littéral")
        if k == 3:
            table = literal(raw[2])
            if table is None or ctx.peek_table is None:
                raise Untranslatable("Peek() avec table non littérale")
            return ctx.peek_table(fname, row, table)
        # table en cours : valeur du champ sur la ligne précédente du résultat
        if row >= 0 or ctx.in_where or ctx.output_field is None:
            raise Untranslatable(f"Peek('{fname}', {row}) sur la table en cours")
        return (f"lag({ctx.output_field(fname)}, {-row}) OVER "
                f"(ORDER BY {ctx.order_by})")
    if n == "noofrows":
        need(1)
        table = literal(raw[0])
        if table is None or ctx.noofrows is None:
            raise Untranslatable("NoOfRows() avec table non littérale")
        return ctx.noofrows(table)
    if n in ("firstvalue", "lastvalue"):
        need(1)
        if not ctx.order_key:
            raise Untranslatable(f"{name}() après un ORDER BY … DESC")
        # 1re / dernière valeur non nulle dans l'ordre de chargement
        fn = "min_by" if n == "firstvalue" else "max_by"
        return (f"{fn}({a[0]}, CASE WHEN {a[0]} IS NOT NULL THEN "
                f"{ctx.order_key} END)")
    if n in ("recno", "rowno", "iterno"):
        if n == "iterno":
            if ctx.iterno is None:
                raise Untranslatable("IterNo() hors AUTOGENERATE … WHILE")
            return ctx.iterno
        if n == "rowno" and ctx.rowno is not None:
            return ctx.rowno
        if ctx.recno is not None:                   # AUTOGENERATE : exact
            return ctx.recno
        if ctx.in_where:
            raise Untranslatable(f"{name}() dans un WHERE")
        # ordre du LOAD … ORDER BY, sinon ordre de lecture du fichier
        order = ctx.order_by or "monotonically_increasing_id()"
        return f"row_number() OVER (ORDER BY {order})"
    if n == "fieldvalue":
        need(2)
        if ctx.fieldvalue is None or literal(raw[0]) != ctx.fieldvalue:
            raise Untranslatable("FieldValue() hors AUTOGENERATE "
                                 "FieldValueCount()")
        return "__fv"
    if n in ("autonumber", "autonumberhash128", "autonumberhash256"):
        if k == 0:
            raise Untranslatable(f"{name}() sans argument")
        # Qlik numérote 1, 2, 3… dans l'ordre de chargement : ici une clé
        # technique stable (même valeur pour la même combinaison, dans
        # toutes les tables), mais pas les mêmes nombres que Qlik
        ctx.technical = True
        vals = a[:1] if n == "autonumber" else a
        return "xxhash64(" + ", ".join(f"CAST({v} AS STRING)" for v in vals) + ")"
    if n in ("hash128", "hash160", "hash256"):
        if k == 0:
            raise Untranslatable(f"{name}() sans argument")
        ctx.technical = True
        joined = ("concat_ws(char(31), " + ", ".join(
            f"COALESCE(CAST({v} AS STRING), '')" for v in a) + ")")
        return {"hash128": f"md5({joined})", "hash160": f"sha1({joined})",
                "hash256": f"sha2({joined}, 256)"}[n]
    if n == "applymap":
        need(2, 3)
        mname = literal(raw[0])
        if mname is None or ctx.applymap is None:
            raise Untranslatable("ApplyMap avec nom de mapping non littéral")
        return ctx.applymap(mname, a[1], a[2] if k == 3 else None)
    raise Untranslatable(f"fonction {name}() non gérée par le traducteur")


def expr(text: str, ctx: ExprCtx) -> str:
    """Expression utilisée comme valeur (champ, GROUP BY, argument)."""
    return as_value(ExprParser(text, ctx).parse())


def cond(text: str, ctx: ExprCtx) -> str:
    """Expression utilisée comme condition (WHERE)."""
    return as_cond(ExprParser(text, ctx).parse())


# ==========================================================================
# Découpage d'une instruction LOAD
# ==========================================================================

def _scan(text: str):
    """Itère (position, profondeur) hors chaînes, crochets et guillemets."""
    depth, quote = 0, None
    for i, ch in enumerate(text):
        if quote:
            if (quote == "[" and ch == "]") or (quote != "[" and ch == quote):
                quote = None
            continue
        if ch in "'\"[":
            quote = ch
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        yield i, depth


def find_keywords(text: str, words: tuple[str, ...]) -> list[tuple[int, str]]:
    """Positions des mots-clés au niveau 0 (hors chaînes et parenthèses)."""
    found, upper = [], text.upper()
    for i, depth in _scan(text):
        if depth != 0 or (i > 0 and (text[i - 1].isalnum() or text[i - 1] in "_#.$")):
            continue
        for w in words:
            end = i + len(w)
            if upper.startswith(w, i) and (end >= len(text) or not (
                    text[end].isalnum() or text[end] in "_#")):
                found.append((i, w))
                break
    return found


def split_top(s: str, sep: str = ",") -> list[str]:
    parts, last = [], 0
    for i, depth in _scan(s):
        if depth == 0 and s[i] == sep:
            parts.append(s[last:i])
            last = i + 1
    parts.append(s[last:])
    return [p.strip() for p in parts if p.strip()]


@dataclass
class Segment:
    fields: list[tuple[str, str | None]]     # (expression Qlik, alias)
    distinct: bool = False
    source: tuple | None = None              # ('file'|'resident'|'inline'|'sql'|'autogen', …)
    where: str | None = None
    group_by: list[str] = field(default_factory=list)
    order_by: str | None = None
    has_while: bool = False
    while_cond: str | None = None


CLAUSES = ("FROM", "RESIDENT", "INLINE", "AUTOGENERATE", "EXTENSION",
           "WHERE", "WHILE", "GROUP BY", "ORDER BY")


def parse_fields(text: str) -> list[tuple[str, str | None]]:
    out = []
    for part in split_top(text):
        m = None
        for pos, _ in reversed(find_keywords(part, ("AS",))):
            m = pos
            break
        if m is not None:
            alias = part[m + 2:].strip().strip("[]\"")
            if not alias:
                raise Untranslatable(f"alias vide : {part[:40]}")
            out.append((part[:m].strip(), alias))
        else:
            out.append((part.strip(), None))
    return out


def parse_segment(text: str) -> Segment:
    """« LOAD [DISTINCT] champs [FROM|RESIDENT|INLINE…] [WHERE] [GROUP BY] »"""
    body = re.sub(r"^\s*LOAD\b", "", text, flags=re.I).strip()
    distinct = False
    m = re.match(r"DISTINCT\b", body, re.I)
    if m:
        distinct, body = True, body[m.end():].strip()
    # un champ peut porter le nom d'un mot-clé (« …, Resident » ou
    # « … AS Resident ») : ce n'est alors pas une clause
    kws = [(pos, kw) for pos, kw in find_keywords(body, CLAUSES)
           if not re.search(r"(\bAS|,)\s*$", body[:pos], re.I)
           and not re.match(r"\s*(,|$)", body[pos + len(kw):])]
    end_fields = kws[0][0] if kws else len(body)
    seg = Segment(parse_fields(body[:end_fields]), distinct)
    for k, (pos, kw) in enumerate(kws):
        nxt = kws[k + 1][0] if k + 1 < len(kws) else len(body)
        arg = body[pos + len(kw):nxt].strip().rstrip(";").strip()
        if kw == "FROM":
            seg.source = ("file", arg)
        elif kw == "RESIDENT":
            seg.source = ("resident", arg.strip("[]\""))
        elif kw == "INLINE":
            seg.source = ("inline", arg)
        elif kw in ("AUTOGENERATE", "EXTENSION"):
            seg.source = ("autogen", arg)
        elif kw == "WHERE":
            seg.where = arg
        elif kw == "WHILE":
            seg.has_while, seg.while_cond = True, arg
        elif kw == "GROUP BY":
            seg.group_by = split_top(arg)
        elif kw == "ORDER BY":
            seg.order_by = arg
    return seg


def split_statement(text: str) -> tuple[list[Segment], str | None, int | None]:
    """Instruction LOAD -> segments (du haut vers le bas), SQL SELECT final
    éventuel, FIRST n éventuel. Le label et les préfixes sont ignorés."""
    first = None
    fm = re.search(r"\bFIRST\s+(\d+)\b", text[:200], re.I)
    loads = find_keywords(text, ("LOAD",))
    sqls = find_keywords(text, ("SQL",))
    if fm and loads and fm.start() < loads[0][0]:
        first = int(fm.group(1))
    sql_text = None
    if sqls and (not loads or sqls[-1][0] > loads[-1][0]):
        sql_text = text[sqls[-1][0] + 3:].strip().rstrip(";").strip()
        text = text[:sqls[-1][0]]
    segs = []
    for k, (pos, _) in enumerate(loads):
        nxt = loads[k + 1][0] if k + 1 < len(loads) else len(text)
        segs.append(parse_segment(text[pos:nxt].strip().rstrip(";")))
    return segs, sql_text, first


def inline_values(arg: str) -> tuple[list[str], list[list[str]]]:
    """Contenu d'un INLINE [ … ] (options éventuelles) -> en-tête, lignes."""
    m = re.match(r"\[(.*)\]\s*(\((.*)\))?\s*$", arg, re.S) or \
        re.match(r"\"(.*)\"\s*(\((.*)\))?\s*$", arg, re.S)
    if not m:
        raise Untranslatable("INLINE non reconnu")
    delim = ","
    if m.group(3):
        dm = re.search(r"delimiter\s+is\s+'([^']*)'", m.group(3), re.I)
        if dm:
            delim = {"\\t": "\t", "tab": "\t"}.get(dm.group(1).lower(),
                                                  dm.group(1))
    lines = [line.strip() for line in m.group(1).strip().splitlines()
             if line.strip()]
    if len(lines) < 1:
        raise Untranslatable("INLINE vide")
    rows = [[c.strip().strip('"') for c in line.split(delim)] for line in lines]
    header, data = rows[0], rows[1:]
    if any(len(r) != len(header) for r in data):
        raise Untranslatable("INLINE avec un nombre de colonnes irrégulier")
    return header, data


def values_sql(header, data) -> str:
    if not data:          # INLINE avec seulement l'en-tête : table vide
        return "SELECT " + ", ".join(f"CAST(NULL AS STRING) AS {ident(h)}"
                                     for h in header) + \
            f", CAST(NULL AS ARRAY<BIGINT>) AS {ORD} WHERE false"
    rows = ",\n    ".join("(" + ", ".join(sql_string(v) for v in r) + ")"
                          for r in data)
    cols = ", ".join(ident(h) for h in header)
    # VALUES est lu sur une seule partition : l'identifiant suit l'ordre écrit
    return (f"SELECT *, array(monotonically_increasing_id()) AS {ORD} FROM VALUES\n  "
            f"{rows.replace(chr(10) + '    ', chr(10) + '  ')}\nAS t({cols})")


# ==========================================================================
# Section : chaîne de CTE
# ==========================================================================



@dataclass
class Cte:
    name: str
    sql: str
    comment: str
    columns: list[str] | None


@dataclass
class SectionContext:
    """Ce que prepare.py sait et que le traducteur utilise."""
    final_table: str
    table_view: object          # clé de table amont -> nom de vue
    input_view: object          # chemin Qlik -> nom de vue (enregistre l'entrée)
    file_schema: object         # chemin Qlik -> colonnes connues ou None
    mapping_insts: object       # clé de mapping -> instructions du mapping
    table_name: object          # clé -> nom Qlik (pour KEEP)
    graph_schema: object        # clé -> colonnes calculées par le graphe
    tables_with_field: object = lambda f: []   # vues amont contenant le champ
    date_format: str | None = None
    timestamp_format: str | None = None


class SectionTranslator:
    def __init__(self, ctx: SectionContext):
        self.ctx = ctx
        self.ctes: list[Cte] = []
        self.cur: dict[str, str] = {}             # table -> CTE courante
        self.cols: dict[str, list[str] | None] = {}
        self.notes: list[str] = []
        self.todos: list[str] = []
        self.counter: dict[str, int] = {}
        self.technical: list[str] = []         # colonnes AutoNumber / Hash
        self.autogen_bottom = None             # "recno" | "fieldvalue" | None
        self.autogen_field = None

    # -- outils
    def new_name(self, table: str) -> str:
        base = view_name(table).lower()
        self.counter[base] = self.counter.get(base, 0) + 1
        return f"{base}_{self.counter[base]}"

    def add(self, table, sql, comment, columns):
        name = self.new_name(table)
        self.ctes.append(Cte(name, sql, comment, columns))
        self.cur[table] = name
        self.cols[name] = columns
        return name

    def source_of(self, table: str) -> tuple[str, list[str] | None]:
        """CTE de la section, sinon vue d'une table amont."""
        if table in self.cur:
            return self.cur[table], self.cols[self.cur[table]]
        view = self.ctx.table_view(table)
        if view is None:
            raise Untranslatable(f"RESIDENT {table} : table introuvable dans "
                                 "le script (créée dynamiquement ou dans un "
                                 "include ?)")
        return view, self.ctx.graph_schema(table)

    # -- instructions
    def translate(self, inst, text: str):
        line = inst.stmt.first_line
        try:
            if inst.kind == "load":
                if "mapping" in inst.constructs:
                    return
                self.load(inst, text, line)
            elif inst.kind == "drop_field":
                self.drop_field(inst, line)
            elif inst.kind in ("store", "drop"):
                return
            elif inst.kind == "rename":
                self.notes.append(f"-- l.{line} : RENAME (rien à traduire)")
            else:
                raise Untranslatable(f"instruction {inst.kind}")
        except Untranslatable as exc:
            self.placeholder(inst, line, str(exc))

    def placeholder(self, inst, line, reason):
        table = inst.target or self.ctx.final_table
        if table in self.cur:
            src = self.cur[table]
        elif inst.sources:
            src = self.ctx.input_view(inst.sources[0])
        elif inst.resident:
            try:
                src = self.source_of(inst.resident)[0]
            except Untranslatable:
                src = None
        else:
            src = None
        self.todos.append(f"l.{line} : {reason}")
        body = (f"-- TODO(model): l.{line} : {reason}\n"
                f"-- (code Qlik complet dans le prompt)\n"
                + (f"SELECT * FROM {src}" if src else "SELECT NULL AS todo"))
        # colonnes attendues si le graphe les connaît : la requête finale
        # reste explicite et check.py peut les contrôler
        cols = None if {"join", "keep", "concatenate"} & inst.constructs \
            else self.ctx.graph_schema(table)
        self.add(table, body, f"l.{line} : à traduire par le modèle", cols)

    def segment_sql(self, seg: Segment, src: str, src_cols, line: int,
                    first: int | None) -> tuple[str, list[str] | None]:
        if seg.has_while and self.autogen_bottom != "while":
            raise Untranslatable("WHILE sur une table (IterNo par ligne)")
        txt = " ".join(e for e, _ in seg.fields) + " " + (seg.where or "")
        joins, mapped = [], {}
        use_q = "applymap" in txt.lower()
        q = "s" if use_q else ""

        def applymap(mname, key_sql, default_sql):
            sig = (mname, key_sql)
            if sig not in mapped:
                alias = f"m{len(mapped) + 1}"
                mapped[sig] = alias
                joins.append(f"LEFT JOIN {self.mapping_sql(mname)} {alias}\n"
                             f"  ON CAST({key_sql} AS STRING) = "
                             f"CAST({alias}.map_key AS STRING)")
            alias = mapped[sig]
            return f"COALESCE({alias}.map_value, {default_sql or key_sql})"

        ectx = self.base_ectx(q, applymap)
        pre = f"{q}." if q else ""
        ord_src = f"{pre}{ORD}"
        if self.autogen_bottom:
            ectx.recno = f"{pre}__recno"
            ectx.fieldvalue = self.autogen_field
        if self.autogen_bottom == "while":
            ectx.recno, ectx.rowno, ectx.iterno = "1", f"{pre}__iter", f"{pre}__iter"
        # ordre de chargement des lignes de la source (Qlik : ordre physique,
        # ou ORDER BY du RESIDENT, à égalité dans l'ordre précédent)
        order_items = [self.order_item(o, ectx) for o in split_top(seg.order_by)] \
            if seg.order_by else []
        ectx.order_by = ", ".join(order_items + [ord_src])
        ectx.order_key = ord_src if not order_items else (
            None if any(i.endswith(" DESC") for i in order_items)
            else "array(" + ", ".join(f"CAST({i.removesuffix(' ASC')} AS STRING)"
                                      for i in order_items) + ")")
        exprs = {(a_ or e).strip("[]\""): e for e, a_ in seg.fields if e != "*"}

        def output_field(name):
            """Expression d'un champ de ce LOAD (Peek sur la table en cours)."""
            e = exprs.get(name)
            if e is None:
                if "*" in [x for x, _ in seg.fields] or not exprs:
                    return f"{pre}{ident(name)}"
                raise Untranslatable(f"Peek('{name}') : champ absent du LOAD")
            if re.search(r"\b(Peek|Previous)\s*\(", e, re.I):
                raise Untranslatable(f"Peek('{name}') récursif (cumul) : "
                                     "à traduire par le modèle")
            return expr(e, ectx)

        ectx.output_field = output_field
        ectx.exists = self.exists_sql
        self.dedup_field = None
        star = any(e == "*" for e, _ in seg.fields)
        # l'ordre change (ORDER BY, GROUP BY, DISTINCT, dédoublonnage) : il faut
        # le recalculer, donc lister les colonnes au lieu de « * »
        reorder = bool(order_items or seg.group_by or seg.distinct)
        fields = seg.fields
        if star and reorder:
            if src_cols is None:
                raise Untranslatable("LOAD * avec ORDER BY / GROUP BY / DISTINCT "
                                     ": colonnes de la source inconnues")
            fields = []
            for e, a_ in seg.fields:
                fields += [(f"[{c}]", None) for c in src_cols] if e == "*" \
                    else [(e, a_)]
            star = False
        select, values, out_cols = [], [], []
        for e, alias in fields:
            if e == "*":
                select.append(f"{q}.*" if q else "*")
                if src_cols is None or out_cols is None:
                    out_cols = None
                else:
                    out_cols += src_cols
                continue
            ectx.technical = False
            sql = expr(e, ectx)
            values.append(sql)
            if ectx.technical and (alias or e):
                self.technical.append(alias or e.strip("[]\""))
            if alias is None:
                if not re.fullmatch(r"\[[^\]]+\]|\"[^\"]+\"|[^\W\d][\w.#%@$]*", e):
                    raise Untranslatable(f"expression sans alias : {e[:40]}")
                name = e.strip("[]\"")
                select.append(sql if not q else f"{sql} AS {ident(name)}")
            else:
                name = alias
                select.append(f"{sql} AS {ident(name)}")
            if out_cols is not None:
                out_cols.append(name)
        where_sql = None
        if seg.where:
            ectx.in_where = self.in_where = True
            self.current_where = seg.where
            where_sql = cond(seg.where, ectx)
            ectx.in_where = self.in_where = False
        # colonne d'ordre du résultat
        group_sql = [expr(g, ectx) for g in seg.group_by]
        if seg.group_by:
            select.append(f"min({ord_src}) AS {ORD}")      # 1re occurrence
        elif seg.distinct:
            group_sql = values                       # DISTINCT : 1re occurrence
            select.append(f"min({ord_src}) AS {ORD}")
        elif order_items:
            select.append(f"array(row_number() OVER (ORDER BY {ectx.order_by})) "
                          f"AS {ORD}")
        elif not star:
            select.append(ord_src)
        source = src
        if self.dedup_field:
            # WHERE NOT Exists(champ) alors que ce LOAD charge le champ : Qlik
            # garde la 1re ligne de chaque valeur, dans l'ordre de chargement
            if joins or star:
                raise Untranslatable("NOT Exists sur la même table avec "
                                     "ApplyMap ou LOAD * : à traduire")
            alias_ = q or "w"
            key = self.dedup_field if q else f"{alias_}.{self.dedup_field}"
            source = (f"(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                      f"{key} ORDER BY {alias_}.{ORD}) "
                      f"AS __rn\n  FROM {src} {alias_} WHERE {where_sql}) d "
                      f"WHERE __rn = 1)")
            where_sql = None
        sql = ("SELECT " + ",\n       ".join(select)
               + f"\nFROM {source}" + (f" {q}" if q else ""))
        for j in joins:
            sql += "\n" + j
        if where_sql:
            sql += f"\nWHERE {where_sql}"
        if group_sql:
            sql += "\nGROUP BY " + ", ".join(group_sql)
        if first:
            sql += f"\nORDER BY {ectx.order_by}\nLIMIT {first}"
        return sql, out_cols

    def base_ectx(self, q: str = "", applymap=None) -> ExprCtx:
        """Contexte d'expression avec accès aux tables (Peek, NoOfRows)."""
        ectx = ExprCtx(q, applymap, self.ctx.date_format,
                       self.ctx.timestamp_format)
        ectx.peek_table = self.peek_table_sql
        ectx.noofrows = self.noofrows_sql
        return ectx

    def table_ref(self, name: str) -> str:
        key = getattr(self.current_inst, "table_refs", {}).get(name, name)
        if key in self.cur:
            return self.cur[key]
        view = self.ctx.table_view(key)
        if view is None:
            raise Untranslatable(f"table {name} introuvable")
        return view

    def peek_table_sql(self, field_name: str, row: int, table: str) -> str:
        """Peek('champ', n, 'T') : n-ième ligne de T (négatif : depuis la fin)."""
        src = self.table_ref(table)
        if row >= 0:
            order, offset = "", row
        else:
            order, offset = " DESC", -row - 1
        return (f"(SELECT {ident(field_name)} FROM {src} ORDER BY {ORD}{order} "
                f"LIMIT 1{f' OFFSET {offset}' if offset else ''})")

    def noofrows_sql(self, table: str) -> str:
        return f"(SELECT count(*) FROM {self.table_ref(table)})"

    def order_item(self, item: str, ectx) -> str:
        m = re.match(r"(.*?)\s+(ASC|DESC)\s*$", item, re.I | re.S)
        e, direction = (m.group(1), " " + m.group(2).upper()) if m else (item, "")
        return expr(e, ectx) + direction

    def exists_sql(self, field_name: str, value: str,
                   value_tokens: list[str] | None = None) -> str:
        """Exists(champ, valeur) : la valeur figure-t-elle dans le champ des
        tables chargées AVANT cette instruction ?"""
        inst = self.current_inst
        keys = getattr(inst, "exists_tables", {}).get(field_name, [])
        # le LOAD en cours produit-il lui-même ce champ ? (sinon, la table
        # cible n'est qu'une source de valeurs déjà chargées comme les autres)
        self_loaded = field_name in [
            (a or e).strip("[]\"") for e, a in self.current_fields
            if e != "*"]
        if self_loaded:
            # WHERE Exists(f) alors que la table charge aussi f : une ligne ne
            # passe que si f existe déjà, donc les lignes chargées n'ajoutent
            # aucune valeur nouvelle : équivalent à une semi-jointure sur les
            # tables précédentes. Avec NOT (ou une autre valeur que f), c'est
            # un dédoublonnage dans l'ordre de chargement : pas d'équivalent sûr.
            where = self.current_where or ""
            negated = re.search(r"\bNOT\s+Exists\s*\(\s*[\['\"]?"
                                + re.escape(field_name), where, re.I)
            plain_value = value.split(".")[-1] == ident(field_name)
            if value_tokens is not None and not plain_value:
                # Exists(f, expr) où ce LOAD définit f comme expr (« expr AS f »)
                for e_, a_ in self.current_fields:
                    if (a_ or "").strip("[]\"") == field_name and \
                            [v.lower() for _, v in tokenize_expr(e_)] == value_tokens:
                        plain_value = True
            if not plain_value or not self.in_where:
                raise Untranslatable(f"Exists({field_name}, …) sur un champ "
                                     "chargé par la même instruction")
            if negated:
                if re.search(r"\bOR\b", where, re.I):
                    raise Untranslatable(f"NOT Exists({field_name}) combiné "
                                         "avec OR")
                self.dedup_field = value          # 1re ligne par valeur
            # une table créée par ce LOAD n'a pas encore de lignes ; pour un
            # CONCATENATE, ses lignes déjà chargées comptent
            if not {"concatenate", "join"} & inst.constructs:
                keys = [k_ for k_ in keys if k_ != inst.target]
        sources = []
        for key in keys:
            if key in self.cur:
                sources.append(self.cur[key])
            else:
                view = self.ctx.table_view(key)
                if view is None:
                    raise Untranslatable(f"Exists({field_name}) : table {key} "
                                         "non disponible")
                sources.append(view)
        if not sources:
            return "false"                      # champ encore jamais chargé
        union = " UNION ALL ".join(f"SELECT {ident(field_name)} AS v FROM {src}"
                                   for src in dict.fromkeys(sources))
        return f"EXISTS (SELECT 1 FROM ({union}) ex WHERE ex.v = {value})"

    def mapping_sql(self, mname: str) -> str:
        insts = [i for i in self.ctx.mapping_insts(mname) if i.kind == "load"]
        if len(insts) != 1:
            raise Untranslatable(f"mapping {mname} défini en plusieurs fois")
        segs, sql_text, _ = split_statement(insts[0].clean_text)
        if len(segs) != 1 or sql_text:
            raise Untranslatable(f"mapping {mname} trop complexe")
        seg = segs[0]
        src, cols = self.base_source(insts[0], seg, None)
        exprs = [e for e, _ in seg.fields]
        if exprs == ["*"]:
            if not cols or len(cols) < 2:
                raise Untranslatable(f"colonnes du mapping {mname} inconnues")
            k, v = ident(cols[0]), ident(cols[1])
        elif len(exprs) >= 2:
            ectx = ExprCtx("", None, self.ctx.date_format,
                           self.ctx.timestamp_format)
            k, v = expr(exprs[0], ectx), expr(exprs[1], ectx)
        else:
            raise Untranslatable(f"mapping {mname} sans 2 colonnes")
        where = ""
        if seg.where:
            where = " WHERE " + expr(seg.where, ExprCtx(
                "", None, self.ctx.date_format, self.ctx.timestamp_format))
        # clés en double : Qlik garde la PREMIÈRE valeur chargée
        return (f"(SELECT map_key, min_by(map_value, __o) AS map_value FROM "
                f"(SELECT {k} AS map_key, {v} AS map_value, {ORD} AS __o "
                f"FROM {src}{where}) GROUP BY map_key)")

    def base_source(self, inst, seg: Segment, sql_text):
        """Source du segment le plus bas : vue, CTE ou VALUES."""
        if sql_text is not None:
            return self.sql_select(inst, sql_text)
        if seg.source is None:
            raise Untranslatable("LOAD sans source")
        kind, arg = seg.source
        if kind == "file":
            if not inst.sources:
                raise Untranslatable("source de fichier non résolue")
            if len(inst.sources) > 1:
                raise Untranslatable("plusieurs sources dans une instruction")
            return self.ctx.input_view(inst.sources[0]), \
                self.ctx.file_schema(inst.sources[0])
        if kind == "resident":
            return self.source_of(inst.resident or arg)
        if kind == "inline":
            header, data = inline_values(arg)
            return f"(\n{values_sql(header, data)}\n)", header
        if kind == "autogen":
            return self.autogenerate(arg, seg)
        raise Untranslatable(f"source {kind}")

    def autogenerate(self, arg: str, seg: Segment):
        """AUTOGENERATE n -> range(1, n + 1) ; AUTOGENERATE FieldValueCount(f)
        avec FieldValue(f, RecNo()) -> valeurs distinctes du champ f."""
        if seg.has_while:
            return self.autogenerate_while(arg, seg)
        if re.match(r"EXTENSION\b", arg, re.I):
            raise Untranslatable("EXTENSION")
        fm = re.fullmatch(r"FieldValueCount\s*\(\s*'([^']+)'\s*\)", arg.strip(),
                          re.I)
        if fm:
            field_name = fm.group(1)
            sources = [self.cur[t] for t in self.cur
                       if field_name in (self.cols[self.cur[t]] or [])]
            sources += self.ctx.tables_with_field(field_name)
            if not sources:
                raise Untranslatable(f"FieldValueCount('{field_name}') : aucune "
                                     "table connue ne contient ce champ")
            union = "\n  UNION ALL\n  ".join(
                f"SELECT {ident(field_name)} AS __fv FROM {src}"
                for src in dict.fromkeys(sources))
            self.autogen_bottom, self.autogen_field = "fieldvalue", field_name
            # valeurs distinctes du champ, toutes tables confondues ; RecNo()
            # numérote ces valeurs (ordre trié, Qlik : ordre de chargement)
            return (f"(\n  SELECT __fv, __recno, array(CAST(__recno AS BIGINT)) "
                    f"AS {ORD} FROM (\n"
                    f"  SELECT __fv, row_number() OVER (ORDER BY __fv) AS "
                    f"__recno FROM (\n  SELECT DISTINCT __fv FROM (\n  {union}"
                    f"\n  )\n  )\n  )\n)"), []
        if re.fullmatch(r"\d+", arg.strip()):
            n = arg.strip()
        else:
            toks = tokenize_expr(arg)
            for k, (kind, val) in enumerate(toks):
                nxt = toks[k + 1] if k + 1 < len(toks) else (None, None)
                if kind in ("word", "field") and nxt != ("op", "("):
                    # sans table source, un nom est une variable Qlik
                    raise Untranslatable(f"AUTOGENERATE {arg[:40]} : variable "
                                         f"{val} non résolue")
            n = f"CAST({expr(arg, self.base_ectx())} AS INT)"
        self.autogen_bottom = "recno"
        if re.fullmatch(r"\d+", n):
            return (f"(SELECT CAST(id AS INT) AS __recno, array(id) AS {ORD} "
                    f"FROM range(1, {n} + 1))"), []
        # borne calculée (variable LET, sous-requête) : range() exige une
        # constante, sequence() accepte une expression
        return (f"(SELECT __recno, array(CAST(__recno AS BIGINT)) AS {ORD} FROM "
                f"(SELECT explode(sequence(1, {n})) AS __recno))"), []

    # nombre maximal d'itérations d'un WHILE (un calendrier de 273 ans)
    WHILE_CAP = 100_000

    def autogenerate_while(self, arg: str, seg: Segment):
        """AUTOGENERATE 1 WHILE <condition sur IterNo()> : une ligne par
        itération, jusqu'à la PREMIÈRE itération où la condition est fausse
        (sémantique exacte du WHILE Qlik, même si la condition redevient
        vraie ensuite)."""
        if arg.strip() != "1":
            raise Untranslatable(f"AUTOGENERATE {arg.strip()} WHILE : une seule "
                                 "ligne source attendue")
        toks = tokenize_expr(seg.while_cond)
        for k, (kind, val) in enumerate(toks):
            nxt = toks[k + 1] if k + 1 < len(toks) else (None, None)
            if kind in ("word", "field") and nxt != ("op", "("):
                # sans table source, un nom est une variable non résolue
                raise Untranslatable(f"WHILE : « {val} » non résolu")
        cap = self.WHILE_CAP
        # range() renvoie des BIGINT : date + BIGINT est refusé par Spark
        c_outer = cond(seg.while_cond, ExprCtx(iterno="CAST(g.id AS INT)"))
        c_inner = cond(seg.while_cond, ExprCtx(iterno="CAST(r.id AS INT)"))
        self.autogen_bottom = "while"
        return (f"(\n  -- WHILE Qlik : jusqu'à la première itération fausse "
                f"(au plus {cap})\n"
                f"  SELECT CAST(g.id AS INT) AS __iter, array(g.id) AS {ORD} "
                f"FROM range(1, {cap + 1}) g\n"
                f"  WHERE g.id < COALESCE((SELECT min(r.id) FROM range(1, "
                f"{cap + 1}) r\n                       WHERE NOT {c_inner}), "
                f"{cap + 1})\n"
                f"    AND {c_outer}\n)"), []

    def sql_select(self, inst, sql_text: str):
        m = re.fullmatch(r"SELECT\s+(.*?)\s+FROM\s+([\w.\[\]\"]+)\s*", sql_text,
                         re.I | re.S)
        if not m or not inst.sources:
            raise Untranslatable("SQL SELECT complexe (jointure, WHERE, "
                                 "sous-requête) : dialecte à reprendre")
        view = self.ctx.input_view(inst.sources[-1])
        cols_sql, cols = [], []
        for part in split_top(m.group(1)):
            pm = re.fullmatch(r"([\w\[\]\"]+)(?:\s+AS\s+([\w\[\]\"]+))?", part,
                              re.I)
            if part.strip() == "*":
                return view, self.ctx.file_schema(inst.sources[-1])
            if not pm:
                raise Untranslatable("SQL SELECT avec expressions : dialecte "
                                     "à reprendre")
            src = pm.group(1).strip("[]\"")
            dst = (pm.group(2) or pm.group(1)).strip("[]\"")
            cols_sql.append(ident(src) + ("" if src == dst else f" AS {ident(dst)}"))
            cols.append(dst)
        return (f"(SELECT {', '.join(cols_sql)}, {ORD} FROM {view})", cols)

    def load(self, inst, text, line):
        segs, sql_text, first = split_statement(text)
        self.current_inst = inst
        self.current_fields = [f for sg in segs for f in sg.fields]
        self.current_where, self.in_where = None, False
        self.autogen_bottom, self.autogen_field = None, None
        if not segs and sql_text is None:
            raise Untranslatable("LOAD non reconnu")
        if not segs:                 # « T: SQL SELECT … ; » sans LOAD
            src, cols = self.sql_select(inst, sql_text)
            segs_sql = [(f"SELECT * FROM {src}", cols)]
        else:
            self.autogen_bottom = self.autogen_field = None
            src, cols = self.base_source(inst, segs[-1], sql_text)
            segs_sql = []
            for k, seg in enumerate(reversed(segs)):
                top = k == len(segs) - 1
                if k > 0:          # segment supérieur : lit le résultat inférieur
                    self.autogen_bottom = self.autogen_field = None
                    sub = segs_sql[-1]
                    src, cols = "(\n" + _indent(sub[0], 2) + "\n)", sub[1]
                segs_sql.append(self.segment_sql(seg, src, cols, line,
                                                 first if top else None))
                # RecNo/IterNo d'un AUTOGENERATE n'existent que dans le
                # segment le plus bas
                self.autogen_bottom, self.autogen_field = None, None
        sql, columns = segs_sql[-1]
        target = inst.target
        kind = self.describe(inst)
        if {"join", "keep"} & inst.constructs:
            tmp = self.add(f"{target}_joined", sql, f"l.{line} : {kind} "
                           "(table jointe)", columns)
            self.join(inst, target, tmp, columns, line, kind)
        elif "concatenate" in inst.constructs and target in self.cur:
            tmp = self.add(f"{target}_added", sql, f"l.{line} : {kind} "
                           "(lignes ajoutées)", columns)
            self.concat(target, tmp, columns, line)
        else:
            self.add(target, sql, f"l.{line} : {kind}", columns)

    def describe(self, inst) -> str:
        name = self.ctx.table_name(inst.target)
        parts = []
        for c in ("join", "keep", "concatenate"):
            for x in inst.constructs:
                if x.startswith(c + ":"):
                    parts.append(f"{x.split(':')[1].upper()} {c.upper()} "
                                 f"dans {name}")
        if "concatenate" in inst.constructs and not parts:
            parts.append(f"CONCATENATE dans {name}")
        if not parts:
            parts.append(name)
        if inst.sources:
            parts.append("depuis " + ", ".join(
                re.split(r"[\\/]", s)[-1] for s in inst.sources))
        if inst.resident:
            parts.append(f"RESIDENT {self.ctx.table_name(inst.resident)}")
        return " ".join(parts)

    def join(self, inst, target, right, right_cols, line, kind):
        how = next(x.split(":")[1] for x in inst.constructs
                   if x.startswith(("join:", "keep:")))
        if "keep" in inst.constructs:
            return self.keep(inst, target, right, right_cols, line, how)
        if target not in self.cur:
            raise Untranslatable(f"JOIN dans {target} absente de la section")
        left = self.cur[target]
        left_cols = self.cols[left]
        if left_cols is None or right_cols is None:
            raise Untranslatable("JOIN : colonnes d'une des tables inconnues, "
                                 "clé implicite incalculable")
        keys = [c for c in right_cols if c in left_cols]
        sql_how = {"outer": "FULL OUTER JOIN", "left": "LEFT JOIN",
                   "right": "RIGHT JOIN", "inner": "INNER JOIN"}[how]
        select = []
        for c in left_cols:
            if c in keys and how in ("outer", "right"):
                select.append(f"COALESCE(a.{ident(c)}, b.{ident(c)}) AS {ident(c)}")
            else:
                select.append(f"a.{ident(c)}")
        select += [f"b.{ident(c)}" for c in right_cols if c not in keys]
        if how in ("outer", "right"):
            # lignes de gauche dans leur ordre, puis lignes seulement à droite
            select.append(f"CASE WHEN a.{ORD} IS NULL THEN concat(array(1L), "
                          f"b.{ORD}) ELSE concat(array(0L), a.{ORD}) END AS {ORD}")
        else:
            select.append(f"a.{ORD}")
        cols = left_cols + [c for c in right_cols if c not in keys]
        if keys:
            on = " AND ".join(f"a.{ident(k)} = b.{ident(k)}" for k in keys)
            sql = (f"SELECT {', '.join(select)}\nFROM {left} a\n{sql_how} "
                   f"{right} b ON {on}")
            comment = f"l.{line} : {kind} sur {', '.join(keys)}"
        else:
            sql = (f"SELECT {', '.join(select)}\nFROM {left} a\n"
                   f"CROSS JOIN {right} b")
            comment = (f"l.{line} : {kind} SANS champ commun : produit "
                       "cartésien, comme dans Qlik")
        self.add(target, sql, comment, cols)

    def keep(self, inst, target, right, right_cols, line, how):
        """KEEP : jointure utilisée comme filtre, les deux tables restent."""
        m = re.search(r"\bKEEP\s*\(\s*(\[[^\]]+\]|\"[^\"]+\"|[^\s)]+)\s*\)",
                      inst.clean_text, re.I)
        if not m:
            raise Untranslatable("KEEP sans table cible explicite")
        other_name = m.group(1).strip("[]\"")
        other_key = getattr(inst, "keep_other", None) or other_name
        other = other_key if other_key in self.cur else next(
            (t for t in self.cur if self.ctx.table_name(t) == other_name), None)
        if other is not None:
            other_src, other_cols = self.cur[other], self.cols[self.cur[other]]
        elif how == "left":
            other_src = self.ctx.table_view(other_key)
            other_cols = self.ctx.graph_schema(other_key)
            if other_src is None:
                raise Untranslatable(f"KEEP ({other_name}) : table introuvable")
        else:
            raise Untranslatable(f"{how.upper()} KEEP sur une table d'une "
                                 "autre section")
        if other_cols is None or right_cols is None:
            raise Untranslatable("KEEP : colonnes inconnues, clé incalculable")
        keys = [c for c in right_cols if c in other_cols]
        if not keys:
            raise Untranslatable("KEEP sans champ commun")
        on = " AND ".join(f"x.{ident(k)} = y.{ident(k)}" for k in keys)
        kl = ", ".join(keys)
        # la table chargée par le KEEP est filtrée sur l'autre (LEFT, INNER)
        if how in ("left", "inner"):
            self.add(target, f"SELECT x.* FROM {right} x\nLEFT SEMI JOIN "
                     f"{other_src} y ON {on}",
                     f"l.{line} : {how.upper()} KEEP ({other_name}) sur {kl}",
                     right_cols)
        else:
            self.add(target, f"SELECT * FROM {right}",
                     f"l.{line} : RIGHT KEEP ({other_name}) : chargée telle "
                     "quelle", right_cols)
        # l'autre table est filtrée sur la table chargée (INNER, RIGHT)
        if how in ("inner", "right"):
            self.add(other, f"SELECT x.* FROM {other_src} x\nLEFT SEMI JOIN "
                     f"{self.cur[target]} y ON {on}",
                     f"l.{line} : {how.upper()} KEEP : {other_name} filtrée "
                     f"sur {kl}", other_cols)

    def concat(self, target, added, added_cols, line):
        base = self.cur[target]
        base_cols = self.cols[base]
        if base_cols is None or added_cols is None:
            raise Untranslatable("CONCATENATE : colonnes inconnues, "
                                 "alignement impossible")
        cols = base_cols + [c for c in added_cols if c not in base_cols]

        def side(have, part):
            return ", ".join([ident(c) if c in have else f"NULL AS {ident(c)}"
                              for c in cols]
                             + [f"concat(array({part}L), {ORD}) AS {ORD}"])
        sql = (f"SELECT {side(base_cols, 0)} FROM {base}\nUNION ALL\n"
               f"SELECT {side(added_cols, 1)} FROM {added}")
        self.add(target, sql, f"l.{line} : CONCATENATE dans "
                 f"{self.ctx.table_name(target)}", cols)

    def drop_field(self, inst, line):
        targets = [inst.target] if inst.target else \
            [t for t in self.cur if self.cols[self.cur[t]] and
             set(inst.fields) & set(self.cols[self.cur[t]])]
        if not targets:
            raise Untranslatable("DROP FIELD sur une table inconnue")
        for t in targets:
            if t not in self.cur or self.cols[self.cur[t]] is None:
                raise Untranslatable("DROP FIELD : colonnes inconnues")
            keep = [c for c in self.cols[self.cur[t]] if c not in inst.fields]
            self.add(t, f"SELECT {', '.join([ident(c) for c in keep] + [ORD])} "
                     f"FROM {self.cur[t]}",
                     f"l.{line} : DROP FIELD {', '.join(inst.fields)}", keep)

    # -- assemblage
    def render(self, header: str) -> tuple[str, list[str] | None]:
        final = self.cur.get(self.ctx.final_table)
        if final is None:
            raise Untranslatable("table finale non produite")
        cols = self.cols[final]
        out = [header, "WITH"]
        for k, c in enumerate(self.ctes):
            body = _indent(c.sql, 2)
            sep = "," if k < len(self.ctes) - 1 else ""
            out.append(f"-- {c.comment}\n{c.name} AS (\n{body}\n){sep}")
        out += self.notes
        sel = ",\n       ".join([ident(c) for c in cols] + [ORD]) if cols else "*"
        out.append(f"SELECT {sel}\nFROM {final}")
        return "\n".join(out) + "\n", cols


def _indent(text: str, n: int) -> str:
    return "\n".join(" " * n + line if line.strip() else line
                     for line in text.splitlines())


def translate_section(insts, texts, ctx: SectionContext, header: str):
    """-> (sql, todos, colonnes finales, colonnes techniques)."""
    tr = SectionTranslator(ctx)
    for inst, text in zip(insts, texts):
        tr.translate(inst, text)
    try:
        sql, cols = tr.render(header)
    except Untranslatable as exc:
        tr.todos.append(str(exc))
        sql, cols = header + f"-- TODO(model): {exc}\n", None
    return sql, tr.todos, cols, list(dict.fromkeys(tr.technical))
