"""Prépare la traduction d'un script Qlik en Spark SQL, sans le modèle.

Étapes :
  1. découpage en instructions, en tenant compte des chaînes, des crochets,
     des commentaires et des LOAD empilés
  2. exécution « à blanc » : on remplace les variables ($(v), $(#v),
     $(v(a,b))), on développe les CALL de SUB, on déroule les FOR à liste
     fixe et on tranche les IF qu'on sait évaluer
  3. graphe des tables (RESIDENT, JOIN, CONCATENATE, ApplyMap, DROP, STORE),
     avec le suivi des colonnes, les clés implicites des JOIN et
     l'auto-concaténation
  4. une section par table finale, dans l'ordre du script (tables TMP
     comprises)
  5. traduction en Spark SQL par qlik2sql.py, une CTE par instruction. Ce
     qu'on ne sait pas traduire à coup sûr devient une CTE « TODO(model) »,
     et on écrit un prompt.md minimal pour le modèle.

Usage :
    python tools/prepare.py script_qvd/ventes.qvs
    python tools/prepare.py --collect script_qvd/*.qvs   # recensement du projet

Sorties dans work/<script>/ :
    manifest.json, rapport.md
    <section>/source.qvs   code Qlik de la table
    <section>/query.sql    requête Spark SQL (complète, ou avec des TODO(model))
    <section>/prompt.md    seulement s'il reste des TODO(model)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conf import IoConf, resolve_location  # noqa: E402
from qlik2sql import (SectionContext, translate_section,  # noqa: E402
                      view_name)

# Skill à joindre au prompt selon ce qu'on trouve dans la partie à compléter.
# Les noms doivent correspondre aux dossiers de .opencode/skills/.
SKILL_BY_CONSTRUCT = {
    "applymap": "qlik-mapping-to-sql",
    "join": "qlik-joins-sql",
    "keep": "qlik-joins-sql",
    "concatenate": "qlik-joins-sql",
    "resident_chain": "qlik-resident-chain-sql",
    "row_functions": "qlik-row-functions-sql",
}

CONTROL_WORDS = {"FOR", "NEXT", "IF", "ELSEIF", "ELSE", "END", "ENDIF",
                 "SUB", "ENDSUB", "DO", "LOOP", "SWITCH", "CASE", "DEFAULT",
                 "ENDSWITCH", "EXIT"}
OPENERS = {"FOR", "DO", "SUB", "SWITCH", "IF"}
# Variables de paramétrage régional (séparateurs, formats de date…). Qlik s'en
# sert pour interpréter les données, on ne les remplace jamais dans le code.
SYSTEM_VARS = {"ThousandSep", "DecimalSep", "MoneyThousandSep",
               "MoneyDecimalSep", "MoneyFormat", "TimeFormat", "DateFormat",
               "TimestampFormat", "MonthNames", "LongMonthNames", "DayNames",
               "LongDayNames", "FirstWeekDay", "BrokenWeeks", "ReferenceDay",
               "FirstMonthOfYear", "CollationLocale", "NumericalAbbreviation",
               "CreateSearchIndexOnReload"}
MAX_UNROLL = 50
NAME = r"(\[[^\]]+\]|\"[^\"]+\"|[^\s,;()\[\]\"'=]+)"


# ==========================================================================
# 1. Découpage en instructions
# ==========================================================================

@dataclass
class Stmt:
    raw: str
    line: int            # ligne du premier caractère de raw
    first_line: int      # ligne du premier caractère qui n'est pas un blanc
    last_line: int

    @property
    def clean(self) -> str:
        return normalize(strip_comments(self.raw))

    @property
    def word(self) -> str:
        return first_word(self.clean)


def first_word(text: str) -> str:
    m = re.match(r"\s*([A-Za-z]+)", text)
    return m.group(1).upper() if m else ""


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_comments(text: str) -> str:
    """Retire les // et /* */ qui ne sont ni dans une chaîne ni entre
    crochets (lib:// n'est donc pas touché)."""
    out, i, n, quote = [], 0, len(text), None
    while i < n:
        ch = text[i]
        if quote:
            out.append(ch)
            if (quote == "[" and ch == "]") or (quote != "[" and ch == quote):
                quote = None
            i += 1
        elif text.startswith("//", i) and (i == 0 or text[i - 1] != ":"):
            j = text.find("\n", i)
            i = n if j == -1 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j == -1 else j + 2
            out.append(" ")
        else:
            if ch in "'\"[":
                quote = ch
            out.append(ch)
            i += 1
    return "".join(out)


def tokenize(text: str) -> list[Stmt]:
    """Découpe le script en instructions. Les instructions de contrôle (FOR,
    IF…THEN, NEXT, ENDIF…) n'ont pas de ';', elles s'arrêtent en fin de
    ligne."""
    stmts: list[Stmt] = []
    i, n, line = 0, len(text), 1
    start, start_line, first_line = 0, 1, None
    quote, useful = None, []

    def flush(end):
        nonlocal start, start_line, first_line, useful
        if "".join(useful).strip():
            stmts.append(Stmt(text[start:end], start_line,
                              first_line or line, line))
        start, start_line, first_line, useful = end, line, None, []

    while i < n:
        ch = text[i]
        if quote:
            useful.append(ch)
            if ch == "\n":
                line += 1
            if (quote == "[" and ch == "]") or (quote != "[" and ch == quote):
                quote = None
            i += 1
            continue
        if text.startswith("//", i) and (i == 0 or text[i - 1] != ":"):
            j = text.find("\n", i)
            i = n if j == -1 else j
            continue
        if text.startswith("/*", i):
            j = text.find("*/", i + 2)
            end = n if j == -1 else j + 2
            line += text.count("\n", i, end)
            i = end
            continue
        if ch == "\n":
            cur = "".join(useful)
            if cur.strip() and first_word(cur) in CONTROL_WORDS \
                    and ";" not in cur:
                flush(i + 1)
                line += 1
                start_line = line
                i += 1
                continue
            line += 1
            useful.append(" ")
            i += 1
            continue
        if not ch.isspace() and first_line is None:
            first_line = line
        useful.append(ch)
        if ch in "'\"[":
            quote = ch
        elif ch == ";":
            flush(i + 1)
        i += 1
    flush(n)
    return stmts


def drop_rem(stmts: list[Stmt]) -> list[Stmt]:
    """« REM … ; » est un commentaire en Qlik, on l'enlève comme // et
    /* */."""
    return [st for st in stmts if st.word != "REM"]


def merge_preceding_loads(stmts: list[Stmt]) -> list[Stmt]:
    """Fusionne les LOAD empilés.

    En Qlik, un preceding load (LOAD sans FROM, RESIDENT, INLINE ni
    AUTOGENERATE) se termine bien par ';', mais il s'applique à
    l'instruction qui suit. L'ensemble ne fait qu'une table."""
    out: list[Stmt] = []
    pending: Stmt | None = None
    for st in stmts:
        if pending is not None:
            st = Stmt(pending.raw + st.raw, pending.line, pending.first_line,
                      st.last_line)
            pending = None
        masked = mask_strings(st.clean)
        is_load = re.search(r"\bLOAD\b", masked, re.I) and \
            st.word not in CONTROL_WORDS | {"SET", "LET"}
        has_source = re.search(r"\b(FROM|RESIDENT|INLINE|AUTOGENERATE|"
                               r"EXTENSION|SELECT)\b", masked, re.I)
        if is_load and not has_source:
            pending = st
            continue
        out.append(st)
    if pending is not None:
        out.append(pending)
    return out


# ==========================================================================
# 2. Structure de contrôle et exécution à blanc
# ==========================================================================

@dataclass
class Node:
    kind: str                      # stmt | for | if | sub | do | switch
    stmt: Stmt | None = None       # l'instruction, ou l'en-tête du bloc
    branches: list = field(default_factory=list)   # [(en-tête, [Node])]
    closer: Stmt | None = None


def parse_blocks(stmts: list[Stmt]) -> tuple[list[Node], list[str]]:
    root: list[Node] = []
    stack: list[tuple[Node, list[Node]]] = []
    warnings = []

    def current():
        return stack[-1][1] if stack else root

    for st in stmts:
        w = st.word
        c = st.clean
        if w == "IF" and not re.search(r"\bTHEN\s*$", c, re.I):
            # IF sur une seule ligne, rare
            current().append(Node("stmt", st))
            warnings.append(f"l.{st.first_line}: IF sur une seule ligne")
            continue
        if w == "END" and re.match(r"\s*END\s*(IF|SUB|SWITCH)\b", c, re.I) \
                or w in ("ENDIF", "ENDSUB", "ENDSWITCH", "NEXT", "LOOP"):
            if stack:
                node, _ = stack.pop()
                node.closer = st
            else:
                warnings.append(f"l.{st.first_line}: fermeture sans ouverture")
            continue
        if w in ("ELSE", "ELSEIF") or (w in ("CASE", "DEFAULT") and stack
                                       and stack[-1][0].kind == "switch"):
            if stack:
                node = stack[-1][0]
                body: list[Node] = []
                node.branches.append((st, body))
                stack[-1] = (node, body)
            continue
        if w in OPENERS:
            node = Node(w.lower(), st)
            body = []
            node.branches.append((st, body))
            current().append(node)
            stack.append((node, body))
            continue
        current().append(Node("stmt", st))
    for node, _ in stack:
        warnings.append(f"l.{node.stmt.first_line}: bloc {node.kind.upper()} "
                        "jamais fermé")
    return root, warnings


@dataclass
class Inst:
    """Une instruction « exécutée » : son texte, variables remplacées."""
    stmt: Stmt
    text: str
    context: list[str] = field(default_factory=list)
    kind: str = "other"
    label: str | None = None
    target: str | None = None
    resident: str | None = None
    sources: list[str] = field(default_factory=list)
    constructs: set[str] = field(default_factory=set)
    fields: list[str] = field(default_factory=list)
    applymaps: list[dict] = field(default_factory=list)
    functions: set[str] = field(default_factory=set)
    dropped: list[str] = field(default_factory=list)
    stored: tuple | None = None
    join_keys: list[str] | None = None
    directory: str = ""
    warnings: list[str] = field(default_factory=list)


LET_TOKEN = re.compile(r"'[^']*'|\[[^\]]*\]|\"[^\"]*\"|([^\W\d][\w.#]*)\s*(\()?")


class Env:
    def let_expression(self, name: str, text: str, pos: int) -> str | None:
        """Pour $(v) quand v est un LET calculé (LET v = Peek(…), Today(),
        NoOfRows(…)…) : on met l'expression à la place, elle sera traduite
        en SQL plus loin.

        On ne le fait que dans une expression (pas dans une chaîne ni dans
        un nom), et seulement si l'expression ne contient que des fonctions
        et des littéraux."""
        rhs = self.lets.get(name)
        if not rhs or "$(" in rhs or _in_quotes(text, pos):
            return None
        for m in LET_TOKEN.finditer(rhs):
            if m.group(1) and not m.group(2):
                # nom nu : variable ou champ ? on ne sait pas
                return None
        return f"({rhs})"

    def bare_variables(self, text: str) -> str:
        """Dans AUTOGENERATE vMax - vMin + 1, il n'y a pas de table source,
        donc un nom nu est forcément une variable : on le remplace par sa
        valeur ou par son expression."""
        m = re.search(r"\bAUTOGENERATE\b(.*?)(;|$)", text, re.I | re.S)
        if not m:
            return text
        clause = m.group(1)

        def repl(w):
            name = w.group(0)
            if self.vars.get(name) is not None:
                return self.vars[name]
            return self.let_expression(name, "", 0) or name
        new = re.sub(r"(?<![\w$.'\[\"])[A-Za-z_][\w.]*(?![\w(])", repl, clause)
        return text[:m.start(1)] + new + text[m.end(1):]

    def __init__(self):
        self.directory = ""             # dernier DIRECTORY rencontré
        self.vars: dict[str, str | None] = {}
        self.lets: dict[str, str] = {}

    def expand(self, text: str) -> str:
        # '$(v)' tout seul entre apostrophes, avec v un LET calculé (date,
        # Peek…) : Qlik interprète la valeur, donc on met directement
        # l'expression.
        def quoted(m):
            sub = self.let_expression(m.group(1), "", 0)
            return sub if sub else m.group(0)
        text = re.sub(r"'\$\(#?([A-Za-z_][\w.]*)\)'", quoted, text)

        def repl(m):
            name, args = m.group(2), m.group(4)
            if name in SYSTEM_VARS:
                return m.group(0)
            if self.vars.get(name) is None:
                return self.let_expression(name, text, m.start()) or m.group(0)
            value = self.vars[name]
            if args is not None:
                for k, a in enumerate(split_top(args), 1):
                    value = value.replace(f"${k}", a.strip())
            return value
        pattern = r"\$\((#?)([A-Za-z_][\w.]*)(\(([^()]*(?:\([^()]*\)[^()]*)*)\))?\)"
        for _ in range(5):
            new = re.sub(pattern, repl, text)
            if new == text:
                break
            text = new
        return text


def _in_quotes(text: str, pos: int) -> bool:
    """Dit si la position tombe dans une chaîne '…' ou dans un nom […] ou
    "…"."""
    quote = None
    for ch in text[:pos]:
        if quote:
            if (quote == "[" and ch == "]") or (quote != "[" and ch == quote):
                quote = None
        elif ch in "'\"[":
            quote = ch
    return quote is not None


def unquote(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
        return v[1:-1]
    return v


def let_value(expr: str) -> str | None:
    """Valeur d'un LET quand c'est un littéral (nombre, chaîne ou
    concaténation de littéraux)."""
    expr = expr.strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", expr):
        return expr
    parts = [p.strip() for p in re.split(r"&(?=(?:[^']*'[^']*')*[^']*$)", expr)]
    if all(re.fullmatch(r"'[^']*'|-?\d+(\.\d+)?", p) for p in parts):
        return "".join(unquote(p) for p in parts)
    return None


def eval_condition(cond: str) -> bool | None:
    """Évalue une condition simple entre littéraux, une fois les variables
    remplacées."""
    lit = r"('[^']*'|-?\d+(?:\.\d+)?)"
    m = re.fullmatch(r"\s*" + lit + r"\s*(=|<>|<=|>=|<|>)\s*" + lit + r"\s*", cond)
    if not m:
        return None
    a, op, b = m.group(1), m.group(2), m.group(3)
    if a.startswith("'") != b.startswith("'"):
        return None
    if a.startswith("'"):
        a, b = a[1:-1], b[1:-1]
    else:
        a, b = float(a), float(b)
    return {"=": a == b, "<>": a != b, "<": a < b, ">": a > b,
            "<=": a <= b, ">=": a >= b}[op]


def execute(nodes, env: Env, ctx, subs, out: list[Inst], notes: list[str],
            depth=0):
    for node in nodes:
        if node.kind == "stmt":
            run_stmt(node.stmt, env, ctx, subs, out, notes, depth)
        elif node.kind == "sub":
            m = re.match(r"\s*SUB\s+([\w.]+)\s*(?:\((.*)\))?", node.stmt.clean,
                         re.I)
            if m:
                params = [p.strip() for p in split_top(m.group(2) or "")]
                subs[m.group(1).lower()] = (m.group(1), params,
                                            node.branches[0][1])
        elif node.kind == "for":
            run_for(node, env, ctx, subs, out, notes, depth)
        elif node.kind == "if":
            run_if(node, env, ctx, subs, out, notes, depth)
        else:   # do, switch : exécutés une fois, et signalés
            label = f"bloc {node.kind.upper()} l.{node.stmt.first_line}"
            out.append(control_inst(node.stmt, env, ctx))
            for _, body in node.branches:
                execute(body, env, ctx + [label], subs, out, notes, depth)
            if node.closer:
                out.append(control_inst(node.closer, env, ctx))


def control_inst(st, env, ctx):
    inst = Inst(st, env.expand(st.raw), list(ctx))
    inst.kind = "control"
    return inst


class StopScript(Exception):
    """Levée sur un EXIT SCRIPT : Qlik s'arrête là."""

    def __init__(self, line: int):
        super().__init__(line)
        self.line = line


def run_stmt(st, env, ctx, subs, out, notes, depth):
    c = st.clean
    w = st.word
    if w == "EXIT" and re.match(r"\s*EXIT\s+SCRIPT\s*;?\s*$", c, re.I):
        # EXIT SCRIPT inconditionnel, et pas dans un IF ou une boucle qu'on n'a
        # pas su trancher : le script s'arrête ici.
        uncertain = [x for x in ctx if x.startswith(("branche", "boucle",
                                                     "bloc"))]
        if not uncertain:
            raise StopScript(st.first_line)
        return
    if w == "EXIT":
        return              # EXIT SCRIPT WHEN…, EXIT FOR/DO… : rien à traduire
    if w in ("SET", "LET"):
        m = re.match(r"\s*(SET|LET)\s+([\w.$()]+)\s*=\s*(.*?)\s*;?\s*$",
                     strip_comments(st.raw).strip(), re.I | re.S)
        if m:
            name = m.group(2)
            rhs = env.expand(m.group(3))
            if w == "SET":
                env.vars[name] = unquote(rhs)
            else:
                env.vars[name] = let_value(rhs)
                if env.vars[name] is None:
                    env.lets[name] = rhs
        inst = Inst(st, env.expand(st.raw), list(ctx))
        inst.kind = "variable"
        out.append(inst)
        return
    if w == "CALL":
        m = re.match(r"\s*CALL\s+([\w.]+)\s*(?:\((.*)\))?\s*;", c, re.I)
        if m and m.group(1).lower() in subs and depth < 5:
            name, params, body = subs[m.group(1).lower()]
            args = [unquote(env.expand(a)) for a in split_top(m.group(2) or "")]
            saved = {p: env.vars.get(p) for p in params}
            for p, a in zip(params, args):
                env.vars[p] = a
            label = f"CALL {name}({', '.join(args)}) l.{st.first_line}"
            execute(body, env, ctx + [label], subs, out, notes, depth + 1)
            for p, v in saved.items():
                env.vars[p] = v
            return
        notes.append(f"l.{st.first_line}: CALL vers une SUB inconnue : {c[:60]}")
    text = env.expand(st.raw)
    if re.search(r"\bAUTOGENERATE\b", text, re.I):
        text = env.bare_variables(text)
    inst = Inst(st, text, list(ctx))
    if w == "DIRECTORY":
        m = re.match(r"\s*DIRECTORY\s*(.*?)\s*;", strip_comments(inst.text),
                     re.I | re.S)
        env.directory = unquote(strip_name(m.group(1))) if m else ""
    inst.directory = env.directory
    out.append(inst)


def run_for(node, env, ctx, subs, out, notes, depth):
    head = env.expand(node.stmt.clean)
    body = node.branches[0][1]
    values, var = None, None
    m = re.match(r"\s*FOR\s+EACH\s+(\w+)\s+IN\s+(.*)$", head, re.I)
    if m and not re.search(r"\w\s*\(", m.group(2)):
        var = m.group(1)
        values = [unquote(v) for v in split_top(m.group(2))]
    m2 = re.match(r"\s*FOR\s+(\w+)\s*=\s*(-?\d+)\s+TO\s+(-?\d+)"
                  r"(?:\s+STEP\s+(-?\d+))?\s*$", head, re.I)
    if m2:
        var = m2.group(1)
        a, b, s = int(m2.group(2)), int(m2.group(3)), int(m2.group(4) or 1)
        values = [str(v) for v in range(a, b + (1 if s > 0 else -1), s)] if s else None
    if values is not None and len(values) <= MAX_UNROLL:
        for v in values:
            env.vars[var] = v
            execute(body, env, ctx + [f"FOR {var}={v} l.{node.stmt.first_line}"],
                    subs, out, notes, depth)
        return
    fl = re.search(r"FileList\s*\(\s*'([^']+)'", head, re.I)
    label = f"boucle non déroulable l.{node.stmt.first_line}: {head[:80]}"
    if fl:
        label += f" | ASTUCE : FileList -> lecture Spark par motif '{fl.group(1)}'"
    out.append(control_inst(node.stmt, env, ctx))
    execute(body, env, ctx + [label], subs, out, notes, depth)
    if node.closer:
        out.append(control_inst(node.closer, env, ctx))


def run_if(node, env, ctx, subs, out, notes, depth):
    undecided = False
    for header, body in node.branches:
        hc = env.expand(header.clean)
        if header.word == "ELSE":
            if undecided:
                out.append(control_inst(header, env, ctx))
                execute(body, env, ctx + [f"branche ELSE l.{header.first_line}"],
                        subs, out, notes, depth)
            else:
                execute(body, env, ctx, subs, out, notes, depth)
            break
        cond = re.sub(r"^\s*(ELSE)?IF\s+|\s+THEN\s*$", "", hc, flags=re.I)
        verdict = None if undecided else eval_condition(cond)
        if verdict is True:
            notes.append(f"l.{header.first_line}: condition « {cond} » vraie, "
                         "branche retenue")
            execute(body, env, ctx, subs, out, notes, depth)
            break
        if verdict is False:
            notes.append(f"l.{header.first_line}: condition « {cond} » fausse, "
                         "branche ignorée")
            continue
        # condition qu'on ne sait pas évaluer : on garde toutes les branches,
        # et on le signale
        undecided = True
        out.append(control_inst(header, env, ctx))
        execute(body, env, ctx + [f"branche conditionnelle l.{header.first_line}"
                                  f": IF {cond[:80]}"], subs, out, notes, depth)
    if undecided and node.closer:
        out.append(control_inst(node.closer, env, ctx))


# ==========================================================================
# 3. Analyse des instructions de chargement
# ==========================================================================

def strip_name(n: str) -> str:
    return n.strip().strip("[]\"").strip()


def split_top(s: str) -> list[str]:
    parts, depth, cur, quote = [], 0, [], None
    for ch in s:
        if quote:
            cur.append(ch)
            if (quote == "[" and ch == "]") or (quote != "[" and ch == quote):
                quote = None
            continue
        if ch in "'\"[":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def mask_strings(s: str) -> str:
    """Remplace le contenu des chaînes par des X, en gardant les
    crochets."""
    return re.sub(r"'[^']*'", lambda m: "'" + "X" * (len(m.group()) - 2) + "'", s)


def analyze(inst: Inst) -> None:
    c = normalize(strip_comments(inst.text))
    w = first_word(c)
    if inst.kind in ("variable", "control"):
        return
    if w in CONTROL_WORDS:
        inst.kind = "control"
        return
    if re.match(r"\$\((must_)?include", c, re.I):
        inst.kind = "include"
        inst.warnings.append("$(include) : contenu inclus non analysé")
        return
    if re.match(r"(LIB|ODBC|OLEDB)?\s*CONNECT\b|DISCONNECT\b", c, re.I):
        inst.kind = "connect"
        return
    if w == "STORE":
        inst.kind = "store"
        m = re.match(r"STORE\s+(?:(.*?)\s+FROM\s+)?" + NAME
                     + r"\s+INTO\s+(.*?)\s*(\(([^)]*)\))?\s*;?$", c, re.I)
        if m:
            fmt = (m.group(5) or "qvd").split(",")[0].strip().lower()
            inst.stored = (strip_name(m.group(2)),
                           resolve_path(unquote(strip_name(m.group(3))),
                                        inst.directory), fmt,
                           m.group(1))
            inst.target = inst.stored[0]
            inst.constructs.add("store")
        else:
            inst.warnings.append("STORE non reconnu")
        return
    if w == "DROP":
        m = re.match(r"DROP\s+TABLES?\s+(.*?);?$", c, re.I)
        if m:
            inst.kind = "drop"
            inst.dropped = [strip_name(t) for t in split_top(m.group(1))]
        else:
            inst.kind = "drop_field"
            m = re.match(r"DROP\s+FIELDS?\s+(.*?)(?:\s+FROM\s+(.*?))?;?$", c, re.I)
            if m:
                inst.fields = [strip_name(f) for f in split_top(m.group(1))]
                if m.group(2):
                    inst.target = strip_name(split_top(m.group(2))[0])
        return
    if w == "RENAME":
        inst.kind = "rename"
        m = re.match(r"RENAME\s+TABLES?\s+" + NAME + r"\s+TO\s+" + NAME, c, re.I)
        if m:
            inst.target = strip_name(m.group(1))
            inst.label = strip_name(m.group(2))
        else:
            inst.warnings.append("RENAME FIELD : renommage de colonnes")
        return

    body = c
    m = re.match(NAME + r"\s*:(?!/)", body)
    if m and first_word(m.group(1)) not in ("LOAD", "SQL", "SELECT"):
        inst.label = strip_name(m.group(1))
        body = body[m.end():].strip()
    lm = re.search(r"\b(LOAD|SELECT)\b", mask_strings(body), re.I)
    if not lm:
        inst.kind = "other"
        return
    inst.kind = "load"
    head = body[:lm.start()]
    masked = mask_strings(body)

    if re.search(r"\bMAPPING\b", head, re.I):
        inst.constructs.add("mapping")
    jm = re.search(r"\b(LEFT|RIGHT|INNER|OUTER)?\s*(JOIN|KEEP)\b\s*(\(\s*"
                   + NAME + r"\s*\))?", head, re.I)
    if jm:
        kind = jm.group(2).lower()
        inst.constructs.add(kind)
        inst.constructs.add(f"{kind}:{(jm.group(1) or 'outer').lower()}")
        if jm.group(4):
            inst.target = strip_name(jm.group(4))
    cm = re.search(r"(?<![Nn][Oo])\bCONCATENATE\b\s*(\(\s*" + NAME + r"\s*\))?",
                   head, re.I)
    if cm and not re.search(r"\bNOCONCATENATE\b", head, re.I):
        inst.constructs.add("concatenate")
        if cm.group(2):
            inst.target = strip_name(cm.group(2))
    if re.search(r"\bNOCONCATENATE\b", head, re.I):
        inst.constructs.add("noconcatenate")
    for kw in ("CROSSTABLE", "GENERIC", "INTERVALMATCH", "HIERARCHY",
               "HIERARCHYBELONGSTO", "QUALIFY"):
        if re.search(rf"\b{kw}\b", head, re.I):
            inst.constructs.add(kw.lower())
            inst.warnings.append(f"{kw} : construction rare, à vérifier de près")
    if inst.target is None:
        inst.target = inst.label

    # dans « … AS Resident » ou « …, Resident », c'est un champ, pas la clause
    rm = next((m for m in re.finditer(r"\bRESIDENT\s+" + NAME, body, re.I)
               if not re.search(r"(\bAS|,)\s*$", body[:m.start()], re.I)
               and m.group(1) != ","), None)
    if rm:
        inst.resident = strip_name(rm.group(1))
        inst.constructs.add("resident")
    sel = re.search(r"\bSELECT\b", masked, re.I)
    if sel:
        # SQL SELECT vers une base : on suppose que chaque table lue a été
        # exportée en Parquet dans data/input/<table>.
        for tm in re.finditer(r"\b(?:FROM|JOIN)\s+(\[[^\]]+\]|\"[^\"]+\"|[\w.$]+)",
                              body[sel.end():], re.I):
            table = strip_name(tm.group(1)).split(".")[-1].strip('"[]')
            if table and table.upper() not in ("SELECT", "LATERAL"):
                inst.sources.append(table)
                inst.warnings.append(f"table de base de données {table} : "
                                     "supposée exportée en Parquet dans "
                                     f"data/input/{table}")
    for fm in re.finditer(r"\bFROM\s+(\[[^\]]+\]|'[^']*'|[^\s;(]+)", body, re.I):
        if re.search(r"\bLOAD\b", masked[:fm.start()], re.I) and \
                not re.search(r"\bSELECT\b", masked[:fm.start()], re.I):
            inst.sources.append(resolve_path(unquote(strip_name(fm.group(1))),
                                             inst.directory))
    if re.search(r"\bINLINE\b", masked, re.I):
        inst.constructs.add("inline")
    if re.search(r"\bSELECT\b", masked, re.I):
        inst.constructs.add("sql")
    if len(re.findall(r"\bLOAD\b", masked, re.I)) > 1:
        inst.constructs.add("preceding_load")
    for kw, tag in (("WHERE", "where"), ("WHILE", "while"),
                    (r"GROUP\s+BY", "group_by"), (r"ORDER\s+BY", "order_by"),
                    ("DISTINCT", "distinct")):
        if re.search(rf"\b{kw}\b", masked, re.I):
            inst.constructs.add(tag)
    for am in re.finditer(r"\bApplyMap\s*\(", body, re.I):
        args = split_top(_balanced(body, am.end() - 1))
        if args:
            inst.applymaps.append({
                "map": unquote(args[0]),
                "key": args[1] if len(args) > 1 else "?",
                "default": args[2] if len(args) > 2 else
                "la clé elle-même (pas de 3e argument)"})
            inst.constructs.add("applymap")
    for f in re.findall(r"\b([A-Za-z_][\w]*#?)\s*\(", masked):
        if f.upper() not in ("LOAD", "SELECT", "FROM", "INLINE", "RESIDENT",
                             "IN", "AND", "OR", "NOT", "JOIN", "KEEP",
                             "CONCATENATE", "WHERE", "INTO", "TXT", "QVD"):
            inst.functions.add(f.lower())
    # Fonctions qui dépendent de l'ordre de chargement. Si l'une d'elles finit
    # chez le modèle, il recevra la skill qlik-row-functions-sql.
    if inst.functions & {"peek", "previous", "above", "below", "firstvalue",
                         "lastvalue", "firstsortedvalue"}:
        inst.constructs.add("row_functions")
    inst.fields = extract_fields(body)
    for name in filter(None, (inst.label, inst.target, inst.resident)):
        if "$(" in name:
            inst.warnings.append(f"nom de table dynamique non résolu : {name}")


def _balanced(s: str, open_idx: int) -> str:
    depth, quote = 0, None
    for j in range(open_idx, len(s)):
        ch = s[j]
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch == "'":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return s[open_idx + 1:j]
    return s[open_idx + 1:]


def extract_fields(body: str) -> list[str]:
    """Colonnes produites par le dernier LOAD, c'est-à-dire celui du haut
    dans un LOAD empilé."""
    masked = mask_strings(body)
    m = re.search(r"\bLOAD\b(?:\s+DISTINCT\b)?", masked, re.I)
    if not m:
        return []
    end = re.search(r"\bFROM\b|\bRESIDENT\b|\bINLINE\b|\bAUTOGENERATE\b|"
                    r"\bWHERE\b|\bGROUP\s+BY\b|;|\bLOAD\b|\bSQL\b|\bSELECT\b",
                    masked[m.end():], re.I)
    seg = body[m.end(): m.end() + end.start()] if end else body[m.end():]
    if re.search(r"\bINLINE\b", masked, re.I) and seg.strip() in ("*", ""):
        im = re.search(r"\bINLINE\s*\[\s*([^\n\]]*)", body, re.I)
        if im:
            return [strip_name(f) for f in split_top(im.group(1))]
    out = []
    for part in split_top(seg):
        alias = re.search(r"\bAS\s+" + NAME + r"\s*$", part, re.I)
        if alias:
            out.append(strip_name(alias.group(1)))
        elif part.strip() == "*":
            out.append("*")
        elif re.fullmatch(NAME, part.strip()):
            out.append(strip_name(part))
        else:
            out.append("?")
    return out


def resolve_path(path: str, directory: str) -> str:
    """Préfixe un chemin relatif par le DIRECTORY courant, comme le fait
    Qlik."""
    if not directory or re.match(r"(lib://|[A-Za-z]:|[\\/]|\.\.|https?:)",
                                 path, re.I):
        return path
    sep = "" if directory.endswith(("/", "\\")) else "/"
    return directory + sep + path


# ==========================================================================
# 4. Graphe des tables et sections
# ==========================================================================

def build_graph(insts: list[Inst]):
    """Construit les tables du script.

    Une table supprimée (DROP) puis rechargée sous le même nom est une
    autre table pour nous : elle prend la clé « NOM__2 », puis « NOM__3 »,
    etc."""
    tables: dict[str, dict] = {}
    schemas: dict[str, list[str]] = {}
    live: dict[str, str] = {}        # nom Qlik -> clé de la version en cours
    versions: dict[str, int] = {}
    last = None
    anon = 0

    def ref(name):
        return live.get(name, name) if name else name

    def create(name):
        if name in live and not tables[live[name]]["dropped"]:
            return live[name]
        versions[name] = versions.get(name, 0) + 1
        key = name if versions[name] == 1 else f"{name}__{versions[name]}"
        live[name] = key
        tables[key] = {"insts": [], "deps": set(), "mapping": False,
                       "dropped": False, "stored": [], "name": name}
        return key

    for k, inst in enumerate(insts):
        analyze(inst)
        if inst.kind == "load":
            explicit = {"join", "keep", "concatenate"} & inst.constructs
            if inst.target is None:
                if explicit and last:
                    inst.target = tables[last]["name"]
                    inst.warnings.append(f"cible implicite = {inst.target} "
                                         "(dernière table chargée)")
                elif inst.sources:
                    inst.target = re.split(r"[\\/]", inst.sources[0])[-1] \
                        .rsplit(".", 1)[0] or f"_UNNAMED_{anon}"
                    inst.warnings.append(f"LOAD sans nom : Qlik la nomme "
                                         f"{inst.target}")
                else:
                    anon += 1
                    inst.target = f"_UNNAMED_{anon}"
                    inst.warnings.append("LOAD sans nom")
            inst.resident = ref(inst.resident)
            for a in inst.applymaps:
                a["map"] = ref(a["map"])
            # Peek('f', n, 'T') / NoOfRows('T') : dépend de l'état de T à ce
            # moment-là
            inst.table_refs = {}
            for tm in re.finditer(r"\b(?:Peek\s*\([^()]*?,[^()]*?|NoOfRows\s*\()"
                                  r"\s*'([^']+)'\s*\)", inst.text, re.I):
                if tm.group(1) in live:
                    inst.table_refs[tm.group(1)] = live[tm.group(1)]
            # Exists(champ) : les tables déjà chargées qui ont ce champ
            inst.exists_tables = {}
            for fm in re.finditer(r"\bExists\s*\(\s*(\[[^\]]+\]|\"[^\"]+\"|"
                                  r"'[^']+'|[^\s,()]+)", inst.text, re.I):
                fname = fm.group(1).strip("[]\"'")
                inst.exists_tables[fname] = [
                    key for key in live.values()
                    if not tables[key]["dropped"] and not tables[key]["mapping"]
                    and fname in schemas.get(key, [])]
            new = []
            for f in inst.fields:
                if f == "*":
                    new += schemas.get(inst.resident, ["*"]) if inst.resident \
                        else ["*"]
                else:
                    new.append(f)
            # auto-concaténation de Qlik : mêmes champs qu'une table déjà
            # chargée
            if not explicit and "noconcatenate" not in inst.constructs \
                    and "mapping" not in inst.constructs \
                    and "*" not in new and "?" not in new:
                for name, key in live.items():
                    t = tables[key]
                    if name != inst.target and not t["dropped"] \
                            and not t["mapping"] \
                            and set(schemas.get(key, [])) == set(new):
                        inst.warnings.append(
                            f"AUTO-CONCATÉNATION : mêmes champs que {name}, "
                            f"Qlik ajoute ces lignes à {name} (pas de nouvelle "
                            "table)")
                        inst.target = name
                        inst.constructs.add("concatenate")
                        explicit = {"concatenate"}
                        break
            if explicit and "keep" not in inst.constructs:
                key = ref(inst.target) if inst.target in live else \
                    create(inst.target)
            elif "keep" in inst.constructs:
                # La table entre parenthèses filtre celle qu'on charge. C'est
                # donc une dépendance : elle compte pour l'ordre des sections
                # et doit exister en vue.
                inst.keep_other = ref(inst.target)
                key = create(inst.label) if inst.label else ref(inst.target)
            else:
                key = create(inst.target)
            if {"join", "keep"} & inst.constructs:
                other = ref(inst.target)
                base = schemas.get(other, [])
                if "*" in base or "*" in new or "?" in new or not base:
                    inst.join_keys = None
                    inst.warnings.append("clé de jointure non calculable "
                                         "(colonnes inconnues : LOAD * ou "
                                         "expression)")
                else:
                    inst.join_keys = [f for f in new if f in base]
                    if not inst.join_keys:
                        inst.warnings.append("AUCUN champ commun : Qlik fait "
                                             "un PRODUIT CARTÉSIEN")
            inst.target = key
            if {"join", "concatenate"} & inst.constructs:
                cur = schemas.setdefault(key, [])
                cur += [f for f in new if f not in cur]
            else:
                schemas[key] = new
            t = tables[key]
            t["insts"].append(k)
            t["mapping"] |= "mapping" in inst.constructs
            if inst.resident and inst.resident != key:
                t["deps"].add(inst.resident)
            t["deps"].update(a["map"] for a in inst.applymaps)
            other = getattr(inst, "keep_other", None)
            if other and other != key:
                t["deps"].add(other)
            for keys in inst.exists_tables.values():
                t["deps"].update(k_ for k_ in keys if k_ != key)
            t["deps"].update(k_ for k_ in inst.table_refs.values() if k_ != key)
            if not explicit:
                last = key
        elif inst.kind == "store" and inst.stored:
            key = ref(inst.stored[0])
            if key not in tables:
                # Même table écrite avec une autre casse (STORE commandes /
                # table Commandes) : on la rattache quand même.
                same = [v for n_, v in live.items()
                        if n_.lower() == inst.stored[0].lower()]
                key = same[0] if len(same) == 1 else key
            inst.target = key
            if key in tables:
                tables[key]["stored"].append(inst.stored)
                tables[key]["insts"].append(k)
            else:
                inst.warnings.append(
                    f"STORE {inst.stored[0]} : table introuvable"
                    + (" (nom dynamique non résolu)" if "$(" in inst.stored[0]
                       else "") + " : aucune sortie écrite")
        elif inst.kind == "drop":
            for name in inst.dropped:
                key = ref(name)
                if key in tables:
                    tables[key]["dropped"] = True
                    tables[key]["insts"].append(k)
        elif inst.kind == "drop_field":
            targets = [ref(inst.target)] if inst.target else \
                [key for key in live.values()
                 if set(inst.fields) & set(schemas.get(key, []))]
            for key in targets:
                if key in schemas:
                    schemas[key] = [f for f in schemas[key]
                                    if f not in inst.fields]
                if key in tables:
                    tables[key]["insts"].append(k)
        elif inst.kind == "rename" and inst.target and inst.label:
            old = ref(inst.target)
            if old in tables:
                tables[old]["insts"].append(k)
                live[inst.label] = old
                live.pop(inst.target, None)
                tables[old]["name"] = inst.label
    return tables, schemas


def build_sections(insts, tables, schemas):
    finals = [n for n, t in tables.items()
              if not t["mapping"] and (t["stored"] or not t["dropped"])]
    consumers: dict[str, set] = {}
    for n, t in tables.items():
        for d in t["deps"]:
            consumers.setdefault(d, set()).add(n)

    def downstream(name, seen=None):
        seen = seen if seen is not None else set()
        res = set()
        for c in consumers.get(name, ()):
            if c not in seen:
                seen.add(c)
                res |= {c} if c in finals else downstream(c, seen)
        return res

    owner = {}
    for n, t in tables.items():
        if t["mapping"] or n in finals:
            owner[n] = n
        else:
            f = downstream(n)
            owner[n] = next(iter(f)) if len(f) == 1 else n
    # Une table interne à une section mais lue par une autre doit devenir une
    # section à part, sinon elle n'existera pas comme vue SQL.
    changed = True
    while changed:
        changed = False
        for n, t in tables.items():
            for d in t["deps"]:
                if d in owner and owner[d] != d and owner[d] != owner[n] \
                        and not tables[d]["mapping"]:
                    owner[d] = d
                    changed = True
    groups: dict[str, list[str]] = {}
    for n in tables:
        groups.setdefault(owner[n], []).append(n)

    sec_of: dict[int, str] = {}
    for g, names in groups.items():
        for n in names:
            for k in tables[n]["insts"]:
                sec_of.setdefault(k, g)
    for k, inst in enumerate(insts):
        if k in sec_of:
            continue
        if inst.kind in ("variable", "connect", "include", "other",
                         "control"):
            # On ne recopie pas les IF/FOR/DO : chaque instruction garde son
            # contexte (boucle, condition, CALL) dans ses Facts.
            sec_of[k] = "__init__"
            continue
        # STORE/DROP d'une table inconnue, RENAME… : on le met dans la section
        # voisine
        nxt = next((sec_of[j] for j in range(k + 1, len(insts))
                    if sec_of.get(j, "__init__") != "__init__"), None)
        prv = next((sec_of[j] for j in range(k - 1, -1, -1)
                    if sec_of.get(j, "__init__") != "__init__"), None)
        sec_of[k] = nxt or prv or "__init__"

    gdeps = {g: {owner[d] for n in names for d in tables[n]["deps"]
                 if d in owner and owner[d] != g}
             for g, names in groups.items()}
    first = {g: min((k for k, s in sec_of.items() if s == g), default=10**9)
             for g in groups}
    order, done = [], set()
    while len(order) < len(groups):
        ready = [g for g in groups if g not in done and gdeps[g] <= done] \
            or [g for g in groups if g not in done]
        g = min(ready, key=lambda x: first[x])
        order.append(g)
        done.add(g)
    return order, groups, gdeps, sec_of, finals


# ==========================================================================
# 5. Génération : manifest, code déterministe, prompts
# ==========================================================================

def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")[:40] or "table"


INPUT_DIR = "data/input"
OUTPUT_DIR = "data/output"


def norm_path(path: str) -> str:
    return path.replace("\\", "/").strip().lower()


def file_stem(path: str) -> str:
    """Nom du fichier sans dossier ni extension. C'est la seule clé qu'on
    utilise pour rapprocher les fichiers."""
    name = re.split(r"[\\/]", path.replace("\\", "/"))[-1]
    return re.sub(r"\.(qvd|txt|csv|parquet|xlsx?)$", "", name, flags=re.I)


def is_output(path: str, stored: dict) -> bool:
    """Vrai si le fichier est écrit par un STORE, de ce script ou d'un autre
    script du projet."""
    return norm_path(path) in stored.get("paths", ()) or \
        file_stem(path).lower() in stored.get("names", ())


def resolve_io(path: str, stored: dict, conf: IoConf | None, store=False):
    """Associe un fichier Qlik à (location, format, entrée io.yml ou None).

    On prend d'abord l'entrée de conf/io.yml qui a le même nom (dans les
    sorties si le fichier est écrit par un STORE, dans les entrées sinon).
    S'il n'y en a pas, règle fixe : data/output/<nom> pour un fichier écrit
    par un STORE, data/input/<nom> sinon, en Parquet."""
    output = store or is_output(path, stored)
    # Une lecture se cherche dans la section input, une écriture dans
    # outputs : read_input() ne lit que ce qui est déclaré en entrée, même
    # si le fichier est produit par un autre script du projet.
    entry = conf.find(path, "output" if store else "input") if conf else None
    if entry:
        return entry.location, entry.format, entry
    if conf and not store and output:
        written = conf.find(path, "output")
        if written:
            # Déclaré seulement en sortie : on garde sa location pour la
            # traduction, mais sans entrée, le script ne pourra pas
            # s'assembler.
            return written.location, written.format, None
    return f"{OUTPUT_DIR if output else INPUT_DIR}/{file_stem(path)}", \
        "parquet", None


def map_path(path: str, stored: dict, conf: IoConf | None = None,
             store=False) -> str:
    return resolve_io(path, stored, conf, store)[0]


def numbered(insts_of_section) -> str:
    """Code de la section sans commentaires ni lignes vides, avec les
    numéros de ligne du script d'origine."""
    lines = []
    for inst in insts_of_section:
        text = strip_comments(inst.text)
        start = inst.stmt.line
        for off, l in enumerate(text.splitlines()):
            if l.strip():
                lines.append(f"{start + off:5d} | {l.rstrip()}")
    return "\n".join(lines)


def load_skill(skills_dir: Path, name: str) -> str:
    f = skills_dir / name / "SKILL.md"
    if not f.exists():
        return ""
    text = f.read_text(encoding="utf-8")
    return re.sub(r"^---.*?---\s*", "", text, flags=re.S).strip()


PROMPT = """# Task
Complete ONE Spark SQL query (Spark 3.5.8) translated from a Qlik Sense
script. A parser already translated most of it: every part it could not
translate safely is a CTE marked `TODO(model)`.

Everything you need is in this message: there is no need to read
documentation, skills or other files, and you cannot write files.

Answer ONLY with the CTEs that replace the `TODO(model)` CTEs, with the SAME
names, in one single ```sql block, for example:
```sql
cumul_1 AS (
  -- l.40 : cumul des montants par client
  SELECT ...
)
```
Do NOT repeat the rest of the query: it is kept as it is. You may add
helper CTEs before them if needed (new names). Be concise.

# Rules
1. Never rewrite the parts that are already translated.
2. A `TODO(model)` CTE keeps its name and produces the columns the next
   steps use. Translate the Qlik lines it names (see Qlik code below).
3. Faithful translation: same column names, same order, same logic.
4. Use only the views and CTEs that exist (listed in Facts). Never invent a
   column: missing information -> `-- TODO:` comment.
5. Native Spark SQL only: no UDF, no CREATE, no INSERT, one single query.
6. Keep one comment `-- l.<n> : <what Qlik does, in French>` per statement.
7. Qlik doubles apostrophes ('l''été'); Spark SQL does NOT: `''` glues two
   strings and the apostrophe disappears. Write "l'été" (double quotes).
8. Qlik booleans are -1 (true) and 0 (false): a condition used as a value is
   `CASE WHEN … THEN -1 ELSE 0 END`.
9. Every view and CTE carries the technical column `_qlik_order`
   (array<bigint>, Qlik load order). Keep it in every CTE you write; use
   `ORDER BY _qlik_order` for Peek, Previous, FirstValue, LastValue.

# Draft query (to complete)
Already translated CTEs are summarised by their columns (`...`): you can
read them, never rewrite them.
```sql
{draft}
```

# Qlik code of the parts to translate (variables substituted, original line numbers)
```qlik
{code}
```

# Facts (computed by the parser: trust them)
{facts}
{mappings}{skills}"""


HOLE_PROMPT = """# Task
Translate Qlik Sense expressions into Spark SQL 3.5.8 EXPRESSIONS.
A parser already translated everything else; only these expressions remain.

Answer ONLY with one line per expression, in one ```sql block:
```sql
h1: <Spark SQL expression>
```
No explanation, no SELECT, no CTE, no comment. Be short.
If information is missing to translate an expression safely, answer
`h1: ?` for it: you will get more context. Never guess.

# Rules
- Native Spark SQL functions only (no UDF).
- Qlik booleans are -1 (true) and 0 (false); any non-zero number is true.
- A string with an apostrophe uses double quotes: "l'été" (never '').
- `_qlik_order` is the Qlik load order (array<bigint>): use it in
  `OVER (ORDER BY …)` for Peek, Previous, RowNo and running totals.
- Use only the columns listed for each expression; types in parentheses
  come from conf/schema (a column without type: see the Qlik statement).

# Expressions
{holes}
{skills}"""


def hole_block(h: dict) -> str:
    types = h.get("types") or {}
    cols = ", ".join(f"{c} ({types[c]})" if c in types else c
                     for c in h["columns"]) if h.get("columns") else \
        "unknown (see the Qlik statement)"
    pre = f" (prefix them with `{h['qualifier']}.`)" if h.get("qualifier") else ""
    lines = [f"### {h['id']} — l.{h['line']}, "
             + (f"column `{h['alias']}`" if h.get("alias") else "WHERE condition")
             + f", {'condition (boolean)' if h['kind'] == 'condition' else 'value'}"
             + (", inside a GROUP BY (aggregate)" if h.get("aggregate") else ""),
             f"Qlik: `{h['qlik']}`",
             f"Why the parser stopped: {h['reason']}",
             f"Columns available{pre}: {cols}, _qlik_order",
             f"Load order: ORDER BY {h.get('order_by') or '_qlik_order'}"]
    if h.get("hint"):
        lines.append(f"A previous, not yet verified, translation of this exact "
                     f"expression: `{h['hint']}` (check it, do not trust it)")
    return "\n".join(lines)


def hole_key(h: dict) -> str:
    """Clé du cache d'expressions : la même expression Qlik dans le même
    contexte."""
    return f"{h['kind']}|{h.get('qualifier') or ''}|{' '.join(h['qlik'].split())}"


def fill_holes(sql: str, answers: dict[str, str]) -> str:
    """Remplace chaque trou /*TODO(model):hN*/ par son expression."""
    for hid, expr_sql in answers.items():
        sql = re.sub(r"\(/\*TODO\(model\):" + re.escape(hid)
                     + r"\*/ (?:NULL|true)\)", lambda m: f"({expr_sql})", sql)
    return sql


def facts_block(sec, insts, schemas, env, views, global_settings) -> str:
    out = []
    for g_ in global_settings:
        out.append(f"- GLOBAL SETTING that changes how this table is loaded "
                   f"(QUALIFY prefixes field names with the table name, "
                   f"NullAsValue/NullValue/NullInterpret change nulls): {g_}")
    if views:
        out.append("- Existing views: " + ", ".join(
            f"`{k}` (= {v.get('path') or 'table ' + v['table']})"
            for k, v in views.items()))
        for k, v in views.items():
            if v.get("types"):
                out.append(f"- Column types of `{k}` (conf/schema): "
                           + ", ".join(f"{c} {t}" for c, t in v["types"].items()))
    out.append(f"- Tables in this section, in script order: "
               f"{' -> '.join(sec['tables'])} (final: **{sec['table']}**)")
    for inst in insts:
        line = inst.stmt.first_line
        for c in sorted(inst.constructs):
            if c.startswith(("join:", "keep:")):
                kind, how = c.split(":")
                keys = inst.join_keys
                ktxt = f"ON {' AND '.join(f'a.{k} = b.{k}' for k in keys)}" \
                    if keys else "keys NOT computable: columns unknown"
                out.append(f"- l.{line}: {how.upper()} {kind.upper()} into "
                           f"{inst.target}: {ktxt}")
        if "preceding_load" in inst.constructs:
            out.append(f"- l.{line}: STACKED LOAD: read it BOTTOM-UP, the "
                       "lowest LOAD/SELECT runs first")
        for w in inst.warnings:
            out.append(f"- l.{line}: WARNING {w}")
    for c in dict.fromkeys(c for i in insts for c in i.context):
        out.append(f"- Context (loop / condition / SUB call): {c}")
    cols = sec.get("columns") or []
    if cols and "*" not in cols and "?" not in cols:
        out.append(f"- Expected output columns, in order: {cols}")
    unresolved = sorted({m for i in insts for m in re.findall(
        r"\$\(#?([A-Za-z_][\w.]*)", strip_comments(i.text))} - SYSTEM_VARS)
    for name in unresolved:
        if name in env.lets:
            hint = f"LET {name} = {env.lets[name]}"
            if re.search(r"\bPeek\s*\(", env.lets[name], re.I):
                hint += ("  -> scalar read from a table: use a scalar "
                         "subquery `(SELECT MAX(...) FROM ...)`")
            out.append(f"- Variable computed at reload time: {hint}")
        else:
            out.append(f"- Variable NOT resolved: $({name}) (loop variable or "
                       "defined dynamically) -> `-- TODO:` if needed")
    return "\n".join(dict.fromkeys(out))


def mapping_code(insts) -> str:
    return "\n".join(l.rstrip() for i in insts if i.kind == "load"
                     for l in strip_comments(i.text).splitlines() if l.strip())


def analyze_script(script: Path):
    """Découpe, exécute à blanc et construit le graphe d'un script."""
    text = script.read_text(encoding="utf-8-sig", errors="replace") \
        .replace("\r\n", "\n")
    stmts = merge_preceding_loads(drop_rem(tokenize(text)))
    tree, warnings = parse_blocks(stmts)
    env, insts, notes = Env(), [], list(warnings)
    try:
        execute(tree, env, [], {}, insts, notes)
    except StopScript as stop:
        rest = len(text.splitlines()) - stop.line
        notes.append(f"l.{stop.line}: EXIT SCRIPT : fin du script, les "
                     f"{rest} ligne(s) suivantes ne sont pas traduites")
    tables, schemas = build_graph(insts)
    return text, env, insts, notes, tables, schemas


def collect_io(script: Path) -> dict:
    """Fichiers lus et écrits par un script (chemins normalisés), plus les
    colonnes connues des fichiers écrits. Un autre script qui fait un
    « LOAD * » sur l'un d'eux connaîtra ainsi ses colonnes."""
    _, _, insts, _, _, schemas = analyze_script(script)
    writes, columns = set(), {}
    for i in insts:
        if i.kind == "store" and i.stored:
            writes.add(norm_path(i.stored[1]))
            cols = schemas.get(i.target) or []
            if cols and "*" not in cols and "?" not in cols:
                columns[file_stem(i.stored[1]).lower()] = cols
    reads = sorted({norm_path(src) for i in insts for src in i.sources})
    return {"writes": sorted(writes), "reads": reads, "columns": columns}


def load_project_stores(path: Path, script_name: str) -> dict:
    """Chemins écrits par l'ensemble des scripts, et noms écrits par les
    autres scripts que celui-ci."""
    if not path.exists():
        return {"paths": set(), "names": {}, "columns": {}}
    data = json.loads(path.read_text(encoding="utf-8"))
    paths, names, columns = set(), {}, {}
    for other, io in data.items():
        paths |= set(io["writes"])
        columns.update(io.get("columns", {}))
        if other != script_name:
            for w in io["writes"]:
                names.setdefault(file_stem(w).lower(), other)
    return {"paths": paths, "names": names, "columns": columns}


def load_cache(out_root: Path) -> dict:
    """Expressions déjà traduites par le modèle et validées
    (work/expressions.json). On les réutilise sans repasser par le modèle,
    et on peut les corriger à la main dans le fichier."""
    f = out_root / "expressions.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}


def prepare(script: Path, out_root: Path, skills_dir: Path,
            project_stores: dict | None = None,
            conf: IoConf | None = None) -> dict:
    cache = load_cache(out_root)
    text, env, insts, notes, tables, schemas = analyze_script(script)
    order, groups, gdeps, sec_of, finals = build_sections(insts, tables, schemas)
    for i in insts:
        i.clean_text = strip_comments(i.text)

    # réglages qui changent la façon dont les LOAD suivants chargent les
    # données
    global_settings = []      # (n° d'instruction, texte, champs ou None)
    for k, i in enumerate(insts):
        c = normalize(i.clean_text)
        m = re.match(r"(UN)?QUALIFY\b|NULLASVALUE\b|NULLASNULL\b", c, re.I)
        if m:
            arg = c[m.end():].strip().rstrip(";")
            fields = None if arg.strip() == "*" else \
                {strip_name(f) for f in split_top(arg)}
            global_settings.append((k, f"l.{i.stmt.first_line}: {c[:100]}",
                                    fields))
        elif re.match(r"SET\s+(NullValue|NullInterpret)\b", c, re.I):
            global_settings.append((k, f"l.{i.stmt.first_line}: {c[:100]}",
                                    None))
    for _, txt, _ in global_settings:
        notes.append(f"RÉGLAGE GLOBAL à reproduire : {txt}")
    for name in sorted(set(SKILL_BY_CONSTRUCT.values())):
        if not (skills_dir / name / "SKILL.md").exists():
            notes.append(f"skill {name} introuvable dans {skills_dir}")

    # fichier écrit par un STORE (ici ou dans un autre script) : data/output
    project_stores = project_stores or {"paths": set(), "names": {},
                                        "columns": {}}
    stored = {"paths": {norm_path(i.stored[1]) for i in insts
                        if i.kind == "store" and i.stored}
              | project_stores["paths"],
              "names": project_stores["names"]}
    for i in insts:
        for src in i.sources:
            other = stored["names"].get(file_stem(src).lower())
            if other and norm_path(src) not in stored["paths"]:
                notes.append(f"l.{i.stmt.first_line}: {src} lu dans "
                             f"data/output : même nom qu'une sortie de "
                             f"{other} (à vérifier)")
    by_name = {}
    for i in insts:
        for path in i.sources + ([i.stored[1]] if i.stored else []):
            by_name.setdefault(map_path(path, stored, conf, path in (
                [i.stored[1]] if i.stored else [])), set()).add(norm_path(path))
    for dest, paths in by_name.items():
        if len(paths) > 1:
            notes.append(f"COLLISION : {sorted(paths)} -> même chemin {dest}")

    base = out_root / script.stem
    base.mkdir(parents=True, exist_ok=True)
    previous = {}
    if (base / "manifest.json").exists():
        for old in json.loads((base / "manifest.json").read_text(
                encoding="utf-8"))["sections"]:
            previous[old["id"]] = old
    has_init = "__init__" in sec_of.values()
    ids = {g: f"{k:03d}_{slug('init' if g == '__init__' else g)}"
           for k, g in enumerate((["__init__"] if has_init else []) + order)}
    table_views = {g: view_name(g) for g in order}
    # types déclarés dans conf/schema, par nom de colonne
    conf_types: dict[str, str] = {}
    if conf:
        for e in conf.inputs + conf.outputs:
            conf_types.update(e.types or {})
    inputs: dict[str, dict] = {}         # vue d'entrée -> location, format
    # colonnes produites par chaque section
    final_columns: dict[str, list] = {}
    # fichiers qu'on n'a pas trouvés dans io.yml
    missing_conf: set[str] = set()

    def input_view(path: str) -> str:
        location, fmt, entry = resolve_io(path, stored, conf)
        if conf and conf.enabled and entry is None:
            only_out = conf.find(path, "output") is not None
            missing_conf.add(f"entrée {file_stem(path)}" + (
                " (déclarée en sortie seulement, à déclarer aussi en "
                "entrée)" if only_out else ""))
        view = view_name("in_" + (entry.name if entry else file_stem(path)))
        # "io" : le nom de l'entrée dans conf/io.yml, celui que le job passe
        # à read_input(). None si le fichier n'y est pas déclaré.
        inputs[view] = {"path": location, "format": fmt,
                        "io": entry.name if entry else None,
                        "columns": file_schema(path)}
        if entry and entry.types:
            inputs[view]["types"] = entry.types
        return view

    def file_schema(path: str):
        # Par ordre de préférence : conf/schema, puis un Parquet déjà présent,
        # puis le STORE d'un autre script.
        entry = resolve_io(path, stored, conf)[2]
        if entry and entry.columns:
            return entry.columns
        return parquet_columns(resolve_location(map_path(path, stored,
                                                         conf))) or \
            project_stores["columns"].get(file_stem(path).lower())

    sections = []
    produced: list[str] = []           # tables déjà calculées (vues SQL)
    for g in (["__init__"] if has_init else []) + order:
        ks = sorted(k for k, sg in sec_of.items() if sg == g)
        sec_insts = [insts[k] for k in ks]
        names = [] if g == "__init__" else groups[g]
        kind = ("init" if g == "__init__" else
                "mapping" if tables[g]["mapping"] else
                "final" if g in finals else "shared")
        constructs = set().union(*(i.constructs for i in sec_insts)) \
            if sec_insts else set()
        if len([n for n in names if not tables[n]["mapping"]]) > 1:
            constructs.add("resident_chain")
        outputs, output_formats, output_io, store_flags = {}, {}, {}, []
        for n in names:
            for (_, path, fmt, _fields) in tables[n]["stored"]:
                if n == g:
                    location, out_fmt, entry = resolve_io(path, stored, conf,
                                                          store=True)
                    if conf and conf.enabled and entry is None:
                        missing_conf.add(f"sortie {file_stem(path)}")
                    outputs[n], output_formats[n] = location, out_fmt
                    # nom de la sortie dans conf/io.yml, pour write_output()
                    output_io[n] = entry.name if entry else None
                else:
                    store_flags.append(f"STORE de la table intermédiaire {n} "
                                       f"vers {path} : non géré, à traiter "
                                       "à la main")
        dep_candidates = {d for n in names for d in tables[n]["deps"]}
        for d in list(dep_candidates):
            if tables.get(d, {}).get("mapping"):
                dep_candidates |= tables[d]["deps"]
        deps_tables = sorted({d for d in dep_candidates
                              if owner_of(d, groups) != g
                              and not tables.get(d, {}).get("mapping")})
        cols_here = {f for n in names for f in schemas.get(n, [])}
        applicable = [txt for (k0, txt, fields) in global_settings
                      if ks and k0 < ks[-1]
                      and (fields is None or fields & cols_here)]
        sec = {"id": ids[g], "kind": kind,
               "table": None if g == "__init__" else g,
               "view": table_views.get(g),
               "tables": names,
               "lines": [sec_insts[0].stmt.first_line,
                         sec_insts[-1].stmt.last_line] if sec_insts else None,
               "depends_on_tables": deps_tables,
               "outputs": outputs,
               "output_formats": output_formats,
               "output_io": output_io,
               "constructs": sorted(c for c in constructs if ":" not in c),
               "statement_lines": sorted({i.stmt.first_line for i in sec_insts
                                          if i.kind in ("load", "drop_field",
                                                        "rename")
                                          and "mapping" not in i.constructs}),
               "flags": sorted({f"l.{i.stmt.first_line}: {w}"
                                for i in sec_insts for w in i.warnings})
               + store_flags,
               "status": "todo"}
        d = base / sec["id"]
        d.mkdir(exist_ok=True)
        (d / "source.qvs").write_text(numbered(sec_insts) + "\n",
                                      encoding="utf-8")
        if kind in ("init", "mapping"):
            sec["status"] = "skip"
            sections.append(sec)
            continue
        produced_now = list(produced)
        extra_views: dict[str, str] = {}      # vues lues par FieldValue()

        def fieldvalue_views(field_name, produced_tables):
            found = [t for t in produced_tables
                     if field_name in (schemas.get(t) or [])]
            for t in found:
                extra_views[table_views[t]] = t
            return [table_views[t] for t in found]
        produced.append(g)

        # ---- traduction sans modèle
        inputs.clear()
        ctx = SectionContext(
            final_table=g,
            table_view=lambda t: table_views.get(t),
            input_view=input_view, file_schema=file_schema,
            mapping_insts=lambda m: [insts[k] for k in tables.get(m, {})
                                     .get("insts", [])],
            table_name=lambda t: tables.get(t, {}).get("name", t),
            graph_schema=lambda t: _known(schemas.get(t)),
            tables_with_field=lambda f: fieldvalue_views(f, produced_now),
            column_types=conf_types,
            date_format=env.vars.get("DateFormat"),
            timestamp_format=env.vars.get("TimestampFormat"))
        header = (f"-- Section {sec['id']} : table {g}"
                  f" (lignes Qlik {sec['lines'][0]}-{sec['lines'][1]})\n"
                  "-- Traduit depuis Qlik par tools/prepare.py\n")
        sql, todos, cols, technical, short_sql, holes, derived = translate_section(
            sec_insts, [i.clean_text for i in sec_insts], ctx, header)
        # Colonnes calculées en date : on les fait connaître aux tables
        # suivantes. Dans le modèle associatif de Qlik, un même nom de champ
        # désigne la même donnée partout.
        for c_, t_ in derived.items():
            conf_types.setdefault(c_, t_)
        # expression déjà traduite et validée ailleurs : pas besoin du modèle
        known = {h["id"]: cache[hole_key(h)]["sql"] for h in holes
                 if cache.get(hole_key(h), {}).get("verified")}
        for h in holes:
            if hole_key(h) in cache and h["id"] not in known:
                # simple piste, pas encore vérifiée
                h["hint"] = cache[hole_key(h)]["sql"]
        if known:
            sql = fill_holes(sql, known)
            short_sql = fill_holes(short_sql, known)
            todos = [t for t in todos
                     if not any(t.endswith(f"(expression {k})") for k in known)]
            holes = [h for h in holes if h["id"] not in known]
            sec["from_cache"] = len(known)
        col_types = {}
        for v in inputs.values():
            col_types.update(v.get("types") or {})
        for h in holes:
            h["types"] = {c: col_types[c] for c in (h.get("columns") or [])
                          if c in col_types}
        sec["holes"] = holes
        sec["technical_columns"] = technical
        for gs in applicable:
            todos.append(f"réglage global à appliquer : {gs}")
        sec["columns"] = cols or schemas.get(g, [])
        final_columns[g] = _known(sec["columns"])
        sec["views"] = {v: dict(p) for v, p in inputs.items()}
        sec["views"].update({table_views[t]: {"table": t,
                                              "columns": final_columns.get(t)}
                             for t in deps_tables if t in table_views})
        sec["views"].update({v: {"table": t, "columns": final_columns.get(t)}
                             for v, t in extra_views.items()})
        sec["todos"] = todos
        draft_hash = hashlib.sha1(sql.encode()).hexdigest()[:12]
        sec["draft_hash"] = draft_hash
        old = previous.get(sec["id"], {})
        unchanged = old.get("draft_hash") == draft_hash and \
            (d / "query.sql").exists()
        if not todos:
            (d / "query.sql").write_text(sql, encoding="utf-8")
            (d / "prompt.md").unlink(missing_ok=True)
            sec["status"] = "deterministic"
        elif unchanged and old.get("status") in ("ok", "escalade"):
            # déjà complétée et pas modifiée depuis
            sec["status"] = old["status"]
        else:
            (d / "query.sql").write_text(sql, encoding="utf-8")
            statement_todos = [t for t in todos if "(expression h" not in t]
            todo_insts = [i for i in sec_insts if any(
                t.startswith(f"l.{i.stmt.first_line} ") for t in statement_todos)] \
                or sec_insts
            tc = set().union(*(i.constructs for i in todo_insts))
            skill_names = sorted({SKILL_BY_CONSTRUCT[c] for c in tc
                                  if c in SKILL_BY_CONSTRUCT})
            skill_txt = "".join(
                f"\n# Skill: {s_}\n{load_skill(skills_dir, s_)}\n"
                for s_ in skill_names if load_skill(skills_dir, s_))
            maps = {a["map"] for i in todo_insts for a in i.applymaps}
            map_txt = "".join(
                f"- Mapping `{m}` (do NOT create it: LEFT JOIN on its source, "
                f"COALESCE with the default):\n```qlik\n"
                f"{mapping_code([insts[k] for k in tables[m]['insts']])}\n```\n"
                for m in sorted(maps) if m in tables)
            prompt = PROMPT.format(
                draft=short_sql.rstrip(),
                code=numbered(todo_insts),
                facts=facts_block(sec, todo_insts, schemas, env, sec["views"],
                                  applicable),
                mappings=("\n# Mappings used\n" + map_txt) if map_txt else "",
                skills=skill_txt)
            # prompt.md, pour les instructions entières à compléter. Pour une
            # table qui n'a que des trous d'expression, il sert de dernier
            # recours si le modèle n'arrive pas à les traduire une par une.
            (d / "prompt.md").write_text(prompt, encoding="utf-8")
            sec["statement_todos"] = bool(statement_todos)
            sec["prompt_tokens"] = len(prompt) // 4 if statement_todos else 0
            (d / "draft_condensed.sql").write_text(short_sql, encoding="utf-8")
            if holes:
                by_line = {i.stmt.first_line: i for i in sec_insts}
                hole_groups = {}
                for h in holes:
                    hole_groups.setdefault(h["line"], []).append(h)
                blocks = []
                for line, hs in hole_groups.items():
                    code = numbered([by_line[line]]) if line in by_line else ""
                    if len(code) > 3000 and not any(re.search(
                            r"\b(peek|previous|above)\b", h["qlik"], re.I)
                            for h in hs):
                        # LOAD énorme : on ne garde que les lignes qui
                        # contiennent les expressions
                        first = code.splitlines()[:2]
                        keep = [ln for ln in code.splitlines()
                                if any(h["qlik"].splitlines()[0].strip()[:40]
                                       in ln for h in hs)]
                        code = "\n".join(first + ["  …"] + keep)
                    blocks.append(f"## Qlik statement l.{line}\n```qlik\n{code}"
                                  "\n```\n\n" + "\n\n".join(hole_block(h)
                                                             for h in hs))
                hole_txt = "\n\n".join(blocks)
                row = any(re.search(r"\b(peek|previous|above|firstsortedvalue)\b",
                                    h["qlik"], re.I) for h in holes)
                skill = load_skill(skills_dir, "qlik-row-functions-sql") if row \
                    else ""
                hprompt = HOLE_PROMPT.format(
                    holes=hole_txt,
                    skills=f"\n# Skill: qlik-row-functions-sql\n{skill}\n"
                    if skill else "")
                (d / "holes_prompt.md").write_text(hprompt, encoding="utf-8")
                sec["prompt_tokens"] += len(hprompt) // 4
            else:
                (d / "holes_prompt.md").unlink(missing_ok=True)
            sec["status"] = "todo"
        sections.append(sec)

    for i in insts:
        for w in i.warnings:
            if w.startswith("STORE "):
                notes.append(f"l.{i.stmt.first_line}: {w}")
    if missing_conf:
        notes.append("absents de conf/io.yml, à y déclarer avant d'assembler "
                     "(le job lit et écrit par nom via spark-foundation-core ; "
                     "migrate.py les ajoute, sauf avec --no-conf) : "
                     + ", ".join(sorted(missing_conf)))
    unresolved = sorted({m for i in insts for m in re.findall(
        r"\$\(#?([A-Za-z_][\w.]*)", strip_comments(i.text))} - SYSTEM_VARS)
    manifest = {"script": script.name, "notes": notes,
                "unresolved_variables": unresolved, "sections": sections}
    (base / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    write_report(base, manifest, len(text.splitlines()))
    return manifest


def _known(cols):
    return cols if cols and "*" not in cols and "?" not in cols else None


def parquet_columns(path: str):
    """Colonnes d'un Parquet déjà présent dans data/, ou None. Ça permet de
    traduire les « LOAD * FROM fichier » et de calculer leurs clés."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    folder, _, name = path.rpartition("/")
    candidates = sorted(Path(folder).glob(name)) if any(
        c in name for c in "*?") else [Path(path)]
    for c in candidates:
        files = [c] if c.is_file() else sorted(c.rglob("*.parquet"))
        if files:
            try:
                return list(pq.read_schema(files[0]).names)
            except Exception:
                return None
    return None


def owner_of(table, groups):
    for g, names in groups.items():
        if table in names:
            return g
    return None


def write_report(base: Path, m: dict, n_lines: int) -> None:
    secs = m["sections"]
    count = {}
    for s_ in secs:
        count[s_["status"]] = count.get(s_["status"], 0) + 1
    tables = [s_ for s_ in secs if s_["status"] != "skip"]
    auto = sum(s_["status"] == "deterministic" for s_ in tables)
    cached = sum(1 for s_ in tables if s_["status"] == "deterministic"
                 and s_.get("from_cache"))
    lines = [f"# Préparation de `{m['script']}`", "",
             f"- {n_lines} lignes, {len(tables)} tables à produire",
             f"- **{auto}/{len(tables)} traduites entièrement sans modèle**"
             + (f" (dont {cached} grâce au cache d'expressions vérifiées)"
                if cached else "")
             + f", {len(tables) - auto} avec des parties à compléter",
             f"- Variables non résolues : "
             f"{', '.join(m['unresolved_variables']) or 'aucune'}",
             "", "| Table | Statut à la préparation | Lignes Qlik | "
             "Parties à compléter |", "|---|---|---|---|"]
    for s_ in secs:
        if s_["status"] == "skip":
            continue
        ln = f"{s_['lines'][0]}-{s_['lines'][1]}" if s_["lines"] else "-"
        todo = "<br>".join(s_.get("todos", [])) or "-"
        lines.append(f"| {s_['id']} | {s_['status']} | {ln} | {todo} |")
    if m["notes"]:
        lines += ["", "## Notes", ""] + [f"- {n}" for n in m["notes"]]
    (base / "rapport.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("scripts", nargs="+", type=Path)
    p.add_argument("--out", type=Path, help="dossier de travail (défaut : work)")
    p.add_argument("--collect", action="store_true",
                   help="seulement recenser les fichiers lus/écrits de chaque "
                        "script dans work/stores.json")
    p.add_argument("--skills", type=Path, default=Path(".opencode/skills"))
    args = p.parse_args(argv)
    out_root = args.out or Path("work")
    stores_path = out_root / "stores.json"
    if args.collect:
        data = json.loads(stores_path.read_text(encoding="utf-8")) \
            if stores_path.exists() else {}
        for script in args.scripts:
            data[script.name] = collect_io(script)
        out_root.mkdir(parents=True, exist_ok=True)
        stores_path.write_text(json.dumps(data, indent=2, ensure_ascii=False),
                               encoding="utf-8")
        print(f"{len(data)} script(s) recensé(s) -> {stores_path}")
        return 0
    conf = IoConf()
    for p_ in conf.problems:
        print(f"⚠ conf : {p_}")
    for script in args.scripts:
        m = prepare(script, out_root, args.skills,
                    load_project_stores(stores_path, script.name), conf)
        st = {}
        for s_ in m["sections"]:
            st[s_["status"]] = st.get(s_["status"], 0) + 1
        print(f"{script.name}: {len(m['sections'])} sections {st} -> "
              f"{out_root / script.stem / 'rapport.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
