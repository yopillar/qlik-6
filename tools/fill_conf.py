"""Complète conf/io.yml et les schémas à partir des scripts Qlik.

migrate.py le lance avant de traduire. La règle : on ajoute ce qui manque,
on ne modifie jamais ce qui existe.
  - une entrée ou une sortie absente de conf/io.yml est ajoutée à la fin de
    sa section, en texte, pour garder les commentaires et la mise en forme
    du fichier
  - un schéma absent est créé ; un schéma existant n'est jamais retouché,
    même s'il lui manque des colonnes (on le signale seulement : il sert au
    socle pour lire le fichier, une colonne en trop pourrait le casser)
  - avant de modifier io.yml, on en garde une copie (io.yml.bak), et si le
    résultat n'est plus du YAML lisible, on remet l'original
Avec --draft, rien n'est touché : tout est écrit dans conf/draft/.

D'où viennent les informations :
  - les entrées et les sorties : les FROM et les STORE des scripts. Un
    fichier écrit par un script et relu par un autre est ajouté dans les
    deux sections, avec la même location.
  - les colonnes : celles que les scripts lisent dans chaque fichier. Si le
    fichier est présent dans data/input/ (ou à sa location), on prend toutes
    ses colonnes, y compris celles que les scripts n'utilisent pas.
  - les types : lus dans le fichier quand c'est un Parquet, donc sûrs.
    Sinon, suggérés d'après l'usage dans les scripts (Year(col) donne date,
    Sum(col) donne double, Date#(col) donne string…) ou d'après le nom. Sans
    aucun indice, on met string, le type que le kit suppose de toute façon.
    Chaque type porte en commentaire d'où il vient.

Usage :
    python tools/fill_conf.py script_qvd/            # complète conf/
    python tools/fill_conf.py script_qvd/ --draft    # brouillon dans conf/draft/
    python tools/fill_conf.py script_qvd/ --var entrees=data/input
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conf import (CONF, IO_FILE, IoConf, _load_yaml,  # noqa: E402
                  find_scripts, resolve_location)
from prepare import (INPUT_DIR, analyze_script, file_stem,  # noqa: E402
                     mask_strings, norm_path, strip_comments)
from qlik2sql import Untranslatable, split_statement, tokenize_expr  # noqa: E402

DRAFT = CONF / "draft"           # sortie de --draft

# Indices de type d'après la fonction qui reçoit la colonne en 1er argument.
# Date#() et Num#() lisent du texte : la colonne source est donc une chaîne.
HINTS = {
    "string": {"date#", "timestamp#", "num#", "left", "right", "mid", "len",
               "upper", "lower", "trim", "ltrim", "rtrim", "subfield",
               "keepchar", "purgechar", "index", "replace", "textbetween",
               "capitalize", "wildmatch", "text"},
    "timestamp": {"timestamp", "hour", "minute", "second"},
    "date": {"date", "year", "month", "day", "week", "weekyear", "weekday",
             "monthstart", "monthend", "yearstart", "yearend",
             "quarterstart", "quarterend", "weekstart", "weekend",
             "addmonths", "addyears", "inmonth", "inyear", "inquarter",
             "inweek", "inday", "monthname"},
    "double": {"sum", "avg", "rangesum", "round", "fabs", "sqrt", "exp",
               "log", "rangeavg"},
}
# en cas d'indices contradictoires, le plus spécifique gagne
PRIORITY = ("string", "timestamp", "date", "double")

ARROW_TYPES = (("large_string", "string"), ("string", "string"),
               ("int64", "bigint"), ("int", "int"), ("uint", "bigint"),
               ("double", "double"), ("float", "double"),
               ("decimal", "decimal"), ("date", "date"),
               ("timestamp", "timestamp"), ("bool", "boolean"))


def yaml_key(name: str) -> str:
    """Nom de colonne ou d'entrée utilisable comme clé YAML."""
    return name if re.fullmatch(r"[A-Za-z_][\w]*", name) else json.dumps(name)


def io_name(path: str) -> str:
    """Nom proposé pour une entrée ou une sortie : le nom du fichier Qlik,
    en minuscules, avec « _ » à la place des caractères spéciaux."""
    return re.sub(r"\W", "_", file_stem(path).lower(), flags=re.ASCII)


def columns_read(text: str) -> tuple[list[str], bool]:
    """Colonnes qu'une instruction LOAD lit dans sa source (le segment du bas
    d'un LOAD empilé). Le booléen dit s'il y a un LOAD *."""
    try:
        segs, _, _ = split_statement(text)
    except Untranslatable:
        return [], False
    if not segs:
        return [], False
    bottom = segs[-1]
    exprs = [e for e, _ in bottom.fields] + bottom.group_by
    if bottom.where:
        exprs.append(bottom.where)
    star = any(e.strip() == "*" for e, _ in bottom.fields)
    cols = []
    for e in exprs:
        if e.strip() == "*":
            continue
        try:
            toks = tokenize_expr(e)
        except Untranslatable:
            continue
        for k, (kind, val) in enumerate(toks):
            nxt = toks[k + 1] if k + 1 < len(toks) else None
            if kind == "field":
                cols.append(val[1:-1])
            elif kind == "word" and nxt != ("op", "("):
                cols.append(val)
    return list(dict.fromkeys(cols)), star


def type_hints(text: str) -> dict[str, Counter]:
    """Indices de type pour chaque colonne passée en 1er argument d'une
    fonction, ou utilisée dans une multiplication ou une division."""
    masked = mask_strings(text)
    hints: dict[str, Counter] = {}
    name = r"(\[[^\]]+\]|\"[^\"]+\"|[^\W\d][\w.]*)"
    for m in re.finditer(r"\b([A-Za-z]\w*#?)\s*\(\s*" + name + r"\s*[,)]",
                         masked):
        func, col = m.group(1).lower(), m.group(2).strip("[]\"")
        for t, funcs in HINTS.items():
            if func in funcs:
                hints.setdefault(col, Counter())[f"{t}:{m.group(1)}()"] += 1
    # lookahead / lookbehind pour attraper les deux côtés de « a * b »
    for m in re.finditer(name + r"(?=\s*[*/])|(?<=[*/])\s*" + name, masked):
        col = (m.group(1) or m.group(2)).strip("[]\"")
        if not re.fullmatch(r"\d+", col):
            hints.setdefault(col, Counter())["double:calcul"] += 1
    return hints


def suggest(col: str, counter: Counter | None) -> tuple[str | None, str]:
    """Type suggéré et l'explication à mettre en commentaire."""
    if not counter:
        # Dernier recours, le nom : date_cmd, dt_fin, DateCommande… Une
        # colonne de dates utilisée seulement dans Min() ou Max(), comme
        # dans un calendrier, n'a pas d'autre indice, et c'est justement là
        # que le type compte.
        if re.search(r"(^|[_ ])(date|dt)([_ ]|$)|^date", col, re.I):
            return "date", "suggéré d'après le nom, à vérifier"
        return "string", "type inconnu : string par défaut, à vérifier"
    by_type: dict[str, list[str]] = {}
    for key in counter:
        t, why = key.split(":", 1)
        by_type.setdefault(t, []).append(why)
    for t in PRIORITY:
        if t in by_type:
            others = [x for x in by_type if x != t]
            note = f"suggéré : {', '.join(sorted(set(by_type[t])))}"
            if others:
                note += f" (aussi vu en {', '.join(others)})"
            return t, note + ", à vérifier"
    return "string", "type inconnu : string par défaut, à vérifier"


def file_columns(location: str) -> tuple[list[str], dict[str, str], str | None]:
    """Colonnes et types d'un fichier de données présent sur le disque.
    Renvoie aussi son format (parquet ou csv), ou None si on ne l'a pas
    trouvé."""
    path = Path(location)
    candidates = [path] + [path.with_suffix(s) for s in (".parquet", ".csv")]
    for p in candidates:
        if not p.exists():
            continue
        parquet = [p] if p.suffix == ".parquet" and p.is_file() else \
            sorted(p.rglob("*.parquet")) if p.is_dir() else []
        if parquet:
            try:
                import pyarrow.parquet as pq
            except ImportError:
                return [], {}, "parquet"
            schema = pq.read_schema(parquet[0])
            types = {}
            for f in schema:
                arrow = str(f.type)
                types[f.name] = next((t for a, t in ARROW_TYPES
                                      if arrow.startswith(a)), "string")
            cols = [c for c in schema.names if c != "_qlik_order"]
            return cols, types, "parquet"
        csv = [p] if p.suffix == ".csv" and p.is_file() else \
            sorted(p.glob("*.csv")) if p.is_dir() else []
        if csv:
            # un CSV ne porte pas de types : on ne prend que l'en-tête
            head = csv[0].read_text(encoding="utf-8-sig",
                                    errors="replace").splitlines()[:1]
            sep = max((";", ",", "\t", "|"),
                      key=lambda s: head[0].count(s)) if head else ","
            cols = [c.strip().strip('"') for c in head[0].split(sep)] \
                if head else []
            return cols, {}, "csv"
    return [], {}, None


def var_prefix(conf: IoConf, kind: str, default: str) -> str:
    """Préfixe de location le plus courant dans io.yml (${entrees}/…), pour
    que le brouillon ressemble à l'existant."""
    pool = conf.inputs if kind == "input" else conf.outputs
    found = Counter(m.group(1) for e in pool
                    if (m := re.match(r"(\$\{\w+\})/", e.location)))
    return found.most_common(1)[0][0] if found else default


DRAFT_HEADER = ["# Brouillon écrit par tools/fill_conf.py --draft : à relire,",
                "# puis à recopier dans conf/io.yml. Ni le kit ni le socle",
                "# ne lisent ce fichier.", ""]


def schema_base(conf: IoConf) -> Path:
    """Dossier depuis lequel les schema_file de io.yml sont résolus : on le
    déduit d'un schéma existant, sinon conf/ (schemas/x.yml donne alors
    conf/schemas/x.yml)."""
    for e in conf.inputs:
        if e.schema_file:
            for b in (IO_FILE.parent, Path("."), CONF / "schema"):
                if (b / e.schema_file).exists():
                    return b
    return IO_FILE.parent


def entry_lines(name: str, props: dict, why: str) -> list[tuple[int, str]]:
    """Lignes d'une entrée, avec leur niveau (0 : la clé, 1 : ses
    propriétés)."""
    today = dt.date.today().isoformat()
    return [(0, f"{yaml_key(name)}:  # ajouté le {today} : {why}")] + \
        [(1, f"{k}: {v}") for k, v in props.items()]


def insert_entries(text: str, section: str,
                   entries: list[list[tuple[int, str]]]) -> str:
    """Ajoute des entrées à la fin d'une section de io.yml, en texte.

    On ne passe pas par un aller-retour YAML, qui perdrait les commentaires
    et la mise en forme. L'indentation est reprise des entrées déjà
    présentes dans la section (2 et 4 espaces sinon)."""
    if not entries:
        return text
    lines = text.splitlines()
    head = next((k for k, l in enumerate(lines)
                 if re.match(rf"{re.escape(section)}\s*:\s*(#.*)?$", l)), None)
    if head is None:
        while lines and not lines[-1].strip():
            lines.pop()
        lines += ([""] if lines else []) + [f"{section}:"]
        head, end = len(lines) - 1, len(lines)
    else:
        end = next((k for k in range(head + 1, len(lines))
                    if lines[k].strip() and not lines[k][0].isspace()
                    and not lines[k].lstrip().startswith("#")), len(lines))
        # Les lignes vides et les commentaires en début de ligne juste avant
        # la section suivante lui appartiennent : on insère au-dessus.
        while end - 1 > head and (not lines[end - 1].strip()
                                  or lines[end - 1].startswith("#")):
            end -= 1
    body = [l for l in lines[head + 1:end]
            if l.strip() and not l.lstrip().startswith("#")]
    if not body:
        # section nouvelle ou vide : on prend l'indentation du reste du
        # fichier, pour que toutes les sections se ressemblent
        body = [l for l in lines if l.strip() and l[0].isspace()
                and not l.lstrip().startswith("#")]
    ind = [len(l) - len(l.lstrip()) for l in body]
    lvl0 = ind[0] if ind else 2
    lvl1 = next((i for i in ind if i > lvl0), lvl0 + 2)
    new = [(" " * (lvl0 if lv == 0 else lvl1)) + t
           for e in entries for lv, t in e]
    return "\n".join(lines[:end] + new + lines[end:]) + "\n"


def write_io(blocks: dict) -> bool:
    """Ajoute les entrées dans conf/io.yml, avec copie de sauvegarde et
    retour arrière si le résultat n'est plus lisible."""
    import yaml
    original = IO_FILE.read_text(encoding="utf-8") if IO_FILE.exists() else ""
    text = original
    for section, entries in blocks.items():
        text = insert_entries(text, section, entries)
    try:
        data = yaml.safe_load(text) or {}
        added = [n for es in blocks.values() for e in es
                 for n in [e[0][1].split(":")[0].strip('"')]]
        flat = {str(k) for sec in data.values() if isinstance(sec, dict)
                for k in sec}
        if not set(added) <= flat:
            raise ValueError(f"entrées absentes après écriture : "
                             f"{sorted(set(added) - flat)}")
    except Exception as exc:
        print(f"⚠ conf/io.yml non modifié : le résultat n'était pas lisible "
              f"({exc}). Relancer avec --draft et recopier à la main.")
        return False
    IO_FILE.parent.mkdir(parents=True, exist_ok=True)
    if original:
        IO_FILE.with_suffix(".yml.bak").write_text(original, encoding="utf-8")
    IO_FILE.write_text(text, encoding="utf-8")
    return True


# Synonymes de types, pour reprendre le vocabulaire des schémas existants.
SYNONYMS = {"int": ("int", "integer"), "bigint": ("bigint", "long"),
            "double": ("double", "float"), "decimal": ("decimal", "numeric"),
            "string": ("string", "varchar", "text", "str"),
            "date": ("date",), "timestamp": ("timestamp", "datetime"),
            "boolean": ("boolean", "bool")}


def schema_style(conf: IoConf, base: Path) -> dict:
    """Forme des schémas existants, pour écrire les nouveaux de la même
    façon : clé englobante (columns:), indentation, et vocabulaire des types
    tel qu'il est écrit (STRING, INTEGER…). Sans schéma existant : la forme
    du socle, « columns: » puis « NomCol: TYPE » en majuscules."""
    style = {"wrap": "columns", "indent": 3, "types": {}}
    found = False
    for e in conf.inputs:
        p = base / e.schema_file if e.schema_file else None
        if not p or not p.exists():
            continue
        for t in e.types.values():
            if t:
                style["types"].setdefault(str(t).lower(), str(t))
        if not found:
            text = p.read_text(encoding="utf-8", errors="replace")
            m = re.search(r"^(\w+):[ \t]*(#.*)?\n(?:[ \t]*(?:#.*)?\n)*([ \t]+)\S",
                          text, re.M)
            if m and m.group(1) in ("columns", "fields", "schema"):
                style["wrap"], style["indent"] = m.group(1), len(m.group(3))
            else:
                style["wrap"], style["indent"] = None, 0
            found = True
    return style


def type_text(t: str, style: dict) -> str:
    """Écrit un type du kit (string, date…) dans le vocabulaire du projet."""
    for syn in SYNONYMS.get(t, (t,)):
        if syn in style["types"]:
            return style["types"][syn]
    return t.upper()


def schema_text(why: str, lines: list, style: dict) -> str:
    today = dt.date.today().isoformat()
    body = [f"# Créé par tools/fill_conf.py le {today} ({why}).",
            "# Les types suivis de « à vérifier » ont été devinés."]
    pad = " " * style["indent"] if style["wrap"] else ""
    if style["wrap"]:
        body.append(f"{style['wrap']}:")
    rows = [(pad + yaml_key(c) + ": " + type_text(t, style), note)
            for c, t, note in lines]
    width = max((len(r) for r, _ in rows), default=0) + 2
    body += [f"{r.ljust(width)}# {note}" for r, note in rows]
    return "\n".join(body) + "\n"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un script Qlik ou le dossier script_qvd/")
    p.add_argument("--var", action="append", default=[], metavar="NOM=VALEUR",
                   help="valeur d'une variable des locations, pour aller lire "
                        "les fichiers de données (ex. --var entrees=data/input)")
    p.add_argument("--draft", action="store_true",
                   help="ne rien modifier, écrire un brouillon dans conf/draft/")
    args = p.parse_args(argv)
    variables = dict(v.split("=", 1) for v in args.var if "=" in v)
    scripts = find_scripts(args.path)
    if not scripts:
        print(f"Aucun script Qlik dans {args.path} (.qvs, .txt ou sans "
              "extension)")
        return 1

    conf = IoConf()
    raw = _load_yaml(IO_FILE) if IO_FILE.exists() else {}
    sections = {k: next((s for s in raw if kind in str(s).lower()), default)
                for k, kind, default in (("input", "input", "input"),
                                         ("output", "output", "outputs"))}
    schema_dir = next((str(Path(e.schema_file).parent) for e in conf.inputs
                       if e.schema_file), "schemas")
    base = schema_base(conf)
    style = schema_style(conf, base)
    in_prefix = var_prefix(conf, "input", "${entrees}")
    out_prefix = var_prefix(conf, "output", "${sorties}")

    # Ce que les scripts lisent et écrivent, fusionné sur tout le dossier.
    reads: dict[str, dict] = {}      # nom -> path, colonnes, star, scripts
    writes: dict[str, dict] = {}     # nom -> path, colonnes, scripts
    hints: dict[str, Counter] = {}
    failed: list[str] = []
    for script in scripts:
        # Un script qu'on n'arrive pas à analyser ne doit pas empêcher de
        # compléter la conf pour les autres : on le signale et on continue.
        try:
            _, _, insts, _, _, schemas = analyze_script(script)
        except Exception as exc:
            failed.append(f"{script.name} : {type(exc).__name__}: {exc}")
            continue
        for i in insts:
            text = strip_comments(i.text)
            for col, c in type_hints(text).items():
                hints.setdefault(col, Counter()).update(c)
            if i.kind == "store" and i.stored:
                w = writes.setdefault(io_name(i.stored[1]), {
                    "path": i.stored[1], "columns": [], "scripts": set()})
                w["scripts"].add(script.name)
                cols = schemas.get(i.target) or []
                if "*" not in cols and "?" not in cols:
                    w["columns"] = list(dict.fromkeys(w["columns"] + cols))
            elif i.kind == "load" and i.sources:
                try:
                    cols, star = columns_read(text)
                except Exception as exc:
                    failed.append(f"{script.name} l.{i.stmt.first_line} : "
                                  f"{type(exc).__name__}: {exc}")
                    cols, star = [], False
                for src in i.sources[:1]:
                    r = reads.setdefault(io_name(src), {
                        "path": src, "columns": [], "star": False,
                        "scripts": set()})
                    r["columns"] = list(dict.fromkeys(r["columns"] + cols))
                    r["star"] |= star
                    r["scripts"].add(script.name)

    produced = {norm_path(w["path"]) for w in writes.values()} | set(writes)
    new_inputs, new_outputs, schemas_out, warnings = [], [], {}, []

    for name, w in sorted(writes.items()):
        if conf.find(w["path"], "output"):
            continue
        new_outputs.append((name, {
            "location": f"{out_prefix}/{file_stem(w['path'])}"},
            f"STORE dans {', '.join(sorted(w['scripts']))}"))

    for name, r in sorted(reads.items()):
        entry = conf.find(r["path"], "input")
        is_produced = norm_path(r["path"]) in produced or name in writes
        if entry and entry.columns:
            missing = [c for c in r["columns"] if c not in entry.columns]
            if missing:
                warnings.append(f"{entry.name} : colonnes lues par les scripts "
                                f"mais absentes de {entry.schema_file} : "
                                f"{', '.join(missing)}")
            continue
        if entry:
            location = resolve_location(entry.location, variables)
        else:
            out = conf.find(r["path"], "output")
            location = out.location if out else (
                f"{out_prefix}/{file_stem(r['path'])}" if is_produced
                else f"{in_prefix}/{file_stem(r['path'])}")
        # le fichier de données, s'il est là, donne les vraies colonnes
        found = [resolve_location(location, variables),
                 f"{INPUT_DIR}/{file_stem(r['path'])}"]
        cols, types, fmt, origin = [], {}, None, None
        for loc in found:
            try:
                cols, types, fmt = file_columns(loc)
            except Exception as exc:          # fichier illisible
                warnings.append(f"{name} : {loc} illisible ({exc})")
                cols, types, fmt = [], {}, None
            if fmt:
                origin = loc
                break
        from_file = bool(cols)
        absent = [c for c in r["columns"] if from_file and c not in cols]
        if absent:
            warnings.append(f"{name} : colonnes lues par les scripts mais "
                            f"absentes de {origin} : {', '.join(absent)}")
        if not cols:
            # Fichier produit par un autre script : le STORE donne toutes ses
            # colonnes, dans l'ordre. Le lecteur n'en utilise souvent qu'une
            # partie, et un LOAD * qui suivrait en perdrait.
            written = (writes.get(name) or {}).get("columns", [])
            cols = list(dict.fromkeys(written + r["columns"]))
        lines = []
        for c in cols:
            if c in types:
                t, note = types[c], f"lu dans {origin}"
            else:
                t, note = suggest(c, hints.get(c))
            lines.append((c, t, note))
        schema_file = f"{schema_dir}/{name}.yml"
        why = (f"lu par {', '.join(sorted(r['scripts']))}"
               + (", produit par un autre script" if is_produced else ""))
        if r["star"] and not from_file:
            why += " ; LOAD * : colonnes incomplètes sans le fichier"
        if not lines:
            # Aucune colonne connue (LOAD * sans le fichier) : un schéma vide
            # risquerait de faire planter la lecture par le socle. On déclare
            # l'entrée sans schema_file et on le signale.
            warnings.append(f"{name} : colonnes inconnues ({why}), pas de "
                            "schéma créé. Déposer le fichier dans data/input/ "
                            "et relancer, ou écrire le schéma à la main.")
            if not entry:
                new_inputs.append((name, {"format": fmt or "parquet",
                                          "location": location},
                                   why + " ; pas de schéma, colonnes inconnues"))
            continue
        schemas_out[schema_file] = (why, lines)
        if entry and entry.schema_file:
            # schema_file déclaré mais fichier absent : on le crée là où il
            # est attendu, sans toucher à l'entrée
            schemas_out[entry.schema_file] = schemas_out.pop(schema_file)
            continue
        if entry:
            warnings.append(f"{entry.name} : pas de schema_file dans io.yml, "
                            f"à ajouter à la main ({schema_file} est créé)")
            continue
        props = {"format": fmt or "parquet", "schema_file": schema_file,
                 "location": location}
        new_inputs.append((name, props, why if fmt else
                           why + " ; format à vérifier"))

    blocks = {sections["input"]: [entry_lines(n, pr, w) for n, pr, w in new_inputs],
              sections["output"]: [entry_lines(n, pr, w)
                                   for n, pr, w in new_outputs]}
    if args.draft:
        DRAFT.mkdir(parents=True, exist_ok=True)
        text = "\n".join(DRAFT_HEADER) + "\n"
        for section, entries in blocks.items():
            text = insert_entries(text, section, entries)
        (DRAFT / "io.yml").write_text(text, encoding="utf-8")
        schema_root, where = DRAFT, f"{DRAFT}/"
    else:
        if new_inputs or new_outputs:
            if not write_io(blocks):
                return 1
        schema_root, where = base, "conf/"
    created = []
    for schema_file, (why, lines) in schemas_out.items():
        target = schema_root / schema_file
        if target.exists() and not args.draft:
            continue                 # jamais d'écrasement d'un vrai schéma
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(schema_text(why, lines, style), encoding="utf-8")
        created.append(target)

    verb = "proposé(s) dans" if args.draft else "ajouté(s) dans"
    print(f"conf : {len(new_inputs)} entrée(s), {len(new_outputs)} sortie(s) "
          f"et {len(created)} schéma(s) {verb} {where} "
          f"({(DRAFT if args.draft else IO_FILE).resolve()})")
    for name, _, _ in new_inputs + new_outputs:
        print(f"  + {name}")
    for target in created:
        print(f"  + {target}")
    guessed = sum(1 for _, lines in schemas_out.values()
                  for _, _, n in lines if n.startswith("suggéré"))
    unknown = sum(1 for _, lines in schemas_out.values()
                  for _, _, n in lines if n.startswith("type inconnu"))
    if guessed or unknown:
        print(f"Types à vérifier : {guessed} suggéré(s) d'après les scripts, "
              f"{unknown} inconnu(s) mis en string (repérables à leur "
              "commentaire dans les schémas).")
    for w in warnings:
        print(f"⚠ {w}")
    for f in failed:
        print(f"⚠ non analysé, entrées et sorties à vérifier à la main : {f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
