"""Complete conf/io.yml et les schemas a partir des scripts Qlik.

migrate.py le lance avant de traduire. La regle : on ajoute ce qui manque,
on ne modifie jamais ce qui existe.
  - une entree ou une sortie absente de conf/io.yml est ajoutee a la fin de
    sa section, en texte, pour garder les commentaires et la mise en forme
    du fichier
  - un schema absent est cree ; un schema existant n'est jamais retouche,
    meme s'il lui manque des colonnes (on le signale seulement : il sert au
    socle pour lire le fichier, une colonne en trop pourrait le casser)
  - avant de modifier io.yml, on en garde une copie (io.yml.bak), et si le
    resultat n'est plus du YAML lisible, on remet l'original
Avec --draft, rien n'est touche : tout est ecrit dans conf/draft/.

D'ou viennent les informations :
  - les entrees et les sorties : les FROM et les STORE des scripts, plus
    les tables du modele de donnees (encore la a la fin du script, sans
    STORE). Un fichier ecrit par un script et relu par un autre est ajoute
    dans les deux sections, avec la meme location. Le # des noms de fichiers
    est deja devenu un _ (voir normalize.py).
  - le format d'une entree : celui que Qlik lit (txt -> csv, qvd -> parquet)
  - les colonnes : celles que les scripts lisent dans chaque fichier. Si le
    fichier est present dans data/input/ (ou a sa location), on prend toutes
    ses colonnes, y compris celles que les scripts n'utilisent pas.
  - les types : lus dans les metadonnees du Parquet ; pour un fichier
    ecrit par un autre script, ceux de la requete Spark qui l'ecrit ;
    sinon deduits des valeurs du CSV. Sans fichier, devines d'apres le
    script (Year(col) donne date, x > 0 donne double, Trim(col) donne
    string...), le nom, ou le champ du meme nom dans une autre table. Sans
    aucun indice, on met string, le type que le kit suppose de toute facon.
    Chaque type porte en commentaire d'ou il vient.

Usage :
    python tools/fill_conf.py script_qvd/            # complete conf/
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
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conf import (
    CONF,
    IO_FILE,
    WORK,
    IoConf,
    _load_yaml,
    find_scripts,
    io_key,  # noqa: E402
    resolve_location,
)
from prepare import (
    INPUT_DIR,
    analyze_script,
    file_stem,  # noqa: E402
    mask_strings,
    norm_path,
    source_format,
    strip_comments,
)
from qlik2sql import Untranslatable, split_statement, tokenize_expr  # noqa: E402

DRAFT = CONF / "draft"  # sortie de --draft

# Indices de type d'apres la fonction qui recoit la colonne en 1er argument.
# Date#() et Num#() lisent du texte : la colonne source est donc une chaine.
HINTS = {
    "string": {
        "date#",
        "timestamp#",
        "num#",
        "left",
        "right",
        "mid",
        "len",
        "upper",
        "lower",
        "trim",
        "ltrim",
        "rtrim",
        "subfield",
        "keepchar",
        "purgechar",
        "index",
        "replace",
        "textbetween",
        "capitalize",
        "wildmatch",
        "text",
    },
    "timestamp": {"timestamp", "hour", "minute", "second"},
    "date": {
        "date",
        "year",
        "month",
        "day",
        "week",
        "weekyear",
        "weekday",
        "monthstart",
        "monthend",
        "yearstart",
        "yearend",
        "quarterstart",
        "quarterend",
        "weekstart",
        "weekend",
        "addmonths",
        "addyears",
        "inmonth",
        "inyear",
        "inquarter",
        "inweek",
        "inday",
        "monthname",
    },
    "double": {
        "sum",
        "avg",
        "rangesum",
        "round",
        "fabs",
        "sqrt",
        "exp",
        "log",
        "rangeavg",
    },
}
# en cas d'indices contradictoires, le plus specifique gagne
PRIORITY = ("string", "timestamp", "date", "double")

ARROW_TYPES = (
    ("large_string", "string"),
    ("string", "string"),
    ("int64", "bigint"),
    ("int", "int"),
    ("uint", "bigint"),
    ("double", "double"),
    ("float", "double"),
    ("decimal", "decimal"),
    ("date", "date"),
    ("timestamp", "timestamp"),
    ("bool", "boolean"),
)


def yaml_key(name: str) -> str:
    """Nom de colonne ou d'entree utilisable comme cle YAML."""
    return name if re.fullmatch(r"[A-Za-z_][\w]*", name) else json.dumps(name)


def io_name(path: str) -> str:
    """Nom propose pour une entree ou une sortie : le nom du fichier Qlik,
    en minuscules, avec "_" a la place des caracteres speciaux. C'est la
    meme cle que celle de IoConf.find()."""
    return re.sub(r"[\W_]+", "_", file_stem(path).lower(), flags=re.ASCII).strip("_")


def source_file(path: str, spec: str | None) -> str:
    """Nom de fichier d'une entree dans sa location : un CSV garde son nom
    complet, extension comprise ; un QVD, suppose converti en Parquet, perd
    son extension."""
    if source_format(path, spec)[0] == "csv":
        return re.split(r"[\\/]", path)[-1]
    return file_stem(path)


def csv_options_note(options: dict[str, str]) -> str | None:
    """Options de lecture CSV vues dans le script Qlik, en commentaire : on
    ne connait pas encore la cle du socle pour les passer (hypothese a
    confirmer), et une cle inconnue pourrait faire refuser io.yml."""
    if not options:
        return None
    shown = {
        "sep": "separateur",
        "header": "en-tete",
        "encoding": "encodage",
        "quote": "guillemets",
    }
    parts = [
        f"{shown[k]} {json.dumps(v, ensure_ascii=False)}" for k, v in options.items()
    ]
    return (
        "options Qlik : " + ", ".join(parts) + " (a passer au socle selon sa syntaxe)"
    )


def columns_read(text: str) -> tuple[list[str], bool]:
    """Colonnes qu'une instruction LOAD lit dans sa source (le segment du bas
    d'un LOAD empile). Le booleen dit s'il y a un LOAD *."""
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
    """Indices de type pour chaque colonne passee en 1er argument d'une
    fonction, ou utilisee dans une multiplication ou une division."""
    masked = mask_strings(text)
    hints: dict[str, Counter] = {}
    name = r"(\[[^\]]+\]|\"[^\"]+\"|[^\W\d][\w.]*)"
    for m in re.finditer(r"\b([A-Za-z]\w*#?)\s*\(\s*" + name + r"\s*[,)]", masked):
        func, col = m.group(1).lower(), m.group(2).strip('[]"')
        for t, funcs in HINTS.items():
            if func in funcs:
                hints.setdefault(col, Counter())[f"{t}:{m.group(1)}()"] += 1
    # Comparaison a une constante : x = 'abc' (texte), x > 0 (nombre),
    # x >= '01/01/2024' (date). Dans les deux sens.
    lit = r"('[^']*'|-?\d+(?:\.\d+)?)"
    op = r"\s*(?:=|<>|<=|>=|<|>)\s*"
    for m in re.finditer(
        name + op + lit + "|" + lit + op + name + r"(?!\s*\(|[\w.])", masked
    ):
        col = (m.group(1) or m.group(4)).strip('[]"')
        value = text[m.start(2 if m.group(1) else 3) : m.end(2 if m.group(1) else 3)]
        if re.fullmatch(r"\d+", col):
            continue
        kind = (
            "date:compare a une date"
            if re.fullmatch(r"'\d{1,4}[/.-]\d{1,2}[/.-]\d{1,4}'", value)
            else "string:compare a un texte"
            if value.startswith("'")
            else "double:compare a un nombre"
        )
        hints.setdefault(col, Counter())[kind] += 1
    # lookahead / lookbehind pour attraper les deux cotes de "a * b". Un
    # nom suivi de "(" est une fonction (x / Count(y)), pas une colonne.
    for m in re.finditer(
        name + r"(?=\s*[*/])|(?<=[*/])\s*" + name + r"(?!\s*\(|[\w.])", masked
    ):
        col = (m.group(1) or m.group(2)).strip('[]"')
        if not re.fullmatch(r"\d+", col):
            hints.setdefault(col, Counter())["double:calcul"] += 1
    return hints


def suggest(col: str, counter: Counter | None) -> tuple[str | None, str]:
    """Type suggere et l'explication a mettre en commentaire."""
    if not counter:
        # Dernier recours, le nom : date_cmd, dt_fin, DateCommande... Une
        # colonne de dates utilisee seulement dans Min() ou Max(), comme
        # dans un calendrier, n'a pas d'autre indice, et c'est justement la
        # que le type compte.
        if re.search(r"(^|[_ ])(date|dt)([_ ]|$)|^date", col, re.I):
            return "date", "devine d'apres le nom de la colonne, a verifier"
        return "string", "type inconnu : string par defaut, a verifier"
    by_type: dict[str, list[str]] = {}
    for key in counter:
        t, why = key.split(":", 1)
        by_type.setdefault(t, []).append(why)
    for t in PRIORITY:
        if t in by_type:
            others = [x for x in by_type if x != t]
            note = (
                "devine d'apres l'usage dans le script : "
                f"{', '.join(sorted(set(by_type[t])))}"
            )
            if others:
                note += f" (aussi vu en {', '.join(others)})"
            return t, note + ", a verifier"
    return "string", "type inconnu : string par defaut, a verifier"


def file_columns(location: str) -> tuple[list[str], dict[str, str], str | None, str]:
    """Colonnes et types d'un fichier de donnees present sur le disque, son
    format (parquet ou csv, None si on ne l'a pas trouve) et d'ou viennent
    les types, pour le commentaire du schema."""
    path = Path(location)
    candidates = [path] + [path.with_name(path.name + s) for s in (".parquet", ".csv")]
    for p in candidates:
        if not p.exists():
            continue
        parquet = (
            [p]
            if p.suffix == ".parquet" and p.is_file()
            else sorted(p.rglob("*.parquet"))
            if p.is_dir()
            else []
        )
        if parquet:
            try:
                import pyarrow.parquet as pq
            except ImportError:
                return [], {}, "parquet", ""
            schema = pq.read_schema(parquet[0])
            types = {}
            for f in schema:
                arrow = str(f.type)
                types[f.name] = next(
                    (t for a, t in ARROW_TYPES if arrow.startswith(a)), "string"
                )
            cols = [c for c in schema.names if c != "_qlik_order"]
            return cols, types, "parquet", f"lu dans le Parquet {parquet[0]}"
        csv_files = (
            [p]
            if p.suffix.lower() in (".csv", ".txt", ".tsv") and p.is_file()
            else sorted(p.glob("*.csv"))
            if p.is_dir()
            else []
        )
        if csv_files:
            cols, types, n = csv_types(csv_files[0])
            return (
                cols,
                types,
                "csv",
                (f"deduit des {n} premiere(s) ligne(s) de {csv_files[0]}"),
            )
    return [], {}, None, ""


CSV_SAMPLE = 5000  # lignes lues pour deviner les types d'un CSV


def csv_types(path: Path) -> tuple[list[str], dict[str, str], int]:
    """En-tete d'un CSV et types deduits de ses valeurs (sur place : rien
    n'en sort du poste). On ne retient que des types surs a la lecture :
    entier, decimal a point. Une date en texte (29/12/2024) ou un decimal a
    virgule restent en string, car Spark ne les lirait pas sans options ; le
    kit lit deja comme une date un texte au DateFormat du script."""
    import csv as csv_mod

    with path.open(encoding="utf-8-sig", errors="replace", newline="") as f:
        head = f.readline()
        sep = max((";", ",", "\t", "|"), key=head.count) if head else ","
        f.seek(0)
        rows = csv_mod.reader(f, delimiter=sep)
        cols = [c.strip() for c in next(rows, [])]
        seen: dict[int, set] = {k: set() for k in range(len(cols))}
        n = 0
        for row in rows:
            if n >= CSV_SAMPLE:
                break
            n += 1
            for k, v in enumerate(row[: len(cols)]):
                v = v.strip()
                if v:
                    seen[k].add(
                        "bigint"
                        if re.fullmatch(r"-?\d{1,18}", v)
                        else "double"
                        if re.fullmatch(r"-?\d*\.\d+([eE][-+]?\d+)?", v)
                        else "string"
                    )
    types = {}
    for k, c in enumerate(cols):
        kinds = seen[k]
        if kinds and kinds <= {"bigint"}:
            types[c] = "bigint"
        elif kinds and kinds <= {"bigint", "double"}:
            types[c] = "double"
        elif kinds:
            types[c] = "string"
    return cols, types, n


def var_prefix(conf: IoConf, kind: str, default: str) -> str:
    """Prefixe de location le plus courant dans io.yml (${entrees}/...), pour
    que le brouillon ressemble a l'existant."""
    pool = conf.inputs if kind == "input" else conf.outputs
    found = Counter(
        m.group(1) for e in pool if (m := re.match(r"(\$\{\w+\})/", e.location))
    )
    return found.most_common(1)[0][0] if found else default


DRAFT_HEADER = [
    "# Brouillon ecrit par tools/fill_conf.py --draft : a relire,",
    "# puis a recopier dans conf/io.yml. Ni le kit ni le socle",
    "# ne lisent ce fichier.",
    "",
]


def schema_base(conf: IoConf) -> Path:
    """Dossier depuis lequel les schema_file de io.yml sont resolus : on le
    deduit d'un schema existant, sinon conf/ (schemas/x.yml donne alors
    conf/schemas/x.yml)."""
    for e in conf.inputs:
        if e.schema_file:
            for b in (IO_FILE.parent, Path("."), CONF / "schema"):
                if (b / e.schema_file).exists():
                    return b
    return IO_FILE.parent


def yaml_value(value: str) -> str:
    """Valeur YAML : entre guillemets si elle contient un "#" (sinon pris
    pour un commentaire apres une espace) ou un ":"."""
    return (
        json.dumps(value, ensure_ascii=False)
        if re.search(r"#|: |^[\s'\"]|\s$", value)
        else value
    )


def entry_lines(name: str, props: dict, why: str, comments=()) -> list[tuple[int, str]]:
    """Lignes d'une entree, avec leur niveau (0 : la cle, 1 : ses
    proprietes et les commentaires)."""
    today = dt.date.today().isoformat()
    return (
        [(0, f"{yaml_key(name)}:  # ajoute le {today} : {why}")]
        + [(1, f"{k}: {yaml_value(str(v))}") for k, v in props.items()]
        + [(1, f"# {c}") for c in comments if c]
    )


def insert_entries(
    text: str, section: str, entries: list[list[tuple[int, str]]]
) -> str:
    """Ajoute des entrees a la fin d'une section de io.yml, en texte.

    On ne passe pas par un aller-retour YAML, qui perdrait les commentaires
    et la mise en forme. L'indentation est reprise des entrees deja
    presentes dans la section (2 et 4 espaces sinon)."""
    if not entries:
        return text
    lines = text.splitlines()
    head = next(
        (
            k
            for k, line in enumerate(lines)
            if re.match(rf"{re.escape(section)}\s*:\s*(#.*)?$", line)
        ),
        None,
    )
    if head is None:
        while lines and not lines[-1].strip():
            lines.pop()
        lines += ([""] if lines else []) + [f"{section}:"]
        head, end = len(lines) - 1, len(lines)
    else:
        end = next(
            (
                k
                for k in range(head + 1, len(lines))
                if lines[k].strip()
                and not lines[k][0].isspace()
                and not lines[k].lstrip().startswith("#")
            ),
            len(lines),
        )
        # Les lignes vides et les commentaires en debut de ligne juste avant
        # la section suivante lui appartiennent : on insere au-dessus.
        while end - 1 > head and (
            not lines[end - 1].strip() or lines[end - 1].startswith("#")
        ):
            end -= 1
    body = [
        line
        for line in lines[head + 1 : end]
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not body:
        # section nouvelle ou vide : on prend l'indentation du reste du
        # fichier, pour que toutes les sections se ressemblent
        body = [
            line
            for line in lines
            if line.strip() and line[0].isspace() and not line.lstrip().startswith("#")
        ]
    ind = [len(line) - len(line.lstrip()) for line in body]
    lvl0 = ind[0] if ind else 2
    lvl1 = next((i for i in ind if i > lvl0), lvl0 + 2)
    new = [(" " * (lvl0 if lv == 0 else lvl1)) + t for e in entries for lv, t in e]
    return "\n".join(lines[:end] + new + lines[end:]) + "\n"


def write_io(blocks: dict) -> bool:
    """Ajoute les entrees dans conf/io.yml, avec copie de sauvegarde et
    retour arriere si le resultat n'est plus lisible."""
    import yaml

    original = IO_FILE.read_text(encoding="utf-8") if IO_FILE.exists() else ""
    text = original
    for section, entries in blocks.items():
        text = insert_entries(text, section, entries)
    try:
        data = yaml.safe_load(text) or {}
        added = [
            n
            for es in blocks.values()
            for e in es
            for n in [e[0][1].split(":")[0].strip('"')]
        ]
        flat = {str(k) for sec in data.values() if isinstance(sec, dict) for k in sec}
        if not set(added) <= flat:
            raise ValueError(
                f"entrees absentes apres ecriture : {sorted(set(added) - flat)}"
            )
    except Exception as exc:
        print(
            f"[!] conf/io.yml non modifie : le resultat n'etait pas lisible "
            f"({exc}). Relancer avec --draft et recopier a la main."
        )
        return False
    IO_FILE.parent.mkdir(parents=True, exist_ok=True)
    if original:
        IO_FILE.with_suffix(".yml.bak").write_text(original, encoding="utf-8")
    IO_FILE.write_text(text, encoding="utf-8")
    return True


# Synonymes de types, pour reprendre le vocabulaire des schemas existants.
SYNONYMS = {
    "int": ("int", "integer"),
    "bigint": ("bigint", "long"),
    "double": ("double", "float"),
    "decimal": ("decimal", "numeric"),
    "string": ("string", "varchar", "text", "str"),
    "date": ("date",),
    "timestamp": ("timestamp", "datetime"),
    "boolean": ("boolean", "bool"),
}


def schema_style(conf: IoConf, base: Path) -> dict:
    """Forme des schemas existants, pour ecrire les nouveaux de la meme
    facon : cle englobante (columns:), indentation, et vocabulaire des types
    tel qu'il est ecrit (STRING, INTEGER...). Sans schema existant : la forme
    du socle, "columns:" puis "NomCol: TYPE" en majuscules."""
    style: dict[str, Any] = {"wrap": "columns", "indent": 3, "types": {}}
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
            m = re.search(
                r"^(\w+):[ \t]*(#.*)?\n(?:[ \t]*(?:#.*)?\n)*([ \t]+)\S", text, re.M
            )
            if m and m.group(1) in ("columns", "fields", "schema"):
                style["wrap"], style["indent"] = m.group(1), len(m.group(3))
            else:
                style["wrap"], style["indent"] = None, 0
            found = True
    return style


def type_text(t: str, style: dict) -> str:
    """Ecrit un type du kit (string, date...) dans le vocabulaire du projet."""
    for syn in SYNONYMS.get(t, (t,)):
        if syn in style["types"]:
            return style["types"][syn]
    return t.upper()


def schema_text(why: str, lines: list, style: dict) -> str:
    today = dt.date.today().isoformat()
    body = [
        f"# Cree par tools/fill_conf.py le {today} ({why}).",
        "# Les types suivis de 'a verifier' ont ete devines.",
    ]
    pad = " " * style["indent"] if style["wrap"] else ""
    if style["wrap"]:
        body.append(f"{style['wrap']}:")
    rows = [
        (pad + yaml_key(c) + ": " + type_text(t, style), note) for c, t, note in lines
    ]
    width = max((len(r) for r, _ in rows), default=0) + 2
    body += [f"{r.ljust(width)}# {note}" for r, note in rows]
    return "\n".join(body) + "\n"


def propagate_types(schemas_out: dict, conf: IoConf) -> None:
    """Dans Qlik, un meme nom de champ designe la meme donnee dans toutes
    les tables (c'est ce qui les relie). Une colonne dont le type n'est que
    devine prend donc celui du champ du meme nom dont le type est connu
    ailleurs : lu dans un Parquet, calcule par Spark, ou deja declare dans
    un schema existant."""
    known: dict[str, tuple[str, str]] = {}
    for e in conf.inputs + conf.outputs:
        for c, typ in (e.types or {}).items():
            if typ:
                known.setdefault(c, (str(typ).lower(), f"schema de {e.name}"))
    for schema_file, (_, lines) in schemas_out.items():
        for c, typ, note in lines:
            if not note.endswith("a verifier") and not note.startswith("type inconnu"):
                known[c] = (typ, Path(schema_file).stem)
    for schema_file, (_, lines) in schemas_out.items():
        for k, (c, _typ, note) in enumerate(lines):
            guessed = note.endswith("a verifier") or note.startswith("type inconnu")
            if guessed and c in known and known[c][1] != Path(schema_file).stem:
                lines[k] = (
                    c,
                    known[c][0],
                    f"meme champ que dans {known[c][1]} (Qlik relie "
                    "les champs du meme nom), a verifier",
                )


def writer_types(path: str, writer: str) -> tuple[dict[str, str], bool] | None:
    """Types des colonnes d'un fichier ecrit par un autre script, tels que
    Spark les calcule pour la requete qui l'ecrit (check.py --parse les
    garde dans le manifest). Ce sont les types exacts du Parquet que le job
    ecrira. None si ce script n'a pas encore ete traduit et controle."""
    mpath = WORK / Path(writer).stem / "manifest.json"
    if not mpath.exists():
        return None
    for sec in json.loads(mpath.read_text(encoding="utf-8"))["sections"]:
        if not sec.get("output_types"):
            continue
        for table, location in sec.get("outputs", {}).items():
            io_name_ = sec.get("output_io", {}).get(table) or ""
            names = {io_key(location), io_key(io_name_)}
            if io_key(path) in names:
                return sec["output_types"], sec.get("output_types_sure", False)
    return None


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("path", type=Path, help="un script Qlik ou le dossier script_qvd/")
    p.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="NOM=VALEUR",
        help="valeur d'une variable des locations, pour aller lire "
        "les fichiers de donnees (ex. --var entrees=data/input)",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="n'afficher que ce qui est ajoute (migrate.py le "
        "relance avant chaque script)",
    )
    p.add_argument(
        "--draft",
        action="store_true",
        help="ne rien modifier, ecrire un brouillon dans conf/draft/",
    )
    args = p.parse_args(argv)
    variables = dict(v.split("=", 1) for v in args.var if "=" in v)
    scripts = find_scripts(args.path)
    if not scripts:
        print(f"Aucun script Qlik dans {args.path} (.qvs, .txt ou sans extension)")
        return 1

    conf = IoConf()
    raw = _load_yaml(IO_FILE) if IO_FILE.exists() else {}
    sections = {
        k: next((s for s in raw if kind in str(s).lower()), default)
        for k, kind, default in (
            ("input", "input", "inputs"),
            ("output", "output", "outputs"),
        )
    }
    schema_dir = next(
        (str(Path(e.schema_file).parent) for e in conf.inputs if e.schema_file),
        "schemas",
    )
    base = schema_base(conf)
    style = schema_style(conf, base)
    in_prefix = var_prefix(conf, "input", "${entrees}")
    out_prefix = var_prefix(conf, "output", "${sorties}")

    # Ce que les scripts lisent et ecrivent, fusionne sur tout le dossier.
    reads: dict[str, dict] = {}  # nom -> path, colonnes, star, scripts
    writes: dict[str, dict] = {}  # nom -> path, colonnes, scripts
    hints: dict[str, Counter] = {}
    failed: list[str] = []
    for script in scripts:
        # Un script qu'on n'arrive pas a analyser ne doit pas empecher de
        # completer la conf pour les autres : on le signale et on continue.
        try:
            _, _, insts, _, tables, schemas = analyze_script(script)
        except Exception as exc:
            failed.append(f"{script.name} : {type(exc).__name__}: {exc}")
            continue
        for i in insts:
            text = strip_comments(i.text)
            for col, counter in type_hints(text).items():
                hints.setdefault(col, Counter()).update(counter)
            if i.kind == "store" and i.stored:
                w = writes.setdefault(
                    io_name(i.stored[1]),
                    {"path": i.stored[1], "columns": [], "scripts": set()},
                )
                w["scripts"].add(script.name)
                stored_cols = schemas.get(i.target) or []
                if "*" not in stored_cols and "?" not in stored_cols:
                    w["columns"] = list(dict.fromkeys(w["columns"] + stored_cols))
            elif i.kind == "load" and i.sources:
                try:
                    cols, star = columns_read(text)
                except Exception as exc:
                    failed.append(
                        f"{script.name} l.{i.stmt.first_line} : "
                        f"{type(exc).__name__}: {exc}"
                    )
                    cols, star = [], False
                for src in i.sources[:1]:
                    r = reads.setdefault(
                        io_name(src),
                        {
                            "path": src,
                            "columns": [],
                            "star": False,
                            "scripts": set(),
                            "spec": i.source_specs.get(src),
                        },
                    )
                    r["columns"] = list(dict.fromkeys(r["columns"] + cols))
                    r["star"] |= star
                    r["scripts"].add(script.name)

        # Tables du modele de donnees (encore la a la fin, sans STORE) :
        # prepare.py les ecrit en sortie <script>_<table>.
        for key, t in tables.items():
            for _, path, store_fmt, _ in t["stored"]:
                if store_fmt == "modele":
                    cols = schemas.get(key) or []
                    writes.setdefault(
                        io_name(path),
                        {
                            "path": path,
                            "scripts": {script.name},
                            "model": t["name"],
                            "columns": [c for c in cols if c not in "*?"],
                        },
                    )

    produced = {norm_path(w["path"]) for w in writes.values()} | set(writes)
    new_inputs, new_outputs, schemas_out, warnings = [], [], {}, []
    waiting: list[str] = []  # fichiers dont l'auteur n'est pas traduit
    unknown_vars: set[str] = set()

    for name, w in sorted(writes.items()):
        if conf.find(w["path"], "output"):
            continue
        new_outputs.append(
            (
                name,
                {"location": f"{out_prefix}/{file_stem(w['path'])}"},
                (
                    f"table {w['model']} du modele de donnees de "
                    f"{', '.join(sorted(w['scripts']))} (pas de STORE dans Qlik)"
                    if w.get("model")
                    else f"STORE dans {', '.join(sorted(w['scripts']))}"
                ),
            )
        )

    for name, r in sorted(reads.items()):
        entry = conf.find(r["path"], "input")
        # Produit par un STORE : meme chemin, ou meme nom ecrit par un AUTRE
        # script. Un script qui lit Sources/produits.qvd et ecrit
        # QVD/produits.qvd lit bien la source, pas sa propre sortie.
        is_produced = norm_path(r["path"]) in produced or bool(
            name in writes and writes[name]["scripts"] - r["scripts"]
        )
        # Fichier ecrit par un autre script : ses types exacts sont ceux de
        # la requete Spark qui l'ecrit. migrate.py traite ce script avant, et
        # relance fill_conf.py avant chaque script : on attend donc qu'il
        # soit traduit plutot que de figer un schema devine.
        written_by = sorted(
            (writes.get(name) or {}).get("scripts", set()) - r["scripts"]
        )
        exact = (
            next(filter(None, (writer_types(r["path"], w) for w in written_by)), None)
            if is_produced and not entry
            else None
        )
        if (
            is_produced
            and not entry
            and written_by
            and exact is None
            and not args.draft
        ):
            waiting.append(
                f"{name} (lu par {', '.join(sorted(r['scripts']))}"
                f", ecrit par {', '.join(written_by)})"
            )
            continue
        if entry and entry.columns:
            missing = [c for c in r["columns"] if c not in entry.columns]
            if missing:
                warnings.append(
                    f"{entry.name} : colonnes lues par les scripts "
                    f"mais absentes de {entry.schema_file} : "
                    f"{', '.join(missing)}"
                )
            continue
        if entry:
            location = resolve_location(entry.location, variables)
        else:
            out = conf.find(r["path"], "output")
            location = (
                out.location
                if out
                else (
                    f"{out_prefix}/{file_stem(r['path'])}"
                    if is_produced
                    else f"{in_prefix}/{source_file(r['path'], r['spec'])}"
                )
            )
        # le fichier de donnees, s'il est la, donne les vraies colonnes
        found = list(
            dict.fromkeys(
                [
                    resolve_location(location, variables),
                    f"{INPUT_DIR}/{source_file(r['path'], r['spec'])}",
                    f"{INPUT_DIR}/{file_stem(r['path'])}",
                ]
            )
        )
        cols = []
        types: dict[str, str] = {}
        fmt = None
        origin, how = None, ""
        for loc in found:
            try:
                cols, types, fmt, how = file_columns(loc)
            except Exception as exc:  # fichier illisible
                warnings.append(f"{name} : {loc} illisible ({exc})")
                cols, types, fmt, how = [], {}, None, ""
            if fmt:
                origin = loc
                break
        if not fmt and "${" in found[0]:
            # Cause la plus frequente de types devines : le Parquet existe,
            # mais sa location contient une variable qu'on ne connait pas.
            unknown_vars.update(re.findall(r"\$\{(\w+)\}", found[0])[:1])
        from_file = bool(cols)
        absent = [c for c in r["columns"] if from_file and c not in cols]
        if absent:
            warnings.append(
                f"{name} : colonnes lues par les scripts mais "
                f"absentes de {origin} : {', '.join(absent)}"
            )
        if not cols:
            # Fichier produit par un autre script : le STORE donne toutes ses
            # colonnes, dans l'ordre. Le lecteur n'en utilise souvent qu'une
            # partie, et un LOAD * qui suivrait en perdrait.
            written = (writes.get(name) or {}).get("columns", [])
            cols = list(dict.fromkeys(written + r["columns"]))
        if exact and not fmt:
            out_types, sure = exact
            cols = list(dict.fromkeys(list(out_types) + cols))
            for c, typ in out_types.items():
                if sure or typ != "STRING":
                    types.setdefault(c, typ)
            how = f"type de la requete Spark qui l'ecrit ({written_by[0]})"
        lines = []
        for c in cols:
            if c in types:
                t, note = types[c], how
            else:
                t, note = suggest(c, hints.get(c))
            lines.append((c, t, note))
        schema_file = f"{schema_dir}/{name}.yml"
        why = f"lu par {', '.join(sorted(r['scripts']))}" + (
            ", produit par un autre script" if is_produced else ""
        )
        if r["star"] and not from_file:
            why += " ; LOAD * : colonnes incompletes sans le fichier"
        # Format : celui du fichier trouve, sinon celui que Qlik lit (txt ->
        # csv, qvd -> parquet), sinon parquet, a verifier.
        qlik_fmt, options = source_format(r["path"], r["spec"])
        if is_produced:
            qlik_fmt, options = "parquet", {}  # ecrit par le job
        fmt = fmt or qlik_fmt
        comments = [csv_options_note(options)] if fmt == "csv" else []
        if not lines:
            # Aucune colonne connue (LOAD * sans le fichier) : un schema vide
            # risquerait de faire planter la lecture par le socle. On declare
            # l'entree sans schema_file et on le signale.
            warnings.append(
                f"{name} : colonnes inconnues ({why}), pas de "
                "schema cree. Deposer le fichier dans data/input/ "
                "et relancer, ou ecrire le schema a la main."
            )
            if not entry:
                new_inputs.append(
                    (
                        name,
                        {"format": fmt or "parquet", "location": location},
                        why + " ; pas de schema, colonnes inconnues",
                        comments,
                    )
                )
            continue
        schemas_out[schema_file] = (why, lines)
        if entry and entry.schema_file:
            # schema_file declare mais fichier absent : on le cree la ou il
            # est attendu, sans toucher a l'entree
            schemas_out[entry.schema_file] = schemas_out.pop(schema_file)
            continue
        if entry:
            warnings.append(
                f"{entry.name} : pas de schema_file dans io.yml, "
                f"a ajouter a la main ({schema_file} est cree)"
            )
            continue
        props = {
            "format": fmt or "parquet",
            "schema_file": schema_file,
            "location": location,
        }
        new_inputs.append(
            (name, props, why if fmt else why + " ; format a verifier", comments)
        )

    propagate_types(schemas_out, conf)
    blocks = {
        sections["input"]: [entry_lines(n, pr, w, c) for n, pr, w, c in new_inputs],
        sections["output"]: [entry_lines(n, pr, w) for n, pr, w in new_outputs],
    }
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
            continue  # jamais d'ecrasement d'un vrai schema
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(schema_text(why, lines, style), encoding="utf-8")
        created.append(target)

    if args.quiet and not (new_inputs or new_outputs or created):
        return 0
    verb = "propose(s) dans" if args.draft else "ajoute(s) dans"
    print(
        f"conf : {len(new_inputs)} entree(s), {len(new_outputs)} sortie(s) "
        f"et {len(created)} schema(s) {verb} {where} "
        f"({(DRAFT if args.draft else IO_FILE).resolve()})"
    )
    for name, *_ in new_inputs + new_outputs:
        print(f"  + {name}")
    for target in created:
        print(f"  + {target}")
    guessed = sum(
        1
        for _, lines in schemas_out.values()
        for _, _, n in lines
        if n.endswith("a verifier") and not n.startswith("type inconnu")
    )
    unknown = sum(
        1
        for _, lines in schemas_out.values()
        for _, _, n in lines
        if n.startswith("type inconnu")
    )
    if guessed or unknown:
        print(
            f"Types a verifier : {guessed} devine(s) d'apres les scripts, "
            f"{unknown} inconnu(s) mis en string (reperables a leur "
            "commentaire dans les schemas). Pour des types surs, il faut "
            "que fill_conf trouve le Parquet : dans data/input/, ou a sa "
            "location avec les variables passees par --var."
        )
    if args.quiet:
        return 0
    if waiting:
        print(
            "En attente, declares avec leurs types exacts quand le script "
            "qui les ecrit aura ete traduit (migrate.py s'en charge) : "
            + " ; ".join(waiting)
        )
    if unknown_vars:
        print(
            "[!] Parquet non lus, types devines : variable(s) "
            f"{', '.join('${' + v + '}' for v in sorted(unknown_vars))} "
            "inconnue(s). Les passer, ex. --var "
            f"{sorted(unknown_vars)[0]}=/chemin/des/fichiers, puis relancer "
            "sur des schemas supprimes (un schema existant n'est jamais "
            "reecrit)."
        )
    for warning in warnings:
        print(f"[!] {warning}")
    for f in failed:
        print(f"[!] non analyse, entrees et sorties a verifier a la main : {f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
