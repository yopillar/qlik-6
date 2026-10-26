"""Lit conf/io.yml et les schemas de conf/schema/.

io.yml donne, pour chaque entree et chaque sortie, le format, la location et
le schema_file. La forme exacte du fichier n'etant pas figee, la lecture
accepte plusieurs variantes :
  - les sections dont le nom contient "input" ou "output"
  - les entrees en dictionnaire (nom: {...}) ou en liste ({name: ..., ...})
  - format, location (ou path), schema_file (ou schema)
Meme chose pour les schemas : {colonne: type}, une liste de {name, type}, ou
l'une des deux rangee sous une cle columns, fields ou schema.

    python tools/conf.py          # pour voir ce qu'il a compris
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

CONF = Path("conf")
IO_FILE = CONF / "io.yml"


# ---------------------------------------------------------------------------
# Chemins du projet, communs a tous les outils (tous partent du dossier
# courant, donc de la racine du projet).
# ---------------------------------------------------------------------------
WORK = Path("work")
# Selon l'export, un script Qlik n'a pas toujours l'extension .qvs : on prend
# aussi les .txt et les fichiers sans extension.
SCRIPT_SUFFIXES = (".qvs", ".txt", "")


# Caracteres que Hadoop lit comme un fragment d'URI (#) ou un motif (les
# autres) : un chemin qui en contient n'est pas retrouve, ni a la lecture ni
# a la relecture d'une sortie. Les crochets et accolades deviennent des
# parentheses, qui gardent le nom lisible ; # * ? deviennent des _.
SAFE_CHARS = str.maketrans(
    {"[": "(", "]": ")", "{": "(", "}": ")", "#": "_", "*": "_", "?": "_"}
)


def safe_name(name: str) -> str:
    """Nom de fichier lisible par Spark : [anon]-TT#TEST.CSV devient
    (anon)-TT_TEST.CSV.

    Le remplacement ne cree jamais de _ en tete de nom : Spark ignore les
    fichiers qui commencent par _ ou . (comme _SUCCESS), il lirait un
    fichier vide sans rien dire."""
    out = []
    for part in re.split(r"([\\/])", name):  # separateurs gardes tels quels
        cleaned = part.translate(SAFE_CHARS)
        if cleaned != part and cleaned.startswith("_") and not part.startswith("_"):
            cleaned = cleaned.lstrip("_") or cleaned
        out.append(cleaned)
    return "".join(out)


def find_scripts(path: Path) -> list[Path]:
    """Scripts Qlik designes par path : le fichier lui-meme, ou ceux du
    dossier (.qvs, .txt ou sans extension, casse ignoree, fichiers caches
    exclus)."""
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(
            p
            for p in path.iterdir()
            if p.is_file()
            and not p.name.startswith(".")
            and p.suffix.lower() in SCRIPT_SUFFIXES
        )
    return []


def work_dir(path: Path) -> Path:
    """Dossier de travail d'un script, work/<script>, cree par prepare.py.

    check, translate, assemble et verify travaillent sur ce dossier, pas sur
    le script. On accepte quand meme le chemin du script
    (script_qvd/ventes.qvs) ou son seul nom (ventes) : on retrouve work/ventes.
    Sinon, on s'arrete avec un message qui dit quoi passer, plutot qu'un
    FileNotFoundError sur manifest.json."""
    if (path / "manifest.json").is_file():
        return path
    cand = WORK / (path.stem if path.is_file() else path.name)
    if (cand / "manifest.json").is_file():
        return cand
    raise SystemExit(
        f"{path} : pas de manifest.json. Cet outil prend le dossier de "
        f"travail d'un script, {WORK}/<script> (ex. {WORK}/ventes), cree par "
        "prepare.py : lancer d'abord migrate.py, ou prepare.py sur le script."
    )


def resolve_location(location: str, variables: dict | None = None) -> str:
    """Remplace les ${var} d'une location (${entrees}, ${sorties}...).

    C'est le socle qui les resout quand le job tourne. Le kit n'en a besoin
    que pour aller voir les fichiers lui-meme (comparaison avec Qlik,
    colonnes d'un Parquet) : on prend les valeurs passees en parametre,
    sinon les variables d'environnement. Une variable inconnue reste telle
    quelle, et le chemin n'existera simplement pas."""

    def repl(m):
        name = m.group(1)
        if variables and name in variables:
            return variables[name]
        return os.environ.get(name, m.group(0))

    return re.sub(r"\$\{(\w+)\}", repl, location)


@dataclass
class IoEntry:
    name: str
    kind: str  # "input" ou "output"
    location: str
    format: str = "parquet"
    schema_file: str | None = None
    columns: list[str] | None = None
    types: dict[str, str] = field(default_factory=dict)


def _stem(text: str) -> str:
    name = re.split(r"[\\/]", text.replace("\\", "/").rstrip("/"))[-1]
    return re.sub(
        r"\.(qvd|txt|csv|parquet|xlsx?|json|orc|delta)$", "", name, flags=re.I
    ).lower()


def io_key(text: str) -> str:
    """Cle de rapprochement d'un nom de fichier ou d'entree."""
    return re.sub(r"[\W_]+", "_", _stem(text), flags=re.ASCII).strip("_")


def _load_yaml(path: Path):
    try:
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "PyYAML absent : pip install pyyaml (lecture de conf/)"
        ) from exc
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _schema_columns(data) -> tuple[list[str], dict[str, str]]:
    """Colonnes d'un schema dans l'ordre, avec leur type, quelle que soit la
    forme du schema."""
    if isinstance(data, dict):
        for key in ("columns", "fields", "schema"):
            if key in data and isinstance(data[key], (list, dict)):
                return _schema_columns(data[key])
        if all(
            not isinstance(v, (list, dict)) or isinstance(v, dict) and "type" in v
            for v in data.values()
        ):
            cols = list(map(str, data))
            types = {
                str(k): str(v["type"] if isinstance(v, dict) else v)
                for k, v in data.items()
            }
            return cols, types
    if isinstance(data, list):
        cols, types = [], {}
        for item in data:
            if isinstance(item, dict):
                name = item.get("name") or item.get("column") or item.get("field")
                # cas "- colonne: type"
                if name is None and len(item) == 1:
                    name, typ = next(iter(item.items()))
                    cols.append(str(name))
                    types[str(name)] = str(typ)
                    continue
                if name is not None:
                    cols.append(str(name))
                    types[str(name)] = str(item.get("type", ""))
            elif isinstance(item, str):
                cols.append(item)
        return cols, types
    return [], {}


def _entries(section, kind: str):
    if isinstance(section, dict):
        items = [(str(k), v) for k, v in section.items()]
    elif isinstance(section, list):
        items = [(str(v.get("name", "")), v) for v in section if isinstance(v, dict)]
    else:
        items = []
    for name, props in items:
        if not isinstance(props, dict):
            continue
        location = props.get("location") or props.get("path")
        if not location:
            continue
        yield IoEntry(
            name or _stem(location),
            kind,
            str(location),
            str(props.get("format", "parquet")).lower(),
            props.get("schema_file") or props.get("schema"),
        )


class IoConf:
    """Entrees et sorties de io.yml, qu'on retrouve a partir du nom de
    fichier utilise dans le script Qlik."""

    def __init__(self, path: Path = IO_FILE):
        self.path = path
        self.inputs: list[IoEntry] = []
        self.outputs: list[IoEntry] = []
        self.problems: list[str] = []
        if not path.exists():
            return
        data = _load_yaml(path)
        if not isinstance(data, dict):
            self.problems.append(f"{path} : contenu non reconnu")
            return
        for key, section in data.items():
            low = str(key).lower()
            kind = "input" if "input" in low else "output" if "output" in low else None
            if kind:
                target = self.inputs if kind == "input" else self.outputs
                target.extend(_entries(section, kind))
        for entry in self.inputs + self.outputs:
            self._load_schema(entry)

    @property
    def enabled(self) -> bool:
        return bool(self.inputs or self.outputs)

    def _load_schema(self, entry: IoEntry) -> None:
        if not entry.schema_file:
            return
        for base in (self.path.parent, Path("."), CONF / "schema"):
            f = base / entry.schema_file
            if f.exists():
                entry.columns, entry.types = _schema_columns(_load_yaml(f))
                if not entry.columns:
                    self.problems.append(f"{f} : aucune colonne reconnue")
                return
        self.problems.append(
            f"{entry.name} : schema_file introuvable ({entry.schema_file})"
        )

    def find(self, qlik_path: str, kind: str) -> IoEntry | None:
        """Cherche l'entree qui correspond a un fichier Qlik, soit par le nom
        de l'entree, soit par le nom de fichier de sa location.

        On compare en ramenant espaces, accents, # et ponctuation a "_",
        comme les noms que fill_conf.py donne aux entrees :
        [anon]-Ventes#2024.qvd retrouve l'entree anon_ventes_2024."""
        stem = io_key(qlik_path)
        pool = self.inputs if kind == "input" else self.outputs
        for e in pool:
            if io_key(e.name) == stem or io_key(e.location) == stem:
                return e
        return None


def main() -> int:
    conf = IoConf()
    if not conf.path.exists():
        print(f"{conf.path} absent : regle par defaut data/input, data/output.")
        return 0
    for e in conf.inputs + conf.outputs:
        cols = f"{len(e.columns)} colonne(s)" if e.columns else "colonnes inconnues"
        print(f"{e.kind:6}  {e.name:30} {e.format:8} {e.location}  ({cols})")
    for p in conf.problems:
        print(f"[!] {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
