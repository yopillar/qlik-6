"""Lit conf/io.yml et les schémas de conf/schema/.

io.yml donne, pour chaque entrée et chaque sortie, le format, la location et
le schema_file. La forme exacte du fichier n'étant pas figée, la lecture
accepte plusieurs variantes :
  - les sections dont le nom contient « input » ou « output »
  - les entrées en dictionnaire (nom: {…}) ou en liste ({name: …, …})
  - format, location (ou path), schema_file (ou schema)
Même chose pour les schémas : {colonne: type}, une liste de {name, type}, ou
l'une des deux rangée sous une clé columns, fields ou schema.

    python tools/conf.py          # pour voir ce qu'il a compris
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

CONF = Path("conf")
IO_FILE = CONF / "io.yml"


@dataclass
class IoEntry:
    name: str
    kind: str                     # "input" ou "output"
    location: str
    format: str = "parquet"
    schema_file: str | None = None
    columns: list[str] | None = None
    types: dict[str, str] = field(default_factory=dict)


def _stem(text: str) -> str:
    name = re.split(r"[\\/]", text.replace("\\", "/").rstrip("/"))[-1]
    return re.sub(r"\.(qvd|txt|csv|parquet|xlsx?|json|orc|delta)$", "", name,
                  flags=re.I).lower()


def _load_yaml(path: Path):
    try:
        import yaml
    except ImportError:
        raise SystemExit("PyYAML absent : pip install pyyaml (lecture de conf/)")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _schema_columns(data) -> tuple[list[str], dict[str, str]]:
    """Colonnes d'un schéma dans l'ordre, avec leur type, quelle que soit la
    forme du schéma."""
    if isinstance(data, dict):
        for key in ("columns", "fields", "schema"):
            if key in data and isinstance(data[key], (list, dict)):
                return _schema_columns(data[key])
        if all(not isinstance(v, (list, dict)) or isinstance(v, dict)
               and "type" in v for v in data.values()):
            cols = list(map(str, data))
            types = {str(k): str(v["type"] if isinstance(v, dict) else v)
                     for k, v in data.items()}
            return cols, types
    if isinstance(data, list):
        cols, types = [], {}
        for item in data:
            if isinstance(item, dict):
                name = item.get("name") or item.get("column") or item.get("field")
                # cas « - colonne: type »
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
        yield IoEntry(name or _stem(location), kind, str(location),
                      str(props.get("format", "parquet")).lower(),
                      props.get("schema_file") or props.get("schema"))


class IoConf:
    """Entrées et sorties de io.yml, qu'on retrouve à partir du nom de
    fichier utilisé dans le script Qlik."""

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
            kind = "input" if "input" in low else \
                "output" if "output" in low else None
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
        self.problems.append(f"{entry.name} : schema_file introuvable "
                             f"({entry.schema_file})")

    def find(self, qlik_path: str, kind: str) -> IoEntry | None:
        """Cherche l'entrée qui correspond à un fichier Qlik, soit par le nom
        de l'entrée, soit par le nom de fichier de sa location."""
        stem = _stem(qlik_path)
        pool = self.inputs if kind == "input" else self.outputs
        for e in pool:
            if e.name.lower() == stem or _stem(e.location) == stem:
                return e
        return None


def main() -> int:
    conf = IoConf()
    if not conf.path.exists():
        print(f"{conf.path} absent : règle par défaut data/input, data/output.")
        return 0
    for e in conf.inputs + conf.outputs:
        cols = f"{len(e.columns)} colonne(s)" if e.columns else "colonnes inconnues"
        print(f"{e.kind:6}  {e.name:30} {e.format:8} {e.location}  ({cols})")
    for p in conf.problems:
        print(f"⚠ {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
