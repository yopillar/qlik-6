"""Faux spark_foundation, pour faire tourner les jobs generes sans le socle.

Il reproduit seulement l'API utilisee par tools/job_template.py, telle
qu'on la voit dans le job dp_test : pipeline(nom), read_input(nom) et
write_output(df, nom). Tout le reste du vrai socle (logs, session
configuree, resolution des chemins) est simplifie. Les signatures sont des
hypotheses, pas verifiees contre le vrai socle.

Les locations de conf/io.yml peuvent contenir ${entrees} et ${sorties} : on
les remplace par data/input et data/output, ou par la variable
d'environnement du meme nom si elle existe.
"""

from __future__ import annotations

import functools
import os
import re
import sys
import tempfile
from pathlib import Path

import yaml

DEFAULTS = {"entrees": "data/input", "sorties": "data/output"}
_spark = None


def _session():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession

        _spark = (
            SparkSession.builder.master("local[2]")
            .appName("fake_spark_foundation")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            # base en memoire : un dossier neuf a chaque lancement,
            # sinon les tables d'un lancement precedent bloquent
            # saveAsTable (dossier deja present)
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def _conf() -> dict:
    return yaml.safe_load(Path("conf/io.yml").read_text(encoding="utf-8")) or {}


def _section(kind: str) -> dict:
    for key, value in _conf().items():
        if kind in str(key).lower() and isinstance(value, dict):
            return value
    return {}


def _location(entry: dict) -> str:
    def repl(m):
        name = m.group(1)
        return os.environ.get(name, DEFAULTS.get(name, m.group(0)))

    return re.sub(r"\$\{(\w+)\}", repl, str(entry["location"]))


def _schema(entry: dict):
    """Schema Spark en DDL, d'apres le schema_file (columns: Nom: TYPE)."""
    if not entry.get("schema_file"):
        return None
    path = Path("conf") / entry["schema_file"]
    cols = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("columns")
    if not cols:
        return None
    return ", ".join(f"`{name}` {typ}" for name, typ in cols.items())


def read_input(name: str):
    entries = _section("input")
    if name not in entries:
        raise KeyError(f"entree {name} absente de conf/io.yml")
    entry = entries[name]
    reader = _session().read
    fmt = str(entry.get("format", "parquet")).lower()
    if fmt == "csv":
        schema = _schema(entry)
        reader = reader.option("header", "true").option("sep", entry.get("sep", ","))
        reader = (
            reader.schema(schema) if schema else reader.option("inferSchema", "false")
        )
        return reader.csv(_location(entry))
    return reader.format(fmt).load(_location(entry))


def write_output(df, name: str) -> None:
    entries = _section("output")
    if name not in entries:
        raise KeyError(f"sortie {name} absente de conf/io.yml")
    df.write.mode("overwrite").parquet(_location(entries[name]))


class Pipeline:
    """Encapsule run(spark, args). Appele sans argument, il prend les
    arguments de la ligne de commande, comme le vrai socle (suppose)."""

    def __init__(self, name: str, func):
        self.name = name
        self.func = func
        functools.update_wrapper(self, func)

    def __call__(self, args: list[str] | None = None):
        return self.func(_session(), sys.argv[1:] if args is None else args)


def pipeline(name: str):
    return lambda func: Pipeline(name, func)
