"""Lecture et ecriture des fichiers declares dans conf/io.yml."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from commons import LOAD_ORDER, cast_void_to_string, drop_columns
from core.internal import config


def read_dataset(spark: SparkSession, name: str) -> DataFrame:
    """Lit une entree de io.yml.

    Un CSV est lu avec son schema (colonnes dans l'ordre du fichier) et ses
    options (header, sep, encoding, mode...), passees telles quelles au
    lecteur CSV de Spark. En-tete par defaut. Les autres formats sont lus
    par leur nom (parquet...).

    Args:
        spark: la session.
        name: le nom de l'entree dans io.yml.

    Returns:
        La table lue.
    """
    description = config.entry("input", name)
    path = config.location(description)
    fmt = str(description.get("format", "parquet")).lower()
    if fmt != "csv":
        return spark.read.format(fmt).load(path)
    reader = spark.read
    options: dict[str, Any] = {"header": True, **(description.get("options") or {})}
    for key, value in options.items():
        reader = reader.option(
            key, str(value).lower() if isinstance(value, bool) else value
        )
    schema = config.schema_ddl(description)
    reader = reader.schema(schema) if schema else reader.option("inferSchema", "false")
    return reader.csv(path)


def output_location(name: str) -> str:
    """Location d'une sortie de io.yml.

    Args:
        name: le nom de la sortie.

    Returns:
        Le chemin ou elle est ecrite.
    """
    return config.location(config.entry("output", name))


def input_location(name: str) -> str:
    """Location d'une entree de io.yml.

    Args:
        name: le nom de l'entree.

    Returns:
        Le chemin ou elle est lue.
    """
    return config.location(config.entry("input", name))


def write_dataset(df: DataFrame, name: str) -> None:
    """Ecrit une table dans une sortie de io.yml, en Parquet, en remplacant
    ce qui y etait.

    La colonne d'ordre de chargement ne sert qu'a l'interieur du job : elle
    n'est pas ecrite. Une colonne void est passee en string.

    Args:
        df: la table.
        name: le nom de la sortie dans io.yml.
    """
    ready = cast_void_to_string(drop_columns(df, LOAD_ORDER))
    ready.write.mode("overwrite").parquet(output_location(name))
