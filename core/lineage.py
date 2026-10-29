"""Tables intermediaires, pour que chaque table Qlik remonte dans
OpenLineage.

Une vue temporaire n'est pas un jeu de donnees pour OpenLineage : la lignee
sautait des fichiers lus aux sorties, sans les tables Qlik entre les deux.
Chaque table Qlik est donc ecrite en Parquet dans une table du catalogue,
<base>.<script>__<table>, puis relue de la.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from commons import cast_void_to_string

logger = logging.getLogger("core")


def ensure_database(spark: SparkSession, database: str) -> None:
    """Cree la base des tables intermediaires si elle n'existe pas.

    Args:
        spark: la session.
        database: le nom de la base.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")


def table_name(database: str, script: str, table: str) -> str:
    """Nom complet de la table intermediaire d'une table Qlik.

    Args:
        database: la base.
        script: le script Qlik.
        table: la table Qlik.

    Returns:
        <base>.<script>__<table>, en minuscules.
    """
    return f"{database}.{script}__{table}".lower()


def materialize(spark: SparkSession, df: DataFrame, table: str) -> DataFrame:
    """Ecrit une table Qlik dans le catalogue, en Parquet, et la relit.

    La table est remplacee a chaque lancement (mode overwrite). Une table
    dont deux colonnes ne different que par la casse (IdBid et IDBid,
    possible dans Qlik) ne peut pas aller dans le catalogue, qui ignore la
    casse : elle reste une vue temporaire, absente de la lignee.

    Args:
        spark: la session.
        df: la table calculee.
        table: son nom complet dans le catalogue.

    Returns:
        La table relue depuis le catalogue (ou figee en memoire).
    """
    lowered = [c.lower() for c in df.columns]
    if len(set(lowered)) < len(lowered):
        logger.warning(
            "%s : colonnes qui ne different que par la casse, table gardee "
            "en vue temporaire (absente de la lignee)",
            table,
        )
        return df.localCheckpoint()
    cast_void_to_string(df).write.mode("overwrite").format("parquet").saveAsTable(table)
    return spark.table(table)
