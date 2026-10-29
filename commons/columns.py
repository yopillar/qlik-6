"""Operations sur les colonnes d'une table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NullType


def cast_void_to_string(df: DataFrame) -> DataFrame:
    """Passe en string les colonnes de type void.

    Une colonne toujours nulle (Null() dans Qlik, NULL AS x en SQL) a le type
    void, que Parquet refuse d'ecrire.

    Args:
        df: la table a ecrire.

    Returns:
        La meme table, sans colonne void.
    """
    void = {f.name for f in df.schema.fields if isinstance(f.dataType, NullType)}
    if not void:
        return df
    return df.select(
        *[
            F.col(f"`{c}`").cast("string").alias(c) if c in void else F.col(f"`{c}`")
            for c in df.columns
        ]
    )


def drop_columns(df: DataFrame, *names: str) -> DataFrame:
    """Retire des colonnes, sans erreur si l'une d'elles n'existe pas.

    Args:
        df: la table.
        names: les colonnes a retirer.

    Returns:
        La table sans ces colonnes.
    """
    return df.drop(*[n for n in names if n in df.columns])
