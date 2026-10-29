"""Ordre de chargement des lignes, comme dans Qlik.

Qlik traite les lignes dans l'ordre ou elles ont ete chargees ; Spark n'a
pas d'ordre. Chaque table porte donc une colonne technique, LOAD_ORDER, que
les requetes generees utilisent pour Peek, Previous, RowNo...
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Nom de la colonne, de type array<bigint>, reprise par les requetes SQL.
LOAD_ORDER = "_qlik_order"


def add_load_order(df: DataFrame) -> DataFrame:
    """Ajoute la colonne LOAD_ORDER a une table qui vient d'etre lue.

    Si elle y est deja (entree preparee a la main), on la garde, en la
    ramenant au type array<bigint>. Sinon on numerote les lignes dans
    l'ordre de lecture : il faut le faire avant toute autre operation, sinon
    l'ordre est perdu.

    Args:
        df: la table lue, avant toute transformation.

    Returns:
        La meme table, avec la colonne LOAD_ORDER.
    """
    if LOAD_ORDER not in df.columns:
        return df.withColumn(LOAD_ORDER, F.array(F.monotonically_increasing_id()))
    if not dict(df.dtypes)[LOAD_ORDER].startswith("array"):
        return df.withColumn(LOAD_ORDER, F.array(F.col(LOAD_ORDER).cast("bigint")))
    return df
