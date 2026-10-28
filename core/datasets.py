"""Lire une entree et ecrire une sortie de conf/io.yml.

read_input("ventes", table="in_ventes")      LOAD ... FROM ventes
write_output("ventes_v2", table="Ventes")    STORE Ventes INTO ventes_v2
"""

from __future__ import annotations

from commons import add_load_order
from core.internal import context, io
from core.internal.session import active_session


def read_input(name: str, table: str) -> None:
    """Charge une entree de conf/io.yml, comme un LOAD ... FROM fichier.

    Elle devient la table `table`, que les requetes du script peuvent lire.
    Les lignes gardent leur ordre de lecture, comme dans Qlik.

    Args:
        name: le nom de l'entree dans conf/io.yml.
        table: le nom sous lequel les requetes la lisent.
    """
    spark = active_session()
    state = context.state()
    add_load_order(io.read_dataset(spark, name)).createOrReplaceTempView(table)
    state.tables.append(table)
    state.read_locations.add(io.input_location(name))


def write_output(name: str, table: str) -> None:
    """Ecrit une table dans une sortie de conf/io.yml, comme un STORE.

    Args:
        name: le nom de la sortie dans conf/io.yml.
        table: la table a ecrire.
    """
    spark = active_session()
    state = context.state()
    df = spark.table(table)
    if state.database is None and io.output_location(name) in state.read_locations:
        # Le script ecrase un fichier qu'il a lu : sans table intermediaire,
        # Spark relirait le fichier en l'ecrivant. On fige la table avant.
        df = df.localCheckpoint()
    io.write_dataset(df, name)
