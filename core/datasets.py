"""Lire une entree de conf/io.yml.

    df = read_input("ventes")                      un DataFrame, pour du PySpark
    read_input("ventes", table="in_ventes")        en plus, une table pour le SQL

Une seule fonction pour les deux usages : avant, datasets.py renvoyait le
DataFrame et tables.py le rangeait dans une vue sans rien renvoyer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from commons import add_load_order
from core.internal import context
from core.internal.io import access
from core.internal.session import active_session


def read_input(name: str, table: str | None = None) -> DataFrame:
    """Lit une entree de conf/io.yml, comme un LOAD ... FROM fichier.

    Avec table, l'entree devient aussi la table `table`, que les requetes
    SQL du script peuvent lire. Ses lignes gardent alors leur ordre de
    lecture, comme dans Qlik (colonne _qlik_order).

    Args:
        name: le nom de l'entree dans conf/io.yml.
        table: le nom sous lequel les requetes SQL la lisent, ou None pour
            n'obtenir que le DataFrame.

    Returns:
        L'entree lue, telle qu'elle est dans le fichier.
    """
    spark = active_session()
    df = access.read_config(spark, name)
    if table is not None:
        state = context.state()
        add_load_order(df).createOrReplaceTempView(table)
        state.tables.append(table)
        state.read_locations.add(access.input_location(name))
    return df
