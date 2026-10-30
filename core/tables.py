"""Calculer une table Qlik avec sa requete du dossier sql/.

run_sql("ventes__01_ventes.sql", table="Ventes")   une table Qlik

Lire et ecrire (read_input, write_output) sont dans datasets.py.
"""

from __future__ import annotations

from core import context, lineage
from core.session import active_session


def run_sql(sql_file: str, table: str) -> None:
    """Calcule une table Qlik avec sa requete du dossier sql/.

    Le resultat devient la table `table`, que les requetes suivantes peuvent
    lire. Il est aussi ecrit dans une table du catalogue, pour que la
    transformation remonte dans OpenLineage (sauf avec --temp-views).

    Args:
        sql_file: le fichier de la requete, dans le dossier sql/ du projet.
        table: le nom de la table Qlik.
    """
    spark = active_session()
    state = context.state()
    df = spark.sql((state.sql_dir / sql_file).read_text(encoding="utf-8"))
    if state.database:
        if not state.database_ready:
            lineage.ensure_database(spark, state.database)
            state.database_ready = True
        name = lineage.table_name(state.database, state.script, table)
        df = lineage.materialize(spark, df, name)
    df.createOrReplaceTempView(table)
    state.tables.append(table)
