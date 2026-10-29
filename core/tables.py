"""Les tables d'un script Qlik : les lire, les calculer, les ecrire.

read_input("ventes", table="in_ventes")            LOAD ... FROM ventes
run_sql("ventes__01_ventes.sql", table="Ventes")   une table Qlik
write_output("ventes_v2", table="Ventes")          STORE Ventes INTO ventes_v2
"""

from __future__ import annotations

from commons import add_load_order
from core.internal import context, lineage
from core.internal.io import access
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
    add_load_order(access.read_config(spark, name)).createOrReplaceTempView(table)
    state.tables.append(table)
    state.read_locations.add(access.input_location(name))


def write_output(name: str, table: str) -> None:
    """Ecrit une table dans une sortie de conf/io.yml, comme un STORE.

    Args:
        name: le nom de la sortie dans conf/io.yml.
        table: la table a ecrire.
    """
    spark = active_session()
    state = context.state()
    df = spark.table(table)
    if state.database is None and access.output_location(name) in state.read_locations:
        # Le script ecrase un fichier qu'il a lu : sans table intermediaire,
        # Spark relirait le fichier en l'ecrivant. On fige la table avant.
        df = df.localCheckpoint()
    access.write_config(df, name)


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
        name = lineage.table_name(state.database, state.script, table)
        df = lineage.materialize(spark, df, name)
    df.createOrReplaceTempView(table)
    state.tables.append(table)
