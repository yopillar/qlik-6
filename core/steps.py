"""Faire tourner une table Qlik, et enchainer les scripts d'un projet.

run_sql("ventes__01_ventes.sql", table="Ventes")   une table Qlik
run_scripts(args, calendrier, ventes, synthese)    les scripts, dans l'ordre
"""

from __future__ import annotations

from types import ModuleType

from core.internal import context, lineage, runner
from core.internal.session import active_session


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


def run_scripts(args: list[str], *scripts: ModuleType) -> None:
    """Fait tourner les scripts du projet, dans l'ordre donne.

    S'arrete au premier script en echec ; on reprend ensuite avec
    --from <script>. --only <script> n'en lance qu'un.

    Args:
        args: les arguments recus par le job.
        scripts: les scripts, dans l'ordre ou ils doivent tourner.
    """
    runner.run(args, scripts)
