"""spark-foundation-core : les fonctions qu'un job migre depuis Qlik appelle.

Que des fichiers, aucun sous-dossier. Ce qu'un job appelle :
    runner.py   pipeline, Pipeline : le job ; script : un script Qlik
    datasets.py read_input : une entree, en DataFrame et/ou en table SQL
    tables.py   run_sql, write_output (et read_input) : les tables d'un script
Le technique, reserve aux data engineers (les jobs ne l'importent pas) :
    session.py  creation et reglages de la session Spark
    config.py   lecture de conf/io.yml et des schemas
    access.py   lecture et ecriture des fichiers
    lineage.py  tables intermediaires, pour OpenLineage
    context.py  etat du job en cours (script, tables, base)
"""

from core.datasets import read_input
from core.runner import Pipeline, pipeline, script
from core.tables import write_output

__all__ = ["Pipeline", "pipeline", "read_input", "script", "write_output"]
