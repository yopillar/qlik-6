"""spark-foundation-core : les fonctions qu'un job migre depuis Qlik appelle.

Meme organisation que le socle spark_foundation :
    runner.py   pipeline, Pipeline, run_scripts : le job et ses scripts
    datasets.py read_input : une entree, en DataFrame et/ou en table SQL
    tables.py   run_sql, write_output (et read_input) : les tables d'un script
Tout le reste est dans core/internal/, reserve aux data engineers.
"""

from core.datasets import read_input
from core.runner import Pipeline, pipeline, run_scripts
from core.tables import write_output

__all__ = ["Pipeline", "pipeline", "read_input", "run_scripts", "write_output"]
