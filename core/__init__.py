"""spark-foundation-core : les fonctions qu'un job migre depuis Qlik appelle.

Meme organisation que le socle spark_foundation :
    runner.py   pipeline, Pipeline, run_scripts : le job et ses scripts
    tables.py   read_input, run_sql, write_output : les tables d'un script
Tout le reste est dans core/internal/, reserve aux data engineers.
"""

from core.runner import Pipeline, pipeline, run_scripts
from core.tables import read_input, write_output

__all__ = ["Pipeline", "pipeline", "read_input", "run_scripts", "write_output"]
