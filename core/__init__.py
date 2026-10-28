"""spark-foundation-core : les fonctions qu'un job migre depuis Qlik appelle.

Un developpeur Qlik n'a besoin que de celles-ci :
    pipeline      declare la fonction principale du job
    read_input    LOAD ... FROM un fichier de conf/io.yml
    run_sql       une table Qlik, calculee par sa requete du dossier sql/
    write_output  STORE d'une table dans un fichier de conf/io.yml
    run_scripts   les scripts du projet, dans l'ordre
Tout le reste est dans core/internal/, reserve aux data engineers.
"""

from core.datasets import read_input, write_output
from core.pipeline import pipeline
from core.steps import run_scripts, run_sql

__all__ = ["pipeline", "read_input", "run_scripts", "run_sql", "write_output"]
