"""Doublure du socle spark_foundation pour les tests du kit.

Le vrai socle n'est pas installe ici. Cette doublure reprend son API
publique (celle qu'importe le code genere) en s'appuyant sur core/ et
commons/ du depot, qui ont servi de base au socle :
    spark_foundation            pipeline, Pipeline, read_input,
                                write_output, run_scripts
    spark_foundation.runner     Pipeline, pipeline, run_scripts
    spark_foundation.tables     read_input, write_output, run_sql
"""

from spark_foundation.runner import Pipeline, pipeline, run_scripts
from spark_foundation.tables import read_input, write_output

__all__ = ["Pipeline", "pipeline", "read_input", "run_scripts", "write_output"]
