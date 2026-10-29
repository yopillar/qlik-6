"""Partie technique de spark-foundation-core, reservee aux data engineers.

Les jobs n'importent jamais ces modules directement : ils passent par les
fonctions de core/ (runner.py : pipeline, run_scripts ; tables.py :
read_input, run_sql, write_output), qui viennent chercher ce qu'il leur
faut ici.
    session    creation et reglages de la session Spark
    config     lecture de conf/io.yml et des schemas
    io/access  lecture et ecriture des fichiers
    lineage    tables intermediaires, pour OpenLineage
    context    etat du job en cours (script, tables, base)
"""
