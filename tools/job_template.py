"""Gabarit du job généré pour chaque script (src/<script>/job.py).

C'est le SEUL endroit où le code généré appelle pyspark-commons : si une
signature change dans la bibliothèque, on corrige ici puis on relance
python tools/assemble.py work/<script> (ou migrate.py).

Fonctions pyspark-commons utilisées (API connue, à vérifier) :
    pyspark_commons.io.read(spark, path, fmt) -> DataFrame
    pyspark_commons.io.write(df, path, fmt, mode)
    pyspark_commons.monitoring.timer(label)      (context manager)
    pyspark_commons.logger.setup_logging()
"""

JOB = '''"""Job {stem} — traduit de script_qvd/{script} (généré, ne pas éditer).

Chaque fichier .sql de ce dossier calcule une table du script Qlik.
Ce job :
  1. expose chaque fichier lu comme une vue SQL (in_<nom>) ;
  2. exécute les requêtes dans l'ordre du script : chaque résultat devient
     une vue au nom de la table Qlik, utilisable par les requêtes suivantes ;
  3. écrit les tables qui avaient un STORE dans Qlik.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark_commons.io import read, write
from pyspark_commons.logger import setup_logging
from pyspark_commons.monitoring import timer

HERE = Path(__file__).resolve().parent
ORDER = "_qlik_order"     # ordre de chargement Qlik, écrit avec chaque sortie

# Fichiers lus : vue SQL -> (location, format)   (conf/io.yml si déclaré)
INPUTS = {{
{inputs}
}}

# Requêtes, dans l'ordre : (fichier SQL, vue produite, sortie ou None, format)
STEPS = [
{steps}
]


def with_order(df):
    """Colonne technique _qlik_order : l'ordre de chargement Qlik.
    Déjà présente si le fichier a été écrit par un job migré ; sinon,
    numérotation dans l'ordre de lecture, AVANT toute autre opération."""
    if ORDER not in df.columns:
        return df.withColumn(ORDER, F.array(F.monotonically_increasing_id()))
    if not dict(df.dtypes)[ORDER].startswith("array"):
        return df.withColumn(ORDER, F.array(F.col(ORDER).cast("bigint")))
    return df


def main(spark: SparkSession) -> None:
    """Exécute le script {stem}."""
    read_paths = {{path for path, _ in INPUTS.values()}}
    for view, (path, fmt) in INPUTS.items():
        with_order(read(spark, path, fmt)).createOrReplaceTempView(view)
    for sql_file, view, output, fmt in STEPS:
        with timer(f"{stem} / {{view}}"):
            df = spark.sql((HERE / sql_file).read_text(encoding="utf-8"))
            if output in read_paths:
                # le fichier écrit est aussi lu : on coupe la lignée
                df = df.localCheckpoint()
            df.createOrReplaceTempView(view)
            if output:
                write(df, output, fmt, "overwrite")


if __name__ == "__main__":
    setup_logging()
    main(SparkSession.builder.appName("{stem}").getOrCreate())
'''
