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

Les requêtes sont dans src/{stem}/sql/, une par table du script Qlik.
Ce job :
  1. expose chaque fichier lu comme une vue SQL (in_<nom>) ;
  2. exécute les requêtes dans l'ordre du script : chaque résultat devient
     une vue au nom de la table Qlik, utilisable par les requêtes suivantes ;
  3. écrit les tables qui avaient un STORE dans Qlik.
Une étape en échec est journalisée et n'arrête pas les suivantes ; le job
se termine en erreur à la fin s'il y a eu au moins un échec.
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark_commons.io import read, write
from pyspark_commons.logger import setup_logging
from pyspark_commons.monitoring import timer

HERE = Path(__file__).resolve().parent           # src/{stem}/
ROOT = HERE.parents[1]                           # racine du projet
SQL_DIR = HERE / "sql"
ORDER = "_qlik_order"     # ordre de chargement Qlik, écrit avec chaque sortie
logger = logging.getLogger("{stem}")

# Fichiers lus : vue SQL -> (location, format)   (conf/io.yml si déclaré)
INPUTS = {{
{inputs}
}}

# Requêtes, dans l'ordre : (fichier SQL, vue produite, sortie ou None, format)
STEPS = [
{steps}
]


def location(path: str) -> str:
    """Chemin relatif -> depuis la racine du projet (pas depuis le dossier
    où l'on lance le job) ; URI (s3a://…) et chemin absolu inchangés."""
    if "://" in path or Path(path).is_absolute():
        return path
    return str(ROOT / path)


def with_order(df):
    """Colonne technique _qlik_order : l'ordre de chargement Qlik.
    Déjà présente si le fichier a été écrit par un job migré ; sinon,
    numérotation dans l'ordre de lecture, AVANT toute autre opération."""
    if ORDER not in df.columns:
        return df.withColumn(ORDER, F.array(F.monotonically_increasing_id()))
    if not dict(df.dtypes)[ORDER].startswith("array"):
        return df.withColumn(ORDER, F.array(F.col(ORDER).cast("bigint")))
    return df


def main(spark: SparkSession) -> list[str]:
    """Exécute le script {stem}. Renvoie la liste des étapes en échec."""
    # Qlik distingue les majuscules dans les noms de champs (IDBid ≠ IdBid)
    spark.conf.set("spark.sql.caseSensitive", "true")
    failures = []
    read_paths = {{location(path) for path, _ in INPUTS.values()}}
    for view, (path, fmt) in INPUTS.items():
        try:
            with_order(read(spark, location(path), fmt)) \\
                .createOrReplaceTempView(view)
        except Exception as exc:                 # fichier absent, illisible…
            logger.error("Lecture de %s (%s) impossible : %s", path, view, exc)
            failures.append(f"lecture {{path}}")
    for sql_file, view, output, fmt in STEPS:
        try:
            with timer(f"{stem} / {{view}}"):
                df = spark.sql((SQL_DIR / sql_file).read_text(encoding="utf-8"))
                if output and location(output) in read_paths:
                    # le fichier écrit est aussi lu : on coupe la lignée
                    df = df.localCheckpoint()
                df.createOrReplaceTempView(view)
                if output:
                    write(df, location(output), fmt, "overwrite")
        except Exception as exc:
            # on continue : les étapes qui dépendent de celle-ci échoueront
            # à leur tour (vue absente), les autres iront au bout
            logger.error("Étape %s (%s) en échec : %s", sql_file, view,
                         str(exc).splitlines()[0])
            failures.append(sql_file)
    if failures:
        logger.error("%d étape(s) en échec : %s", len(failures),
                     ", ".join(failures))
    return failures


if __name__ == "__main__":
    setup_logging()
    sys.exit(1 if main(SparkSession.builder.appName("{stem}").getOrCreate())
             else 0)
'''
