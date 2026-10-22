"""Gabarit de src/<nom-projet>/job.py, le job du projet.

Tout le code généré passe par ici pour appeler pyspark-commons, et nulle
part ailleurs. Si une signature change dans la lib, on corrige ce fichier
et on relance python tools/orchestrate.py (ou migrate.py).

Fonctions de pyspark-commons utilisées (signatures supposées, pas encore
vérifiées sur le vrai code) :
    pyspark_commons.session.get_local_spark_session()
    pyspark_commons.io.read(spark, path, fmt) -> DataFrame
    pyspark_commons.io.write(df, path, fmt, mode)
    pyspark_commons.monitoring.timer(label)      (context manager)
    pyspark_commons.logger.setup_logging()
"""

import os
import re
from pathlib import Path


def project_name() -> str:
    """Nom du projet : la variable QLIK_PROJECT si elle existe, sinon le
    nom du dossier courant."""
    name = os.environ.get("QLIK_PROJECT") or Path.cwd().name
    return re.sub(r"[^\w.-]", "_", name)


def project_dir() -> Path:
    """Dossier src/<nom-projet>/, qui contient job.py et sql/."""
    return Path("src") / project_name()


JOB = '''"""Job du projet {project}.

Fichier généré par tools/orchestrate.py : ne pas le modifier à la main, il
sera écrasé à la prochaine génération.

Il enchaîne les scripts Qlik migrés depuis script_qvd/, dans l'ordre de
leurs dépendances (un script qui lit un fichier écrit par un autre passe
après lui). Les requêtes sont dans sql/, une par table Qlik. Pour chaque
script :
  1. chaque fichier lu devient une vue SQL (in_<nom>)
  2. les requêtes tournent dans l'ordre du script, et chaque résultat devient
     une vue au nom de la table Qlik, que les requêtes suivantes peuvent lire
  3. les tables qui avaient un STORE dans Qlik sont écrites
Une étape qui plante est loggée mais n'empêche pas les autres de tourner. En
revanche, un script qui dépend d'un script en échec n'est pas lancé. Le code
retour vaut 1 dès qu'il y a eu un échec.

    python src/{project}/job.py                  # tout
    python src/{project}/job.py --from ventes    # reprendre à un script
    python src/{project}/job.py --only ventes    # un seul script
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from pyspark_commons.io import read, write
from pyspark_commons.logger import setup_logging
from pyspark_commons.monitoring import timer
from pyspark_commons.session import get_local_spark_session

HERE = Path(__file__).resolve().parent           # src/{project}/
ROOT = HERE.parents[1]                           # racine du projet
SQL_DIR = HERE / "sql"
ORDER = "_qlik_order"     # ordre de chargement Qlik, écrit dans les sorties
logger = logging.getLogger("{project}")

# Les scripts, dans l'ordre où ils tournent.
#   inputs : vue SQL -> (location, format), pris dans conf/io.yml si déclaré
#   steps  : (fichier SQL, vue produite, sortie ou None, format)
SCRIPTS = {{
{scripts}
}}


def location(path: str) -> str:
    """Résout un chemin relatif depuis la racine du projet, et pas depuis
    le dossier d'où on lance le job. Les URI (s3a://…) et les chemins
    absolus sont renvoyés tels quels."""
    if "://" in path or Path(path).is_absolute():
        return path
    return str(ROOT / path)


def with_order(df):
    """Ajoute la colonne _qlik_order, qui porte l'ordre de chargement Qlik.

    Si le fichier vient de ce job, elle y est déjà. Sinon on numérote les
    lignes dans l'ordre de lecture, et il faut le faire avant toute autre
    opération, sinon l'ordre est perdu."""
    if ORDER not in df.columns:
        return df.withColumn(ORDER, F.array(F.monotonically_increasing_id()))
    if not dict(df.dtypes)[ORDER].startswith("array"):
        return df.withColumn(ORDER, F.array(F.col(ORDER).cast("bigint")))
    return df


def run_script(spark, name: str, script: dict) -> list[str]:
    """Fait tourner un script et renvoie la liste des étapes en échec."""
    failures = []
    read_paths = {{location(path) for path, _ in script["inputs"].values()}}
    for view, (path, fmt) in script["inputs"].items():
        try:
            df = with_order(read(spark, location(path), fmt))
            df.createOrReplaceTempView(view)
        except Exception as exc:                 # fichier absent, illisible…
            logger.error("%s : lecture de %s (%s) impossible : %s", name, path,
                         view, exc)
            failures.append(f"lecture {{path}}")
    for sql_file, view, output, fmt in script["steps"]:
        try:
            with timer(f"{{name}} / {{view}}"):
                df = spark.sql((SQL_DIR / sql_file).read_text(encoding="utf-8"))
                if output and location(output) in read_paths:
                    # Le script relit le fichier qu'il va écraser : on coupe
                    # la lignée, sinon Spark lirait le fichier en l'écrivant.
                    df = df.localCheckpoint()
                df.createOrReplaceTempView(view)
                if output:
                    write(df, location(output), fmt, "overwrite")
        except Exception as exc:
            # On ne s'arrête pas : les étapes qui dépendent de celle-ci
            # planteront à leur tour (vue absente), les autres iront au bout.
            logger.error("%s : étape %s (%s) en échec : %s", name, sql_file,
                         view, str(exc).splitlines()[0])
            failures.append(sql_file)
    # on nettoie les vues pour que le script suivant ne tombe pas dessus
    for view in list(script["inputs"]) + [s[1] for s in script["steps"]]:
        spark.catalog.dropTempView(view)
    return failures


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Job du projet {project}.")
    p.add_argument("--from", dest="start", help="reprendre à ce script")
    p.add_argument("--only", help="ne lancer que ce script")
    args = p.parse_args(argv)
    names = list(SCRIPTS)
    for opt in (args.start, args.only):
        if opt and opt not in names:
            logger.error("Script inconnu : %s (connus : %s)", opt, names)
            return 1
    if args.only:
        names = [args.only]
    elif args.start:
        names = names[names.index(args.start):]

    spark = get_local_spark_session()
    # Qlik est sensible à la casse des noms de champs (IDBid ≠ IdBid)
    spark.conf.set("spark.sql.caseSensitive", "true")
    t_all = time.perf_counter()
    failed, skipped = [], []
    for name in names:
        blocked = [d for d in SCRIPTS[name]["depends_on"]
                   if d in failed + skipped]
        if blocked:
            # il lirait des données absentes ou périmées, on ne le lance pas
            logger.error("%s non lancé : dépend de %s en échec", name,
                         ", ".join(blocked))
            skipped.append(name)
            continue
        logger.info("=== %s", name)
        t0 = time.perf_counter()
        steps_failed = run_script(spark, name, SCRIPTS[name])
        if steps_failed:
            failed.append(name)
            logger.error("%s : %d étape(s) en échec : %s", name,
                         len(steps_failed), ", ".join(steps_failed))
        else:
            logger.info("%s terminé en %.0fs", name, time.perf_counter() - t0)
    if failed or skipped:
        logger.error("En échec : %s ; non lancés : %s. Reprendre avec --from "
                     "<script> une fois corrigé.", ", ".join(failed) or "aucun",
                     ", ".join(skipped) or "aucun")
        return 1
    logger.info("Terminé : %d script(s) en %.0fs", len(names),
                time.perf_counter() - t_all)
    return 0


if __name__ == "__main__":
    setup_logging()
    sys.exit(main())
'''
