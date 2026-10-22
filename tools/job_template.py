"""Gabarit de src/<nom-projet>/job.py, le job du projet.

Le job s'appuie sur spark-foundation-core, le socle technique : c'est lui
qui crée la session Spark, configure les logs et sait où lire et écrire
chaque entrée et sortie déclarée dans conf/io.yml. pyspark-commons ne garde
que les fonctions de transformation, dont le SQL généré n'a pas besoin : le
job ne l'importe donc plus.

Tout le code généré passe par ici pour appeler le socle, et nulle part
ailleurs. Si son API change, on corrige ce fichier et on relance
python tools/orchestrate.py (ou migrate.py).

API du socle utilisée, d'après un job existant (dp_test) :
    @spark_foundation.pipeline(nom)    décore run(spark, args) ; le job se
                                       lance par raise SystemExit(run())
    spark_foundation.read_input(nom) -> DataFrame
    spark_foundation.write_output(df, nom)
Appelé sans argument, le traitement reçoit les arguments de la ligne de
commande, d'où --from et --only. Ce qu'on suppose sans l'avoir vérifié :
une exception levée dans run() donne un code retour non nul.
"""

import os
import re
from pathlib import Path


def project_name() -> str:
    """Nom du projet : la variable QLIK_PROJECT si elle existe, sinon le
    nom du dossier courant.

    Il sert à la fois de nom de package Python (src/<nom>/, comme dp_test)
    et de nom de traitement pour @pipeline. On le ramène donc à un
    identifiant Python : minuscules, ASCII, et « _ » à la place de tout le
    reste (mon-projet devient mon_projet)."""
    name = os.environ.get("QLIK_PROJECT") or Path.cwd().name
    name = re.sub(r"\W", "_", name.strip().lower(), flags=re.ASCII)
    # un identifiant ne peut pas commencer par un chiffre
    return name if name and not name[0].isdigit() else f"p_{name}"


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
  1. chaque entrée qu'il lit est chargée avec read_input() et devient une
     vue SQL (in_<nom>)
  2. les requêtes tournent dans l'ordre du script, et chaque résultat devient
     une vue au nom de la table Qlik, que les requêtes suivantes peuvent lire
  3. les tables qui avaient un STORE dans Qlik sont écrites avec
     write_output()
Les noms passés à read_input() et write_output() sont ceux de conf/io.yml,
où spark-foundation-core trouve la location et le format.

Une étape qui plante est loggée mais n'empêche pas les autres de tourner. En
revanche, un script qui dépend d'un script en échec n'est pas lancé. S'il y
a eu le moindre échec, le job se termine par une erreur qui liste les
scripts concernés.

    python src/{project}/job.py                  # tout
    python src/{project}/job.py --from ventes    # reprendre à un script
    python src/{project}/job.py --only ventes    # un seul script
"""

import argparse
import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_foundation import pipeline, read_input, write_output

SQL_DIR = Path(__file__).resolve().parent / "sql"
ORDER = "_qlik_order"     # ordre de chargement Qlik, écrit dans les sorties
logger = logging.getLogger("{project}")

# Les scripts, dans l'ordre où ils tournent.
#   inputs : vue SQL -> nom de l'entrée dans conf/io.yml
#   steps  : (fichier SQL, vue produite, nom de la sortie dans conf/io.yml
#             ou None, True si le script relit ce qu'il écrit)
SCRIPTS = {{
{scripts}
}}


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


def run_script(spark: SparkSession, name: str, script: dict) -> list[str]:
    """Fait tourner un script et renvoie la liste des étapes en échec."""
    failures = []
    for view, io_name in script["inputs"].items():
        try:
            with_order(read_input(io_name)).createOrReplaceTempView(view)
        except Exception as exc:                 # fichier absent, illisible…
            logger.error("%s : lecture de %s (%s) impossible : %s", name,
                         io_name, view, exc)
            failures.append(f"lecture {{io_name}}")
    for sql_file, view, output, rereads in script["steps"]:
        t0 = time.perf_counter()
        try:
            df = spark.sql((SQL_DIR / sql_file).read_text(encoding="utf-8"))
            if rereads:
                # Le script écrase un fichier qu'il a lu : on coupe la
                # lignée, sinon Spark relirait le fichier en l'écrivant.
                df = df.localCheckpoint()
            df.createOrReplaceTempView(view)
            if output:
                write_output(df, output)
                # Spark est paresseux : c'est l'écriture qui déclenche le
                # calcul, donc on ne chronomètre que les étapes qui écrivent.
                logger.info("%s / %s écrit en %.1fs", name, output,
                            time.perf_counter() - t0)
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


@pipeline("{project}")
def run(spark: SparkSession, args: list[str]) -> None:
    p = argparse.ArgumentParser(prog="job.py",
                                description="Job du projet {project}.")
    p.add_argument("--from", dest="start", help="reprendre à ce script")
    p.add_argument("--only", help="ne lancer que ce script")
    opts = p.parse_args(args)
    names = list(SCRIPTS)
    for opt in (opts.start, opts.only):
        if opt and opt not in names:
            raise ValueError(f"Script inconnu : {{opt}} (connus : {{names}})")
    if opts.only:
        names = [opts.only]
    elif opts.start:
        names = names[names.index(opts.start):]

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
        # On lève une erreur plutôt que de renvoyer un code : run() est
        # typé -> None par le socle, et une exception donne de toute façon
        # un code retour non nul.
        raise RuntimeError(
            f"En échec : {{', '.join(failed) or 'aucun'}} ; non lancés : "
            f"{{', '.join(skipped) or 'aucun'}}. Reprendre avec --from "
            "<script> une fois corrigé.")
    logger.info("Terminé : %d script(s) en %.0fs", len(names),
                time.perf_counter() - t_all)


if __name__ == "__main__":
    raise SystemExit(run())
'''
