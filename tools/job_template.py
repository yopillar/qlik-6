"""Gabarit de src/<nom-projet>/job.py, le job du projet.

Le job s'appuie sur spark-foundation-core, le socle technique : c'est lui
qui cree la session Spark, configure les logs et sait ou lire et ecrire
chaque entree et sortie declaree dans conf/io.yml. pyspark-commons ne garde
que les fonctions de transformation, dont le SQL genere n'a pas besoin : le
job ne l'importe donc plus.

Tout le code genere passe par ici pour appeler le socle, et nulle part
ailleurs. Si son API change, on corrige ce fichier et on relance
python tools/orchestrate.py (ou migrate.py).

API du socle utilisee, d'apres un job existant (dp_test) :
    @spark_foundation.pipeline(nom)    decore run(spark, args) ; le job se
                                       lance par raise SystemExit(run())
    spark_foundation.read_input(nom) -> DataFrame
    spark_foundation.write_output(df, nom)
Appele sans argument, le traitement recoit les arguments de la ligne de
commande, d'ou --from et --only. Ce qu'on suppose sans l'avoir verifie :
une exception levee dans run() donne un code retour non nul.
"""

import os
import re
from pathlib import Path


def project_name() -> str:
    """Nom du projet : la variable QLIK_PROJECT si elle existe, sinon le
    nom du dossier courant.

    Il sert a la fois de nom de package Python (src/<nom>/, comme dp_test)
    et de nom de traitement pour @pipeline. On le ramene donc a un
    identifiant Python : minuscules, ASCII, et "_" a la place de tout le
    reste (mon-projet devient mon_projet)."""
    name = os.environ.get("QLIK_PROJECT") or Path.cwd().name
    name = re.sub(r"\W", "_", name.strip().lower(), flags=re.ASCII)
    # un identifiant ne peut pas commencer par un chiffre
    return name if name and not name[0].isdigit() else f"p_{name}"


def project_dir() -> Path:
    """Dossier src/<nom-projet>/, qui contient job.py et sql/."""
    return Path("src") / project_name()


JOB = '''"""Job du projet {project}.

Fichier genere par tools/orchestrate.py : ne pas le modifier a la main, il
sera ecrase a la prochaine generation.

Il enchaine les scripts Qlik migres depuis script_qvd/, dans l'ordre de
leurs dependances (un script qui lit un fichier ecrit par un autre passe
apres lui). Les requetes sont dans sql/, une par table Qlik. Pour chaque
script :
  1. chaque entree qu'il lit est chargee avec read_input() et devient une
     vue SQL (in_<nom>)
  2. les requetes tournent dans l'ordre du script, et chaque resultat devient
     une vue au nom de la table Qlik, que les requetes suivantes peuvent lire
  3. les tables qui avaient un STORE dans Qlik sont ecrites avec
     write_output()
Les noms passes a read_input() et write_output() sont ceux de conf/io.yml,
ou spark-foundation-core trouve la location et le format.

Une etape qui plante est loggee mais n'empeche pas les autres de tourner. En
revanche, un script qui depend d'un script en echec n'est pas lance. S'il y
a eu le moindre echec, le job se termine par une erreur qui liste les
scripts concernes.

    python src/{project}/job.py                  # tout
    python src/{project}/job.py --from ventes    # reprendre a un script
    python src/{project}/job.py --only ventes    # un seul script

Sur le cluster, depuis la racine du projet (conf/io.yml et les chemins
relatifs partent du dossier courant) :

    spark-submit \\
        --name {project} \\
        --deploy-mode client \\
        --py-files <archive de spark-foundation-core, s'il n'est pas installe> \\
        src/{project}/job.py [--from <script> | --only <script>]

En --deploy-mode client, le driver tourne ici et lit les requetes dans
src/{project}/sql/. En mode cluster, il faudrait aussi envoyer ce dossier
et conf/ (--archives) : non prevu pour l'instant. Les reglages SQL dont
dependent les requetes (casse, ANSI, analyse des dates) sont poses par le
job lui-meme, au debut de run() : pas besoin de --conf.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_foundation import pipeline, read_input, write_output

SQL_DIR = Path(__file__).resolve().parent / "sql"
ORDER = "_qlik_order"     # ordre de chargement Qlik, ecrit dans les sorties
logger = logging.getLogger("{project}")

# Les scripts, dans l'ordre ou ils tournent.
#   inputs : vue SQL -> nom de l'entree dans conf/io.yml
#   steps  : (fichier SQL, vue produite, nom de la sortie dans conf/io.yml
#             ou None, True si le script relit ce qu'il ecrit)
SCRIPTS = {{
{scripts}
}}


def with_order(df):
    """Ajoute la colonne _qlik_order, qui porte l'ordre de chargement Qlik.

    Si le fichier vient de ce job, elle y est deja. Sinon on numerote les
    lignes dans l'ordre de lecture, et il faut le faire avant toute autre
    operation, sinon l'ordre est perdu."""
    if ORDER not in df.columns:
        return df.withColumn(ORDER, F.array(F.monotonically_increasing_id()))
    if not dict(df.dtypes)[ORDER].startswith("array"):
        return df.withColumn(ORDER, F.array(F.col(ORDER).cast("bigint")))
    return df


def table_name(database: str, script: str, view: str) -> str:
    """Table intermediaire d'une etape : <base>.<script>__<vue>."""
    return f"{{database}}.{{script}}__{{view}}".lower()


def materialize(spark: SparkSession, df, table: str):
    """Ecrit l'etape en Parquet dans une table du catalogue et la relit.

    C'est pour OpenLineage : une vue temporaire n'est pas un jeu de donnees,
    donc la lignee sautait directement des fichiers lus aux sorties, sans
    les tables Qlik entre les deux. Avec une vraie table par etape, chaque
    transformation remonte avec ses entrees et sa sortie.

    Une table du catalogue ne distingue pas la casse des colonnes : si deux
    colonnes ne different que par la casse (IdBid et IDBid, possible dans
    Qlik), on garde la vue temporaire pour cette etape."""
    lowered = [c.lower() for c in df.columns]
    if len(set(lowered)) < len(lowered):
        logger.warning("%s : colonnes qui ne different que par la casse, "
                       "etape gardee en vue temporaire (absente de la lignee)",
                       table)
        return df.localCheckpoint()
    df.write.mode("overwrite").format("parquet").saveAsTable(table)
    return spark.table(table)


def run_script(spark: SparkSession, name: str, script: dict,
               database: str | None) -> list[str]:
    """Fait tourner un script et renvoie la liste des etapes en echec.
    Avec database, chaque etape est ecrite dans une table de cette base."""
    failures = []
    for view, io_name in script["inputs"].items():
        try:
            with_order(read_input(io_name)).createOrReplaceTempView(view)
        except Exception as exc:                 # fichier absent, illisible...
            logger.error("%s : lecture de %s (%s) impossible : %s", name,
                         io_name, view, exc)
            failures.append(f"lecture {{io_name}}")
    for sql_file, view, output, rereads in script["steps"]:
        t0 = time.perf_counter()
        try:
            df = spark.sql((SQL_DIR / sql_file).read_text(encoding="utf-8"))
            if database:
                # la table coupe aussi la lignee, comme localCheckpoint
                df = materialize(spark, df, table_name(database, name, view))
            elif rereads:
                # Le script ecrase un fichier qu'il a lu : on coupe la
                # lignee, sinon Spark relirait le fichier en l'ecrivant.
                df = df.localCheckpoint()
            df.createOrReplaceTempView(view)
            if output:
                write_output(df, output)
                # Spark est paresseux : c'est l'ecriture qui declenche le
                # calcul, donc on ne chronometre que les etapes qui ecrivent.
                logger.info("%s / %s ecrit en %.1fs", name, output,
                            time.perf_counter() - t0)
        except Exception as exc:
            # On ne s'arrete pas : les etapes qui dependent de celle-ci
            # planteront a leur tour (vue absente), les autres iront au bout.
            logger.error("%s : etape %s (%s) en echec : %s", name, sql_file,
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
    p.add_argument("--from", dest="start", help="reprendre a ce script")
    p.add_argument("--only", help="ne lancer que ce script")
    p.add_argument("--database", default="{project}_qlik",
                   help="base des tables intermediaires, une par table Qlik "
                        "(defaut : {project}_qlik)")
    p.add_argument("--temp-views", action="store_true",
                   help="garder les etapes en vues temporaires, sans table "
                        "(plus rapide, mais rien dans OpenLineage)")
    opts = p.parse_args(args)
    database = None if opts.temp_views else opts.database
    if database:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {{database}}")
    names = list(SCRIPTS)
    for opt in (opts.start, opts.only):
        if opt and opt not in names:
            raise ValueError(f"Script inconnu : {{opt}} (connus : {{names}})")
    if opts.only:
        names = [opts.only]
    elif opts.start:
        names = names[names.index(opts.start):]

    # Qlik est sensible a la casse des noms de champs (IDBid != IdBid)
    spark.conf.set("spark.sql.caseSensitive", "true")
    # Les requetes sont verifiees hors mode ANSI (check.py) : une conversion
    # impossible y donne null, comme dans Qlik, au lieu d'une erreur.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Sinon try_to_timestamp leve une erreur sur une date incoherente au
    # lieu de renvoyer null.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    t_all = time.perf_counter()
    failed, skipped = [], []
    for name in names:
        blocked = [d for d in SCRIPTS[name]["depends_on"]
                   if d in failed + skipped]
        if blocked:
            # il lirait des donnees absentes ou perimees, on ne le lance pas
            logger.error("%s non lance : depend de %s en echec", name,
                         ", ".join(blocked))
            skipped.append(name)
            continue
        logger.info("=== %s", name)
        t0 = time.perf_counter()
        steps_failed = run_script(spark, name, SCRIPTS[name], database)
        if steps_failed:
            failed.append(name)
            logger.error("%s : %d etape(s) en echec : %s", name,
                         len(steps_failed), ", ".join(steps_failed))
        else:
            logger.info("%s termine en %.0fs", name, time.perf_counter() - t0)
    if failed or skipped:
        # On leve une erreur plutot que de renvoyer un code : run() est
        # type -> None par le socle, et une exception donne de toute facon
        # un code retour non nul.
        raise RuntimeError(
            f"En echec : {{', '.join(failed) or 'aucun'}} ; non lances : "
            f"{{', '.join(skipped) or 'aucun'}}. Reprendre avec --from "
            "<script> une fois corrige.")
    logger.info("Termine : %d script(s) en %.0fs", len(names),
                time.perf_counter() - t_all)


if __name__ == "__main__":
    raise SystemExit(run())
'''
