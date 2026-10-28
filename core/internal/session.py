"""Creation et reglages de la session Spark."""

from __future__ import annotations

from pyspark.sql import SparkSession

# Reglages dont dependent les requetes traduites depuis Qlik.
QLIK_SETTINGS = {
    # Qlik est sensible a la casse des noms de champs (IDBid et IdBid)
    "spark.sql.caseSensitive": "true",
    # Les requetes sont verifiees hors mode ANSI (check.py) : une conversion
    # impossible y donne null, comme dans Qlik, au lieu d'une erreur.
    "spark.sql.ansi.enabled": "false",
    # Sinon try_to_timestamp leve une erreur sur une date incoherente.
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
}


def get_session(app_name: str) -> SparkSession:
    """Renvoie la session Spark du job, avec les reglages Qlik.

    Le support Hive est active : les tables intermediaires sont declarees
    dans un metastore. Sur le cluster, c'est celui du cluster. En local, Spark
    en cree un dans le dossier courant (metastore_db/), ce qui permet de
    relancer un job : sans metastore, Spark oublie les tables entre deux
    lancements mais garde leurs fichiers, et refuse ensuite de les recreer.

    Args:
        app_name: nom du job, affiche dans l'interface Spark.

    Returns:
        La session, creee si besoin.
    """
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    for key, value in QLIK_SETTINGS.items():
        spark.conf.set(key, value)
    return spark


def active_session() -> SparkSession:
    """Renvoie la session ouverte par @pipeline.

    Returns:
        La session active.

    Raises:
        RuntimeError: aucune session ouverte (fonction appelee hors d'un job).
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "Aucune session Spark : read_input, run_sql et write_output "
            "s'appellent depuis un job decore par @pipeline."
        )
    return spark
