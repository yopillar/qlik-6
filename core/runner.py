"""Le decorateur @pipeline, point d'entree de chaque job, et l'enchainement
des scripts d'un projet.

    @pipeline("demo")
    def run(spark: SparkSession, args: list[str]) -> None:
        run_scripts(args, calendrier, ventes, synthese)
"""

from __future__ import annotations

import argparse
import functools
import inspect
import logging
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType

from pyspark.sql import SparkSession

from core.internal import context, lineage, session

logger = logging.getLogger("core")

# La fonction principale d'un job : run(spark, args).
JobFunction = Callable[[SparkSession, list[str]], None]


class Pipeline:
    """Un job : sa fonction principale et son nom."""

    def __init__(self, name: str, func: JobFunction) -> None:
        """Enregistre la fonction principale.

        Args:
            name: le nom du job.
            func: la fonction run(spark, args) du job.
        """
        self.name = name
        self.func = func
        functools.update_wrapper(self, func)

    def __call__(self, args: list[str] | None = None) -> None:
        """Lance le job : ouvre la session Spark, puis appelle la fonction
        principale.

        Args:
            args: les arguments du job ; par defaut ceux de la ligne de
                commande.
        """
        spark = session.get_session(self.name)
        context.start_job(self.name)
        self.func(spark, sys.argv[1:] if args is None else args)


def pipeline(name: str) -> Callable[[JobFunction], Pipeline]:
    """Declare la fonction principale d'un job.

    Exemple :

        @pipeline("ventes")
        def run(spark: SparkSession, args: list[str]) -> None:
            ...

        if __name__ == "__main__":
            raise SystemExit(run())

    Args:
        name: le nom du job.

    Returns:
        Le decorateur.
    """

    def wrap(func: JobFunction) -> Pipeline:
        """Enveloppe la fonction principale."""
        return Pipeline(name, func)

    return wrap


def script_name(module: ModuleType) -> str:
    """Nom d'un script : celui de son module (scripts/ventes.py -> ventes).

    Args:
        module: le module du script.

    Returns:
        Le nom, tel qu'on le passe a --from et --only.
    """
    return module.__name__.rsplit(".", 1)[-1]


def parse_options(args: list[str]) -> argparse.Namespace:
    """Options de la ligne de commande d'un job.

    Args:
        args: les arguments recus par le job.

    Returns:
        start (--from), only (--only), database (--database) et
        temp_views (--temp-views).
    """
    p = argparse.ArgumentParser(prog="job.py")
    p.add_argument("--from", dest="start", help="reprendre a ce script")
    p.add_argument("--only", help="ne lancer que ce script")
    p.add_argument(
        "--database",
        help="base des tables intermediaires, une par table Qlik "
        "(defaut : <nom du job>_qlik)",
    )
    p.add_argument(
        "--temp-views",
        action="store_true",
        help="garder les tables en vues temporaires (plus rapide, mais rien "
        "dans OpenLineage)",
    )
    return p.parse_args(args)


def select(
    scripts: Sequence[ModuleType], start: str | None, only: str | None
) -> list[ModuleType]:
    """Les scripts a lancer, d'apres --from et --only.

    Args:
        scripts: tous les scripts du job, dans l'ordre.
        start: reprendre a ce script, ou None.
        only: ne lancer que ce script, ou None.

    Returns:
        Les scripts a lancer, dans l'ordre.

    Raises:
        ValueError: --from ou --only nomme un script inconnu.
    """
    names = [script_name(m) for m in scripts]
    for wanted in (start, only):
        if wanted and wanted not in names:
            raise ValueError(f"Script inconnu : {wanted} (connus : {names})")
    if only:
        return [scripts[names.index(only)]]
    if start:
        return list(scripts[names.index(start) :])
    return list(scripts)


def _caller_sql_dir() -> Path:
    """Dossier sql/ a cote du fichier qui a appele run_scripts (job.py)."""
    frame = inspect.currentframe()
    # deux crans : _caller_sql_dir, puis run_scripts
    caller = frame.f_back.f_back if frame and frame.f_back else None
    if caller is None:
        raise RuntimeError("run_scripts : impossible de trouver le job appelant")
    return Path(caller.f_code.co_filename).resolve().parent / "sql"


def run_scripts(args: list[str], *scripts: ModuleType) -> None:
    """Fait tourner les scripts du projet, dans l'ordre donne.

    Les requetes sont lues dans le dossier sql/ a cote du job qui appelle
    cette fonction. Comme un rechargement Qlik, on s'arrete au premier
    script en echec : les suivants liraient des donnees absentes ou d'hier.
    Une fois corrige, on reprend avec --from <script>. Chaque table etant
    ecrite en mode overwrite, relancer ne cree pas de doublon. --only
    <script> n'en lance qu'un.

    Args:
        args: les arguments recus par le job.
        scripts: les scripts, dans l'ordre ou ils doivent tourner.

    Raises:
        RuntimeError: un script a echoue ; le message dit d'ou reprendre.
    """
    opts = parse_options(args)
    state = context.state()
    state.sql_dir = _caller_sql_dir()
    spark = session.active_session()
    if not opts.temp_views:
        state.database = opts.database or f"{state.job_name}_qlik"
        lineage.ensure_database(spark, state.database)
    chosen = select(scripts, opts.start, opts.only)
    t_all = time.perf_counter()
    for module in chosen:
        name = script_name(module)
        logger.info("=== %s", name)
        t0 = time.perf_counter()
        try:
            with context.script_scope(name):
                module.run()
        except Exception as exc:
            logger.error("%s en echec : %s", name, str(exc).splitlines()[0])
            raise RuntimeError(
                f"Script {name} en echec. Une fois corrige, reprendre avec "
                f"--from {name}."
            ) from exc
        logger.info("%s termine en %.0fs", name, time.perf_counter() - t0)
    logger.info(
        "Termine : %d script(s) en %.0fs", len(chosen), time.perf_counter() - t_all
    )
