"""Le decorateur @pipeline, point d'entree de chaque job, et le decorateur
@script, pose sur chaque script Qlik du projet.

    @pipeline("demo")
    def run(spark: SparkSession, args: list[str]) -> None:
        calendrier.run()
        ventes.run()
        synthese.run()
"""

from __future__ import annotations

import argparse
import functools
import inspect
import logging
import sys
import time
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import SparkSession

from core import context, session

logger = logging.getLogger("core")

# La fonction principale d'un job : run(spark, args).
JobFunction = Callable[[SparkSession, list[str]], None]
# La fonction run() d'un script Qlik.
ScriptFunction = Callable[[], None]


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
        """Lance le job : ouvre la session Spark, lit les options (--from,
        --only, --database, --temp-views), puis appelle la fonction
        principale. Les arguments qu'il ne connait pas sont laisses au job.

        Args:
            args: les arguments du job ; par defaut ceux de la ligne de
                commande.
        """
        spark = session.get_session(self.name)
        argv = sys.argv[1:] if args is None else args
        opts = parse_options(argv)
        context.start_job(
            self.name,
            # les requetes sont dans sql/, a cote du job
            sql_dir=Path(inspect.getfile(self.func)).resolve().parent / "sql",
            database=None if opts.temp_views else opts.database or f"{self.name}_qlik",
            start=opts.start,
            only=opts.only,
        )
        self.func(spark, argv)
        state = context.state()
        wanted = opts.only or opts.start
        if wanted and wanted not in state.seen:
            raise ValueError(f"Script inconnu : {wanted} (connus : {state.seen})")


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


def parse_options(args: list[str]) -> argparse.Namespace:
    """Options de la ligne de commande d'un job migre depuis Qlik.

    Args:
        args: les arguments recus par le job ; ceux qu'on ne connait pas
            sont ignores ici.

    Returns:
        start (--from), only (--only), database (--database) et
        temp_views (--temp-views).
    """
    p = argparse.ArgumentParser(prog="job.py", add_help=False)
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
    return p.parse_known_args(args)[0]


def _should_run(name: str) -> bool:
    """Dit si le script doit tourner, d'apres --from et --only."""
    state = context.state()
    if state.only:
        return name == state.only
    if state.start and name == state.start:
        state.started = True
    return state.started or not state.start


def script(func: ScriptFunction) -> ScriptFunction:
    """Declare la fonction run() d'un script Qlik.

    Le job appelle les scripts dans l'ordre. Chacun tourne avec ses propres
    vues, supprimees a la fin, et un script en echec arrete le job, comme
    un rechargement Qlik : les suivants liraient des donnees absentes ou
    d'hier. Le message dit comment reprendre (--from <script>) ; chaque
    table etant ecrite en overwrite, relancer ne cree pas de doublon.

    Args:
        func: la fonction run() du script.

    Returns:
        La meme fonction, qui respecte --from et --only.
    """
    name = func.__module__.rsplit(".", 1)[-1]

    @functools.wraps(func)
    def run() -> None:
        """Fait tourner le script, sauf si --from ou --only l'excluent."""
        context.state().seen.append(name)
        if not _should_run(name):
            logger.info("%s saute (--from / --only)", name)
            return
        logger.info("=== %s", name)
        t0 = time.perf_counter()
        try:
            with context.script_scope(name):
                func()
        except Exception as exc:
            logger.error("%s en echec : %s", name, str(exc).splitlines()[0])
            raise RuntimeError(
                f"Script {name} en echec. Une fois corrige, reprendre avec "
                f"--from {name}."
            ) from exc
        logger.info("%s termine en %.0fs", name, time.perf_counter() - t0)

    return run
