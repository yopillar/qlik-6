"""Le decorateur @pipeline, point d'entree de chaque job."""

from __future__ import annotations

import functools
import inspect
import sys
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import SparkSession

from core.internal import context, session

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
        """Lance le job.

        Ouvre la session Spark, trouve les requetes dans le dossier sql/ a
        cote du job, puis appelle la fonction principale.

        Args:
            args: les arguments du job ; par defaut ceux de la ligne de
                commande.
        """
        spark = session.get_session(self.name)
        sql_dir = Path(inspect.getfile(self.func)).resolve().parent / "sql"
        context.start_job(self.name, sql_dir)
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
