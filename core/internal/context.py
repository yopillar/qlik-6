"""Etat du job en cours : dossier des requetes, base des tables
intermediaires, script courant et ce qu'il a lu."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from core.internal.session import active_session


@dataclass
class JobState:
    """Ce que les fonctions de core/ ont besoin de savoir pendant le job."""

    job_name: str = ""  # nom donne a @pipeline
    sql_dir: Path = Path("sql")  # requetes du projet, une par table Qlik
    database: str | None = None  # base des tables intermediaires, ou None
    script: str = ""  # script Qlik en cours
    tables: list[str] = field(default_factory=list)  # vues du script en cours
    read_locations: set[str] = field(default_factory=set)  # fichiers lus


_STATE = JobState()


def state() -> JobState:
    """Etat du job en cours.

    Returns:
        L'etat partage par toutes les fonctions de core/.
    """
    return _STATE


def start_job(job_name: str) -> None:
    """Remet l'etat a zero au lancement d'un job. Le dossier des requetes
    est pose ensuite par run_scripts.

    Args:
        job_name: le nom du job.
    """
    global _STATE
    _STATE = JobState(job_name=job_name)


@contextmanager
def script_scope(name: str) -> Iterator[None]:
    """Le temps d'un script : son nom, puis le menage de ses vues.

    Les vues d'un script sont supprimees a la fin, pour que le script
    suivant ne tombe pas sur une table du meme nom.

    Args:
        name: le nom du script.

    Yields:
        Rien : le script tourne dans le bloc.
    """
    _STATE.script = name
    _STATE.tables = []
    _STATE.read_locations = set()
    try:
        yield
    finally:
        spark = active_session()
        for table in _STATE.tables:
            spark.catalog.dropTempView(table)
        _STATE.script = ""
