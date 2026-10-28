"""Enchainement des scripts d'un projet : options de la ligne de commande,
reprise, chronometrage et erreurs."""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Sequence
from types import ModuleType

from core.internal import context, lineage
from core.internal.session import active_session

logger = logging.getLogger("core")


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


def run(args: list[str], scripts: Sequence[ModuleType]) -> None:
    """Fait tourner les scripts dans l'ordre, et s'arrete au premier echec.

    Comme un rechargement Qlik qui s'arrete a la premiere erreur : les
    scripts suivants lisent souvent ce que le script en echec devait ecrire,
    ils tourneraient sur des donnees absentes ou d'hier. Une fois corrige, on
    reprend avec --from <script>. Chaque table etant ecrite en mode
    overwrite, relancer ne cree pas de doublon.

    Args:
        args: les arguments de la ligne de commande.
        scripts: les modules des scripts, dans l'ordre.

    Raises:
        RuntimeError: un script a echoue ; le message dit d'ou reprendre.
    """
    opts = parse_options(args)
    state = context.state()
    spark = active_session()
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
