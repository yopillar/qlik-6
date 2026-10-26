"""Renomme les fichiers du projet avant toute autre etape.

    python tools/normalize.py              # renomme
    python tools/normalize.py --dry-run    # montre seulement ce qui changerait

Deux regles, lancees par migrate.py avant tout le reste :
  - les scripts de script_qvd/ passent en minuscules. Le nom du script sert
    partout ensuite (work/<script>, fichiers sql, job), et Ventes.qvs et
    ventes.qvs ne doivent pas donner deux projets differents selon le poste.
  - dans data/input/ et data/qlik_outputs/, le # des noms de fichiers devient
    un _. Spark et Hadoop lisent le # comme le debut d'un fragment d'URI et
    coupent le nom du fichier a cet endroit. Le kit fait la meme chose sur
    les chemins lus dans les scripts Qlik, donc les deux restent d'accord.

Si deux fichiers donnent le meme nom une fois renommes, on s'arrete sans
rien toucher : c'est a regler a la main.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPTS = Path("script_qvd")
DATA_DIRS = (Path("data/input"), Path("data/qlik_outputs"))


def planned_renames() -> list[tuple[Path, Path]]:
    """Les renommages a faire, (ancien, nouveau)."""
    moves = []
    if SCRIPTS.is_dir():
        for p in sorted(SCRIPTS.iterdir()):
            if p.is_file() and p.name != p.name.lower():
                moves.append((p, p.with_name(p.name.lower())))
    for d in DATA_DIRS:
        if d.is_dir():
            for p in sorted(d.iterdir()):
                if "#" in p.name:
                    moves.append((p, p.with_name(p.name.replace("#", "_"))))
    return moves


def check_collisions(moves: list[tuple[Path, Path]]) -> list[str]:
    """Noms d'arrivee deja pris, ou pris deux fois."""
    problems, seen = [], set()
    sources = {old for old, _ in moves}
    for old, new in moves:
        # sur un disque insensible a la casse, Ventes.qvs et ventes.qvs sont
        # le meme fichier : ce n'est pas une collision
        same_file = new.exists() and old.exists() and new.samefile(old)
        if (new.exists() and new not in sources and not same_file) or new in seen:
            problems.append(f"{old} -> {new} : ce nom existe deja")
        seen.add(new)
    return problems


def rename(old: Path, new: Path) -> None:
    # Passage par un nom temporaire : sur un disque insensible a la casse,
    # renommer Ventes.qvs en ventes.qvs directement ne fait rien.
    tmp = old.with_name(old.name + ".renommage")
    old.rename(tmp)
    tmp.rename(new)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run", action="store_true", help="afficher les renommages sans les faire"
    )
    args = p.parse_args(argv)
    moves = planned_renames()
    problems = check_collisions(moves)
    if problems:
        print("Renommage annule, rien n'a ete touche :")
        for line in problems:
            print(f"  {line}")
        return 1
    for old, new in moves:
        print(f"  {old} -> {new.name}")
        if not args.dry_run:
            rename(old, new)
    if moves and not args.dry_run:
        print(f"{len(moves)} fichier(s) renomme(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
