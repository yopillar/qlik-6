"""Renomme les fichiers du projet avant toute autre etape.

    python tools/normalize.py              # renomme
    python tools/normalize.py --dry-run    # montre seulement ce qui changerait

Deux regles, lancees par migrate.py avant tout le reste :
  - les scripts de script_qvd/ passent en minuscules. Le nom du script sert
    partout ensuite (work/<script>, fichiers sql, job), et Ventes.qvs et
    ventes.qvs ne doivent pas donner deux projets differents selon le poste.
  - dans data/input/ et data/qlik_outputs/, les noms de fichiers perdent
    les caracteres que Hadoop ne sait pas lire (voir conf.safe_name) :
    [anon]-TT#TEST.CSV devient (anon)-TT_TEST.CSV. Le kit fait la meme
    chose sur les chemins lus dans les scripts Qlik, donc les deux restent
    d'accord.

Si deux fichiers donnent le meme nom une fois renommes, on s'arrete sans
rien toucher : c'est a regler a la main.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conf import safe_name  # noqa: E402

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
                if safe_name(p.name) != p.name:
                    moves.append((p, p.with_name(safe_name(p.name))))
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
    """Renomme un fichier, meme sur un disque insensible a la casse."""
    # Passage par un nom temporaire : sur un disque insensible a la casse,
    # renommer Ventes.qvs en ventes.qvs directement ne fait rien.
    tmp = old.with_name(old.name + ".renommage")
    old.rename(tmp)
    tmp.rename(new)


def main(argv: list[str] | None = None) -> int:
    """Point d'entree en ligne de commande.

    Args:
        argv: les arguments ; par defaut ceux de la ligne de commande.

    Returns:
        Le code retour du programme.
    """
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
    # Spark ignore sans rien dire un fichier dont le nom commence par _ ou .
    # (comme _SUCCESS) : il serait lu vide. On ne le renomme pas, on previent.
    for d in DATA_DIRS:
        if d.is_dir():
            for f in sorted(d.iterdir()):
                if f.is_file() and f.name.startswith(("_", ".")):
                    print(
                        f"[!] {f} : Spark ignore les fichiers qui commencent "
                        "par _ ou ., il sera lu vide. A renommer."
                    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
