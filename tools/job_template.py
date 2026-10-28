"""Gabarits du code genere pour un projet : job.py et un fichier par script.

Ce code est lu par des developpeurs Qlik qui ne connaissent pas PySpark. Il
ne contient donc que des appels aux fonctions de spark-foundation-core
(core/) : pipeline, read_input, run_sql, write_output, run_scripts. Tout ce
qui est technique (session, lecture, ecriture, tables pour OpenLineage,
reprise) est dans core/internal/, et les transformations dans
pyspark-commons (commons/).

    src/<projet>/job.py                le job : les scripts, dans l'ordre
    src/<projet>/scripts/<script>.py   un script Qlik : ses LOAD, ses tables,
                                       ses STORE
    src/<projet>/sql/                  une requete par table Qlik
"""

from __future__ import annotations

import keyword
import os
import re
import textwrap
from pathlib import Path


def project_name() -> str:
    """Nom du projet : la variable QLIK_PROJECT si elle existe, sinon le nom
    du dossier courant, ramene a un identifiant Python (mon-projet devient
    mon_projet).

    Returns:
        Le nom, qui sert de nom de package (src/<nom>/) et de nom de job.
    """
    name = os.environ.get("QLIK_PROJECT") or Path.cwd().name
    name = re.sub(r"\W", "_", name.strip().lower(), flags=re.ASCII)
    # un identifiant ne peut pas commencer par un chiffre
    return name if name and not name[0].isdigit() else f"p_{name}"


def project_dir() -> Path:
    """Dossier du projet genere.

    Returns:
        src/<nom-projet>/, qui contient job.py, scripts/ et sql/.
    """
    return Path("src") / project_name()


def module_name(script_stem: str) -> str:
    """Nom du module Python d'un script Qlik, celui qu'on passe a --from et
    --only.

    Args:
        script_stem: le nom du script sans extension (ventes).

    Returns:
        Un identifiant Python : ventes, mon_script, s_2024...
    """
    name = re.sub(r"\W", "_", script_stem.lower(), flags=re.ASCII)
    if not name or name[0].isdigit():
        name = f"s_{name}"
    return f"{name}_" if keyword.iskeyword(name) else name


JOB = '''"""Job du projet {project}.

Fichier genere par tools/orchestrate.py : ne pas le modifier a la main.

Il fait tourner les scripts Qlik du projet, dans l'ordre ou Qlik les
rechargeait : un script qui lit un fichier ecrit par un autre passe apres
lui. Le detail de chaque script est dans scripts/, ses requetes dans sql/.
Le job s'arrete au premier script en echec.

    python src/{project}/job.py                  # tout
    python src/{project}/job.py --from ventes    # reprendre a un script
    python src/{project}/job.py --only ventes    # un seul script

Sur le cluster, depuis la racine du projet :

    spark-submit --name {project} --deploy-mode client \\\\
        --py-files <spark-foundation-core et pyspark-commons, s'ils ne sont
                    pas installes> \\\\
        src/{project}/job.py [--from <script> | --only <script>]
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from core import pipeline, run_scripts
{imports}


@pipeline("{project}")
def run(spark: SparkSession, args: list[str]) -> None:
    """Fait tourner les scripts du projet, dans l'ordre.

    Args:
        spark: la session Spark, ouverte par @pipeline.
        args: les arguments de la ligne de commande (--from, --only).
    """
    run_scripts(
        args,
{calls}
    )


if __name__ == "__main__":
    raise SystemExit(run())
'''

SCRIPT = '''"""Script Qlik {script}, traduit en Spark SQL.

Fichier genere par tools/orchestrate.py : ne pas le modifier a la main.
Chaque ligne est une etape du script Qlik, dans l'ordre :
    read_input    LOAD ... FROM un fichier de conf/io.yml
    run_sql       une table Qlik, calculee par sa requete du dossier sql/
    write_output  STORE de la table dans un fichier de conf/io.yml
"""

from __future__ import annotations
{imports}

def run() -> None:
    """Rejoue le script {script}.

{summary}
    """
{body}
'''


def _summary_line(label: str, names: list[str]) -> str:
    """Une ligne de la docstring d'un script : "Lit : a, b.", repliee.

    Args:
        label: Lit, Calcule ou Ecrit.
        names: les noms a lister, dans l'ordre, sans doublon.

    Returns:
        Le texte indente pour la docstring, ou "" s'il n'y a rien.
    """
    if not names:
        return ""
    return textwrap.fill(
        f"{label} : {', '.join(dict.fromkeys(names))}.",
        width=84,
        initial_indent="    ",
        subsequent_indent="        ",
        break_on_hyphens=False,
    )


def render_job(project: str, scripts: list[tuple[str, list[str]]]) -> str:
    """Code de job.py.

    Args:
        project: le nom du projet.
        scripts: (module, modules dont il depend) pour chaque script, dans
            l'ordre ou ils tournent.

    Returns:
        Le code du job.
    """
    names = sorted(m for m, _ in scripts)
    imports = f"from scripts import {', '.join(names)}" if names else ""
    calls = "\n".join(
        f"        {m},"
        + (f"  # apres {', '.join(deps)}, dont il lit les sorties" if deps else "")
        for m, deps in scripts
    )
    return JOB.format(project=project, imports=imports, calls=calls)


def render_script(script: str, inputs: dict[str, str], steps: list[list]) -> str:
    """Code de scripts/<script>.py.

    Args:
        script: le nom du fichier du script Qlik (ventes.qvs).
        inputs: table (vue SQL) -> nom de l'entree dans conf/io.yml.
        steps: [fichier sql, table, nom de la sortie ou None, ...] dans
            l'ordre du script.

    Returns:
        Le code du module.
    """
    lines = [f'    read_input("{io}", table="{view}")' for view, io in inputs.items()]
    for sql_file, table, output, *_ in steps:
        lines.append(f'    run_sql("{sql_file}", table="{table}")')
        if output:
            lines.append(f'    write_output("{output}", table="{table}")')
    used = ["read_input"] if inputs else []
    used += ["run_sql"] if steps else []
    used += ["write_output"] if any(st[2] for st in steps) else []
    summary = [
        _summary_line("Lit", list(inputs.values())),
        _summary_line("Calcule", [st[1] for st in steps]),
        _summary_line("Ecrit", [st[2] for st in steps if st[2]]),
    ]
    return SCRIPT.format(
        script=script,
        imports=f"\nfrom core import {', '.join(used)}\n" if used else "",
        summary="\n".join(x for x in summary if x) or "    Rien a faire.",
        body="\n".join(lines) or "    pass",
    )
