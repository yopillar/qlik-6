"""Gabarits du code genere pour un projet : job.py et un fichier par script.

Ce code est lu par des developpeurs Qlik qui ne connaissent pas PySpark. Il
ne contient donc que des appels aux fonctions du socle spark_foundation :
    spark_foundation            pipeline (runner.py) : le job
    spark_foundation.runner     script : la fonction run() d'un script Qlik
    spark_foundation.datasets   read_input, write_output : lire et ecrire
    spark_foundation.tables     run_sql : calculer une table Qlik
Tout ce qui est technique (session, io.yml, lecture, ecriture, tables pour
OpenLineage, reprise) est dans les autres fichiers du socle, et les
transformations dans pyspark_commons.

    src/<projet>/job.py          le job : il appelle les scripts, dans l'ordre
    src/<projet>/<script>.py     un script Qlik : ses LOAD, ses tables, ses
                                 STORE (a cote de job.py, pas de dossier)
    src/<projet>/sql/            une requete par table Qlik
"""

from __future__ import annotations

import json
import keyword
import os
import re
import sys
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
        src/<nom-projet>/, qui contient job.py, un fichier par script et sql/.
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
    if keyword.iskeyword(name) or name in TAKEN_NAMES:
        # Les scripts sont des fichiers a cote de job.py : un script nomme
        # calendar ou json cacherait le module Python du meme nom.
        name += "_qlik"
    return name


# Noms deja pris : job.py, le socle, et les modules de Python.
TAKEN_NAMES = {
    "job",
    "pyspark",
    "py4j",
    "spark_foundation",
    "pyspark_commons",
    "core",
    "commons",
    *getattr(sys, "stdlib_module_names", ()),
}


JOB = '''"""Job du projet {project}.

Fichier genere par tools/orchestrate.py : ne pas le modifier a la main.

Il fait tourner les scripts Qlik du projet, dans l'ordre ou Qlik les
rechargeait : un script qui lit un fichier ecrit par un autre passe apres
lui. Chaque script est un fichier a cote de celui-ci, ses requetes sont
dans sql/. Le job s'arrete au premier script en echec.

    python src/{project}/job.py                  # tout
    python src/{project}/job.py --from ventes    # reprendre a un script
    python src/{project}/job.py --only ventes    # un seul script

Sur le cluster, depuis la racine du projet :

    spark-submit --name {project} --deploy-mode client \\\\
        --py-files <spark_foundation et pyspark_commons, s'ils ne sont
                    pas installes> \\\\
        src/{project}/job.py [--from <script> | --only <script>]
"""

from __future__ import annotations

{imports}from pyspark.sql import SparkSession

from spark_foundation import pipeline


@pipeline("{project}")
def run(spark: SparkSession, args: list[str]) -> None:
    """Fait tourner les scripts du projet, dans l'ordre.

    Args:
        spark: la session Spark, ouverte par @pipeline.
        args: les arguments de la ligne de commande (--from, --only), deja
            pris en compte par @pipeline.
    """
{calls}

if __name__ == "__main__":
    raise SystemExit(run())
'''

SCRIPT = '''"""Script Qlik {script}, traduit en Spark SQL.

Fichier genere par tools/orchestrate.py : ne pas le modifier a la main.
Le script lit ses fichiers d'entree, puis calcule ses tables une par une,
dans l'ordre du script Qlik, et ecrit celles qui avaient un STORE.
"""

from __future__ import annotations

from spark_foundation.datasets import read_input, write_output
from spark_foundation.runner import script
from spark_foundation.tables import run_sql

# Fichiers lus par le script (LOAD ... FROM) :
# nom dans conf/io.yml -> nom de la table dans les requetes SQL
INPUTS: dict[str, str] = {inputs}

# Tables du script, dans l'ordre :
# (requete du dossier sql/, nom de la table, sortie dans conf/io.yml ou None)
TABLES: list[tuple[str, str, str | None]] = [{tables}]


@script
def run() -> None:
    """Rejoue le script {script}."""
    for name, table in INPUTS.items():
        read_input(name, table=table)
    for sql_file, table, output in TABLES:
        run_sql(sql_file, table=table)
        if output:
            write_output(output, table=table)
'''


def render_job(project: str, scripts: list[tuple[str, list[str]]]) -> str:
    """Code de job.py.

    Args:
        project: le nom du projet.
        scripts: (module, modules dont il depend) pour chaque script, dans
            l'ordre ou ils tournent.

    Returns:
        Le code du job.
    """
    imports = "".join(f"import {m}\n" for m in sorted(m for m, _ in scripts))
    calls = (
        "".join(
            f"    {m}.run()"
            + (f"  # apres {', '.join(deps)}, dont il lit les sorties" if deps else "")
            + "\n"
            for m, deps in scripts
        )
        or "    pass\n"
    )
    return JOB.format(project=project, imports=imports, calls=calls)


def _literal(value: str | None) -> str:
    """Valeur Python ecrite dans le code genere : "texte" ou None."""
    return "None" if value is None else json.dumps(value)


def _table_line(sql_file: str, table: str, output: str | None) -> str:
    """Une ligne de TABLES, mise en forme comme le ferait ruff format.

    Args:
        sql_file: la requete.
        table: le nom de la table.
        output: la sortie, ou None.

    Returns:
        La ligne, ou le tuple sur plusieurs lignes s'il depasse 88 caracteres.
    """
    values = [_literal(sql_file), _literal(table), _literal(output)]
    line = f"    ({', '.join(values)}),"
    if len(line) <= 88:
        return line
    inner = "".join(f"        {v},\n" for v in values)
    return f"    (\n{inner}    ),"


def render_script(script: str, inputs: dict[str, str], steps: list[list]) -> str:
    """Code de <script>.py : deux listes et deux boucles, quel que
    soit le nombre de tables.

    Args:
        script: le nom du fichier du script Qlik (ventes.qvs).
        inputs: table (vue SQL) -> nom de l'entree dans conf/io.yml.
        steps: [fichier sql, table, nom de la sortie ou None, ...] dans
            l'ordre du script.

    Returns:
        Le code du module.
    """
    entries = "".join(
        f"    {_literal(io)}: {_literal(view)},\n" for view, io in inputs.items()
    )
    tables = "".join(
        _table_line(sql_file, table, output or None) + "\n"
        for sql_file, table, output, *_ in steps
    )
    return SCRIPT.format(
        script=script,
        inputs=f"{{\n{entries}}}" if entries else "{}",
        tables=f"\n{tables}" if tables else "",
    )
