"""Migration complete du projet de demo, avec Spark et la doublure du socle :
deux scripts chaines (synthese lit la sortie de ventes), un calendrier, un
script en .txt et un sans extension, un io.yml a 3 espaces commente, une
entree CSV et une sortie avec "#" dans leur nom, une table du modele de
donnees sans STORE."""

import os
import shutil
import subprocess
import sys

import pandas as pd
import pytest

from conftest import JOB_PATH, ROOT, Project

pytestmark = pytest.mark.spark


def test_migrate_demo_identical_to_qlik(project: Project) -> None:
    """La migration complete du projet de demo donne les memes donnees que Qlik."""
    res = project.run(
        "migrate.py", "script_qvd/", "--skip-selftest", "--var", "sorties=data/output"
    )
    summary = res.stdout.split("RESUME", 1)[1]
    for script in ("calendrier", "ventes.qvs", "synthese.txt"):
        assert (
            f"- {script} : assemble dans src/, donnees identiques a Qlik" in summary
        ), res.stdout
    # ventes passe avant synthese, qui lit ce qu'il ecrit
    assert summary.index("ventes.qvs") < summary.index("synthese.txt")

    # l'ordre de chargement est garde, jusque dans la sortie ecrite
    project.run(
        "verify.py",
        "work/ventes",
        "--no-run",
        "--strict-order",
        "--var",
        "sorties=data/output",
    )
    # la sortie ventes_enrichies#v2 est ecrite sans le #
    out = pd.read_parquet(project / "data/output/ventes_enrichies_v2")
    # _qlik_order ne sert qu'a l'interieur du job : absente des sorties
    assert "_qlik_order" not in out.columns
    assert sorted(out["Produit"]) == ["A", "B", "C"]

    # la table du modele de donnees (Dates, sans STORE) est ecrite et
    # comparee, dans l'ordre de chargement
    assert "Dates" in (project / "work/calendrier/comparaison.md").read_text(
        encoding="utf-8"
    )
    project.run(
        "verify.py",
        "work/calendrier",
        "--no-run",
        "--strict-order",
        "--var",
        "sorties=data/output",
    )
    job = (project / "src/demo/job.py").read_text(encoding="utf-8")
    assert "spark-submit" in job


def test_csv_without_header(project: Project) -> None:
    """Un CSV sans en-tete (no labels) donne le meme resultat que Qlik."""
    # (txt, no labels) : Qlik nomme les colonnes @1, @2... ; on les lit en
    # _c0, _c1... et le resultat doit etre celui de Qlik
    (project / "script_qvd/codes.qvs").write_text(
        "Codes:\n"
        "LOAD @1 as Code, Upper(@3) as Libelle, Null() as Vide\n"
        "FROM [lib://data/codes.csv] (txt, no labels, delimiter is ';');\n"
        "STORE Codes INTO [lib://out/codes_sortie.qvd] (qvd);\n"
    )
    (project / "data/input/codes.csv").write_text("1;x;abc\n2;y;def\n")
    (project / "data/qlik_outputs/codes_sortie.csv").write_text(
        "Code,Libelle,Vide\n1,ABC,\n2,DEF,\n"
    )
    res = project.run(
        "migrate.py",
        "script_qvd/codes.qvs",
        "--skip-selftest",
        "--var",
        "sorties=data/output",
    )
    assert "- codes.qvs : assemble dans src/, donnees identiques a Qlik" in (
        res.stdout
    ), res.stdout
    # Null() donne une colonne void, que Parquet refuse : ecrite en string
    import pyarrow.parquet as pq

    schema = pq.read_schema(
        next((project / "data/output/codes_sortie").glob("*.parquet"))
    )
    assert str(schema.field("Vide").type) == "string"
    out = pd.read_parquet(project / "data/output/codes_sortie")
    assert out["Vide"].isna().all()


def test_job_is_simple_and_each_table_is_in_the_catalog(project: Project) -> None:
    """Le job ne contient que des appels au socle, et chaque table Qlik est une
    table du catalogue (pour OpenLineage)."""
    project.run(
        "migrate.py", "script_qvd/", "--skip-selftest", "--var", "sorties=data/output"
    )
    job = (project / "src/demo/job.py").read_text(encoding="utf-8")
    # rien de technique dans le job : pipeline et un appel par script
    assert "from spark_foundation import pipeline" in job
    assert "run_scripts" not in job and len(job.splitlines()) < 60
    # l'ordre des scripts est l'ordre des appels
    assert (
        job.index("calendrier.run()")
        < job.index("ventes.run()")
        < job.index("synthese.run()")
    )
    # les scripts sont des fichiers a cote de job.py, sans dossier (hors le
    # cache que Python cree en lancant le job)
    src = project / "src/demo"
    folders = sorted(p.name for p in src.iterdir() if p.is_dir())
    assert [f for f in folders if f != "__pycache__"] == ["sql"]
    ventes = (src / "ventes.py").read_text(encoding="utf-8")
    assert "from spark_foundation.runner import script" in ventes
    assert "@script\ndef run() -> None:" in ventes
    assert (
        "from spark_foundation.datasets import read_input, write_output\n"
        "from spark_foundation.runner import script\n"
        "from spark_foundation.tables import run_sql\n"
    ) in ventes
    # une liste des entrees, une liste des tables, et deux boucles
    assert '"ventes": "in_ventes",' in ventes
    assert '("ventes__01_ventes.sql", "Ventes", "ventes_enrichies_v2"),' in ventes
    assert ventes.count("read_input(") == 1 and ventes.count("run_sql(") == 1
    check_generated_code(project)
    # le metastore local garde les tables d'un lancement a l'autre ; on
    # relance ventes pour verifier qu'une table existante est bien remplacee
    project.run_job("--only", "ventes")
    # reprise a un script, et nom de script inconnu
    project.run_job("--from", "synthese")
    with pytest.raises(AssertionError, match="Script inconnu : ventess"):
        project.run_job("--only", "ventess")
    check = (
        "from core.session import get_session\n"
        "spark = get_session('demo')\n"
        "print(sorted(t.name for t in spark.catalog.listTables('demo_qlik')))\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", check],
        cwd=project.path,
        env=dict(os.environ, PYTHONPATH=str(ROOT)),
        capture_output=True,
        text=True,
    )
    assert "ventes__ventes" in res.stdout, res.stderr[-2000:]


def check_generated_code(project: Project) -> None:
    """Le code genere passe les memes controles que le kit : docstrings et
    annotations de type partout (ruff D1, ANN), types coherents (mypy)."""
    src = project / "src/demo"
    config = str(ROOT / "pyproject.toml")
    ruff = shutil.which("ruff")
    if ruff:
        for cmd in (["format", "--check"], ["check", "--no-cache"]):
            # depuis la racine du projet, comme on lance ruff en vrai
            res = subprocess.run(
                [ruff, *cmd, "--config", config, "src/demo"],
                cwd=project.path,
                capture_output=True,
                text=True,
            )
            assert res.returncode == 0, res.stdout + res.stderr
    mypy = shutil.which("mypy")
    if mypy:
        res = subprocess.run(
            [mypy, "--config-file", config, "--cache-dir", os.devnull]
            + [
                str(p.name) for p in sorted(src.glob("*.py")) if p.name != "__init__.py"
            ],
            cwd=src,
            env=dict(os.environ, MYPYPATH=JOB_PATH),
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0, res.stdout + res.stderr


def test_read_input_gives_a_dataframe_and_optionally_a_table(
    project: Project,
) -> None:
    """read_input renvoie le DataFrame ; avec table, il cree aussi la vue."""
    code = (
        "from core.session import get_session\n"
        "from core import context\n"
        "from spark_foundation.datasets import read_input\n"
        "spark = get_session('demo')\n"
        "context.start_job('demo')\n"
        "df = read_input('ventes')\n"
        "print('cols', df.columns)\n"
        "print('vue', spark.catalog.tableExists('in_ventes'))\n"
        "df = read_input('ventes', table='in_ventes')\n"
        "print('cols', df.columns)\n"
        "print('vue', spark.table('in_ventes').columns[-1])\n"
        "t = spark.sql('SELECT file_time FROM in_ventes__file').first()[0]\n"
        "print('fichier', type(t).__name__)\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", code],
        cwd=project.path,
        env=dict(os.environ, PYTHONPATH=JOB_PATH, entrees="data/input"),
        capture_output=True,
        text=True,
    )
    out = res.stdout.splitlines()
    assert out[1] == "vue False", res.stderr[-2000:]
    # meme DataFrame dans les deux cas ; la vue a en plus l'ordre Qlik
    assert out[0] == out[2] and "_qlik_order" not in out[0]
    assert out[3] == "vue _qlik_order"
    # FileTime() : date du fichier, dans la vue in_ventes__file
    assert out[4] == "fichier datetime"
