"""Migration complete du projet de demo, avec Spark et le faux socle :
deux scripts chaines (synthese lit la sortie de ventes), un calendrier, un
script en .txt et un sans extension, un io.yml a 3 espaces commente, une
entree CSV et une sortie avec "#" dans leur nom, une table du modele de
donnees sans STORE."""

import os
import subprocess
import sys

import pandas as pd
import pytest

from conftest import ROOT, Project

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
    """Le job ne contient que des appels a core/, et chaque table Qlik est une
    table du catalogue (pour OpenLineage)."""
    project.run(
        "migrate.py", "script_qvd/", "--skip-selftest", "--var", "sorties=data/output"
    )
    job = (project / "src/demo/job.py").read_text(encoding="utf-8")
    # rien de technique dans le job : pipeline, run_scripts, les scripts
    assert "from core import pipeline, run_scripts" in job
    assert "SCRIPTS = " not in job and "def " in job and len(job.splitlines()) < 60
    # l'ordre des scripts est l'ordre des appels
    calls = job[job.index("run_scripts(\n") :]
    assert (
        calls.index("calendrier,") < calls.index("ventes,") < calls.index("synthese,")
    )
    ventes = (project / "src/demo/scripts/ventes.py").read_text(encoding="utf-8")
    assert 'read_input("ventes", table="in_ventes")' in ventes
    assert 'write_output("ventes_enrichies_v2", table="Ventes")' in ventes
    # le metastore local garde les tables d'un lancement a l'autre ; on
    # relance ventes pour verifier qu'une table existante est bien remplacee
    project.run_job("--only", "ventes")
    check = (
        "from core.internal.session import get_session\n"
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
