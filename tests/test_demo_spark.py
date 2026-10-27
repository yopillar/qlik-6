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

from conftest import FAKELIB

pytestmark = pytest.mark.spark


def test_migrate_demo_identical_to_qlik(project):
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

    # chaque table Qlik est une table du catalogue (pour OpenLineage), pas
    # seulement une vue temporaire
    assert "saveAsTable" in job and "--temp-views" in job


def test_csv_without_header(project):
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
    out = pd.read_parquet(project / "data/output/codes_sortie")
    assert str(out["Vide"].dtype) in ("object", "string")
    assert out["Vide"].isna().all()


def test_job_writes_each_step_as_a_table(project):
    project.run(
        "migrate.py", "script_qvd/", "--skip-selftest", "--var", "sorties=data/output"
    )
    # le job et la lecture du catalogue dans le meme processus : le
    # catalogue du faux socle est en memoire
    check = (
        "import runpy, sys\n"
        "sys.argv = ['job.py', '--only', 'ventes']\n"
        "try:\n"
        "    runpy.run_path('src/demo/job.py', run_name='__main__')\n"
        "except SystemExit:\n"
        "    pass\n"
        "from spark_foundation import _session\n"
        "print(sorted(t.name for t in "
        "_session().catalog.listTables('demo_qlik')))\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", check],
        cwd=project.path,
        env=dict(os.environ, PYTHONPATH=str(FAKELIB)),
        capture_output=True,
        text=True,
    )
    assert "ventes__ventes" in res.stdout, res.stderr[-2000:]
