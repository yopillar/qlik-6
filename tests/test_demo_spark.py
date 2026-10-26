"""Migration complète du projet de démo, avec Spark et le faux socle :
deux scripts chaînés (synthese lit la sortie de ventes), un calendrier, un
script en .txt et un sans extension, un io.yml à 3 espaces commenté, une
entrée CSV et une sortie avec « # » dans leur nom, une table du modèle de
données sans STORE."""

import pandas as pd
import pytest

pytestmark = pytest.mark.spark


def test_migrate_demo_identical_to_qlik(project):
    res = project.run("migrate.py", "script_qvd/", "--skip-selftest",
                      "--var", "sorties=data/output")
    summary = res.stdout.split("RÉSUMÉ", 1)[1]
    for script in ("calendrier", "ventes.qvs", "synthese.txt"):
        assert f"- {script} : assemblé dans src/, données identiques à Qlik" \
            in summary, res.stdout
    # ventes passe avant synthese, qui lit ce qu'il écrit
    assert summary.index("ventes.qvs") < summary.index("synthese.txt")

    # l'ordre de chargement est gardé, jusque dans la sortie écrite
    project.run("verify.py", "work/ventes", "--no-run", "--strict-order",
                "--var", "sorties=data/output")
    # la sortie ventes_enrichies#v2 est écrite sans le #
    out = pd.read_parquet(project / "data/output/ventes_enrichiesv2")
    assert "_qlik_order" in out.columns
    out = out.sort_values("_qlik_order", key=lambda s: s.map(tuple))
    assert list(out["Produit"]) == ["A", "B", "C"]

    # la table du modèle de données (Dates, sans STORE) est écrite et
    # comparée, dans l'ordre de chargement
    assert "Dates" in (project / "work/calendrier/comparaison.md").read_text(
        encoding="utf-8")
    project.run("verify.py", "work/calendrier", "--no-run", "--strict-order",
                "--var", "sorties=data/output")
    job = (project / "src/demo/job.py").read_text(encoding="utf-8")
    assert "spark-submit" in job
