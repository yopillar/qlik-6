"""Migration complète du projet de démo, avec Spark et le faux socle :
deux scripts chaînés (synthese lit la sortie de ventes), un calendrier, un
script en .txt et un sans extension, un io.yml à 3 espaces commenté."""

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
    out = pd.read_parquet(project / "data/output/ventes_enrichies")
    assert "_qlik_order" in out.columns
    out = out.sort_values("_qlik_order", key=lambda s: s.map(tuple))
    assert list(out["Produit"]) == ["A", "B", "C"]
