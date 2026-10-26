"""Traductions qui diffèrent entre Qlik et Spark (AddMonths) et contrôle
des noms de fonctions inventés."""

import json

import pytest

from qlik2sql import ExprCtx, Untranslatable, expr


def test_addmonths_modes():
    ctx = ExprCtx()
    assert expr("AddMonths(d, 1)", ctx) == "add_months(d, 1)"
    assert expr("AddYears(d, 2)", ctx) == "add_months(d, 12 * (2))"
    assert "last_day" in expr("AddMonths(d, 1, 1)", ctx)
    with pytest.raises(Untranslatable):
        expr("AddMonths(Now(), 1)", ctx)      # add_months perdrait l'heure


@pytest.mark.spark
def test_addmonths_values_like_qlik():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    ctx = ExprCtx()
    # valeurs de la documentation Qlik
    cases = {"AddMonths(MakeDate(2003,1,31), 1)": "2003-02-28",
             "AddMonths(MakeDate(1996,2,29), 1, 1)": "1996-03-31",
             "AddMonths(MakeDate(1996,2,28), 1, 1)": "1996-03-30",
             "AddMonths(MakeDate(2003,3,31), -1)": "2003-02-28",
             "AddYears(MakeDate(1996,2,29), 1)": "1997-02-28"}
    for qlik, expected in cases.items():
        got = spark.sql(f"SELECT {expr(qlik, ctx)} AS d").first()["d"]
        assert str(got) == expected, qlik


def test_check_refuses_invented_date_functions(project):
    project.run("prepare.py", "script_qvd/ventes.qvs")
    manifest = json.loads((project / "work/ventes/manifest.json").read_text(
        encoding="utf-8"))
    sec = next(s for s in manifest["sections"] if s["outputs"])
    query = project / f"work/ventes/{sec['id']}/query.sql"
    query.write_text(query.read_text(encoding="utf-8").replace(
        "Site,", "add_month(current_date(), 1) AS x, Site,", 1),
        encoding="utf-8")
    res = project.run("check.py", "work/ventes", "--only", sec["id"],
                      check=False)
    assert res.returncode != 0
    assert "add_month() inconnue de Spark" in res.stdout


def test_job_template_has_spark_submit():
    from job_template import JOB
    code = JOB.format(project="demo", scripts="")
    compile(code, "job.py", "exec")
    head = code.split('"""')[1]
    assert "spark-submit" in head and "src/demo/job.py" in head
