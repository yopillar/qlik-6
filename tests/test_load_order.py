"""Ordre de chargement (_qlik_order) sans monotonically_increasing_id()."""

from pathlib import Path

from check import check_section
from qlik2sql import values_sql


def test_inline_order_written_on_each_row() -> None:
    """Un INLINE porte son numero de ligne en dur, sans fonction dans VALUES."""
    sql = values_sql(["Code", "Lib"], [["1", "Un"], ["2", "Deux"]])
    assert "monotonically_increasing_id" not in sql
    assert "('1', 'Un', 0)" in sql and "('2', 'Deux', 1)" in sql
    assert sql.startswith("SELECT Code, Lib, array(CAST(__n AS BIGINT)) AS _qlik_order")
    assert sql.endswith("AS t(Code, Lib, __n)")


def test_check_refuses_monotonically_increasing_id(tmp_path: Path) -> None:
    """Le controle refuse la fonction, meme ecrite par le modele."""
    query = tmp_path / "query.sql"
    query.write_text(
        "-- l.1 : T\n"
        "SELECT Code, min(array(monotonically_increasing_id())) AS _qlik_order\n"
        "FROM VALUES ('1') AS v(Code) GROUP BY Code\n",
        encoding="utf-8",
    )
    errors, _ = check_section({"views": {}, "statement_lines": [1]}, query)
    assert any("monotonically_increasing_id() interdit" in e for e in errors)
