"""Ordre de chargement (_qlik_order) sans monotonically_increasing_id()."""

from pathlib import Path

from check import check_section
from qlik2sql import values_sql


def test_inline_order_written_on_each_row() -> None:
    """Un INLINE porte son ordre en dur : array(0L), array(1L)..."""
    sql = values_sql(["Code", "Lib"], [["1", "Un"], ["2", "Deux"]])
    assert "monotonically_increasing_id" not in sql
    assert "('1', 'Un', array(0L))" in sql and "('2', 'Deux', array(1L))" in sql
    assert sql.endswith("AS t(Code, Lib, _qlik_order)")


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
