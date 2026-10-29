"""Controle des prefixes x.colonne dans une requete (check.py)."""

import re

from check import strip_sql_comments, strip_sql_strings, unknown_prefixes


def prefixes(sql: str, views: tuple[str, ...] = ()) -> list[str]:
    """Prefixes inconnus d'une requete, comme les voit check_section."""
    code = strip_sql_strings(strip_sql_comments(sql))
    ctes = {m.lower() for m in re.findall(r"(\w+)\s+AS\s*\(", code, re.I)}
    return unknown_prefixes(code, ctes | set(views))


def test_invented_prefix_on_inline() -> None:
    """INLINE aliase v(...) : _qlik_order sorti en pos._qlik_order est refuse."""
    sql = """WITH t_1 AS (
      SELECT v.Code, row_number() OVER (ORDER BY pos._qlik_order) AS Pos,
             pos._qlik_order
      FROM (SELECT Code, Lib, array(CAST(__n AS BIGINT)) AS _qlik_order
            FROM VALUES ('1', 'a', 0), ('2', 'b', 1) AS v(Code, Lib, __n)) v
    ) SELECT * FROM t_1"""
    assert prefixes(sql) == ["pos._qlik_order"]
    assert prefixes(sql.replace("pos.", "v.")) == []


def test_known_prefixes() -> None:
    """Alias de table, de sous-requete, de fonction-table et nom de CTE."""
    sql = """WITH c AS (SELECT 1 AS id)
    SELECT a.x, b.id, g.id, c.id, m1.map_value
    FROM range(1, 5) g
    JOIN (SELECT 1 AS id) b ON 1 = 1
    LEFT SEMI JOIN in_t AS a ON a.x = b.id
    LEFT JOIN (SELECT k AS map_value FROM in_t) m1 ON 1 = 1
    CROSS JOIN c
    WHERE EXISTS (SELECT 1 FROM (SELECT x AS v FROM in_t) ex WHERE ex.v = 1)"""
    assert prefixes(sql, ("in_t",)) == []


def test_column_alias_is_not_a_table_alias() -> None:
    """ ") AS Pos" nomme une colonne : il ne rend pas pos.x valide."""
    sql = "SELECT count(*) AS Pos, pos.x FROM in_t"
    assert prefixes(sql, ("in_t",)) == ["pos.x"]
