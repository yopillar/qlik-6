"""Recensement des lectures et des ecritures : STORE dans des boucles,
encodages, formats des sources."""

import json

from prepare import source_format


def stores(project, *scripts):
    project.run("prepare.py", "--collect", *scripts)
    data = json.loads((project / "work/stores.json").read_text(encoding="utf-8"))
    return {k: v["writes"] for k, v in data.items()}


def test_store_all_tables_loop(project):
    # la boucle classique qui ecrit toutes les tables restantes
    (project / "script_qvd/boucle.qvs").write_text("""
A: LOAD x FROM [lib://s/a.csv] (txt);
B: LOAD y FROM [lib://s/b.csv] (txt);
M: Mapping LOAD k, v FROM [lib://s/m.csv] (txt);
FOR i = NoOfTables()-1 TO 0 STEP -1
  LET vTable = TableName($(i));
  STORE [$(vTable)] INTO [lib://qvd/$(vTable).qvd] (qvd);
  DROP TABLE [$(vTable)];
NEXT i
""")
    assert stores(project, "script_qvd/boucle.qvs")["boucle.qvs"] == [
        "lib://qvd/a.qvd",
        "lib://qvd/b.qvd",
    ]


def test_utf16_and_bom_scripts(project):
    (project / "script_qvd/u16.qvs").write_text(
        "E: LOAD v FROM [lib://s/e.csv] (txt);\n"
        "STORE E INTO [lib://qvd/E.qvd] (qvd);\n",
        encoding="utf-16",
    )
    (project / "script_qvd/bom.qvs").write_bytes(
        "\ufeffD:\r\nLOAD w FROM [lib://s/d.csv] (txt);\r\n"
        "STORE D INTO [lib://qvd/D.qvd] (qvd);\r\n".encode("utf-8")
    )
    found = stores(project, "script_qvd/u16.qvs", "script_qvd/bom.qvs")
    assert found == {"u16.qvs": ["lib://qvd/e.qvd"], "bom.qvs": ["lib://qvd/d.qvd"]}


def test_source_format_from_qlik_spec():
    assert source_format(
        "lib://s/Clients#2024.csv",
        "txt, codepage is 28591, embedded labels, delimiter is ';', msq",
    ) == ("csv", {"encoding": "ISO-8859-1", "header": "true", "sep": ";", "quote": '"'})
    assert source_format("lib://s/a.qvd", "qvd") == ("parquet", {})
    assert source_format("lib://s/a.dat", "txt, no labels, delimiter is '|'") == (
        "csv",
        {"header": "false", "sep": "|"},
    )
    assert source_format("lib://s/a.xlsx", "ooxml, embedded labels")[0] == "excel"
    assert source_format("table_en_base") == (None, {})
