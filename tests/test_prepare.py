import json


def manifest(project, script):
    return json.loads((project / f"work/{script}/manifest.json")
                      .read_text(encoding="utf-8"))


def test_demo_translated_without_model(project):
    for script in ("calendrier", "synthese.txt", "ventes.qvs"):
        project.run("prepare.py", f"script_qvd/{script}")
    for script in ("calendrier", "synthese", "ventes"):
        statuses = {s["status"] for s in manifest(project, script)["sections"]}
        assert statuses <= {"deterministic", "skip"}, (script, statuses)


def test_exit_script_ignores_the_rest(project):
    project.run("prepare.py", "script_qvd/synthese.txt")
    tables = [s.get("table") for s in manifest(project, "synthese")["sections"]]
    assert "Ignore" not in " ".join(map(str, tables))


def test_date_format_kept_for_comparison(project):
    project.run("prepare.py", "script_qvd/calendrier")
    assert manifest(project, "calendrier")["date_format"] == "DD/MM/YYYY"


def test_wide_table_prompt_lists_only_used_columns(project):
    cols = [f"Col{i:03d}" for i in range(322)]
    (project / "conf/schemas/large.yml").write_text(
        "columns:\n" + "".join(f"  {c}: STRING\n" for c in cols))
    with open(project / "conf/io.yml", "a", encoding="utf-8") as f:
        f.write("   large:\n       format: parquet\n"
                "       schema_file: schemas/large.yml\n"
                "       location: ${sorties}/large\n")
    io = (project / "conf/io.yml").read_text(encoding="utf-8")
    # l'entrée doit être dans input, pas à la suite des outputs
    io = io.replace("outputs:\n", "").replace(
        "   synthese:\n       location: ${sorties}/synthese\n", "")
    io += "outputs:\n   synthese:\n       location: ${sorties}/synthese\n"
    (project / "conf/io.yml").write_text(io, encoding="utf-8")
    (project / "script_qvd/large.qvs").write_text(
        "Large:\nLOAD *, ConvertToLocalTime(Col001) as Locale\n"
        "FROM [lib://data/large.parquet] (parquet);\n")

    project.run("prepare.py", "script_qvd/large.qvs")
    (sec,) = manifest(project, "large")["sections"]
    assert sec["status"] == "todo"
    for name in ("holes_prompt.md", "prompt.md"):
        text = (project / f"work/large/{sec['id']}/{name}").read_text(
            encoding="utf-8")
        assert "Col001" in text
        assert "Col200" not in text
        assert len(text) < 6000, (name, len(text))
