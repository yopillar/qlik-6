"""FileTime() de Qlik, traduit sans le modele."""

import json

from check import check_section
from conftest import Project


def section_of(project: Project, script: str, table: str) -> dict:
    """Section du manifest qui calcule la table."""
    manifest = json.loads(
        (project / f"work/{script}/manifest.json").read_text(encoding="utf-8")
    )
    return next(s for s in manifest["sections"] if s.get("table") == table)


def test_filetime_of_the_loaded_file(project: Project) -> None:
    """FileTime() et FileTime('fichier du LOAD') lisent la vue <entree>__file."""
    (project / "script_qvd/charge.qvs").write_text(
        "Charge:\n"
        "LOAD Site, FileTime() AS Lu, FileTime('lib://in/ventes.csv') AS Lu2\n"
        "FROM [lib://in/ventes.csv] (txt, utf8, embedded labels, delimiter is ',');\n"
        "STORE Charge INTO [lib://out/charge.qvd] (qvd);\n"
    )
    project.run("prepare.py", "script_qvd/charge.qvs")
    sec = section_of(project, "charge", "Charge")
    assert sec["status"] == "deterministic", sec.get("todos")
    query = project / "work/charge" / sec["id"] / "query.sql"
    sql = query.read_text(encoding="utf-8")
    assert sql.count("(SELECT file_time FROM in_ventes__file)") == 2
    errors, _ = check_section(sec, query)
    assert not errors


def test_filetime_of_another_file_left_to_the_model(project: Project) -> None:
    """FileTime d'un autre fichier : peut-etre absent de conf/io.yml."""
    (project / "script_qvd/charge.qvs").write_text(
        "Charge:\n"
        "LOAD Site, FileTime('lib://in/autre.qvd') AS Lu\n"
        "FROM [lib://in/ventes.csv] (txt, utf8, embedded labels, delimiter is ',');\n"
        "STORE Charge INTO [lib://out/charge.qvd] (qvd);\n"
    )
    project.run("prepare.py", "script_qvd/charge.qvs")
    sec = section_of(project, "charge", "Charge")
    assert "autre fichier" in " ".join(sec.get("todos", []))
