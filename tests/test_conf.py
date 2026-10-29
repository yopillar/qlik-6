"""Lecture de conf/io.yml et recherche des scripts."""

from pathlib import Path

import pytest

from conf import IoConf, find_scripts, resolve_location
from conftest import Project


def test_find_scripts_takes_qvs_txt_and_no_extension(project: Project) -> None:
    """Les scripts .qvs, .txt et sans extension sont trouves."""
    names = [p.name for p in find_scripts(project / "script_qvd")]
    assert names == ["calendrier", "synthese.txt", "ventes.qvs"]


def test_find_scripts_skips_hidden_and_other_files(tmp_path: Path) -> None:
    """Les fichiers caches et les autres extensions sont ignores."""
    for name in ("a.qvs", ".cache", "notes.md", "B.QVS"):
        (tmp_path / name).write_text("")
    assert [p.name for p in find_scripts(tmp_path)] == ["B.QVS", "a.qvs"]


def test_resolve_location_prefers_given_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Les variables passees en parametre passent avant l'environnement."""
    monkeypatch.setenv("sorties", "/env")
    assert (
        resolve_location("${sorties}/x", {"sorties": "data/output"}) == "data/output/x"
    )
    assert resolve_location("${sorties}/x") == "/env/x"
    assert resolve_location("${inconnue}/x") == "${inconnue}/x"


def test_io_conf_reads_three_space_indent_and_schemas(
    project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    """io.yml a 3 espaces et ses schemas sont bien lus."""
    monkeypatch.chdir(project.path)
    conf = IoConf(Path("conf/io.yml"))
    assert not conf.problems
    ventes = conf.find("lib://data/ventes.csv", "input")
    assert ventes is not None
    assert ventes.columns == ["Site", "Produit", "Montant"]
    assert ventes.types["Montant"] == "DOUBLE"
    synthese = conf.find("lib://out/synthese.qvd", "output")
    assert synthese is not None and synthese.name == "synthese"
