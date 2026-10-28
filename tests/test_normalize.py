"""Renommage des fichiers avant toute autre etape, et lecture des chemins
Qlik qui contiennent des crochets ou un #."""

from conftest import Project
from prepare import Inst, Stmt, analyze


def test_scripts_lowercase_and_hash_in_data(project: Project) -> None:
    """normalize.py met les scripts en minuscules et retire les # des fichiers."""
    (project / "script_qvd/ventes.qvs").rename(project / "script_qvd/Ventes.QVS")
    res = project.run("normalize.py")
    assert (project / "script_qvd/ventes.qvs").is_file()
    assert (project / "data/input/(ref)-sites_2024.csv").is_file()
    assert (project / "data/qlik_outputs/ventes_enrichies_v2.csv").is_file()
    assert "3 fichier(s) renomme(s)" in res.stdout
    # deja fait : rien a renommer
    assert project.run("normalize.py").stdout == ""


def test_brackets_in_data_file_names(project: Project) -> None:
    """Les crochets des noms de fichiers deviennent des parentheses."""
    (project / "data/input/[anon]-TT.csv").write_text("a\n1\n")
    project.run("normalize.py")
    assert (project / "data/input/(anon)-TT.csv").is_file()


def test_collision_stops_without_touching(project: Project) -> None:
    """Deux fichiers qui tomberaient sur le meme nom : rien n'est renomme."""
    (project / "data/input/(ref)-sites_2024.csv").write_text("x\n")
    res = project.run("normalize.py", check=False)
    assert res.returncode == 1 and "existe deja" in res.stdout
    assert (project / "data/input/[ref]-sites#2024.csv").is_file()


def sources(code: str) -> list[str]:
    """Fichiers lus par une instruction Qlik."""
    inst = Inst(Stmt(code, 1, 1, 1), code)
    analyze(inst)
    return inst.sources


def test_brackets_inside_bracketed_path() -> None:
    """Un chemin entre crochets peut contenir des crochets, et ]] vaut ]."""
    # le prefixe [anon]- se perdait : le nom s'arretait au premier ].
    # Les crochets deviennent _ comme le # : Hadoop les lit comme un motif
    assert sources("T: LOAD a FROM [lib://d/[anon]-TT#TEST.CSV] (txt);") == [
        "lib://d/(anon)-TT_TEST.CSV"
    ]
    # ]] est l'echappement de ] dans Qlik
    assert sources("T: LOAD a FROM [lib://d/[anon]]-TT.CSV] (txt);") == [
        "lib://d/(anon)-TT.CSV"
    ]
    assert sources("T: LOAD a FROM 'lib://d/[anon]-TT.CSV' (txt);") == [
        "lib://d/(anon)-TT.CSV"
    ]


def test_store_path_with_brackets_and_hash() -> None:
    """Le chemin d'un STORE est rendu lisible par Spark."""
    code = "STORE T INTO [lib://o/[anon]-TT#OUT.QVD] (qvd);"
    inst = Inst(Stmt(code, 1, 1, 1), code)
    analyze(inst)
    assert inst.stored is not None
    assert inst.stored[1] == "lib://o/(anon)-TT_OUT.QVD"
