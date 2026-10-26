"""Renommage des fichiers avant toute autre etape, et lecture des chemins
Qlik qui contiennent des crochets ou un #."""

from prepare import Inst, Stmt, analyze


def test_scripts_lowercase_and_hash_in_data(project):
    (project / "script_qvd/ventes.qvs").rename(project / "script_qvd/Ventes.QVS")
    res = project.run("normalize.py")
    assert (project / "script_qvd/ventes.qvs").is_file()
    assert (project / "data/input/sites_2024.csv").is_file()
    assert (project / "data/qlik_outputs/ventes_enrichies_v2.csv").is_file()
    assert "3 fichier(s) renomme(s)" in res.stdout
    # deja fait : rien a renommer
    assert project.run("normalize.py").stdout == ""


def test_collision_stops_without_touching(project):
    (project / "data/input/sites_2024.csv").write_text("x\n")
    res = project.run("normalize.py", check=False)
    assert res.returncode == 1 and "existe deja" in res.stdout
    assert (project / "data/input/sites#2024.csv").is_file()


def sources(code):
    inst = Inst(Stmt(code, 1, 1, 1), code)
    analyze(inst)
    return inst.sources


def test_brackets_inside_bracketed_path():
    # le prefixe [anon]- se perdait : le nom s'arretait au premier ]
    assert sources("T: LOAD a FROM [lib://d/[anon]-TT#TEST.CSV] (txt);") == [
        "lib://d/[anon]-TT_TEST.CSV"
    ]
    # ]] est l'echappement de ] dans Qlik
    assert sources("T: LOAD a FROM [lib://d/[anon]]-TT.CSV] (txt);") == [
        "lib://d/[anon]-TT.CSV"
    ]
    assert sources("T: LOAD a FROM 'lib://d/[anon]-TT.CSV' (txt);") == [
        "lib://d/[anon]-TT.CSV"
    ]


def test_store_path_with_brackets_and_hash():
    code = "STORE T INTO [lib://o/[anon]-TT#OUT.QVD] (qvd);"
    inst = Inst(Stmt(code, 1, 1, 1), code)
    analyze(inst)
    assert inst.stored is not None
    assert inst.stored[1] == "lib://o/[anon]-TT_OUT.QVD"
