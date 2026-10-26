import yaml


def test_fill_conf_adds_without_touching_existing(project):
    before = (project / "conf/io.yml").read_text(encoding="utf-8")
    project.run("prepare.py", "--collect", "script_qvd/calendrier",
                "script_qvd/synthese.txt", "script_qvd/ventes.qvs")
    project.run("fill_conf.py", "script_qvd/")
    after = (project / "conf/io.yml").read_text(encoding="utf-8")

    # l'existant est gardé tel quel, commentaires compris
    assert after.startswith(before.split("outputs:")[0])
    assert "# référentiel" in after
    assert (project / "conf/io.yml.bak").read_text(encoding="utf-8") == before
    conf = yaml.safe_load(after)
    # le fichier écrit par ventes et relu par synthese est dans les deux sections
    assert "ventes_enrichies" in conf["input"]
    assert {"ventes_enrichies", "calendrier", "synthese"} <= set(conf["outputs"])
    # ajouts à la même indentation (3 espaces) que le fichier
    assert "\n   ventes_enrichies:" in after
    schema = yaml.safe_load((project / "conf/schemas/ventes_enrichies.yml")
                            .read_text(encoding="utf-8"))
    assert list(schema["columns"]) == ["Site", "Produit", "MontantTTC",
                                       "Region", "Taille"]

    # relancer n'ajoute rien
    project.run("fill_conf.py", "script_qvd/")
    assert (project / "conf/io.yml").read_text(encoding="utf-8") == after
