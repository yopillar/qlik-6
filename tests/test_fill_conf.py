import yaml

SCRIPTS = ("script_qvd/calendrier", "script_qvd/synthese.txt",
           "script_qvd/ventes.qvs")


def fill(project):
    project.run("prepare.py", "--collect", *SCRIPTS)
    return project.run("fill_conf.py", "script_qvd/")


def test_fill_conf_adds_without_touching_existing(project):
    before = (project / "conf/io.yml").read_text(encoding="utf-8")
    fill(project)
    after = (project / "conf/io.yml").read_text(encoding="utf-8")

    # l'existant est gardé tel quel, commentaires compris
    assert after.startswith(before.split("outputs:")[0].rstrip("\n"))
    assert (project / "conf/io.yml.bak").read_text(encoding="utf-8") == before
    conf = yaml.safe_load(after)
    # sections au pluriel, comme le socle les attend
    assert set(conf) == {"inputs", "outputs"}
    # ajouts à la même indentation (3 espaces) que le fichier
    assert "\n   sites2024:" in after

    # relancer n'ajoute rien
    fill(project)
    assert (project / "conf/io.yml").read_text(encoding="utf-8") == after


def test_hash_kept_in_input_removed_in_output(project):
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # une entrée garde son vrai nom de fichier, # compris, et son format csv
    sites = conf["inputs"]["sites2024"]
    assert sites["location"] == "${entrees}/sites#2024.csv"
    assert sites["format"] == "csv"
    # une sortie est renommée sans #, et le script suivant la relit là
    assert conf["outputs"]["ventes_enrichiesv2"]["location"] == \
        "${sorties}/ventes_enrichiesv2"
    assert conf["inputs"]["ventes_enrichiesv2"]["location"] == \
        "${sorties}/ventes_enrichiesv2"
    assert conf["inputs"]["ventes_enrichiesv2"]["format"] == "parquet"
    assert not any("#" in k for s in conf.values() for k in s)
    assert not any("#" in v["location"] for v in conf["outputs"].values())


def test_csv_options_noted(project):
    fill(project)
    text = (project / "conf/io.yml").read_text(encoding="utf-8")
    assert '# options Qlik : encodage "UTF-8", en-tête "true", séparateur ","' \
        in text


def test_model_tables_become_outputs(project):
    # Dates reste en mémoire à la fin du calendrier, sans STORE : c'est le
    # modèle de données de l'app, il doit être écrit
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    assert conf["outputs"]["calendrier_dates"]["location"] == \
        "${sorties}/calendrier_Dates"
    # pas les tables supprimées, ni celles qui ont un STORE
    assert "calendrier_bornes" not in conf["outputs"]
    assert "ventes_ventes" not in conf["outputs"]


def test_schema_of_produced_file_has_all_stored_columns(project):
    fill(project)
    schema = yaml.safe_load((project / "conf/schemas/ventes_enrichiesv2.yml")
                            .read_text(encoding="utf-8"))
    assert list(schema["columns"]) == ["Site", "Produit", "MontantTTC",
                                       "Region", "Taille"]


def test_types_from_csv_values(project):
    fill(project)
    text = (project / "conf/schemas/sites2024.yml").read_text(encoding="utf-8")
    assert "Site: STRING" in text and "déduit des 3 première(s)" in text


def test_types_from_parquet_metadata(project):
    import pandas as pd
    (project / "script_qvd/produits.qvs").write_text(
        "P:\nLOAD * FROM [lib://src/produits.qvd] (qvd);\n"
        "STORE P INTO [lib://qvd/produits.qvd] (qvd);\n")
    (project / "data/input/produits").mkdir()
    pd.DataFrame({"Code": ["a"], "Prix": [1.5], "Stock": [3],
                  "Jour": pd.to_datetime(["2024-12-29"]).date}).to_parquet(
        project / "data/input/produits/part-0.parquet")
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/produits.qvs")
    project.run("fill_conf.py", "script_qvd/")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # un QVD est lu en Parquet, sous le même nom, depuis sa source (et pas
    # depuis la sortie du même nom écrite par ce script)
    assert conf["inputs"]["produits"] == {
        "format": "parquet", "schema_file": "schemas/produits.yml",
        "location": "${entrees}/produits"}
    schema = yaml.safe_load((project / "conf/schemas/produits.yml")
                            .read_text(encoding="utf-8"))
    assert schema["columns"] == {"Code": "STRING", "Prix": "DOUBLE",
                                 "Stock": "BIGINT", "Jour": "DATE"}


def test_unknown_location_variable_is_reported(project):
    (project / "conf/io.yml").write_text(
        "inputs:\n  dummy:\n    location: ${lac}/dummy\n", encoding="utf-8")
    (project / "script_qvd/p.qvs").write_text(
        "P:\nLOAD a FROM [lib://src/p.qvd] (qvd);\n")
    project.run("prepare.py", "--collect", "script_qvd/p.qvs")
    res = project.run("fill_conf.py", "script_qvd/p.qvs")
    assert "${lac} inconnue(s)" in res.stdout


def test_no_function_name_taken_for_a_column():
    from fill_conf import type_hints
    hints = type_hints("LOAD Sum(Montant) / Count(Ligne) as M, "
                       "x * PurgeChar(Ref, 'a') as P FROM t;")
    assert "Count" not in hints and "PurgeChar" not in hints
    assert "x" in hints
