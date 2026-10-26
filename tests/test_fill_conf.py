import pytest
import yaml

SCRIPTS = ("script_qvd/calendrier", "script_qvd/synthese.txt", "script_qvd/ventes.qvs")


def fill(project):
    project.run("normalize.py")
    project.run("prepare.py", "--collect", *SCRIPTS)
    return project.run("fill_conf.py", "script_qvd/")


def test_fill_conf_adds_without_touching_existing(project):
    before = (project / "conf/io.yml").read_text(encoding="utf-8")
    fill(project)
    after = (project / "conf/io.yml").read_text(encoding="utf-8")

    # l'existant est garde tel quel, commentaires compris
    assert after.startswith(before.split("outputs:")[0].rstrip("\n"))
    assert (project / "conf/io.yml.bak").read_text(encoding="utf-8") == before
    conf = yaml.safe_load(after)
    # sections au pluriel, comme le socle les attend
    assert set(conf) == {"inputs", "outputs"}
    # ajouts a la meme indentation (3 espaces) que le fichier
    assert "\n   sites_2024:" in after

    # relancer n'ajoute rien
    fill(project)
    assert (project / "conf/io.yml").read_text(encoding="utf-8") == after


def test_hash_becomes_underscore(project):
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # sites#2024.csv : renomme sites_2024.csv, lu en csv sous ce nom
    sites = conf["inputs"]["sites_2024"]
    assert sites["location"] == "${entrees}/sites_2024.csv"
    assert sites["format"] == "csv"
    # ventes_enrichies#v2.qvd : sortie ventes_enrichies_v2
    assert (
        conf["outputs"]["ventes_enrichies_v2"]["location"]
        == "${sorties}/ventes_enrichies_v2"
    )
    assert not any("#" in k for s in conf.values() for k in s)
    assert not any("#" in v["location"] for v in conf["outputs"].values())


def test_csv_options_noted(project):
    fill(project)
    text = (project / "conf/io.yml").read_text(encoding="utf-8")
    assert '# options Qlik : encodage "UTF-8", en-tete "true", separateur ","' in text


def test_model_tables_become_outputs(project):
    # Dates reste en memoire a la fin du calendrier, sans STORE : c'est le
    # modele de donnees de l'app, il doit etre ecrit
    fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    assert (
        conf["outputs"]["calendrier_dates"]["location"] == "${sorties}/calendrier_Dates"
    )
    # pas les tables supprimees, ni celles qui ont un STORE
    assert "calendrier_bornes" not in conf["outputs"]
    assert "ventes_ventes" not in conf["outputs"]


def test_produced_file_waits_for_its_writer(project):
    # synthese relit ce que ventes ecrit : tant que ventes n'est pas
    # traduit, on ne fige pas un schema devine
    res = fill(project)
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    assert "ventes_enrichies_v2" not in conf["inputs"]
    assert "En attente" in res.stdout and "ventes_enrichies_v2" in res.stdout


@pytest.mark.spark
def test_produced_file_gets_exact_spark_types(project):
    fill(project)
    project.run("prepare.py", "script_qvd/ventes.qvs")
    project.run("check.py", "work/ventes", "--parse")
    project.run("fill_conf.py", "script_qvd/")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # relue la ou ventes l'ecrit
    assert conf["inputs"]["ventes_enrichies_v2"] == {
        "format": "parquet",
        "schema_file": "schemas/ventes_enrichies_v2.yml",
        "location": "${sorties}/ventes_enrichies_v2",
    }
    schema = yaml.safe_load(
        (project / "conf/schemas/ventes_enrichies_v2.yml").read_text(encoding="utf-8")
    )
    # toutes les colonnes du STORE, dans l'ordre, avec les types de Spark
    assert schema["columns"] == {
        "Site": "STRING",
        "Produit": "STRING",
        "MontantTTC": "DOUBLE",
        "Region": "STRING",
        "Taille": "STRING",
    }


def test_types_from_script_comparisons_and_shared_fields(project):
    (project / "script_qvd/c.qvs").write_text(
        "C:\nLOAD Statut, Qte, DateCde, Site FROM [lib://src/c.qvd] (qvd)\n"
        "WHERE Statut = 'OK' and Qte > 0 and DateCde >= '01/01/2024';\n"
    )
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/c.qvs")
    project.run("fill_conf.py", "script_qvd/")
    text = (project / "conf/schemas/c.yml").read_text(encoding="utf-8")
    assert "Statut: STRING" in text and "compare a un texte" in text
    assert "Qte: DOUBLE" in text and "DateCde: DATE" in text
    # Site n'a aucun indice ici, mais c'est le meme champ que dans ventes
    assert "Site: STRING" in text and "meme champ que dans" in text


def test_types_from_csv_values(project):
    fill(project)
    text = (project / "conf/schemas/sites_2024.yml").read_text(encoding="utf-8")
    assert "Site: STRING" in text and "deduit des 3 premiere(s)" in text


def test_types_from_parquet_metadata(project):
    import pandas as pd

    (project / "script_qvd/produits.qvs").write_text(
        "P:\nLOAD * FROM [lib://src/produits.qvd] (qvd);\n"
        "STORE P INTO [lib://qvd/produits.qvd] (qvd);\n"
    )
    (project / "data/input/produits").mkdir()
    pd.DataFrame(
        {
            "Code": ["a"],
            "Prix": [1.5],
            "Stock": [3],
            "Jour": pd.to_datetime(["2024-12-29"]).date,
        }
    ).to_parquet(project / "data/input/produits/part-0.parquet")
    project.run("prepare.py", "--collect", *SCRIPTS, "script_qvd/produits.qvs")
    project.run("fill_conf.py", "script_qvd/")
    conf = yaml.safe_load((project / "conf/io.yml").read_text(encoding="utf-8"))
    # un QVD est lu en Parquet, sous le meme nom, depuis sa source (et pas
    # depuis la sortie du meme nom ecrite par ce script)
    assert conf["inputs"]["produits"] == {
        "format": "parquet",
        "schema_file": "schemas/produits.yml",
        "location": "${entrees}/produits",
    }
    schema = yaml.safe_load(
        (project / "conf/schemas/produits.yml").read_text(encoding="utf-8")
    )
    assert schema["columns"] == {
        "Code": "STRING",
        "Prix": "DOUBLE",
        "Stock": "BIGINT",
        "Jour": "DATE",
    }


def test_unknown_location_variable_is_reported(project):
    (project / "conf/io.yml").write_text(
        "inputs:\n  dummy:\n    location: ${lac}/dummy\n", encoding="utf-8"
    )
    (project / "script_qvd/p.qvs").write_text(
        "P:\nLOAD a FROM [lib://src/p.qvd] (qvd);\n"
    )
    project.run("prepare.py", "--collect", "script_qvd/p.qvs")
    res = project.run("fill_conf.py", "script_qvd/p.qvs")
    assert "${lac} inconnue(s)" in res.stdout


def test_no_function_name_taken_for_a_column():
    from fill_conf import type_hints

    hints = type_hints(
        "LOAD Sum(Montant) / Count(Ligne) as M, x * PurgeChar(Ref, 'a') as P FROM t;"
    )
    assert "Count" not in hints and "PurgeChar" not in hints
    assert "x" in hints
