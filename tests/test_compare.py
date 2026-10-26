import pandas as pd

from compare import compare, strptime_format


def test_strptime_format():
    assert strptime_format("DD/MM/YYYY") == "%d/%m/%Y"
    assert strptime_format("YYYY-MM-DD hh:mm:ss") == "%Y-%m-%d %H:%M:%S"
    # noms de mois, millisecondes : on ne devine pas
    assert strptime_format("DD MMM YYYY") is None
    assert strptime_format("DD/MM/YYYY hh:mm:ss[.fff]") is None


def test_text_dates_of_qlik_export_match_spark_dates():
    qlik = pd.DataFrame({"Jour": ["29/12/2024", "30/12/2024"]})
    spark = pd.DataFrame({"Jour": pd.to_datetime(["2024-12-29", "2024-12-30"])})
    assert not compare(qlik, spark, 6, [])[0]
    assert compare(qlik, spark, 6, [], ["%d/%m/%Y"])[0]


def test_column_partly_like_dates_stays_text():
    qlik = pd.DataFrame({"Code": ["01/02/2024", "ABC"]})
    spark = pd.DataFrame({"Code": ["01/02/2024", "ABC"]})
    assert compare(qlik, spark, 6, [], ["%d/%m/%Y"])[0]


def test_duplicates_are_counted():
    qlik = pd.DataFrame({"a": ["x", "x"]})
    spark = pd.DataFrame({"a": ["x"]})
    assert not compare(qlik, spark, 6, [])[0]


def spark_side(rows, order):
    df = pd.DataFrame({"a": rows})
    df["_qlik_order"] = [[o] for o in order]
    return df


def test_order_checked_on_qlik_order():
    qlik = pd.DataFrame({"a": ["x", "y", "z"]})
    # fichier ecrit dans le desordre, mais _qlik_order donne le bon ordre
    ok, report, order = compare(
        qlik, spark_side(["z", "x", "y"], [3, 1, 2]), 6, ["_qlik_order"]
    )
    assert ok and order == "identique"


def test_order_gap_reported_and_strict():
    qlik = pd.DataFrame({"a": ["x", "y"]})
    spark = spark_side(["x", "y"], [2, 1])
    ok, report, order = compare(qlik, spark, 6, ["_qlik_order"])
    assert ok and order == "different"
    assert any("position 1" in line for line in report)
    ok, _, order = compare(qlik, spark, 6, ["_qlik_order"], strict_order=True)
    assert not ok and order == "different"


def test_order_array_compared_element_by_element():
    # [1, 10] passe apres [1, 2] : tri numerique, pas sur le texte
    qlik = pd.DataFrame({"a": ["x", "y"]})
    spark = pd.DataFrame({"a": ["y", "x"], "_qlik_order": [[1, 10], [1, 2]]})
    assert compare(qlik, spark, 6, ["_qlik_order"])[2] == "identique"


def test_order_not_checked_without_column():
    qlik = pd.DataFrame({"a": ["x"]})
    assert compare(qlik, pd.DataFrame({"a": ["x"]}), 6, [])[2] == "non verifie"
