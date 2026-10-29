# Regles de travail sur le kit

AGENTS.md s'adresse au modele traducteur (Ornith), pas a nous.

Avant chaque commit ou push, dans cet ordre, et tout doit passer :

```bash
ruff format . && ruff check . && mypy     # configuration dans pyproject.toml
python -m pytest                          # les tests Spark ont le marqueur spark
```

- Aucun accent ni caractere non ASCII dans les fichiers, commentaires et
  textes affiches compris.
- Code et API en anglais ; docstrings et commentaires en francais, courts,
  ecrits comme par un humain.
- Toute fonction, methode, classe et module a une docstring, et tout
  argument et tout retour une annotation de type (un DataFrame est type
  `DataFrame`). Ruff (D1, ANN) et mypy le verifient.
- Le job genere (`job.py`, `scripts/`) reste lisible par un developpeur
  Qlik : pipeline, run_scripts, read_input, run_sql, write_output, rien
  d'autre. Il importe le socle `spark_foundation` (`runner.py`,
  `tables.py`, `datasets.py`) ; le technique est dans les autres fichiers du
  socle (aucun sous-dossier) et les
  transformations dans `pyspark_commons`.
- `core/` et `commons/` suivent les fichiers et les noms du socle
  (correspondance dans le README) : un changement du socle se reporte ici.
  Les tests passent par la doublure `tests/fakelib/spark_foundation`.
- La cible est Spark 3.5.8 : verifier aussi avec PySpark 3.5.8 quand c'est
  possible, pas seulement 4.x.
- Branche de travail, puis MR vers develop.
