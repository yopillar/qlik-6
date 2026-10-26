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
- La cible est Spark 3.5.8 : verifier aussi avec PySpark 3.5.8 quand c'est
  possible, pas seulement 4.x.
- Branche de travail, puis MR vers develop.
