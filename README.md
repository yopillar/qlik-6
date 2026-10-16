# Migration Qlik → Spark SQL

Un script Qlik devient **des requêtes Spark SQL** (une par table) et **un
`job.py` court** qui les enchaîne en n'appelant que `pyspark-commons`.

Le Python traduit lui-même la majorité du code Qlik (sans modèle). Le
modèle n'est appelé que pour les parties qu'il ne sait pas traduire avec
certitude, marquées `TODO(model)` dans une requête déjà écrite.

## Arborescence (un projet par zone médaillon)

```
mon-projet/
├── script_qvd/          scripts .qvs à traduire                      (toi)
├── data/input/          données lues, en Parquet                     (toi)
├── qlik_outputs/        sorties Qlik pour comparer, optionnel        (toi)
├── data/output/         données écrites par les jobs                 (généré)
├── src/<script>/        01_<table>.sql, 02_<table>.sql…, job.py      (généré)
├── src/run_all.py       job principal : tous les scripts, dans l'ordre (généré)
├── work/                travail intermédiaire + journal.csv          (généré)
├── docs/qlik-functions-sql.md
├── .opencode/agents/translator.md
├── .opencode/skills/    4 skills SQL (jointures, mappings, chaînes TMP, Peek/Previous)
├── AGENTS.md
└── tools/
```

## Les deux bibliothèques

- `spark-foundation-core` (interne, PySpark) : jamais appelée directement
  par le code généré.
- `pyspark-commons` (pour les gens de Qlik) : le `job.py` généré n'utilise
  que `read`, `write`, `timer` et `setup_logging`. Ces appels sont tous dans
  `tools/job_template.py` : si une signature change, on corrige ce seul
  fichier et on relance `migrate.py`.

## La commande

```bash
python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # 1re fois, puis relire
python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier
python src/run_all.py                                     # exécuter le résultat
```

Pour chaque script : traduction sans modèle, complétion des `TODO(model)`
par le modèle (une table par appel, syntaxe validée par le parseur Spark,
une relance si erreur), assemblage dans `src/<script>/`, puis, si
`qlik_outputs/` existe, exécution et comparaison avec Qlik.
Relançable à volonté : ce qui est fait et inchangé n'est pas refait.

`work/<script>/rapport.md` dit, table par table, ce qui a été traduit sans
modèle et ce qui reste à compléter, avec la raison.

## Ce que le traducteur fait sans modèle

- Champs et expressions : `If`, `&`, arithmétique, comparaisons, `like`,
  fonctions texte, dates (`Date#` avec conversion du format Qlik), nombres,
  `Match`/`WildMatch`/`Pick`, `RangeSum`, agrégations, `Count(DISTINCT …)`.
- `WHERE`, `GROUP BY`, `DISTINCT`, `FIRST n`, LOAD empilés.
- Sources : fichier, `RESIDENT`, `INLINE`, `SQL SELECT` simple.
- `JOIN` (clés implicites calculées, ordre des colonnes de Qlik), `KEEP`,
  `CONCATENATE` (colonnes alignées), auto-concaténation, `DROP FIELD`.
- `ApplyMap` → `LEFT JOIN` sur la source du mapping + `COALESCE`.
- Variables, `CALL` de SUB, boucles `FOR` à liste fixe, `IF` évaluables.

Les colonnes d'un `LOAD *` sont lues dans le Parquet de `data/input/` s'il
est présent, ou reprises du `STORE` d'un autre script du projet : **dépose
les données avant de lancer**, la part traduite sans modèle augmente
fortement.

## Chemins : une règle fixe

Les dossiers Qlik et l'extension sont retirés, seul le nom du fichier reste :
- fichier écrit par un `STORE` (ce script ou un autre) → `data/output/<nom>/` ;
- tout autre fichier lu → `data/input/<nom>/` (vue SQL `in_<nom>`) ;
- table de base lue par `SQL SELECT ... FROM dbo.clients` → `data/input/clients`.

## Voir ce que le modèle a fait

Pour chaque table envoyée au modèle, dans `work/<script>/<table>/` :

| Fichier | Contenu |
|---|---|
| `prompt.md` | ce que le modèle a reçu (brouillon SQL, code Qlik, faits) |
| `model_1.log`, `model_2.log` | commande, message, durée et **réponse brute** du modèle, par tentative |
| `query.attempt1.sql`, `query.attempt2.sql` | la requête après chaque tentative |
| `retry.md` | les erreurs renvoyées au modèle pour sa 2e tentative |

La console signale aussi les deux comportements classiques d'un modèle
faible : « n'a PAS modifié query.sql » (il n'a rien écrit), ou « a AFFICHÉ
la requête sans écrire le fichier » (récupérée automatiquement depuis sa
réponse).

## Quand une table est à corriger

1. Corrige `work/<script>/<table>/query.sql` (`prompt.md` à côté contient le
   code Qlik et les faits).
2. `python tools/check.py work/<script> --only <table> --parse`
3. Relance `migrate.py` : il assemble et compare.
4. Note la cause dans la colonne `resolution` de `work/journal.csv`.

Une table en `escalade` n'est jamais retraduite automatiquement.

## Les outils

| Outil | Rôle |
|---|---|
| `migrate.py` | la seule commande à lancer |
| `prepare.py` | analyse du script, sections, chemins, rapport |
| `qlik2sql.py` | traducteur Qlik → Spark SQL sans modèle |
| `translate.py` | le modèle complète les `TODO(model)` |
| `check.py` | contrôles, syntaxe Spark avec `--parse` |
| `assemble.py` | `src/<script>/` : les `.sql` + `job.py` |
| `job_template.py` | gabarit du `job.py`, seuls appels à `pyspark-commons` |
| `verify.py` | lance le job, compare avec `qlik_outputs/` |
| `compare.py` | comparaison de deux tables |
| `orchestrate.py` | `src/run_all.py`, ordre des scripts entre eux |
| `journal.py` | `work/journal.csv` |

## Limites

- Non traduits sans modèle : `Peek`/`Previous`/`RowNo`, `Exists`,
  `FirstValue`/`LastValue`, `AutoNumber`, `AUTOGENERATE`, boucles sur
  `FileList`, `SQL SELECT` avec jointure ou `WHERE` (dialecte de la base).
- `ApplyMap` : en cas de clés en double, Qlik garde la première valeur, la
  traduction prend la plus petite.
- `Date(x)` sur un nombre (date Qlik en jours) n'est pas converti.
- `QUALIFY`, `NullAsValue`, `SET NullInterpret/NullValue` : signalés, à
  compléter.
- L'ordre entre scripts ne voit que les fichiers, pas les bases de données.
- Testé ici avec un vrai Spark 4.0 (ta cible est 3.5.8) et un faux
  `pyspark-commons` reproduisant son API ; pas avec un vrai opencode.
