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
├── conf/io.yml          entrées / sorties : format, location, schéma (toi)
├── conf/schema/*.yml    colonnes et types des entrées                (toi)
├── data/input/          données lues (si absentes de conf/io.yml)    (toi)
├── qlik_outputs/        sorties Qlik pour comparer, optionnel        (toi)
├── data/output/         données écrites par les jobs                 (généré)
├── src/<script>/        01_<table>.sql, 02_<table>.sql…, job.py      (généré)
├── src/run_all.py       job principal : tous les scripts, dans l'ordre (généré)
├── work/                travail intermédiaire + journal.csv          (généré)
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

- Expressions : `If`, `&`, arithmétique, comparaisons, `like`, texte
  (`Index` avec n-ième occurrence, `SubField`, `KeepChar`…), dates (`Date#`
  avec conversion du format, `WeekYear`, `MakeDate`, `AddMonths`…), nombres,
  `IsNum`, `Match`/`WildMatch`/`Pick`, `RangeSum`, agrégations.
- **Booléens à la Qlik** : vrai vaut -1, faux 0, et tout nombre non nul est
  vrai dans une condition.
- `WHERE`, `GROUP BY`, `ORDER BY`, `DISTINCT`, `FIRST n`, LOAD empilés.
- Sources : fichier, `RESIDENT`, `INLINE`, `SQL SELECT` simple,
  `AUTOGENERATE n`, `AUTOGENERATE 1 WHILE … IterNo()` (calendriers),
  `AUTOGENERATE FieldValueCount()`.
- **Ordre de chargement Qlik** (voir plus bas) : `RowNo`, `RecNo`, `Previous`,
  `Peek` (champ simple, ou `Peek('f', n, 'T')` sur une autre table),
  `FirstValue`, `LastValue`, `NoOfRows`, `FIRST n`, `DISTINCT` et mappings
  (première valeur gardée), dans l'ordre exact de Qlik.
- `Exists(champ[, expr])` et `NOT Exists(…)`, y compris le dédoublonnage
  quand le LOAD charge lui-même le champ.
- Variables `LET` calculées (`Peek`, `NoOfRows`, `Today()`…) : remplacées par
  leur expression, y compris dans `AUTOGENERATE vMax - vMin + 1`.
- Dates à la Qlik : `date - date` donne un nombre de jours.
- `Dual(texte, nombre)` : le texte est gardé (Parquet n'a qu'un type).
- `JOIN` (clés implicites, ordre des colonnes de Qlik), `KEEP`,
  `CONCATENATE`, auto-concaténation, `DROP FIELD`.
- `ApplyMap` → `LEFT JOIN` sur la source du mapping + `COALESCE`.
- `AutoNumber`, `AutoNumberHash128/256` → `xxhash64` : clé stable entre les
  tables, mais pas les mêmes nombres que Qlik ; ces colonnes sont exclues
  automatiquement de la comparaison.
- Variables, `CALL` de SUB, boucles `FOR` à liste fixe, `IF` évaluables.
- Commentaires (`//`, `/* */`, `REM`) retirés ; tout ce qui suit un
  `EXIT SCRIPT;` est ignoré.
- Apostrophes : `'l''été'` (Qlik) devient `"l'été"`. En Spark SQL, `''` colle
  deux chaînes (« lété ») : `check.py` le refuse dans une requête.

## L'ordre des lignes, comme dans Qlik

Qlik traite les lignes dans leur ordre physique de chargement ; Spark n'a
pas d'ordre. Chaque table porte donc une colonne technique `_qlik_order`
(`array<bigint>`) :

- **lecture** : reprise du fichier si elle y est (fichier écrit par un job
  migré), sinon numérotation dans l'ordre de lecture, avant toute opération ;
- **propagation** selon les règles de Qlik : `ORDER BY` (puis ordre
  précédent à égalité), `CONCATENATE` (lignes ajoutées après), `JOIN` (ordre
  de la table de gauche, puis lignes sans correspondance), `GROUP BY` et
  `DISTINCT` (première occurrence) ;
- **écriture** : gardée dans chaque sortie, le script suivant hérite de
  l'ordre exact, comme avec un QVD.

`compare.py` l'ignore. Pour des données d'entrée converties depuis des QVD,
le plus fiable est d'y ajouter le numéro de ligne d'origine dans une colonne
`_qlik_order` ; à défaut, l'ordre de lecture d'un fichier Parquet unique est
le bon.

## Entrées et sorties : `conf/io.yml`

Chaque fichier Qlik est rattaché à l'entrée (ou la sortie) de `conf/io.yml`
de même nom, ou dont la `location` porte ce nom. Sa `location` et son
`format` sont utilisés par `job.py` ; son `schema_file` donne les colonnes
(les `LOAD *` deviennent traduisibles sans aucune donnée) et leurs types.

```bash
python tools/conf.py      # affiche ce qui a été compris de conf/io.yml
```

Un fichier absent de `conf/io.yml` suit la règle par défaut : écrit par un
`STORE` → `data/output/<nom>`, sinon `data/input/<nom>`, en Parquet. Le
rapport liste ces fichiers.

## Le modèle n'écrit rien

Le prompt complet (brouillon SQL, code Qlik, faits, skills utiles) est
envoyé **dans le message**, et le modèle répond par la requête dans un bloc
```` ```sql ````. C'est `translate.py` qui l'écrit dans `query.sql`.
L'agent `translator` peut **lire** le projet (`.opencode/` compris) s'il y
tient, mais ne peut ni écrire, ni exécuter, ni aller sur le web : aucune
demande de permission ne peut le bloquer.

## Vérifier que le modèle répond

```bash
python tools/translate.py --selftest
```

Envoie une question triviale à l'agent et affiche : code retour, outils que
le modèle a tenté d'utiliser, texte reçu, SQL extrait. `migrate.py` fait ce
test automatiquement avant de traduire et s'arrête s'il échoue, plutôt que
d'enchaîner des échecs sur toutes les tables.

`translate.py` lance `opencode run --format json` et lit le flux
d'événements : il distingue ainsi un modèle qui a répondu, un modèle qui a
tenté un outil (refusé) au lieu de répondre, et un modèle qui n'a produit que
du raisonnement. Version d'opencode sans `--format json` : repli
automatique sur la sortie texte.

## Voir ce que le modèle a fait

Pour chaque table envoyée au modèle, dans `work/<script>/<table>/` :

| Fichier | Contenu |
|---|---|
| `prompt.md` | le prompt de la table |
| `model_1.log`, `model_2.log` | commande, **message envoyé**, durée et **réponse brute** (stdout puis stderr), par tentative |
| `query.attempt1.sql`, `query.attempt2.sql` | la requête extraite de chaque réponse |

En tête de chaque log, une ligne `# Note` résume ce qui s'est passé :
« a tenté d'utiliser un outil (read…) au lieu de répondre », « n'a produit
que du raisonnement », « réponse VIDE », « aucune requête SQL trouvée » ou
« opencode a échoué (code retour non nul) ». Dans les trois cas, la
tentative est relancée une fois ; si aucun appel n'aboutit, la table passe
en `erreur_modele` (relancée au prochain `migrate.py`).

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
| `conf.py` | lecture de `conf/io.yml` et des schémas |
| `assemble.py` | `src/<script>/` : les `.sql` + `job.py` |
| `job_template.py` | gabarit du `job.py`, seuls appels à `pyspark-commons` |
| `verify.py` | lance le job, compare avec `qlik_outputs/` |
| `compare.py` | comparaison de deux tables |
| `orchestrate.py` | `src/run_all.py`, ordre des scripts entre eux |
| `journal.py` | `work/journal.csv` |

## Limites

- Laissés au modèle : `Peek` récursif (cumuls), `FirstSortedValue`,
  `ConvertToLocalTime` sans fuseau explicite, `RIGHT`/`INNER KEEP` vers une
  table d'une autre section, boucles sur `FileList`, `SQL SELECT` avec
  jointure ou `WHERE` (dialecte de la base).
- `JOIN` (ordre des lignes) : Qlik ne documente pas l'ordre après une
  jointure ; la règle « gauche puis lignes sans correspondance » est à
  confirmer par la comparaison.
- `AUTOGENERATE 1 WHILE` : au plus 100 000 itérations.
- `ApplyMap` : en cas de clés en double, Qlik garde la première valeur, la
  traduction prend la plus petite.
- `Date(x)` sur un nombre (date Qlik en jours) n'est pas converti.
- `QUALIFY`, `NullAsValue`, `SET NullInterpret/NullValue` : signalés, à
  compléter.
- L'ordre entre scripts ne voit que les fichiers, pas les bases de données.
- Testé ici avec un vrai Spark 4.0 (ta cible est 3.5.8) et un faux
  `pyspark-commons` reproduisant son API ; pas avec un vrai opencode.
