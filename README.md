# Migration Qlik → Spark SQL

Chaque script Qlik est traduit en requêtes Spark SQL, une par table, rangées
dans `src/<nom-projet>/sql/`. Un job unique, `src/<nom-projet>/job.py`, les
enchaîne pour tout le projet en s'appuyant sur le socle technique
`spark-foundation-core`.

L'essentiel de la traduction est fait en Python, sans modèle. On n'appelle
le modèle que pour les morceaux qu'on ne sait pas traduire à coup sûr. Ils
sont marqués `TODO(model)` dans une requête par ailleurs déjà écrite.

## Arborescence (un projet par zone médaillon)

```
mon-projet/
├── script_qvd/               scripts Qlik à traduire (.qvs, .txt ou sans extension) (à fournir)
├── conf/io.yml               entrées / sorties : format, location, schéma (à fournir)
├── conf/schema/*.yml         colonnes et types des entrées                (à fournir)
├── data/input/               données lues (si absentes de conf/io.yml)    (à fournir)
├── data/qlik_outputs/        sorties Qlik pour comparer, facultatif       (à fournir)
├── data/output/              données écrites par le job                   (généré)
├── src/<nom-projet>/job.py   job du projet, tous les scripts dans l'ordre (généré)
├── src/<nom-projet>/sql/     <script>__01_<table>.sql, …                  (généré)
├── work/                     travail intermédiaire, journal.csv           (généré)
├── .opencode/agents/translator.md
├── .opencode/skills/         4 skills SQL (jointures, mappings, chaînes TMP, Peek/Previous)
├── AGENTS.md
└── tools/
```

`<nom-projet>` est le nom du dossier du projet, ou la valeur de la variable
`QLIK_PROJECT` si elle est définie. Comme `src/<nom-projet>/` est un package
Python (le kit y crée un `__init__.py` s'il manque) et que ce nom est aussi
celui du traitement passé à `@pipeline`, il est ramené à un identifiant
Python : `mon-projet` devient `mon_projet`. Tous les scripts partagent le même
dossier `sql/`, d'où le préfixe `<script>__` dans les noms de fichiers : deux
scripts peuvent avoir une table du même nom. Le numéro suit l'ordre des
tables dans le script.

## Les deux bibliothèques

- `spark-foundation-core` (importé `spark_foundation`) : le socle
  technique. Il crée la session, configure les logs, et lit ou écrit chaque
  entrée et sortie à partir de son nom dans `conf/io.yml`. Le job utilise
  `pipeline`, `read_input` et `write_output`, comme n'importe quel job du
  socle.
- `pyspark-commons` : ne garde plus que des fonctions de transformation. Le
  SQL généré n'en a pas besoin, donc le job ne l'importe pas.

Tous les appels au socle sont regroupés dans `tools/job_template.py`. Si
son API change, on corrige ce fichier et on relance `migrate.py`, rien
d'autre à toucher.

## La commande

Toutes les commandes se lancent depuis la racine du projet : les chemins
(`conf/`, `src/`, `work/`, `data/`) partent du dossier courant.

Deux sortes d'arguments, à ne pas confondre :
- `migrate.py`, `prepare.py` et `fill_conf.py` prennent un script Qlik ou
  le dossier `script_qvd/`. Les scripts sans extension ou en `.txt` sont
  pris aussi.
- `check.py`, `translate.py`, `assemble.py` et `verify.py` prennent le
  dossier de travail d'un script, `work/<script>`, créé par `prepare.py`. Si
  on leur passe le script lui-même (`script_qvd/ventes.qvs`) ou juste son
  nom (`ventes`), ils retrouvent `work/ventes` tout seuls.

```bash
python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois, puis on relit
python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier
python src/<nom-projet>/job.py                            # lancer le résultat
```

Pour chaque script, `migrate.py` traduit ce qu'il peut sans modèle, fait
compléter les `TODO(model)` par le modèle (une table par appel, syntaxe
vérifiée par le parseur Spark, une relance en cas d'erreur), range le tout
dans `src/`, puis lance et compare avec Qlik s'il trouve des sorties dans
`data/qlik_outputs/`.

On peut le relancer autant qu'on veut : ce qui est fait et n'a pas changé
n'est pas refait.

Pour savoir où on en est, `work/<script>/rapport.md` dit table par table ce
qui a été traduit sans modèle, ce qui reste à compléter, et pourquoi.

## Le job du projet : `src/<nom-projet>/job.py`

```bash
python src/<nom-projet>/job.py                  # tous les scripts
python src/<nom-projet>/job.py --from ventes    # reprendre après un échec
python src/<nom-projet>/job.py --only ventes    # un seul script
```

- Les scripts tournent dans l'ordre de leurs dépendances (et `migrate.py`
  les traite dans ce même ordre) : un script qui lit
  un fichier écrit par un autre passe après lui, comme avec l'enchaînement
  des tâches de rechargement dans Qlik. L'ordre est calculé à partir de
  `work/stores.json`, qui recense ce que chaque script lit et écrit.
- Le job a la forme habituelle du socle : une fonction
  `run(spark, args)` décorée par `@pipeline("<nom-projet>")`, lancée par
  `raise SystemExit(run())`. C'est le socle qui fournit la session.
- Les lectures et écritures passent par `read_input("<nom>")` et
  `write_output(df, "<nom>")`, avec les noms de `conf/io.yml`. La location,
  le format et la résolution des chemins relatifs sont donc l'affaire du
  socle.
- Une lecture ou une étape SQL qui plante est loggée, et les suivantes
  continuent. Un script qui dépend d'un script en échec n'est pas lancé. À
  la fin, s'il y a eu le moindre échec, le job lève une erreur qui liste les
  scripts concernés.
- Les vues d'un script sont supprimées quand il a fini, pour que deux
  scripts puissent avoir une table du même nom.
- Chaque `STORE` du script Qlik devient une écriture. Un `STORE` qu'on
  n'arrive à rattacher à aucune table est signalé dans le rapport et à
  l'assemblage.
- Seuls les scripts entièrement validés entrent dans le job. Les autres sont
  listés par `orchestrate.py`, qui prévient aussi si un script du job dépend
  de l'un d'eux.

## Ce que le traducteur sait faire sans modèle

- Les expressions : `If`, `&`, arithmétique, comparaisons, `like`, texte
  (`Index` avec n-ième occurrence, `SubField`, `KeepChar`…), dates (`Date#`
  avec conversion du format, `WeekYear`, `MakeDate`, `AddMonths`…), nombres,
  `IsNum`, `Match`/`WildMatch`/`Pick`, `RangeSum`, agrégations.
- Les booléens façon Qlik : vrai vaut -1, faux vaut 0, et dans une condition
  tout nombre non nul est vrai.
- `WHERE`, `GROUP BY`, `ORDER BY`, `DISTINCT`, `FIRST n` et les LOAD empilés.
- Les sources : fichier, `RESIDENT`, `INLINE`, `SQL SELECT` simple et
  `AUTOGENERATE` (détaillé plus bas).
- Tout ce qui dépend de l'ordre de chargement (voir la section sur l'ordre
  des lignes) : `RowNo`, `RecNo`, `Previous`, `Peek` (champ simple, ou
  `Peek('f', n, 'T')` sur une autre table), `FirstValue`, `LastValue`,
  `NoOfRows`, `FIRST n`, `DISTINCT` et les mappings, qui gardent la première
  valeur chargée comme Qlik.
- `Exists(champ[, expr])` et `NOT Exists(…)`, y compris le dédoublonnage
  quand le LOAD charge lui-même le champ.
- Les variables `LET` calculées (`Peek`, `NoOfRows`, `Today()`…), remplacées
  par leur expression, y compris dans `AUTOGENERATE vMax - vMin + 1`.
- Les dates à la Qlik : `date - date` donne un nombre de jours, `Floor` et
  `Ceil` sur une date ou un horodatage gardent le jour. `Min`, `Max`,
  `FirstValue`, `LastValue` et `Peek` d'une colonne de type date restent des
  dates, ce qui permet de traduire le calendrier classique (`LET vMin =
  Num(Peek('MinDate', 0, 'T'))` puis `AUTOGENERATE vMax - vMin + 1`). Une colonne déclarée
  texte dans `conf/schema` mais au format `DateFormat` du script
  (`DD/MM/YYYY`…) est lue comme une date dans les calculs, les comparaisons
  et les fonctions de date, comme Qlik le fait au chargement.
- La casse des noms de champs est respectée, comme dans Qlik.
- `Dual(texte, nombre)` : on garde le texte, Parquet n'ayant qu'un type par
  colonne (choix à valider).
- `JOIN` (clés implicites, ordre des colonnes de Qlik), `KEEP`,
  `CONCATENATE`, l'auto-concaténation et `DROP FIELD`.
- `ApplyMap`, traduit en `LEFT JOIN` sur la source du mapping, avec un
  `COALESCE` pour la valeur par défaut.
- `AutoNumber` et `AutoNumberHash128/256`, traduits en `xxhash64`. La clé
  est stable d'une table à l'autre, mais ce ne sont pas les mêmes nombres
  que dans Qlik, donc ces colonnes sont exclues d'office de la comparaison.
- Les variables, les `CALL` de SUB, les boucles `FOR` sur une liste fixe et
  les `IF` qu'on peut évaluer.
- Les commentaires (`//`, `/* */`, `REM`) sont retirés, et tout ce qui suit
  un `EXIT SCRIPT;` est ignoré.
- Les apostrophes : `'l''été'` côté Qlik devient `"l'été"`. Attention, en
  Spark SQL `''` colle deux chaînes et on obtient « lété ». `check.py`
  refuse donc cette écriture dans une requête.

## `AUTOGENERATE`, toujours traduit sans modèle

| Forme Qlik | Spark SQL généré |
|---|---|
| `AUTOGENERATE 10` | `range(1, 11)` |
| `AUTOGENERATE vMax - vMin + 1` (variables, même des `LET` calculés par `Peek`) | `explode(sequence(1, <expression>))` |
| `AUTOGENERATE 1 WHILE <condition sur IterNo()>` (calendriers) | une ligne par itération jusqu'à la première fausse, comme Qlik (100 000 au plus) |
| `AUTOGENERATE FieldValueCount('F')` avec `FieldValue('F', RecNo())` | les valeurs distinctes du champ `F` dans toutes les tables déjà chargées |

`RecNo()`, `RowNo()` et `IterNo()` y sont exacts. Si le modèle est appelé
sur une table qui contient un `AUTOGENERATE`, c'est pour une autre
expression de la table, ou parce que la borne dépend d'une variable qu'on
n'a pas trouvée (boucle, include). Le rapport le précise.

## L'ordre des lignes, comme dans Qlik

Qlik traite les lignes dans l'ordre où elles ont été chargées, alors que
Spark n'a pas d'ordre du tout. Chaque table porte donc une colonne
technique `_qlik_order` (`array<bigint>`) :

- à la lecture, on la reprend si elle est déjà dans le fichier (fichier
  écrit par un job migré). Sinon on numérote les lignes dans l'ordre de
  lecture, avant toute autre opération.
- elle suit ensuite les règles de Qlik : `ORDER BY` (et à égalité l'ordre
  d'avant), `CONCATENATE` (les lignes ajoutées passent après), `JOIN`
  (l'ordre de la table de gauche, puis les lignes sans correspondance),
  `GROUP BY` et `DISTINCT` (première occurrence).
- elle est écrite dans chaque sortie, si bien que le script suivant récupère
  exactement le même ordre, comme avec un QVD.

`compare.py` compare d'abord le contenu sans tenir compte de l'ordre. Si
le contenu est identique, il trie la sortie Spark sur `_qlik_order` et la
compare ligne à ligne avec l'export Qlik, qui est dans l'ordre de
chargement. Un écart d'ordre donne le verdict « identique, ordre
différent » : il est signalé, mais ne compte comme un écart qu'avec
`--strict-order` (`compare.py` ou `verify.py`), tant que la règle d'ordre
après un `JOIN` n'est pas confirmée.

Pour des données d'entrée converties depuis des QVD, le plus sûr est d'y
ajouter le numéro de ligne d'origine dans une colonne `_qlik_order`. À
défaut, l'ordre de lecture d'un fichier Parquet unique convient.

## Entrées et sorties : `conf/io.yml`

Chaque fichier lu ou écrit par le script Qlik est rattaché à l'entrée (ou la
sortie) de `conf/io.yml` qui porte le même nom, ou dont la `location` porte
ce nom. Le job passe ce nom à `read_input()` ou `write_output()`. Le
`schema_file` donne les colonnes et leurs types, ce qui permet de traduire
les `LOAD *` sans avoir les données.

```bash
python tools/conf.py      # pour voir ce qui a été compris de conf/io.yml
```

```yaml
input:
  sites:
    format: csv
    schema_file: schemas/sites.yml
    location: ${entrees}/sites.csv
outputs:
  disponibilite_equipement:
    location: ${sorties}/disponibilite_equipement
```

Comme le socle ne lit et n'écrit que par nom, tout fichier lu ou écrit par
un script doit être déclaré dans `conf/io.yml`. Une lecture se cherche dans
`input`, une écriture dans `outputs`. Un fichier produit par un script et lu
par un autre doit donc apparaître dans les deux sections, avec la même
location : `read_input()` ne lit que ce qui est déclaré en entrée. Le rapport liste ceux qui
manquent, et `assemble.py` refuse d'assembler un script tant qu'il en reste.
`migrate.py` l'indique alors dans son résumé. Pour la traduction elle-même,
un fichier non déclaré est cherché par défaut dans `data/input/<nom>` (ou
`data/output/<nom>` s'il est écrit par un `STORE`), ce qui permet d'avancer
sur les requêtes en attendant de compléter `conf/io.yml`.

### `migrate.py` complète `conf/io.yml` et les schémas

Avant de traduire, `migrate.py` lance `tools/fill_conf.py`, qui ajoute ce
qui manque pour les scripts du dossier. La règle : on ajoute, on ne modifie
jamais ce qui existe.

- Une entrée ou une sortie absente est ajoutée à la fin de sa section, en
  texte, avec l'indentation du fichier : les commentaires et la mise en
  forme sont conservés. Chaque ajout porte un commentaire daté qui dit
  pourquoi il est là.
- Un schéma absent est créé dans la forme des schémas déjà présents : par
  défaut celle du socle, `columns:` puis `NomCol: TYPE`, types en
  majuscules. Les noms de colonnes gardent la casse du script Qlik (ou du
  fichier de données), puisque Qlik y est sensible. Un schéma existant n'est
  jamais retouché, même s'il lui manque des colonnes : c'est signalé, car le
  socle s'en sert pour lire le fichier et une colonne en trop pourrait le
  casser.
- Avant de modifier `io.yml`, une copie est gardée dans `conf/io.yml.bak`.
  Si le résultat n'est plus du YAML lisible, l'original est conservé.
- Relancer ne rajoute rien : ce qui a été ajouté est désormais déclaré.

```bash
python tools/migrate.py script_qvd/ --no-conf          # sans toucher à conf/
python tools/fill_conf.py script_qvd/                  # seulement compléter conf/
python tools/fill_conf.py script_qvd/ --draft          # brouillon dans conf/draft/
```

D'où viennent les informations :

- Les entrées et sorties viennent des `FROM` et des `STORE` des scripts,
  nommées d'après les fichiers Qlik, avec le même préfixe de location
  (`${entrees}/…`) que ce qu'il y a déjà dans `io.yml`.
- Les colonnes sont celles que les scripts lisent. Si le fichier de données
  est là (dans `data/input/` ou à sa location), on prend toutes ses
  colonnes, et on signale celles que les scripts lisent mais qui n'y sont
  pas. Pour un fichier écrit par un autre script, on prend toutes les
  colonnes de son `STORE`, dans l'ordre, même celles que le lecteur
  n'utilise pas.
- Les types sont lus dans le fichier quand c'est un Parquet : ceux-là sont
  sûrs. Sinon ils sont suggérés d'après l'usage (`Year(col)` donne `date`,
  `Sum(col)` ou un calcul donne `double`, `Date#(col)` ou `Upper(col)` donne
  `string`), ou en dernier recours d'après le nom (`date_…`). Chaque type
  porte en commentaire d'où il vient. Sans aucun indice, la colonne est mise
  en `string`, le type que le kit suppose de toute façon, avec le
  commentaire « type inconnu ». Une entrée dont on ne connaît aucune colonne
  (`LOAD *` sans le fichier) est déclarée sans schéma, et c'est signalé.
- Pour les entrées déjà déclarées, il signale les colonnes utilisées par les
  scripts qui manquent dans leur schéma.

Les variables des locations (`${entrees}`, `${sorties}`…) sont résolues par
le socle quand le job tourne. Le kit, lui, en a besoin pour aller lire les
sorties au moment de la comparaison avec Qlik : on les lui donne avec
`--var` (ou par des variables d'environnement du même nom).

```bash
python tools/migrate.py script_qvd/ --var sorties=data/output
python tools/verify.py work/ventes --no-run --var sorties=data/output
```

Pour la comparaison, l'export Qlik de `data/qlik_outputs/` peut porter le
nom de la sortie dans `io.yml` ou celui du fichier de sa location. Dans un
export CSV, Qlik écrit les dates au `DateFormat` du script (`29/12/2024`) :
la comparaison les relit avec ce format, que `prepare.py` garde dans le
manifest.

## Le modèle ne traduit que des petits morceaux

Quand le traducteur bloque, c'est presque toujours sur une seule expression
d'un LOAD (`ConvertToLocalTime`, un `Peek` récursif, une fonction qu'il ne
connaît pas). Il écrit quand même toute l'instruction et laisse juste un
trou à la place de l'expression. Ensuite, pour chaque table :

1. Les expressions isolées (`holes_prompt.md`). Le modèle reçoit
   l'expression, l'instruction Qlik qui la contient, les colonnes
   disponibles avec leur type (`conf/schema`) et l'ordre de chargement, et
   répond `h1: <expression>`. Ça fait quelques centaines de tokens en
   entrée et quelques dizaines en sortie. S'il répond `h1: ?`, on le relance
   avec plus de contexte (la table résumée).
2. Les instructions entières (`prompt.md`), seulement pour ce qui bloque une
   instruction complète (colonnes inconnues, `KEEP` entre tables…), ou en
   dernier recours si les expressions isolées n'ont rien donné. Le modèle ne
   renvoie que les CTE à compléter, qu'on greffe dans le brouillon. Dans le
   brouillon qu'on lui montre, seul ce que ces CTE lisent apparaît en
   entier.

Aucune valeur de donnée n'est envoyée au modèle : uniquement des noms et des
types de colonnes, et les messages d'erreur de Spark.

Sur une table de plus de 30 colonnes, les prompts ne citent que les colonnes
que le code Qlik à traduire utilise, avec le nombre des autres. Le modèle
reçoit la consigne de garder ces dernières avec `*`, et le `SELECT` final,
écrit par l'outil, les liste toutes. Sans ça, une table de 300 colonnes
était répétée dans les faits, le brouillon et chaque expression : un prompt
passait de 2 000 à 45 000 tokens, au point de dépasser la taille maximale
d'un argument de ligne de commande (« Argument list too long »). Les erreurs
de Spark renvoyées au modèle sont aussi raccourcies, car « colonne
introuvable » y ajoute la liste de toutes les colonnes.

## Le contrôle avant l'assemblage

Avant d'assembler un script, `migrate.py` fait analyser toutes ses requêtes
par Spark (`check.py --parse --mark`), sans données : on crée des vues vides
avec les bonnes colonnes, typées d'après `conf/schema` et sensibles à la
casse comme dans Qlik. Une erreur certaine (syntaxe, `GROUP BY`, référence
ambiguë…) bloque la table avant qu'elle n'arrive dans le job. Une colonne
introuvable est seulement signalée quand les colonnes de la table amont ont
été déduites du script, faute de schéma, parce qu'on n'en est pas sûr.

Les erreurs déjà présentes dans le brouillon avant que le modèle n'y touche
viennent du traducteur. On ne les lui renvoie jamais, sinon il cherche à
corriger ce qui n'est pas de son fait.

## Le cache d'expressions : `work/expressions.json`

Chaque expression traduite par le modèle y est enregistrée comme non
vérifiée. Elle passe en vérifiée quand la comparaison de sa table avec Qlik
est identique, et elle est alors réutilisée telle quelle, sans modèle, dans
tous les scripts suivants. Tant qu'elle n'est pas vérifiée, elle est juste
proposée au modèle comme piste.

Le fichier peut se corriger à la main. Attention à ne pas le supprimer en
vidant `work/`.

## Vérifier que le modèle répond

```bash
python tools/translate.py --selftest
```

Ça envoie une question bête à l'agent et affiche le code retour, les outils
que le modèle a essayé d'utiliser, le texte reçu et le SQL extrait.
`migrate.py` fait ce test tout seul avant de traduire, et s'arrête s'il
échoue, plutôt que d'enchaîner les échecs sur toutes les tables.

`translate.py` lance `opencode run --format json` et lit les événements un
par un. Ça permet de faire la différence entre un modèle qui a répondu, un
modèle qui a tenté un outil (refusé) au lieu de répondre, et un modèle qui
n'a produit que du raisonnement. Avec une version d'opencode qui n'a pas
`--format json`, on se rabat sur la sortie texte.

## Voir ce que le modèle a fait

Pour chaque table passée au modèle, dans `work/<script>/<table>/` :

| Fichier | Contenu |
|---|---|
| `prompt.md` | le prompt de la table |
| `model_1.log`, `model_2.log` | par tentative : la commande, le message envoyé, la durée et la réponse brute (stdout puis stderr) |
| `query.attempt1.sql`, `query.attempt2.sql` | la requête extraite de chaque réponse |

La première ligne de chaque log, `# Note`, résume ce qui s'est passé :
« a tenté d'utiliser un outil (read…) au lieu de répondre », « n'a produit
que du raisonnement », « réponse VIDE », « aucune requête SQL trouvée » ou
« opencode a échoué (code retour non nul) ». Dans tous ces cas, la tentative
est relancée une fois. Si aucun appel n'aboutit, la table passe en
`erreur_modele`, et elle sera reprise au prochain `migrate.py`.

## Corriger une table à la main

1. Corriger `work/<script>/<table>/query.sql` (jamais un
   `query.attemptN.sql`). Le `prompt.md` d'à côté contient le code Qlik et
   ce qu'on sait de la table.
2. Valider la correction :
   `python tools/check.py work/<script> --only <table> --parse`
3. Relancer `migrate.py`, qui assemble et compare.
4. Noter la cause dans la colonne `resolution` de `work/journal.csv`.

Une table en `escalade` n'est jamais retraduite automatiquement, pour ne
pas écraser une correction manuelle (`translate.py --retry-escalated` si on
veut la refaire exprès).

## Tests

```bash
pip install pyspark pyyaml pandas pyarrow pytest
python -m pytest -m "not spark"     # rapide, sans Spark
python -m pytest                    # tout, Spark et Java requis
```

`tests/demo/` est un petit projet complet : deux scripts chaînés
(`synthese.txt` lit la sortie de `ventes.qvs`), un calendrier sans
extension (`Min`/`Max` de dates, `LET` avec `Peek`, `AUTOGENERATE`,
`WeekYear`), un `io.yml` à 3 espaces avec des commentaires, et les exports
Qlik attendus. Chaque test le copie dans un dossier temporaire. Le cas de la
table de 322 colonnes est construit par le test lui-même.

Les tests marqués `spark` lancent `migrate.py` de bout en bout avec un faux
`spark_foundation` (`tests/fakelib/`), qui reproduit l'API vue dans le job
`dp_test` : `pipeline(nom)`, `read_input(nom)`, `write_output(df, nom)`. Ce
ne sont que des hypothèses sur le vrai socle. Sans pyspark, ces tests sont
sautés.

## Les outils

| Outil | Rôle |
|---|---|
| `migrate.py` | la seule commande à lancer |
| `prepare.py` | analyse du script, sections, chemins, rapport |
| `qlik2sql.py` | traduction Qlik → Spark SQL sans modèle |
| `translate.py` | complétion des `TODO(model)` par le modèle |
| `check.py` | contrôles, et syntaxe Spark avec `--parse` |
| `conf.py` | lecture de `conf/io.yml` et des schémas |
| `fill_conf.py` | complète `conf/io.yml` et les schémas (lancé par `migrate.py`) |
| `assemble.py` | copie des requêtes d'un script dans `src/<nom-projet>/sql/` |
| `job_template.py` | gabarit du job, le seul endroit qui appelle `spark-foundation-core` |
| `verify.py` | lance le job et compare avec `data/qlik_outputs/` |
| `compare.py` | comparaison de deux tables |
| `orchestrate.py` | génère `src/<nom-projet>/job.py` et ordonne les scripts |
| `journal.py` | tenue de `work/journal.csv` |

## Limites connues

- Ce qui reste au modèle : `Peek` récursif (cumuls), `FirstSortedValue`,
  `ConvertToLocalTime` sans fuseau explicite, `RIGHT`/`INNER KEEP` vers une
  table d'une autre section, boucles sur `FileList`, `SQL SELECT` avec
  jointure ou `WHERE` (le dialecte dépend de la base).
- L'ordre des lignes après un `JOIN` : Qlik ne le documente pas. La règle
  « la gauche, puis les lignes sans correspondance » reste à confirmer sur
  de vrais exports, avec `verify.py --strict-order`.
- Avec `FieldValue('F', RecNo())`, les valeurs sont numérotées dans l'ordre
  trié, alors que Qlik suit l'ordre de chargement.
- `Date(x)` appliqué à un nombre (date Qlik en jours) n'est pas converti.
- Piège de Spark hors mode ANSI : `FROM t WHERE (x)` est lu comme l'alias de
  table `WHERE`, et le filtre disparaît sans erreur. Le traducteur double
  les parenthèses dans ce cas, et `check.py` refuse cette écriture si le
  modèle la produit.
- Le typage des dates repose sur `conf/schema`. Une colonne de dates sans
  type déclaré (ou déclarée en texte) n'est pas reconnue comme date par
  `Min`, `Max` ou `Peek`. Dans ce cas, le contrôle ne peut pas trancher sur
  les types et laisse passer, et c'est le job qui plantera sur `date - date`.
- `QUALIFY`, `NullAsValue`, `SET NullInterpret/NullValue` : signalés, à
  compléter à la main.
- L'ordre entre scripts ne tient compte que des fichiers, pas des tables en
  base de données.
- Testé avec Spark 4.0 en local (la cible est la 3.5.8) et une version
  bouchonnée de `spark-foundation-core` qui reproduit l'API vue dans le job
  `dp_test`. Pas encore testé avec le vrai socle ni avec un vrai opencode.
- Ce qu'on suppose du socle sans l'avoir vérifié : `@pipeline` passe à
  `run()` les arguments de la ligne de commande (pour `--from` et
  `--only`), et une exception levée dans `run()` donne un code retour non
  nul (`verify.py` s'en sert pour savoir si le job a planté).
