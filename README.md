# Migration Qlik -> Spark SQL

Chaque script Qlik est traduit en requetes Spark SQL, une par table, rangees
dans `src/<nom-projet>/sql/`. Un job unique, `src/<nom-projet>/job.py`, les
enchaine pour tout le projet. Le job ne contient aucun code PySpark : il
appelle des fonctions du socle `spark_foundation` (`spark-foundation-core`),
qui s'appuie sur `pyspark_commons`. Un developpeur Qlik peut le lire sans
connaitre Spark.

L'essentiel de la traduction est fait en Python, sans modele. On n'appelle
le modele que pour les morceaux qu'on ne sait pas traduire a coup sur. Ils
sont marques `TODO(model)` dans une requete par ailleurs deja ecrite.

## Arborescence (un projet par zone medaillon)

```
mon-projet/
|-- script_qvd/               scripts Qlik a traduire (.qvs, .txt ou sans extension) (a fournir)
|-- conf/io.yml               entrees / sorties : format, location, schema (a fournir)
|-- conf/schema/*.yml         colonnes et types des entrees                (a fournir)
|-- data/input/               donnees lues (si absentes de conf/io.yml)    (a fournir)
|-- data/qlik_outputs/        sorties Qlik pour comparer, facultatif       (a fournir)
|-- data/output/              donnees ecrites par le job                   (genere)
|-- src/<nom-projet>/job.py   job du projet, tous les scripts dans l'ordre (genere)
|-- src/<nom-projet>/scripts/ un module par script Qlik : ses etapes        (genere)
|-- src/<nom-projet>/sql/     <script>__01_<table>.sql, ...                  (genere)
|-- work/                     travail intermediaire, journal.csv           (genere)
|-- .opencode/agents/translator.md
|-- .opencode/skills/         4 skills SQL (jointures, mappings, chaines TMP, Peek/Previous)
|-- AGENTS.md
`-- tools/
```

`<nom-projet>` est le nom du dossier du projet, ou la valeur de la variable
`QLIK_PROJECT` si elle est definie. Comme `src/<nom-projet>/` est un package
Python (le kit y cree un `__init__.py` s'il manque) et que ce nom est aussi
celui du traitement passe a `@pipeline`, il est ramene a un identifiant
Python : `mon-projet` devient `mon_projet`. Tous les scripts partagent le meme
dossier `sql/`, d'ou le prefixe `<script>__` dans les noms de fichiers : deux
scripts peuvent avoir une table du meme nom. Le numero suit l'ordre des
tables dans le script.

## Les deux bibliotheques

Le code genere n'importe que le socle, et seulement ces fonctions :

| Import dans le code genere | Fichier du socle | Role |
|---|---|---|
| `from spark_foundation import pipeline, run_scripts` (`job.py`) | `spark_foundation/runner.py` | `@pipeline("<nom-projet>")` ouvre la session ; `run_scripts(args, script, ...)` enchaine les scripts et trouve `sql/` a cote de `job.py` |
| `from spark_foundation.tables import read_input, run_sql, write_output` (`scripts/`) | `spark_foundation/tables.py` | lire une entree de `conf/io.yml`, calculer une table Qlik avec sa requete, l'ecrire |

- `spark_foundation/internal/` : session et reglages SQL (`session.py`),
  lecture de `conf/io.yml` (`config.py`), etat du job (`context.py`),
  tables du catalogue pour OpenLineage (`lineage.py`), lecture et ecriture
  des fichiers (`io/access.py` : `read_config`, `write_config`,
  `input_location`, `output_location`). Reserve aux data engineers.
- `pyspark_commons` : uniquement des transformations de DataFrame (ordre de
  chargement Qlik, colonnes). Le socle s'en sert ; le job ne l'importe
  jamais directement.

`core/` et `commons/` dans ce depot sont la version d'origine qui a servi
de base au socle. Les tests s'en servent comme doublure du socle :
`tests/fakelib/spark_foundation/` reprend son API publique (`runner.py`,
`tables.py`) au-dessus de `core/`. Si l'API du socle change, c'est
`tools/job_template.py` (les imports du code genere) et cette doublure
qu'il faut suivre.

Toute fonction, classe et module de `core/`, `commons/` et `tools/` a une
docstring et des annotations de type (`DataFrame` pour un DataFrame) :
`ruff` (regles `D1` et `ANN`) et `mypy` le verifient. Le code genere
(`job.py`, `scripts/`) aussi : la docstring de chaque script dit ce qu'il
lit, calcule et ecrit, et le test de bout en bout passe le projet genere a
`ruff` et `mypy`.

Le texte du job et des modules de scripts est dans
`tools/job_template.py`. Si on veut changer leur forme, on corrige ce
fichier et on relance `migrate.py` (ou `orchestrate.py`).

## La commande

Toutes les commandes se lancent depuis la racine du projet : les chemins
(`conf/`, `src/`, `work/`, `data/`) partent du dossier courant.

Avant tout le reste, `migrate.py` lance `tools/normalize.py`, qui renomme
les fichiers du projet : les scripts de `script_qvd/` passent en minuscules,
et dans `data/input/` et `data/qlik_outputs/` les noms perdent les
caracteres que Hadoop ne sait pas lire (voir plus bas).
Si deux fichiers tombent sur le meme nom, rien n'est renomme et on
s'arrete. `python tools/normalize.py --dry-run` montre ce qui changerait.
Les fichiers du cluster ne sont pas touches : ceux qui ont ces caracteres sont a
renommer de la meme facon.

Deux sortes d'arguments, a ne pas confondre :
- `migrate.py`, `prepare.py` et `fill_conf.py` prennent un script Qlik ou
  le dossier `script_qvd/`. Les scripts sans extension ou en `.txt` sont
  pris aussi.
- `check.py`, `translate.py`, `assemble.py` et `verify.py` prennent le
  dossier de travail d'un script, `work/<script>`, cree par `prepare.py`. Si
  on leur passe le script lui-meme (`script_qvd/ventes.qvs`) ou juste son
  nom (`ventes`), ils retrouvent `work/ventes` tout seuls.

```bash
python tools/migrate.py script_qvd/ventes.qvs --pilot 3   # la 1re fois, puis on relit
python tools/migrate.py script_qvd/ --jobs 4              # tout le dossier
python src/<nom-projet>/job.py                            # lancer le resultat
```

Pour chaque script, `migrate.py` traduit ce qu'il peut sans modele, fait
completer les `TODO(model)` par le modele (une table par appel, syntaxe
verifiee par le parseur Spark, une relance en cas d'erreur), range le tout
dans `src/`, puis lance et compare avec Qlik s'il trouve des sorties dans
`data/qlik_outputs/`.

On peut le relancer autant qu'on veut : ce qui est fait et n'a pas change
n'est pas refait.

Pour savoir ou on en est, `work/<script>/rapport.md` dit table par table ce
qui a ete traduit sans modele, ce qui reste a completer, et pourquoi.

## Le job du projet : `src/<nom-projet>/job.py`

```bash
python src/<nom-projet>/job.py                  # tous les scripts
python src/<nom-projet>/job.py --from ventes    # reprendre apres un echec
python src/<nom-projet>/job.py --only ventes    # un seul script
```

Sur le cluster, depuis la racine du projet (la commande est rappelee en tete
de `job.py`) :

```bash
spark-submit --name <nom-projet> --deploy-mode client \
    --py-files <spark-foundation-core et pyspark-commons, s'ils ne sont pas installes> \
    src/<nom-projet>/job.py [--from <script> | --only <script>]
```

En mode client, le driver lit les requetes dans `src/<nom-projet>/sql/`. Le
mode cluster demanderait d'envoyer aussi ce dossier et `conf/` : pas prevu
pour l'instant. Les reglages SQL dont dependent les requetes (sensibilite a
la casse, ANSI desactive, `timeParserPolicy=CORRECTED`) sont poses par
le socle a l'ouverture de la session, il n'y a donc pas de `--conf` a passer.

Le job tient en quelques lignes :

```python
@pipeline("demo")
def run(spark: SparkSession, args: list[str]) -> None:
    run_scripts(
        args,
        calendrier,
        ventes,
        synthese,  # apres ventes, dont il lit les sorties
    )
```

et chaque script Qlik devient un module de `scripts/`, une ligne par etape :

```python
def run() -> None:
    """Rejoue le script ventes.qvs."""
    read_input("ventes", table="in_ventes")
    run_sql("ventes__01_ventes.sql", table="Ventes")
    write_output("ventes_enrichies_v2", table="Ventes")
```

- L'ordre des scripts est l'ordre des appels dans `run_scripts`, il n'y a
  pas de liste a part. Il compte encore : un script qui lit un fichier
  ecrit par un autre doit passer apres lui, comme avec l'enchainement des
  taches de rechargement dans Qlik. `orchestrate.py` le calcule a partir de
  `work/stores.json` (ce que chaque script lit et ecrit) et le note en
  commentaire. Dans un script, l'ordre des etapes est celui du script Qlik.
- Le job s'arrete au premier script en echec, comme un rechargement Qlik :
  les suivants liraient des donnees absentes ou d'hier. L'erreur dit d'ou
  reprendre (`--from <script>`). Toutes les ecritures sont en overwrite,
  relancer ne cree donc pas de doublon.
- Les lectures et ecritures passent par `read_input` et `write_output`,
  avec les noms de `conf/io.yml`. La location, le format et la resolution
  des chemins relatifs sont l'affaire du socle.
- Les vues d'un script sont supprimees quand il a fini, pour que deux
  scripts puissent avoir une table du meme nom.
- Chaque `STORE` du script Qlik devient une ecriture. Un `STORE` qu'on
  n'arrive a rattacher a aucune table est signale dans le rapport et a
  l'assemblage. La boucle qui ecrit toutes les tables
  (`FOR i = NoOfTables()-1 TO 0`, `LET vTable = TableName($(i))`) est
  deroulee sur les tables presentes a ce moment.
- Les tables encore en memoire a la fin du script, sans `STORE`, forment le
  modele de donnees de l'app : c'est le resultat d'une app de restitution.
  Elles sont ecrites aussi, sous le nom de la table. Si un autre script a
  deja une sortie de ce nom, chacun prefixe la sienne par son nom de script
  (`<script>_<table>`), pour ne pas s'ecraser.
- Une sortie est ecrite sous le nom du fichier du `STORE` (sans extension,
  casse gardee), et sa cle dans `io.yml` est ce nom en minuscules :
  `STORE T INTO [lib://qvd/Clients.qvd]` donne la cle `clients`, de location
  `${sorties}/Clients`. Le fichier relu par un autre script a la meme cle en
  entree.
- Les noms de fichiers sont rendus lisibles par Spark, partout et des le
  debut : Hadoop prend le # pour un fragment d'URI et `[ ] { } * ?` pour un
  motif, et ne retrouve pas le fichier. Les crochets et accolades deviennent
  des parentheses, `# * ?` des _ : `[anon]-TT#TEST.CSV` devient
  `(anon)-TT_TEST.CSV`. Jamais de _ en tete de nom : Spark ignore sans rien
  dire les fichiers qui commencent par _ ou . (comme `_SUCCESS`).
  `normalize.py` renomme les fichiers de `data/input/` et
  `data/qlik_outputs/` de la meme facon, et signale ceux qui commencent deja
  par _ ou .
- Chaque table Qlik est ecrite dans une table du catalogue,
  `<base>.<script>__<table>` (base `<nom-projet>_qlik` par defaut,
  `--database` pour en changer), en overwrite, puis relue de la. C'est pour OpenLineage :
  une vue temporaire n'est pas un jeu de donnees, la lignee sautait des
  fichiers lus aux sorties sans montrer les tables entre les deux. Avec
  `--temp-views`, le job garde les vues temporaires (plus rapide, mais rien
  dans la lignee). Une table dont deux colonnes ne different que par la
  casse reste en vue temporaire : le catalogue ne les distingue pas.
  En local, sans metastore Hive, le socle en cree un dans `metastore_db/`
  et les fichiers vont dans `spark-warehouse/` (tous deux ignores par git) :
  sans lui, remplacer une table existante echoue d'un lancement a l'autre.
- Seuls les scripts entierement valides entrent dans le job. Les autres sont
  listes par `orchestrate.py`, qui previent aussi si un script du job depend
  de l'un d'eux.

## Ce que le traducteur sait faire sans modele

- Les expressions : `If`, `&`, arithmetique, comparaisons, `like`, texte
  (`Index` avec n-ieme occurrence, `SubField`, `KeepChar`...), dates (`Date#`
  avec conversion du format, `WeekYear`, `MakeDate`, `AddMonths` avec ses
  deux modes...), nombres,
  `IsNum`, `Match`/`WildMatch`/`Pick`, `RangeSum`, agregations.
- Les booleens facon Qlik : vrai vaut -1, faux vaut 0, et dans une condition
  tout nombre non nul est vrai.
- `WHERE`, `GROUP BY`, `ORDER BY`, `DISTINCT`, `FIRST n` et les LOAD empiles.
- Les sources : fichier, `RESIDENT`, `INLINE`, `SQL SELECT` simple et
  `AUTOGENERATE` (detaille plus bas).
- Tout ce qui depend de l'ordre de chargement (voir la section sur l'ordre
  des lignes) : `RowNo`, `RecNo`, `Previous`, `Peek` (champ simple, ou
  `Peek('f', n, 'T')` sur une autre table), `FirstValue`, `LastValue`,
  `NoOfRows`, `FIRST n`, `DISTINCT` et les mappings, qui gardent la premiere
  valeur chargee comme Qlik.
- `Exists(champ[, expr])` et `NOT Exists(...)`, y compris le dedoublonnage
  quand le LOAD charge lui-meme le champ.
- Les variables `LET` calculees (`Peek`, `NoOfRows`, `Today()`...), remplacees
  par leur expression, y compris dans `AUTOGENERATE vMax - vMin + 1`.
- Les dates a la Qlik : `date - date` donne un nombre de jours, `Floor` et
  `Ceil` sur une date ou un horodatage gardent le jour. `Min`, `Max`,
  `FirstValue`, `LastValue` et `Peek` d'une colonne de type date restent des
  dates, ce qui permet de traduire le calendrier classique (`LET vMin =
  Num(Peek('MinDate', 0, 'T'))` puis `AUTOGENERATE vMax - vMin + 1`). Une colonne declaree
  texte dans `conf/schema` mais au format `DateFormat` du script
  (`DD/MM/YYYY`...) est lue comme une date dans les calculs, les comparaisons
  et les fonctions de date, comme Qlik le fait au chargement.
- La casse des noms de champs est respectee, comme dans Qlik.
- `Dual(texte, nombre)` : on garde le texte, Parquet n'ayant qu'un type par
  colonne (choix a valider).
- `JOIN` (cles implicites, ordre des colonnes de Qlik), `KEEP`,
  `CONCATENATE`, l'auto-concatenation et `DROP FIELD`.
- `ApplyMap`, traduit en `LEFT JOIN` sur la source du mapping, avec un
  `COALESCE` pour la valeur par defaut.
- `AutoNumber` et `AutoNumberHash128/256`, traduits en `xxhash64`. La cle
  est stable d'une table a l'autre, mais ce ne sont pas les memes nombres
  que dans Qlik, donc ces colonnes sont exclues d'office de la comparaison.
- Les variables, les `CALL` de SUB, les boucles `FOR` sur une liste fixe et
  les `IF` qu'on peut evaluer.
- Les commentaires (`//`, `/* */`, `REM`) sont retires, et tout ce qui suit
  un `EXIT SCRIPT;` est ignore.
- Les apostrophes : `'l''ete'` cote Qlik devient `"l'ete"`. Attention, en
  Spark SQL `''` colle deux chaines et on obtient "lete". `check.py`
  refuse donc cette ecriture dans une requete.

## `AUTOGENERATE`, toujours traduit sans modele

| Forme Qlik | Spark SQL genere |
|---|---|
| `AUTOGENERATE 10` | `range(1, 11)` |
| `AUTOGENERATE vMax - vMin + 1` (variables, meme des `LET` calcules par `Peek`) | `explode(sequence(1, <expression>))` |
| `AUTOGENERATE 1 WHILE <condition sur IterNo()>` (calendriers) | une ligne par iteration jusqu'a la premiere fausse, comme Qlik (100 000 au plus) |
| `AUTOGENERATE FieldValueCount('F')` avec `FieldValue('F', RecNo())` | les valeurs distinctes du champ `F` dans toutes les tables deja chargees |

`RecNo()`, `RowNo()` et `IterNo()` y sont exacts. Si le modele est appele
sur une table qui contient un `AUTOGENERATE`, c'est pour une autre
expression de la table, ou parce que la borne depend d'une variable qu'on
n'a pas trouvee (boucle, include). Le rapport le precise.

## L'ordre des lignes, comme dans Qlik

Qlik traite les lignes dans l'ordre ou elles ont ete chargees, alors que
Spark n'a pas d'ordre du tout. Chaque table porte donc une colonne
technique `_qlik_order` (`array<bigint>`) :

- a la lecture, on la reprend si elle est deja dans le fichier (fichier
  ecrit par un job migre). Sinon on numerote les lignes dans l'ordre de
  lecture, avant toute autre operation.
- elle suit ensuite les regles de Qlik : `ORDER BY` (et a egalite l'ordre
  d'avant), `CONCATENATE` (les lignes ajoutees passent apres), `JOIN`
  (l'ordre de la table de gauche, puis les lignes sans correspondance),
  `GROUP BY` et `DISTINCT` (premiere occurrence).
- elle reste a l'interieur du job (vues et tables intermediaires) et n'est
  pas ecrite dans les sorties. Un script qui relit la sortie d'un autre
  renumerote donc ses lignes dans l'ordre de lecture du fichier.

`compare.py` compare le contenu sans tenir compte de l'ordre. Les sorties
n'ayant plus `_qlik_order`, l'ordre des lignes n'est pas verifie
(`--strict-order` ne sert que pour un fichier qui aurait encore cette
colonne).

Pour des donnees d'entree converties depuis des QVD, le plus sur est d'y
ajouter le numero de ligne d'origine dans une colonne `_qlik_order`. A
defaut, l'ordre de lecture d'un fichier Parquet unique convient.

Avant toute ecriture Parquet (sortie ou table intermediaire), une colonne
toujours nulle (`Null()` dans Qlik), de type `void` que Parquet refuse, est
passee en `string`.

## Entrees et sorties : `conf/io.yml`

Chaque fichier lu ou ecrit par le script Qlik est rattache a l'entree (ou la
sortie) de `conf/io.yml` qui porte le meme nom, ou dont la `location` porte
ce nom. Le job passe ce nom a `read_input()` ou `write_output()`. Le
`schema_file` donne les colonnes et leurs types, ce qui permet de traduire
les `LOAD *` sans avoir les donnees.

```bash
python tools/conf.py      # pour voir ce qui a ete compris de conf/io.yml
```

```yaml
inputs:
  sites:
    format: csv
    schema_file: schemas/sites.yml
    location: ${entrees}/sites.csv
outputs:
  disponibilite_equipement:
    location: ${sorties}/disponibilite_equipement
```

Comme le socle ne lit et n'ecrit que par nom, tout fichier lu ou ecrit par
un script doit etre declare dans `conf/io.yml`. Une lecture se cherche dans
`inputs`, une ecriture dans `outputs`. Un fichier produit par un script et lu
par un autre doit donc apparaitre dans les deux sections, avec la meme
location : `read_input()` ne lit que ce qui est declare en entree. Le rapport liste ceux qui
manquent, et `assemble.py` refuse d'assembler un script tant qu'il en reste.
`migrate.py` l'indique alors dans son resume. Pour la traduction elle-meme,
un fichier non declare est cherche par defaut dans `data/input/<nom>` (ou
`data/output/<nom>` s'il est ecrit par un `STORE`), ce qui permet d'avancer
sur les requetes en attendant de completer `conf/io.yml`.

### `migrate.py` complete `conf/io.yml` et les schemas

Avant de traduire, `migrate.py` lance `tools/fill_conf.py`, qui ajoute ce
qui manque pour les scripts du dossier. La regle : on ajoute, on ne modifie
jamais ce qui existe.

- Une entree ou une sortie absente est ajoutee a la fin de sa section, en
  texte, avec l'indentation du fichier : les commentaires et la mise en
  forme sont conserves. Chaque ajout porte un commentaire date qui dit
  pourquoi il est la.
- Un schema absent est cree dans la forme des schemas deja presents : par
  defaut celle du socle, `columns:` puis `NomCol: TYPE`, types en
  majuscules. Les noms de colonnes gardent la casse du script Qlik (ou du
  fichier de donnees), puisque Qlik y est sensible. Un schema existant n'est
  jamais retouche, meme s'il lui manque des colonnes : c'est signale, car le
  socle s'en sert pour lire le fichier et une colonne en trop pourrait le
  casser.
- Avant de modifier `io.yml`, une copie est gardee dans `conf/io.yml.bak`.
  Si le resultat n'est plus du YAML lisible, l'original est conserve.
- Relancer ne rajoute rien : ce qui a ete ajoute est desormais declare.

```bash
python tools/migrate.py script_qvd/ --no-conf          # sans toucher a conf/
python tools/fill_conf.py script_qvd/                  # seulement completer conf/
python tools/fill_conf.py script_qvd/ --draft          # brouillon dans conf/draft/
```

D'ou viennent les informations :

- Les entrees et sorties viennent des `FROM` et des `STORE` des scripts,
  nommees d'apres les fichiers Qlik, avec le meme prefixe de location
  (`${entrees}/...`) que ce qu'il y a deja dans `io.yml`. Les sections
  s'appellent `inputs` et `outputs`, comme dans le socle. Les tables du
  modele de donnees (voir le job) sont ajoutees aux `outputs`.
- Le format d'une entree est celui que Qlik lit : `(txt, ...)` ou un `.csv`
  donne `csv`, avec le nom complet du fichier dans la location (crochets
  rendus lisibles, comme `(anon)-TT_TEST.CSV`) ; un QVD est suppose converti en
  Parquet sous le meme nom.
- Un CSV recoit une cle `options`, avec les noms des options du lecteur CSV
  de Spark, d'apres la specification Qlik : `header` (toujours ecrit),
  `sep`, `encoding`, `quote`. Hypothese a confirmer : le socle les passe
  telles quelles a `spark.read.options()`.

  ```yaml
  codes:
     format: csv
     schema_file: schemas/codes.yml
     location: ${entrees}/codes.csv
     options: {header: false, sep: ";"}
  ```
- Un CSV sans en-tete (`no labels` chez Qlik) : Qlik nomme ses colonnes
  `@1`, `@2`... Le kit les appelle `_c0`, `_c1`..., les noms que Spark leur
  donne lui-meme, qui s'ecrivent sans backticks : `@1 as Code` devient
  `_c0 AS Code`. Le schema liste toutes les colonnes, dans l'ordre, puisque
  Spark les affecte par position : celles du fichier s'il est dans
  `data/input/`, sinon de `_c0` jusqu'a la derniere que le script lit.
- Les scripts sont lus quel que soit leur encodage (UTF-16, UTF-8 avec ou
  sans BOM, Windows-1252).
- Les colonnes sont celles que les scripts lisent. Si le fichier de donnees
  est la (dans `data/input/` ou a sa location), on prend toutes ses
  colonnes, et on signale celles que les scripts lisent mais qui n'y sont
  pas. Pour un fichier ecrit par un autre script, on prend toutes les
  colonnes de son `STORE`, dans l'ordre, meme celles que le lecteur
  n'utilise pas.
- Les types, du plus sur au moins sur :
  1. les metadonnees du Parquet, trouve dans `data/input/` ou a sa location
     (variables passees par `--var`). Si une location contient une variable
     inconnue, `fill_conf.py` le dit : c'est la cause habituelle d'un
     Parquet non lu ;
  2. pour un fichier ecrit par un autre script du projet, les types de la
     requete Spark qui l'ecrit, calcules par `check.py --parse` : ce sont
     exactement ceux du Parquet que le job ecrira. Ce fichier n'est donc
     declare en entree qu'une fois son auteur traduit (`migrate.py` traite
     les auteurs d'abord et relance `fill_conf.py` avant chaque script) ;
  3. les valeurs d'un CSV (5 000 premieres lignes, lues sur place) :
     entier, decimal a point, sinon texte. Une date en texte ou un decimal
     a virgule restent en `string`, que Spark lirait mal sans options ;
  4. le script Qlik : la fonction qui recoit la colonne (`Year(col)` donne
     `date`, `Sum(col)` donne `double`, `Trim(col)` ou `Date#(col)` donne
     `string`) ou la constante a laquelle elle est comparee (`= 'OK'`,
     `> 0`, `>= '01/01/2024'`), sinon le nom (`date_...`) ;
  5. le meme champ ailleurs : dans Qlik, un nom de champ designe la meme
     donnee dans toutes les tables. Une colonne sans type sur prend celui du
     champ du meme nom connu par 1 ou 2, ou deja declare dans un schema.
  Chaque type porte en commentaire d'ou il vient, et "a verifier" des
  qu'il est devine (4 et 5). Sans aucun indice, la colonne est mise
  en `string`, le type que le kit suppose de toute facon, avec le
  commentaire "type inconnu". Une entree dont on ne connait aucune colonne
  (`LOAD *` sans le fichier) est declaree sans schema, et c'est signale.
- Pour les entrees deja declarees, il signale les colonnes utilisees par les
  scripts qui manquent dans leur schema.

Les variables des locations (`${entrees}`, `${sorties}`...) sont resolues par
le socle quand le job tourne. Le kit, lui, en a besoin pour aller lire les
sorties au moment de la comparaison avec Qlik : on les lui donne avec
`--var` (ou par des variables d'environnement du meme nom).

```bash
python tools/migrate.py script_qvd/ --var sorties=data/output
python tools/verify.py work/ventes --no-run --var sorties=data/output
```

Pour la comparaison, l'export Qlik de `data/qlik_outputs/` peut porter le
nom de la sortie dans `io.yml` ou celui du fichier de sa location. Dans un
export CSV, Qlik ecrit les dates au `DateFormat` du script (`29/12/2024`) :
la comparaison les relit avec ce format, que `prepare.py` garde dans le
manifest.

## Le modele ne traduit que des petits morceaux

Quand le traducteur bloque, c'est presque toujours sur une seule expression
d'un LOAD (`ConvertToLocalTime`, un `Peek` recursif, une fonction qu'il ne
connait pas). Il ecrit quand meme toute l'instruction et laisse juste un
trou a la place de l'expression. Ensuite, pour chaque table :

1. Les expressions isolees (`holes_prompt.md`). Le modele recoit
   l'expression, l'instruction Qlik qui la contient, les colonnes
   disponibles avec leur type (`conf/schema`) et l'ordre de chargement, et
   repond `h1: <expression>`. Ca fait quelques centaines de tokens en
   entree et quelques dizaines en sortie. S'il repond `h1: ?`, on le relance
   avec plus de contexte (la table resumee).
2. Les instructions entieres (`prompt.md`), seulement pour ce qui bloque une
   instruction complete (colonnes inconnues, `KEEP` entre tables...), ou en
   dernier recours si les expressions isolees n'ont rien donne. Le modele ne
   renvoie que les CTE a completer, qu'on greffe dans le brouillon. Dans le
   brouillon qu'on lui montre, seul ce que ces CTE lisent apparait en
   entier.

Aucune valeur de donnee n'est envoyee au modele : uniquement des noms et des
types de colonnes, et les messages d'erreur de Spark.

Sur une table de plus de 30 colonnes, les prompts ne citent que les colonnes
que le code Qlik a traduire utilise, avec le nombre des autres. Le modele
recoit la consigne de garder ces dernieres avec `*`, et le `SELECT` final,
ecrit par l'outil, les liste toutes. Sans ca, une table de 300 colonnes
etait repetee dans les faits, le brouillon et chaque expression : un prompt
passait de 2 000 a 45 000 tokens, au point de depasser la taille maximale
d'un argument de ligne de commande ("Argument list too long"). Les erreurs
de Spark renvoyees au modele sont aussi raccourcies, car "colonne
introuvable" y ajoute la liste de toutes les colonnes.

## Le controle avant l'assemblage

Avant d'assembler un script, `migrate.py` fait analyser toutes ses requetes
par Spark (`check.py --parse --mark`), sans donnees : on cree des vues vides
avec les bonnes colonnes, typees d'apres `conf/schema` et sensibles a la
casse comme dans Qlik. Une erreur certaine (syntaxe, `GROUP BY`, reference
ambigue...) bloque la table avant qu'elle n'arrive dans le job. Une colonne
introuvable est seulement signalee quand les colonnes de la table amont ont
ete deduites du script, faute de schema, parce qu'on n'en est pas sur.

Les erreurs deja presentes dans le brouillon avant que le modele n'y touche
viennent du traducteur. On ne les lui renvoie jamais, sinon il cherche a
corriger ce qui n'est pas de son fait.

## Le cache d'expressions : `work/expressions.json`

Chaque expression traduite par le modele y est enregistree comme non
verifiee. Elle passe en verifiee quand la comparaison de sa table avec Qlik
est identique, et elle est alors reutilisee telle quelle, sans modele, dans
tous les scripts suivants. Tant qu'elle n'est pas verifiee, elle est juste
proposee au modele comme piste.

Le fichier peut se corriger a la main. Attention a ne pas le supprimer en
vidant `work/`.

## Verifier que le modele repond

```bash
python tools/translate.py --selftest
```

Ca envoie une question bete a l'agent et affiche le code retour, les outils
que le modele a essaye d'utiliser, le texte recu et le SQL extrait.
`migrate.py` fait ce test tout seul avant de traduire, et s'arrete s'il
echoue, plutot que d'enchainer les echecs sur toutes les tables.

`translate.py` lance `opencode run --format json` et lit les evenements un
par un. Ca permet de faire la difference entre un modele qui a repondu, un
modele qui a tente un outil (refuse) au lieu de repondre, et un modele qui
n'a produit que du raisonnement. Avec une version d'opencode qui n'a pas
`--format json`, on se rabat sur la sortie texte.

## Voir ce que le modele a fait

Pour chaque table passee au modele, dans `work/<script>/<table>/` :

| Fichier | Contenu |
|---|---|
| `prompt.md` | le prompt de la table |
| `model_1.log`, `model_2.log` | par tentative : la commande, le message envoye, la duree et la reponse brute (stdout puis stderr) |
| `query.attempt1.sql`, `query.attempt2.sql` | la requete extraite de chaque reponse |

La premiere ligne de chaque log, `# Note`, resume ce qui s'est passe :
"a tente d'utiliser un outil (read...) au lieu de repondre", "n'a produit
que du raisonnement", "reponse VIDE", "aucune requete SQL trouvee" ou
"opencode a echoue (code retour non nul)". Dans tous ces cas, la tentative
est relancee une fois. Si aucun appel n'aboutit, la table passe en
`erreur_modele`, et elle sera reprise au prochain `migrate.py`.

## Corriger une table a la main

1. Corriger `work/<script>/<table>/query.sql` (jamais un
   `query.attemptN.sql`). Le `prompt.md` d'a cote contient le code Qlik et
   ce qu'on sait de la table.
2. Valider la correction :
   `python tools/check.py work/<script> --only <table> --parse`
3. Relancer `migrate.py`, qui assemble et compare.
4. Noter la cause dans la colonne `resolution` de `work/journal.csv`.

Une table en `escalade` n'est jamais retraduite automatiquement, pour ne
pas ecraser une correction manuelle (`translate.py --retry-escalated` si on
veut la refaire expres).

## Avant chaque commit

```bash
ruff format . && ruff check . && mypy     # configuration dans pyproject.toml
python -m pytest                          # tout doit passer
```

Aucun accent dans les fichiers, commentaires compris. Toute fonction a une
docstring et des annotations de type.

## Tests

```bash
pip install pyspark pyyaml pandas pyarrow pytest
python -m pytest -m "not spark"     # rapide, sans Spark
python -m pytest                    # tout, Spark et Java requis
```

`tests/demo/` est un petit projet complet : deux scripts chaines
(`synthese.txt` lit la sortie de `ventes.qvs`), un calendrier sans
extension (`Min`/`Max` de dates, `LET` avec `Peek`, `AUTOGENERATE`,
`WeekYear`), un `io.yml` a 3 espaces avec des commentaires, et les exports
Qlik attendus. Chaque test le copie dans un dossier temporaire. Le cas de la
table de 322 colonnes est construit par le test lui-meme.

Les tests marques `spark` lancent `migrate.py` de bout en bout, puis le
job genere avec la doublure du socle (`tests/fakelib/`). Ils verifient aussi que le job reste
court et que chaque table Qlik arrive dans le catalogue. Sans pyspark, ces
tests sont sautes.

## Les outils

| Outil | Role |
|---|---|
| `migrate.py` | la seule commande a lancer |
| `prepare.py` | analyse du script, sections, chemins, rapport |
| `qlik2sql.py` | traduction Qlik -> Spark SQL sans modele |
| `translate.py` | completion des `TODO(model)` par le modele |
| `check.py` | controles, et syntaxe Spark avec `--parse` |
| `conf.py` | lecture de `conf/io.yml` et des schemas |
| `fill_conf.py` | complete `conf/io.yml` et les schemas (lance par `migrate.py`) |
| `assemble.py` | copie des requetes d'un script dans `src/<nom-projet>/sql/` |
| `job_template.py` | texte du job et des modules de `scripts/` |
| `verify.py` | lance le job et compare avec `data/qlik_outputs/` |
| `compare.py` | comparaison de deux tables |
| `orchestrate.py` | genere `job.py` et `scripts/`, ordonne les scripts |
| `journal.py` | tenue de `work/journal.csv` |

## Limites connues

- Ce qui reste au modele : `Peek` recursif (cumuls), `FirstSortedValue`,
  `ConvertToLocalTime` sans fuseau explicite, `RIGHT`/`INNER KEEP` vers une
  table d'une autre section, boucles sur `FileList`, `SQL SELECT` avec
  jointure ou `WHERE` (le dialecte depend de la base).
- L'ordre des lignes apres un `JOIN` : Qlik ne le documente pas. La regle
  "la gauche, puis les lignes sans correspondance" reste a confirmer sur
  de vrais exports, avec `verify.py --strict-order`.
- Avec `FieldValue('F', RecNo())`, les valeurs sont numerotees dans l'ordre
  trie, alors que Qlik suit l'ordre de chargement.
- `Date(x)` applique a un nombre (date Qlik en jours) n'est pas converti.
- Piege de Spark hors mode ANSI : `FROM t WHERE (x)` est lu comme l'alias de
  table `WHERE`, et le filtre disparait sans erreur. Le traducteur double
  les parentheses dans ce cas, et `check.py` refuse cette ecriture si le
  modele la produit.
- Le typage des dates repose sur `conf/schema`. Une colonne de dates sans
  type declare (ou declaree en texte) n'est pas reconnue comme date par
  `Min`, `Max` ou `Peek`. Dans ce cas, le controle ne peut pas trancher sur
  les types et laisse passer, et c'est le job qui plantera sur `date - date`.
- `QUALIFY`, `NullAsValue`, `SET NullInterpret/NullValue` : signales, a
  completer a la main.
- L'ordre entre scripts ne tient compte que des fichiers, pas des tables en
  base de donnees.
- Tests pytest passes en local avec PySpark 3.5.8 (la cible) et 4.0.1,
  Java 21. Pas encore teste sur le cluster, ni avec un vrai opencode.
- Les tests tournent sur la doublure du socle, pas sur le vrai
  `spark_foundation` : on suppose que son API publique est celle decrite
  plus haut (noms et signatures de `runner.py` et `tables.py`).
- Les tables du catalogue sont en Parquet, ecrasees a chaque lancement : on
  n'a que la derniere version. Un overwrite Parquet n'est pas atomique, un
  echec au milieu laisse la table incomplete jusqu'au prochain lancement
  (Delta ou Iceberg le seraient).
