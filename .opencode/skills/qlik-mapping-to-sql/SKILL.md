---
name: qlik-mapping-to-sql
description: Translate Qlik ApplyMap calls into a Spark SQL LEFT JOIN + COALESCE on the mapping source.
---

# ApplyMap -> LEFT JOIN + COALESCE

The mapping table is never created. The prompt gives its Qlik definition
and, for each ApplyMap, the key expression and the default value.

1. One CTE for the lookup, built from the mapping source:
   - mapping FROM a file: `SELECT key, value FROM <view> WHERE <mapping WHERE>`;
   - mapping INLINE: `SELECT * FROM VALUES ('FR', 'France') AS t(code, libelle)`.
2. Deduplicate on the key: Qlik keeps the FIRST value for a key.
   `SELECT key, MIN(value) ... GROUP BY key` picks one: add a `-- TODO:` if
   keys can be duplicated with different values.
3. LEFT JOIN on the key expression, then `COALESCE(m.value, <default>)`.
4. No default (2 arguments): ApplyMap returns the key itself ->
   `COALESCE(m.value, <key expression>)`.

```sql
WITH
-- l.9 : mapping des libelles clients (clients actifs)
map_clients AS (
  SELECT id_client AS map_key, MIN(libelle_client) AS map_value
  FROM clients
  WHERE actif = 1
  GROUP BY id_client
),
-- l.51 : libelle client via MAP_CLIENTS, defaut 'Inconnu'
step_2 AS (
  SELECT c.*, COALESCE(m.map_value, 'Inconnu') AS libelle_client
  FROM step_1 c
  LEFT JOIN map_clients m ON c.id_client = m.map_key
)
```
