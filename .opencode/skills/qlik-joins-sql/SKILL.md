---
name: qlik-joins-sql
description: Translate Qlik JOIN, KEEP and CONCATENATE into Spark SQL joins and unions.
---

# JOIN / KEEP / CONCATENATE

The prompt gives, for each JOIN or KEEP, the SQL join type and the keys.
Use them exactly with `USING (...)`: like Qlik, `USING` merges the key
columns into one column.

| Qlik | Spark SQL |
|---|---|
| `JOIN (T)` (no prefix) | `FULL OUTER JOIN ... USING (keys)` |
| `LEFT JOIN (T)` | `LEFT JOIN ... USING (keys)` |
| `RIGHT JOIN (T)` | `RIGHT JOIN ... USING (keys)` |
| `INNER JOIN (T)` | `INNER JOIN ... USING (keys)` |
| `LEFT KEEP (A) LOAD ... B` | A unchanged; B = `B LEFT SEMI JOIN A USING (keys)` |
| `INNER KEEP` | both A and B filtered by `LEFT SEMI JOIN` |
| `CONCATENATE (T)` | `UNION ALL`, same columns in the same order, `NULL AS col` where missing |

- The joined LOAD is a CTE selecting only its columns, then joined.
- "keys NOT computable" in the prompt: list the columns with the same name
  in both tables yourself, and add `-- TODO: verify join keys`.
- "AUCUN champ commun / PRODUIT CARTESIEN": Qlik does a cross join ->
  `CROSS JOIN` + `-- TODO: cross join in Qlik, check it is intended`.
- Duplicated keys multiply rows, in Qlik as in Spark: never deduplicate.
- `UNION ALL` matches columns BY POSITION in Spark 3.5: list them explicitly
  on both sides, in the same order.

```sql
-- l.41 : LEFT JOIN (TMP_CMD1) sur id_produit
step_2 AS (
  SELECT *
  FROM step_1
  LEFT JOIN (SELECT id_produit, prix_unitaire FROM produits) p
  USING (id_produit)
)
```
