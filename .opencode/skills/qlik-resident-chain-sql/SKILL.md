---
name: qlik-resident-chain-sql
description: Translate a chain of temporary Qlik tables (RESIDENT, NoConcatenate, DROP) into one Spark SQL query with CTEs.
---

# Temporary tables -> CTEs of one query

TMP1 -> TMP2 -> FINAL becomes one `WITH` clause: one CTE per Qlik LOAD,
named `step_1`, `step_2`... with a `-- l.<n>` comment, then the final SELECT.

| Qlik | Spark SQL |
|---|---|
| `T2: NoConcatenate LOAD * RESIDENT T1` | next CTE `SELECT * FROM step_1` |
| `LOAD *, expr AS c RESIDENT T` | `SELECT *, expr AS c FROM step_n` |
| `LOAD a, b RESIDENT T` | `SELECT a, b FROM step_n` |
| `... WHERE cond` | `WHERE cond` |
| `... GROUP BY a` | `GROUP BY a` |
| `... ORDER BY a` | ignored, UNLESS Peek/Previous/RowNo: then `OVER (ORDER BY a)` |
| `DROP FIELD x FROM T` | next CTE listing all remaining columns explicitly |
| `DROP TABLE T` | nothing |
| stacked LOAD (LOAD without FROM above a LOAD) | read BOTTOM-UP: the lower LOAD is the earlier CTE |

- A column created in a LOAD can only be used in the NEXT LOAD: next CTE.
- The final SELECT lists the output columns explicitly, in Qlik order.

```sql
WITH
-- l.32 : commandes depuis le fichier
step_1 AS (SELECT id_commande, quantite, prix_unitaire FROM commandes),
-- l.47 : montant = quantite * prix_unitaire
step_2 AS (SELECT *, quantite * prix_unitaire AS montant FROM step_1)
-- l.58 : agregation par commande
SELECT id_commande, SUM(montant) AS montant_total
FROM step_2
GROUP BY id_commande
```
