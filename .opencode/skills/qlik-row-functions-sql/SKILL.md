---
name: qlik-row-functions-sql
description: Translate Qlik row functions (Peek, Previous, Above, RowNo, RecNo) into Spark SQL window functions.
---

# Peek / Previous / RowNo -> window functions

Qlik evaluates these row by row, in load order. SQL has no load order: the
order MUST come from the `ORDER BY` of the Qlik `LOAD ... RESIDENT T ORDER BY`.
No ORDER BY in Qlik -> `-- TODO: load order unknown`.

| Qlik | Spark SQL |
|---|---|
| `Previous(x)` | `lag(x) OVER (ORDER BY <order>)` |
| `Peek('x')` / `Peek('x', -1)` | `lag(x) OVER (ORDER BY <order>)` |
| `Peek('x', -2)` | `lag(x, 2) OVER (ORDER BY <order>)` |
| `Above(x)` | `lag(x) OVER (...)` |
| `RowNo()` / `RecNo()` | `row_number() OVER (ORDER BY <order>)` |
| `If(id = Previous(id), …)` | same, with `lag(id) OVER (...)` |

- A running total `If(id = Previous(id), Peek('total') + x, x)` is a
  cumulative sum: `sum(x) OVER (PARTITION BY id ORDER BY <order> ROWS
  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
- `Peek('x', 0, 'T')` / `Peek('x', -1, 'T')` (first / last value of another
  table) -> scalar subquery on that table's view or CTE.
- Window functions cannot be used in WHERE: compute them in one CTE, filter
  in the next.

```sql
-- l.40 : cumul des montants par client, dans l'ordre des dates
step_2 AS (
  SELECT *,
         sum(montant) OVER (PARTITION BY id_client ORDER BY date_cmd
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           AS cumul
  FROM step_1
)
```
