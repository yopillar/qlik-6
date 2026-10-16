---
name: qlik-row-functions-sql
description: Translate Qlik row functions (Peek, Previous, Above, RowNo, RecNo) into Spark SQL window functions.
---

# Peek / Previous / RowNo -> window functions

Qlik evaluates these row by row, in load order. Every view and CTE carries
that order in the technical column `_qlik_order` (array<bigint>): use
`ORDER BY _qlik_order` (after the columns of a Qlik `ORDER BY`, if any).
The parser already translates the simple cases (Previous, Peek of a plain
field, RowNo, FirstValue, LastValue, NOT Exists): what reaches you is usually
a RECURSIVE Peek, such as a running total.

| Qlik | Spark SQL |
|---|---|
| `Previous(x)` | `lag(x) OVER (ORDER BY _qlik_order)` |
| `Peek('x')` / `Peek('x', -1)` | `lag(x) OVER (ORDER BY _qlik_order)` |
| `Peek('x', -2)` | `lag(x, 2) OVER (ORDER BY _qlik_order)` |
| `Above(x)` | `lag(x) OVER (...)` |
| `RowNo()` / `RecNo()` | `row_number() OVER (ORDER BY _qlik_order)` |
| `If(id = Previous(id), …)` | same, with `lag(id) OVER (...)` |

- A running total `If(id = Previous(id), Peek('total') + x, x)` is a
  cumulative sum: `sum(x) OVER (PARTITION BY id ORDER BY _qlik_order ROWS
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

## Also order-dependent

| Qlik | Spark SQL |
|---|---|
| `FirstValue(x)` / `LastValue(x)` (with GROUP BY) | `first_value(x) OVER (PARTITION BY <group> ORDER BY _qlik_order)` / `last_value(...)` with `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`, then one row per group |
| `FirstSortedValue(x, sort)` | `max_by(x, -sort)` / `min_by(x, sort)` ; Qlik returns null when several rows share the best sort value |
| `WHERE NOT Exists(Key)` while the same LOAD produces `Key` | keep the first row per key in load order: `row_number() OVER (PARTITION BY Key ORDER BY _qlik_order) = 1`, and exclude keys already present in the earlier tables (`NOT EXISTS (...)`) |
