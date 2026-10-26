# Qlik Sense -> Spark SQL 3.5.8 migration

Each request contains everything needed. No need to read documentation;
never write files. Think in English; SQL comments in French. Answer with one
```sql block, in the exact format the request asks, and nothing else.

## Qlik in short
- A script runs top to bottom and builds in-memory tables. `NAME:` names the table.
- `LOAD ... FROM file` reads a file; `LOAD ... RESIDENT T` re-reads table T.
- Stacked LOADs (a LOAD without FROM above another) read BOTTOM-UP.
- A LOAD with exactly the fields of an existing table is APPENDED to it,
  unless prefixed by `NoConcatenate` (the prompt flags it as AUTO-CONCATENATION).
- `JOIN` never names its key: Qlik joins on ALL same-named fields.
  `JOIN` alone = OUTER. KEEP = join used as a filter, tables stay separate.
- `CONCATENATE (T)` = union into T, missing columns become null.
- `Mapping LOAD` + `ApplyMap('M', x, default)` = lookup. Without a default,
  ApplyMap returns x itself. First value wins for duplicate keys.
- `ORDER BY` only matters for `Peek`, `Previous`, `RowNo`, `Above`.
- Booleans are -1 (true) and 0 (false); any non-zero number is true.
- `'l''été'` in Qlik is `"l'été"` in Spark SQL (never `''` in Spark).
- `STORE` writes, `DROP TABLE` frees memory (not translated).
- `Date(x, 'fmt')`, `Num(x, 'fmt')` are display formats only: keep the type.
- `AddMonths(d, n)` is `add_months(d, n)` (plural). `add_month`, `month_add`
  and `dateadd` do not exist in Spark. `AddYears(d, n)` is
  `add_months(d, 12 * n)`.
- An empty string `''` is not null in Qlik.
