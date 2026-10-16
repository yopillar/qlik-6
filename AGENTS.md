# Qlik Sense -> Spark SQL 3.5.8 migration

Each request contains everything needed: the draft query, the Qlik code and
the facts. Do not look for documentation or files and do not use tools.
Think in English; SQL comments in French. Answer with one ```sql block.

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
- `STORE` writes, `DROP TABLE` frees memory (not translated).
- `Date(x, 'fmt')`, `Num(x, 'fmt')` are display formats only: keep the type.
- An empty string `''` is not null in Qlik.
