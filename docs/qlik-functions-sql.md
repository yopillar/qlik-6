# Qlik functions -> Spark SQL

Rows marked ⚠ hide a Qlik trap: add a `-- TODO:` if in doubt. Format
required by prepare.py: the first cell holds the function name followed by `(`.

## Logic and missing values
| Qlik | Spark SQL | Note |
|---|---|---|
| `If(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | |
| `Alt(a, b)` | `COALESCE(a, b)` | |
| `IsNull(x)` | `x IS NULL` | ⚠ an empty string is NOT null in Qlik |
| `Match(x, 'a', 'b')` | `CASE x WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END` | returns a rank, not a boolean |
| `WildMatch(x, 'a*')` | `lower(x) LIKE 'a%'` | ⚠ case-insensitive in Qlik |
| `Pick(n, a, b)` | `element_at(array(a, b), n)` | |
| `Exists(field, x)` | `x IN (SELECT field FROM ...)` | ⚠ depends on values loaded BEFORE this statement |
| `Null()` | `NULL` | |

## Text
| Qlik | Spark SQL | Note |
|---|---|---|
| `Trim(x)`, `Upper(x)`, `Lower(x)` | `trim(x)`, `upper(x)`, `lower(x)` | |
| `Len(x)` | `length(x)` | |
| `Left(x, n)` | `left(x, n)` | |
| `Right(x, n)` | `right(x, n)` | |
| `Mid(x, start, n)` | `substring(x, start, n)` | 1-based on both sides |
| `Index(x, s)` | `instr(x, s)` | 3rd Qlik argument not supported |
| `SubField(x, ';', n)` | `element_at(split(x, ';'), n)` | ⚠ split takes a regex: escape `|` and `.` |
| `Replace(x, a, b)` | `replace(x, a, b)` | |
| `PurgeChar(x, 'ab')` | `regexp_replace(x, '[ab]', '')` | |
| `KeepChar(x, '0123456789')` | `regexp_replace(x, '[^0-9]', '')` | |
| `Text(x)` | `CAST(x AS STRING)` | |
| `Num#(x)` | `CAST(x AS DOUBLE)` | |
| `Capitalize(x)` | `initcap(x)` | |

`a & b` (concatenation) -> `concat_ws('', a, b)` ⚠ `concat` and `||` return
null if one side is null, Qlik does not.

## Dates
| Qlik | Spark SQL | Note |
|---|---|---|
| `Date#(x, 'YYYYMMDD')` | `CAST(try_to_timestamp(x, 'yyyyMMdd') AS DATE)` | Qlik YYYY/MM/DD -> Spark yyyy/MM/dd |
| `Timestamp#(x, 'fmt')` | `try_to_timestamp(x, 'fmt')` | |
| `Date(x, 'fmt')` | keep the DATE | display format only |
| `Timestamp(x)` | keep the TIMESTAMP | display format only |
| `Floor(d)` on a timestamp | `to_date(d)` | date part |
| `Today()` | `current_date()` | |
| `Now()` | `current_timestamp()` | |
| `Year(d)`, `Day(d)` | `year(d)`, `dayofmonth(d)` | |
| `Month(d)` | `month(d)` | ⚠ dual value in Qlik (name + number) |
| `WeekDay(d)` | `(dayofweek(d) + 5) % 7` | ⚠ Monday = 0 by default in Qlik; dual value |
| `MakeDate(y, m, d)` | `make_date(y, m, d)` | |
| `AddMonths(d, n)` | `add_months(d, n)` | |
| `AddYears(d, n)` | `add_months(d, 12 * n)` | |
| `MonthStart(d)` | `trunc(d, 'MM')` | |
| `YearStart(d)` | `trunc(d, 'YYYY')` | |
| `MonthEnd(d)` | `last_day(d)` | ⚠ Qlik returns a timestamp 23:59:59 |
| `ConvertToLocalTime(t)` | `from_utc_timestamp(t, '<zone>')` | ⚠ zone to confirm, add a TODO |

`try_to_date` does not exist in Spark 3.5: use `CAST(try_to_timestamp(...) AS DATE)`.

## Numbers
| Qlik | Spark SQL | Note |
|---|---|---|
| `Round(x)` | `round(x)` | |
| `Round(x, 0.5)` | `round(x / 0.5) * 0.5` | ⚠ 2nd Qlik argument = step, not decimals |
| `Floor(x)`, `Ceil(x)` | `floor(x)`, `ceil(x)` | |
| `Fabs(x)` | `abs(x)` | |
| `Mod(a, b)` | `a % b` | |
| `Num(x, 'fmt')` | nothing | display format only |

## Aggregations (with GROUP BY)
| Qlik | Spark SQL | Note |
|---|---|---|
| `Sum(x)`, `Min(x)`, `Max(x)`, `Avg(x)`, `Count(x)` | `sum`, `min`, `max`, `avg`, `count` | |
| `Count(DISTINCT x)` | `count(DISTINCT x)` | |
| `Concat(x, ',')` | `concat_ws(',', array_sort(collect_list(x)))` | ⚠ Qlik sorts, collect_list does not |
| `FirstSortedValue(x, sort)` | `max_by(x, sort)` / `min_by` | ⚠ Qlik returns null on ties |
| `Only(x)` | `CASE WHEN count(DISTINCT x) = 1 THEN min(x) END` | |

## Row functions (order matters)
| Qlik | Spark SQL | Note |
|---|---|---|
| `Peek('x')`, `Previous(x)` | `lag(x) OVER (ORDER BY ...)` | ⚠ ORDER BY of the RESIDENT, mandatory |
| `RowNo()`, `RecNo()` | `row_number() OVER (ORDER BY ...)` | same |
| `AutoNumber(x)` | `dense_rank() OVER (ORDER BY x)` | ⚠ numbers differ from Qlik |
