# DECISIONS.md

1. **Indexes.** `attendance_logs`: unique `(emp_code, date)` (one record per day, monthly summary, join from employees); `(date desc, emp_code)` for the list sort and date ranges; `(status, date desc, emp_code)` for status filters; `(emp_code, punch_in desc)` for punch-out. `employees`: unique `emp_code`, `(department, emp_code)`, `(joined_on)`. Rejected: an index with `late_minutes` — the date range already narrows the scan enough.

2. **Punch-in race.** Both requests pass validation and call `insert_one`. The unique index lets exactly one succeed; the other gets `DuplicateKeyError` and returns 409. There is no read-then-write.

3. **Ties.** I rank first (`$rank`, so 1, 2, 2, 4) and only then keep `rank <= limit`. Everyone tied at the cutoff is returned, so the list can be longer than `limit`. Tied employees are ordered by `emp_code` ascending.

4. **Headcount.** The summary pipeline starts from `employees` (joined on or before month end) and joins each person's logs with `$lookup`. Someone with no logs gets an empty array, sums to zero, and is still counted by `$group`. Employees who join after the period end are excluded by the initial `$match`.

5. **100x data.** Precompute: keep a daily rollup per department, updated on punch and correction, so analytics read a few rows instead of joining raw logs. Alternatively, materialized views or time-series collections with automatic aggregation would scale better.