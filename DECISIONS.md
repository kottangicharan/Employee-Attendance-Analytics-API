# DECISIONS.md

1. **Indexes.** `employees`: unique `emp_code` (identity, 409 on duplicates, `$lookup` target), `(department, joined_on)` (department filters, trend headcount), `joined_on` (summary headcount). `attendance_logs`: unique `(emp_code, date)` (one record per day, per-employee month queries), `(date -1, emp_code 1)` (matches the list sort exactly, and serves date-range analytics). I rejected an index on `status`: it has only five values, so it filters too little to be worth it.

2. **Punch-in race.** Both requests read the employee and compute the same `date`. Both call `insert_one`. The unique `(emp_code, date)` index lets exactly one insert succeed. The other gets `DuplicateKeyError`, which I return as 409. There is no check-then-insert.

3. **Ties.** `$setWindowFields` with `$rank` gives competition ranks (1, 2, 2, 4). I filter `rank <= limit` after ranking, so everyone tied at the cutoff is returned, ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (`joined_on <= month end`), `$lookup`s their logs, then `$unwind`s with `preserveNullAndEmptyArrays`. Employees with no logs still produce one row, and `$addToSet` on `emp_code` counts them.

5. **100x data.** I would precompute daily per-department aggregates when records are written, so analytics read a small summary collection.
