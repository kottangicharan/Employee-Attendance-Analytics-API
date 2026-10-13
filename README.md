# Employee Attendance & Analytics API

FastAPI + PyMongo. All code is in `app/main.py`.

## Run
```bash
python -m venv venv && venv\Scripts\activate      # or: source venv/bin/activate
pip install -r requirements.txt
# set MONGO_URI and MONGO_DB (env vars, or a local .env; real env vars win)
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). MongoDB 6.0+ is required (`$densify`, `$setWindowFields`, `$documents`).

Rule-helper self-check: `python -m app.main`.

## Notes
- Instants are epoch ms in the API and stored as UTC BSON dates. Instants are truncated to whole seconds (R1).
- Analytics are aggregation pipelines. R8 half-up rounding inside pipelines uses Decimal128 `floor(x*10^n + 0.5)`, because `$round` is half-to-even.
- Trend `moving_avg_7d` averages the reported (4-dp) `attendance_rate` values.
- Working days (R7) are computed with calendar arithmetic in Python. Everything that reads documents runs in MongoDB.
- `PATCH` uses optimistic concurrency on the history length: a concurrent loser gets 409 and no history entry is lost.
