# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # MONGO_URI / MONGO_DB (real environment variables take priority over .env)
# MongoDB 6.0+ is needed, e.g.:  docker run -p 27017:27017 mongo:7
uvicorn app.main:app --port 8000
```
Indexes are created automatically at startup (idempotent). `GET /health` returns 200 once MongoDB answers.

## Assumptions worth knowing
* `moving_avg_7d` is the mean of the *reported* (4-decimal, half-up) daily rates in the window, then rounded half-up again.
* `PATCH` returns 409 to the loser of a concurrent edit (optimistic check on the record's state and history length), so no history entry is ever lost. A request is "no change" (422) when status, punch_in and punch_out all stay the same.
* The attendance trend rows are generated inside MongoDB with `$range` (gap filling) and `$setWindowFields` (moving average); rounding is done in integer arithmetic so it is exactly half-up.
* `ABSENT`/`LEAVE` records and weekend records are never counted in `present_days`; weekend records still count toward late and overtime totals.

## What is missing
Nothing intentionally omitted.
