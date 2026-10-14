"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (a local .env is loaded for convenience; real environment variables win)

Layout of this file (everything lives here, as the assignment requires):
    1. Configuration & database handles
    2. Time helpers          (epoch-ms <-> BSON datetime, IST, truncation)
    3. Business rules        (R1-R5: attendance date, late, overtime, work hours, half day)
    4. Serialisation         (Mongo documents -> API JSON)
    5. Request models        (pydantic validation)
    6. Query / pipeline builders (shared by the real endpoints AND the explain endpoint)
    7. Endpoints
"""
import calendar
import json
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration & database handles
# --------------------------------------------------------------------------- #
load_dotenv()  # override=False: variables already in the environment take priority over .env

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True: PyMongo hands back timezone-aware (UTC) datetimes instead of naive ones, so
# every datetime we touch is unambiguous.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
EMP = db["employees"]
LOGS = db["attendance_logs"]

PRESENCE_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]  # R6
ALL_STATUSES = ["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"]
Status = Literal["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60  # R2
MIN_OVERTIME_MINUTES = 30  # R3
HALF_DAY_CENTS = 450  # R5: work_hours < 4.50  (compared as integer hundredths)
MAX_SHIFT_SECONDS = 24 * 3600  # punch-out must be within 24h of punch-in
MAX_TREND_DAYS = 92

MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


# --------------------------------------------------------------------------- #
# Indexes (created at startup, idempotent)
# --------------------------------------------------------------------------- #
INDEXES = {
    "employees": [
        # Uniqueness of emp_code is enforced by the database (concurrent creates -> one 201, rest 409)
        # and serves every lookup by emp_code.
        dict(keys=[("emp_code", ASCENDING)], unique=True, name="uniq_emp_code"),
        # GET /employees?department=..  (sorted by emp_code), the department filter of analytics and the trend.
        dict(keys=[("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp_code"),
        # Headcount (R9) for the all-departments summary: joined_on <= end of month.
        dict(keys=[("joined_on", ASCENDING)], name="joined_on"),
    ],
    "attendance_logs": [
        # One record per (emp_code, date): enforced by the database, so punch-in is race-safe.
        # Also serves: per-employee monthly summary, per-employee list, and the $lookup join from employees.
        dict(keys=[("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_date"),
        # GET /attendance sorted by date desc, emp_code asc (no in-memory sort); date range scans of the
        # leaderboard.
        dict(keys=[("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp"),
        # GET /attendance?status=..  (equality first, then the sort keys).
        dict(keys=[("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp"),
        # Punch-out: "latest record of this employee whose punch_in <= punched_at".
        dict(keys=[("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_punch_in"),
    ],
}


def ensure_indexes() -> None:
    for coll, specs in INDEXES.items():
        for spec in specs:
            opts = {k: v for k, v in spec.items() if k != "keys"}
            db[coll].create_index(spec["keys"], **opts)


def _ensure_indexes_with_retry() -> None:
    """Background retry so a slow MongoDB start does not block (or crash) the app."""
    while True:
        try:
            ensure_indexes()
            log.info("indexes ready")
            return
        except PyMongoError as exc:
            log.warning("index creation failed (%s); retrying in 2s", exc)
            time.sleep(2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:
        log.warning("index creation at startup failed (%s); retrying in background", exc)
        threading.Thread(target=_ensure_indexes_with_retry, daemon=True).start()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    # A database outage is a 503, never an unhandled 500. The cause is logged so a bad query is not hidden.
    log.exception("MongoDB error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Time helpers
# --------------------------------------------------------------------------- #
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MS_MIN = 100_000_000_000  # below this a value "looks like seconds"
MS_MAX = 4_102_444_800_000

EpochMs = Annotated[int, Field(strict=True, ge=MS_MIN, le=MS_MAX)]  # strict: no floats, strings or bools


def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime (exact integer arithmetic, no float rounding)."""
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    if dt.tzinfo is None:  # defensive: a naive value coming from Mongo is UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def instant_from_ms(ms: Optional[int]) -> datetime:
    """R1: truncate to whole seconds before ANY calculation; the truncated value is what we store."""
    if ms is None:
        ms = now_ms()
    return ms_to_dt(ms - ms % 1000)


def to_date_or_422(value: str, name: str) -> date:
    if not _DATE_RE.match(value):
        raise HTTPException(422, f"{name} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(422, f"{name} is not a valid calendar date")


def month_bounds_or_422(month: str):
    """'2026-07' -> ('2026-07-01', '2026-07-31', date, date)."""
    try:
        year, mon = int(month[:4]), int(month[5:7])
        first = date(year, mon, 1)
        last = date(year, mon, calendar.monthrange(year, mon)[1])
    except (ValueError, IndexError):
        raise HTTPException(422, "month must be YYYY-MM")
    return first.isoformat(), last.isoformat(), first, last


def count_weekdays(first: date, last: date) -> int:
    """Number of Mon-Fri days in [first, last] (R7). At most 31 iterations: pure calendar arithmetic."""
    n, d = 0, first
    while d <= last:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


# --------------------------------------------------------------------------- #
# 3. Business rules R1 - R5
# --------------------------------------------------------------------------- #
def _hm(value: str):
    h, m = value.split(":")
    return int(h), int(m)


def is_overnight(emp: dict) -> bool:
    return emp["shift_end"] <= emp["shift_start"]  # zero-padded HH:MM compares correctly as a string


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in belongs to the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp):
        eh, em = _hm(emp["shift_end"])
        secs = local.hour * 3600 + local.minute * 60 + local.second
        if secs < eh * 3600 + em * 60:
            day -= timedelta(days=1)
    return day


def shift_start_at(emp: dict, day: date) -> datetime:
    h, m = _hm(emp["shift_start"])
    return datetime(day.year, day.month, day.day, h, m, tzinfo=IST)


def shift_end_at(emp: dict, day: date) -> datetime:
    h, m = _hm(emp["shift_end"])
    end = datetime(day.year, day.month, day.day, h, m, tzinfo=IST)
    return end + timedelta(days=1) if is_overnight(emp) else end  # overnight shift ends the next calendar day


def compute_late_minutes(punch_in: datetime, emp: dict, day: date) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift_start; then floor-minutes since shift_start."""
    diff = int((punch_in - shift_start_at(emp, day)).total_seconds())
    return diff // 60 if diff > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, emp: dict, day: date) -> int:
    """R3: floor-minutes after shift_end, counted only when >= 30."""
    diff = int((punch_out - shift_end_at(emp, day)).total_seconds())
    minutes = diff // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def work_hours_cents(punch_in: datetime, punch_out: datetime) -> int:
    """R4 in integer hundredths of an hour, rounded HALF-UP.
    cents = floor(seconds * 100 / 3600 + 1/2) = floor((200 * seconds + 3600) / 7200). No floats involved."""
    seconds = int((punch_out - punch_in).total_seconds())
    return (200 * seconds + 3600) // 7200


def derive(emp: dict, day: date, status: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All derived fields for a record, from its final status and punch times (used by punch-in/out and PATCH)."""
    out = {"late_minutes": 0, "work_hours": None, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE_STATUSES or punch_in is None:
        return out
    out["late_minutes"] = compute_late_minutes(punch_in, emp, day)
    if punch_out is not None:
        cents = work_hours_cents(punch_in, punch_out)
        out["work_hours"] = cents / 100
        out["overtime_minutes"] = compute_overtime_minutes(punch_out, emp, day)
        out["half_day"] = cents < HALF_DAY_CENTS
    return out


# --------------------------------------------------------------------------- #
# 4. Serialisation (Mongo -> API)
# --------------------------------------------------------------------------- #
def _api_value(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    changes = {
        field: {"from": _api_value(ch.get("from")), "to": _api_value(ch.get("to"))}
        for field, ch in (entry.get("changes") or {}).items()
    }
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Missing legacy fields are defaulted; _id never leaks."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = EMP.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 5. Request models
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM_PATTERN)
    shift_end: str = Field(default="18:30", pattern=HHMM_PATTERN)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_joined_on(cls, v: str) -> str:
        if not _DATE_RE.match(v):
            raise ValueError("joined_on must be YYYY-MM-DD")
        date.fromisoformat(v)  # raises ValueError for 2026-02-31 etc.
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # The contract types these as non-nullable; omitted means "unchanged", an explicit null is invalid.
        for field in ("status", "punch_in", "punch_out"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} must not be null")
        return self


# --------------------------------------------------------------------------- #
# 6. Query / pipeline builders (shared with the explain endpoint)
# --------------------------------------------------------------------------- #
ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def attendance_list_filter(emp_code, date_from, date_to, status) -> dict:
    flt: dict = {}
    d_from = to_date_or_422(date_from, "date_from") if date_from else None
    d_to = to_date_or_422(date_to, "date_to") if date_to else None
    if d_from and d_to and d_from > d_to:
        raise HTTPException(422, "date_from must not be after date_to")
    if emp_code:
        flt["emp_code"] = emp_code
    if d_from or d_to:
        flt["date"] = {}
        if d_from:
            flt["date"]["$gte"] = d_from.isoformat()
        if d_to:
            flt["date"]["$lte"] = d_to.isoformat()
    if status:
        flt["status"] = status
    return flt


def _log_metrics(include_date: bool = False) -> dict:
    """$project stage body that turns a stored log into small integers the $group stages can sum.
    It READS the stored derived values (late_minutes, overtime_minutes, half_day, work_hours); nothing is recomputed
    from punch times.

    units           = half-units of presence on Mon-Fri (full day 2, half day 1, weekend/absent/leave 0), R5-R7
    presence_units  = half-units of presence on ALL days (full day 2, half day 1, absent/leave 0), for trend present_count
    late            = 1 when late_minutes > 0              late_min = late_minutes        ot = overtime_minutes
    leave/duty      = 1 for LEAVE / ON_DUTY records
    wh_cents        = work_hours in whole hundredths, only for presence-status records that have a work_hours; wh_n = 1 then
    Working in integers keeps every sum exact; rounding to 2 / 4 decimals happens once, half-up, at the end."""
    is_present = {"$in": ["$status", PRESENCE_STATUSES]}
    dow = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None, "onNull": None}}}
    is_weekday = {"$and": [{"$gte": [dow, 2]}, {"$lte": [dow, 6]}]}  # 1 = Sunday ... 7 = Saturday
    half = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
    has_wh = {"$and": [is_present, {"$ne": [{"$ifNull": ["$work_hours", None]}, None]}]}
    metrics = {
        "_id": 0,
        "units": {"$cond": [{"$and": [is_present, is_weekday]}, {"$cond": [half, 1, 2]}, 0]},
        "presence_units": {"$cond": [is_present, {"$cond": [half, 1, 2]}, 0]},
        "late": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]},
        "late_min": {"$ifNull": ["$late_minutes", 0]},
        "ot": {"$ifNull": ["$overtime_minutes", 0]},
        "leave": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]},
        "duty": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]},
        "wh_cents": {"$cond": [has_wh, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]},
        "wh_n": {"$cond": [has_wh, 1, 0]},
    }
    if include_date:
        metrics["date"] = 1
    return metrics


def monthly_pipeline(emp_code: str, start: str, end: str) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},  # IXSCAN uniq_emp_date
        {"$project": _log_metrics()},
        {"$group": {
            "_id": None,
            "units": {"$sum": "$units"},
            "leave_days": {"$sum": "$leave"},
            "late_count": {"$sum": "$late"},
            "total_late_minutes": {"$sum": "$late_min"},
            "total_overtime_minutes": {"$sum": "$ot"},
        }},
    ]


def department_summary_pipeline(start: str, end: str, department: Optional[str]) -> list:
    """Runs on `employees` so that people with zero logs still count (R9); logs are joined in per employee."""
    emp_match: dict = {"joined_on": {"$lte": end}}  # R9: joined on or before the end of the month
    if department:
        emp_match["department"] = department
    return [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {"$project": _log_metrics()},
            ],
            "as": "logs",
        }},
        # One small document per employee (an empty `logs` array simply sums to 0)
        {"$project": {
            "_id": 0,
            "department": 1,
            "units": {"$sum": "$logs.units"},
            "late": {"$sum": "$logs.late"},
            "late_min": {"$sum": "$logs.late_min"},
            "leave": {"$sum": "$logs.leave"},
            "duty": {"$sum": "$logs.duty"},
            "wh_cents": {"$sum": "$logs.wh_cents"},
            "wh_n": {"$sum": "$logs.wh_n"},
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "units": {"$sum": "$units"},
            "late_count": {"$sum": "$late"},
            "total_late_minutes": {"$sum": "$late_min"},
            "leave_count": {"$sum": "$leave"},
            "on_duty_count": {"$sum": "$duty"},
            "wh_cents": {"$sum": "$wh_cents"},
            "wh_n": {"$sum": "$wh_n"},
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(start: str, end: str, limit: int, department: Optional[str]) -> list:
    emp_lookup_pipeline: list = []
    if department:
        emp_lookup_pipeline.append({"$match": {"department": department}})
    emp_lookup_pipeline.append({"$project": {"_id": 0, "name": 1, "department": 1}})
    return [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},  # IXSCAN date_desc_emp
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        # Join employees BEFORE ranking: logs of unknown employees are dropped by $unwind, and a department
        # filter must restrict the ranking population itself.
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "pipeline": emp_lookup_pipeline,
            "as": "emp",
        }},
        {"$unwind": "$emp"},
        # $rank = standard competition ranking: ties share a rank and the next rank is skipped (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking: tied rows are never cut
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ]


def trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """One row per calendar day, built inside MongoDB:
       1. employees of the department who joined on/before `to`  (IXSCAN dept_emp_code)
       2. $lookup each employee's logs in the range (IXSCAN uniq_emp_date) reduced to tiny numbers
       3. $facet -> {joins: employee counts per joined_on, daily: sums per date}
       4. $range generates exactly n_days rows (gap filling: weekends / empty days included)
       5. $setWindowFields computes the 7-row moving average of the (rounded) daily rates."""
    n_days = (d_to - d_from).days + 1
    f_str, t_str = d_from.isoformat(), d_to.isoformat()
    start_dt = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    iso_from0 = d_from.isoweekday() - 1  # Monday = 0

    day_row = {"$let": {
        "vars": {"d": {"$dateToString": {"format": "%Y-%m-%d", "date": {
            "$dateAdd": {"startDate": start_dt, "unit": "day", "amount": "$$i"}}}}},
        "in": {"$let": {
            "vars": {"m": {"$arrayElemAt": [{"$filter": {"input": "$daily", "as": "x", "cond": {"$eq": ["$$x._id", "$$d"]}}}, 0]}},
            "in": {
                "date": "$$d",
                # Monday..Friday  <=>  (iso_from0 + i) % 7 < 5
                "is_working_day": {"$lt": [{"$mod": [{"$add": [iso_from0, "$$i"]}, 7]}, 5]},
                # R9: employees of the department with joined_on <= this day (ISO strings compare correctly)
                "headcount": {"$reduce": {
                    "input": "$joins", "initialValue": 0,
                    "in": {"$cond": [{"$lte": ["$$this._id", "$$d"]}, {"$add": ["$$value", "$$this.n"]}, "$$value"]}}},
                "units": {"$ifNull": ["$$m.units", 0]},
                "presence_units": {"$ifNull": ["$$m.presence_units", 0]},
                "late": {"$ifNull": ["$$m.late", 0]},
            },
        }},
    }}

    rate_applies = {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}
    # rate scaled by 10^4, rounded HALF-UP in integer arithmetic:  floor((10000 * units + hc) / (2 * hc))
    # (units are half-units, so rate = units / (2 * hc))
    rate_scaled = {"$let": {
        "vars": {"n": {"$add": [{"$multiply": ["$units", 10000]}, "$headcount"]}, "d": {"$multiply": ["$headcount", 2]}},
        "in": {"$divide": [{"$subtract": ["$$n", {"$mod": ["$$n", "$$d"]}]}, "$$d"]},
    }}
    # mean of the scaled rates in the window, half-up:  floor((2 * sum + n) / (2 * n))
    ma_scaled = {"$let": {
        "vars": {"n": {"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]}, "d": {"$multiply": ["$win_n", 2]}},
        "in": {"$divide": [{"$subtract": ["$$n", {"$mod": ["$$n", "$$d"]}]}, "$$d"]},
    }}
    window = {"documents": [-6, 0]}  # the current row and up to 6 preceding rows, inside the range only

    return [
        {"$match": {"department": department, "joined_on": {"$lte": t_str}}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": f_str, "$lte": t_str}}},
                {"$project": _log_metrics(include_date=True)},
            ],
            "as": "logs",
        }},
        {"$facet": {
            "joins": [{"$group": {"_id": "$joined_on", "n": {"$sum": 1}}}],
            "daily": [
                {"$unwind": "$logs"},
                {"$group": {"_id": "$logs.date", "units": {"$sum": "$logs.units"}, "presence_units": {"$sum": "$logs.presence_units"}, "late": {"$sum": "$logs.late"}}},
            ],
        }},
        {"$project": {"_id": 0, "days": {"$map": {"input": {"$range": [0, n_days]}, "as": "i", "in": day_row}}}},
        {"$unwind": {"path": "$days", "includeArrayIndex": "idx"}},
        {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$days", {"idx": "$idx"}]}}},
        {"$addFields": {
            "has_rate": {"$cond": [rate_applies, 1, 0]},
            "r4": {"$cond": [rate_applies, rate_scaled, None]},
        }},
        {"$setWindowFields": {
            "sortBy": {"idx": 1},
            "output": {
                "win_sum": {"$sum": "$r4", "window": window},      # null rates are ignored by $sum
                "win_n": {"$sum": "$has_rate", "window": window},   # how many non-null rates are in the window
            },
        }},
        {"$sort": {"idx": 1}},
        {"$project": {
            "_id": 0,
            "date": 1,
            "is_working_day": 1,
            "headcount": 1,
            "present_count": {"$divide": ["$presence_units", 2]},
            "late_count": "$late",
            "attendance_rate": {"$cond": [{"$eq": ["$has_rate", 1]}, {"$divide": ["$r4", 10000]}, None]},
            "moving_avg_7d": {"$cond": [{"$gt": ["$win_n", 0]}, {"$divide": [ma_scaled, 10000]}, None]},
        }},
    ]


def _half_up_div(numerator: int, denominator: int) -> int:
    """round-half-up(numerator / denominator) for non-negative integers."""
    return (2 * numerator + denominator) // (2 * denominator)


# --------------------------------------------------------------------------- #
# 7. Endpoints
# --------------------------------------------------------------------------- #
PageQ = Annotated[int, Query(ge=1)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]
MonthQ = Annotated[str, Query(pattern=MONTH_PATTERN)]


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    return {"status": "ok"}


# ---- employees ------------------------------------------------------------ #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = ms_to_dt(now_ms())
    response = employee_out(doc)  # build the response BEFORE insert_one adds `_id` to doc
    try:
        EMP.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return response


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageQ = 1, page_size: PageSizeQ = 20):
    flt = {"department": department} if department else {}
    total = EMP.count_documents(flt) if flt else EMP.estimated_document_count()
    cursor = EMP.find(flt, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# ---- attendance ----------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = instant_from_ms(body.punched_at)
    day = attendance_date(ts, emp)  # R1
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(emp, day, body.status, ts, None),
        "history": [],
    }
    try:
        LOGS.insert_one(doc)  # unique (emp_code, date): exactly one of N concurrent requests wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    out_at = instant_from_ms(body.punched_at)

    # Most recent record whose punch_in <= punched_at (works across midnight: it is found by instant, not by date)
    rec = LOGS.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": out_at}}, sort=[("punch_in", DESCENDING)])
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")

    seconds = int((out_at - rec["punch_in"]).total_seconds())
    if seconds <= 0:
        raise HTTPException(422, "punched_at must be after punch_in")
    if seconds > MAX_SHIFT_SECONDS:
        raise HTTPException(422, "punched_at is more than 24 hours after punch_in")

    day = date.fromisoformat(rec["date"])
    derived = derive(emp, day, rec["status"], rec["punch_in"], out_at)
    update = {"punch_out": out_at, "work_hours": derived["work_hours"],
              "overtime_minutes": derived["overtime_minutes"], "half_day": derived["half_day"]}
    # The filter `punch_out: None` makes the update atomic: of two simultaneous punch-outs only one matches.
    updated = LOGS.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None}, {"$set": update}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    flt = attendance_list_filter(emp_code, date_from, date_to, status)
    total = LOGS.count_documents(flt) if flt else LOGS.estimated_document_count()
    cursor = LOGS.find(flt, {"_id": 0}).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: str, body: RegularizeIn):  # noqa: A002 (name fixed by the contract)
    day = to_date_or_422(date, "date")
    emp = get_employee_or_404(emp_code)
    rec = LOGS.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    old_status = rec["status"]
    old_in, old_out = rec.get("punch_in"), rec.get("punch_out")
    old_history = rec.get("history") or []

    # ---- final values after applying the request --------------------------------------------------------------
    new_status = body.status or old_status
    if new_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise HTTPException(422, "ABSENT / LEAVE clear the punch times; do not send punch_in or punch_out")
        new_in = new_out = None
    else:
        new_in = instant_from_ms(body.punch_in) if body.punch_in is not None else old_in
        new_out = instant_from_ms(body.punch_out) if body.punch_out is not None else old_out
        if new_in is None:
            raise HTTPException(422, "a presence status requires a punch_in")
        if attendance_date(new_in, emp) != day:
            raise HTTPException(422, "punch_in must stay on the record's attendance date")
        if new_out is not None:
            seconds = int((new_out - new_in).total_seconds())
            if seconds <= 0:
                raise HTTPException(422, "punch_out must be after punch_in")
            if seconds > MAX_SHIFT_SECONDS:
                raise HTTPException(422, "punch_out must be within 24 hours of punch_in")

    if (new_status, new_in, new_out) == (old_status, old_in, old_out):
        raise HTTPException(422, "the request changes nothing")

    derived = derive(emp, day, new_status, new_in, new_out)
    new_values = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derived}
    old_values = {
        "status": old_status, "punch_in": old_in, "punch_out": old_out,
        "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day", False)),
    }
    changes = {f: {"from": old_values[f], "to": new_values[f]} for f in new_values if new_values[f] != old_values[f]}
    entry = {"at": ms_to_dt(now_ms()), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # ---- optimistic concurrency ----------------------------------------------------------------------------------
    # The update only applies if the record is still exactly as we read it: same status / punches and the history
    # has exactly len(old_history) entries. A concurrent writer therefore makes this one match nothing -> 409,
    # instead of silently overwriting (and losing) the other's history entry.
    n = len(old_history)
    guard = {
        "_id": rec["_id"], "status": old_status, "punch_in": old_in, "punch_out": old_out,
        f"history.{n}": {"$exists": False},
    }
    if n:
        guard[f"history.{n - 1}"] = {"$exists": True}
    result = LOGS.update_one(guard, {"$set": new_values, "$push": {"history": entry}})
    if result.matched_count == 0:
        raise HTTPException(409, "the record was modified concurrently; retry")

    return record_out({**rec, **new_values, "history": old_history + [entry]})


# ---- analytics ------------------------------------------------------------ #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthQ):
    start, end, first, last = month_bounds_or_422(month)
    emp = get_employee_or_404(emp_code)
    row = next(iter(LOGS.aggregate(monthly_pipeline(emp_code, start, end))), None) or {}

    joined = date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(first, joined), last)  # R7: counted from joined_on for mid-month joiners
    units = int(row.get("units", 0))  # half-units: a half day is 1, a full day 2
    pct = None
    if working_days > 0:
        # present_days / working_days * 100 = units * 100 / (2 * working_days), 2 decimals half-up (R8)
        pct = _half_up_div(units * 100 * 100, 2 * working_days) / 100
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": units / 2,
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthQ, department: Optional[str] = None):
    start, end, _, _ = month_bounds_or_422(month)
    items = []
    for r in EMP.aggregate(department_summary_pipeline(start, end, department)):
        n = int(r["wh_n"])
        avg = _half_up_div(int(r["wh_cents"]), n) / 100 if n else None  # mean over RECORDS, half-up 2 decimals
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": int(r["units"]) / 2,
            "avg_work_hours": avg,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["total_late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthQ, limit: Annotated[int, Query(ge=1, le=50)] = 10, department: Optional[str] = None):
    start, end, _, _ = month_bounds_or_422(month)
    items = [
        {
            "rank": int(r["rank"]),
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": int(r["total_late_minutes"]),
            "late_count": int(r["late_count"]),
        }
        for r in LOGS.aggregate(late_leaderboard_pipeline(start, end, limit, department))
    ]
    return {"month": month, "items": items}


def _trend_range_or_422(from_: str, to: str):
    d_from, d_to = to_date_or_422(from_, "from"), to_date_or_422(to, "to")
    if d_to < d_from:
        raise HTTPException(422, "to must not be before from")
    if (d_to - d_from).days + 1 > MAX_TREND_DAYS:
        raise HTTPException(422, f"range must not exceed {MAX_TREND_DAYS} days")
    return d_from, d_to


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: Annotated[str, Query(alias="from")], to: str):
    d_from, d_to = _trend_range_or_422(from_, to)
    if EMP.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "unknown department")
    items = []
    for r in EMP.aggregate(trend_pipeline(department, d_from, d_to)):
        items.append({
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": float(r["present_count"]),
            "late_count": int(r["late_count"]),
            "attendance_rate": None if r["attendance_rate"] is None else float(r["attendance_rate"]),
            "moving_avg_7d": None if r["moving_avg_7d"] is None else float(r["moving_avg_7d"]),
        })
    return {"department": department, "items": items}


# ---- admin ---------------------------------------------------------------- #
def _require(**params):
    missing = [k for k, v in params.items() if v is None]
    if missing:
        raise HTTPException(422, f"missing required query parameter(s): {', '.join(missing)}")


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_PATTERN)] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    """executionStats explain of the endpoint's main query, built by the SAME builder functions the endpoint uses."""
    if endpoint == "attendance_list":
        flt = attendance_list_filter(emp_code, date_from, date_to, status)
        collection = "attendance_logs"
        command = {"find": collection, "filter": flt, "projection": {"_id": 0}, "sort": dict(ATTENDANCE_SORT),
                   "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint == "employee_monthly":
            _require(emp_code=emp_code, month=month)
            start, end, _, _ = month_bounds_or_422(month)
            collection, pipeline = "attendance_logs", monthly_pipeline(emp_code, start, end)
        elif endpoint == "department_summary":
            _require(month=month)
            start, end, _, _ = month_bounds_or_422(month)
            collection, pipeline = "employees", department_summary_pipeline(start, end, department)
        elif endpoint == "late_leaderboard":
            _require(month=month)
            start, end, _, _ = month_bounds_or_422(month)
            collection, pipeline = "attendance_logs", late_leaderboard_pipeline(start, end, limit, department)
        else:  # department_trend
            _require(department=department, **{"from": from_, "to": to})
            d_from, d_to = _trend_range_or_422(from_, to)
            collection, pipeline = "employees", trend_pipeline(department, d_from, d_to)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    result = db.command({"explain": command, "verbosity": "executionStats"})
    # Convert BSON types (dates, Int64, ...) to plain JSON
    explain = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": explain}
