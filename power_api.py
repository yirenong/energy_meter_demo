#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Power API (Blocks • Levels • Panels • Trends • Plant)

Serves energy analytics from energy_meters.sqlite (written by your collector),
plus a Wacnet-driven metadata refresher to map meters to Blocks, Levels, and
"Panels" (panel labels like "LCP-AHU-C-7-1-PM").

RUN:
  pip install fastapi uvicorn requests pydantic
  python -m uvicorn power_api:app --host 0.0.0.0 --port 8081 --reload

NOTES:
- Swagger UI: http://127.0.0.1:8081/docs   (use 127.0.0.1 on Windows)
- After changing parsing rules, call POST /meta/refresh to rebuild meter_meta.
"""

import os
import re
import csv
import io
import sqlite3
from datetime import datetime, timedelta
from calendar import monthrange

from typing import List, Optional, Tuple, Dict, Any, Pattern

import requests
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --------------------- CONFIG ---------------------

DB_PATH = os.environ.get("DB_PATH", "energy_meters.sqlite")
WACNET_BASE = os.environ.get("WACNET_BASE", "http://localhost:47800/api/v1/bacnet")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "200"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

DEVICE_IDS: List[str] = [
    "4582", "1563", "1121", "2121", "6151", "1058", "9141", "6582", "1024", "4131",
    "7502", "1124", "8241", "3582", "1258", "5582", "3151", "2342", "1342", "9582",
    "1784", "1158", "7231", "5131"
]
if os.environ.get("DEVICE_IDS_CSV"):
    DEVICE_IDS = [x.strip() for x in os.environ["DEVICE_IDS_CSV"].split(",") if x.strip()]

# --------------------- APP ------------------------

app = FastAPI(
    title="Power API",
    version="2.2.0",
    description="Energy analytics by Block/Level/Panel/Device + Trends + Plant/Efficiency",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------- MODELS ---------------------

class BlockLevelRow(BaseModel):
    block: str
    level: str
    kwh: float
    kw: float
    meters: int

class BlockRow(BaseModel):
    block: str
    kwh: float
    kw: float
    meters: int

class DeviceRow(BaseModel):
    device_id: str
    kwh: float
    kw: float
    meters: int

class MeterRow(BaseModel):
    meter_key: str
    device_id: str
    block: str
    level: str
    panel: Optional[str] = None
    kwh: float
    kw: float
    object_name: Optional[str] = None

class BlockLevelResponse(BaseModel):
    window: str
    ts: Optional[str] = None
    date: Optional[str] = None
    rows: List[BlockLevelRow]
    total_kwh: float
    total_kw: float

class GenericListResponse(BaseModel):
    window: str
    ts: Optional[str] = None
    date: Optional[str] = None
    rows: List[Any]
    total_kwh: float
    total_kw: float

class TrendPoint(BaseModel):
    ts: str
    kwh: float

class TrendSeries(BaseModel):
    key: str
    points: List[TrendPoint]

class TrendResponse(BaseModel):
    start: str
    end: str
    series: List[TrendSeries]

class TopRow(BaseModel):
    rank: int
    meter_key: str
    device_id: str
    block: str
    level: str
    kwh: float
    kw: float
    object_name: Optional[str] = None

class PlantHour(BaseModel):
    ts: str
    rt: Optional[float]
    flow_ls: Optional[float]
    dt_C: Optional[float]
    plant_kw: Optional[float]

class PlantDay(BaseModel):
    date: str
    ton_hours: float
    rt_avg: float
    rt_peak: float
    flow_ls_avg: float
    dt_C_avg: float

class EfficiencyHour(BaseModel):
    ts: str
    airside_kw: Optional[float]
    rt: Optional[float]
    kw_per_rt: Optional[float]

class EfficiencyDay(BaseModel):
    date: str
    airside_kwh: float
    ton_hours: float
    kw_per_rt: Optional[float]

class MetaStatus(BaseModel):
    meters_in_db: int
    meters_with_meta: int
    unmapped_meters: int

class LevelsResponse(BaseModel):
    rows: List[Dict[str, Any]]

class HierarchyResponse(BaseModel):
    by_block_level: Dict[str, Dict[str, List[str]]]
    by_panel: Dict[str, Dict[str, Any]]

# --------------------- DISPLAY HELPERS ------------

def display_level(level: Optional[str]) -> str:
    """
    UI helper:
    - if level is NULL or 'UNK' → display as "1"
    - otherwise pass through (e.g. L1, L2, B1, etc.)
    """
    if not level:
        return "1"
    s = str(level).upper()
    if s == "UNK":
        return "1"
    return level

# --------------------- LOGGING --------------------

def log(level: str, msg: str) -> None:
    wanted = ["DEBUG", "INFO", "WARN", "ERROR"]
    if level not in wanted:
        level = "INFO"
    if wanted.index(level) >= wanted.index(LOG_LEVEL):
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {level}: {msg}", flush=True)

# --------------------- DB UTILS -------------------

def get_db() -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connect failed: {e}")

def ensure_meta_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_meta(
          meter_key   TEXT PRIMARY KEY,
          device_id   TEXT,
          block       TEXT,
          level       TEXT,
          object_name TEXT,
          panel       TEXT
        )
    """)
    # Add 'panel' column if the table pre-existed without it
    cols = [r[1] for r in cur.execute("PRAGMA table_info(meter_meta)").fetchall()]
    if "panel" not in cols:
        cur.execute("ALTER TABLE meter_meta ADD COLUMN panel TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meta_block ON meter_meta(block)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meta_level ON meter_meta(level)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meta_panel ON meter_meta(panel)")
    conn.commit()

def latest_hour_ts(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(ts) FROM device_hourly").fetchone()
    if row and row[0]:
        return row[0]
    return None

def day_string_or_today(d: Optional[str]) -> str:
    if d:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        return d
    return datetime.now().strftime("%Y-%m-%d")

def parse_ts(ts: Optional[str]) -> str:
    if not ts:
        raise HTTPException(status_code=400, detail="ts is required")
    try:
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid ts format: {e}")
    return ts

def parse_range(start: str, end: str) -> Tuple[str, str]:
    try:
        s = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
        e = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
        if e <= s:
            raise ValueError("end must be after start")
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"Invalid range: {ex}")
    return start, end

def respond_csv(rows: List[Dict[str, Any]], filename: str = "export.csv") -> Response:
    if not rows:
        return Response(content="", media_type="text/csv")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# --------------------- PARSERS --------------------

_active_energy_re = re.compile(r"active\s*energy", re.IGNORECASE)
_block_re = re.compile(r"BLK-([A-Z])")

def is_active_energy(obj: dict) -> bool:
    name = (obj.get("object-name") or "")
    units = (obj.get("units") or "").lower()
    return bool(_active_energy_re.search(name)) or units == "kilowatt hours"

def panel_label_from_object_name(object_name: str, device_id: str) -> str:
    parts = (object_name or "").split(".")
    panel = parts[-2] if len(parts) >= 2 else (object_name or f"dev{device_id}")
    return panel.strip()

def meter_key_from_object_name(object_name: str, device_id: str) -> str:
    panel = panel_label_from_object_name(object_name, device_id)
    return f"{device_id}:{panel}"

def block_from_object_name(object_name: str) -> str:
    m = _block_re.search(object_name or "")
    return m.group(1) if m else "UNK"

def normalize_level_token(tok: str) -> Optional[str]:
    s = tok.strip().upper()
    m = re.search(r'(?:^|[^\d])([LB]?)(\d+)', s)
    if not m:
        return None
    prefix, num = m.group(1), m.group(2)
    if prefix == 'B':
        return f"B{num}"
    return f"L{num}"

def level_from_object_name(object_name: str) -> str:
    parts = (object_name or "").split(".")
    for tok in parts + [seg for p in parts for seg in p.split("-")]:
        lvl = normalize_level_token(tok)
        if lvl:
            return lvl
    return "UNK"

# ---- Anomaly overrides ----
OVERRIDE_LEVEL_BY_PANEL: List[Tuple[Pattern[str], str]] = [
    re.compile(r"(?i)\bLCP-AHU-A/\s*7-3-PM\b"),  # -> L7
]
OVERRIDE_BLOCK_BY_PANEL: List[Tuple[Pattern[str], str]] = [
    re.compile(r"(?i)\bLCP-PAHU-A/\s*8-1-PM\b"),  # -> Block A
]

def apply_level_override(panel: str, current: str) -> str:
    # only one override currently (A/7-3-PM -> L7)
    if OVERRIDE_LEVEL_BY_PANEL[0].search(panel):
        return "L7"
    return current

def apply_block_override(panel: str, current: str) -> str:
    if OVERRIDE_BLOCK_BY_PANEL[0].search(panel):
        return "A"
    return current

# --------------------- WACNET (meta refresh) ------

def fetch_device_objects(device_id: str) -> List[dict]:
    objects: List[dict] = []
    next_href: Optional[str] = (
        f"{WACNET_BASE}/devices/{device_id}/objects"
        "?properties=object-name"
        "&properties=present-value"
        "&properties=units"
        "&properties=status-flags"
        "&properties=reliability"
        "&properties=description"
        f"&limit={PAGE_LIMIT}&page=1"
    )
    while next_href:
        r = requests.get(next_href, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        payload = r.json()
        objects.extend(payload.get("objects", []))
        nxt = payload.get("next")
        next_href = nxt.get("href") if (nxt and nxt.get("href")) else None
    return objects

def build_or_refresh_meta(conn: sqlite3.Connection, device_ids: List[str]) -> Dict[str, Dict[str, str]]:
    ensure_meta_table(conn)
    cur = conn.cursor()
    result: Dict[str, Dict[str, str]] = {}
    for dev in device_ids:
        try:
            objs = fetch_device_objects(dev)
        except Exception as e:
            log("WARN", f"[meta] fetch device {dev} failed: {e}")
            continue
        for obj in objs:
            if not is_active_energy(obj):
                continue
            name = obj.get("object-name") or ""
            panel = panel_label_from_object_name(name, dev)
            meter_key = meter_key_from_object_name(name, dev)

            blk = block_from_object_name(name)
            lvl = level_from_object_name(name)

            # apply overrides
            blk = apply_block_override(panel, blk)
            lvl = apply_level_override(panel, lvl)

            row = {
                "device_id": dev,
                "block": blk,
                "level": lvl,
                "object_name": name,
                "panel": panel,
            }
            result[meter_key] = row
            cur.execute(
                """
                INSERT INTO meter_meta(meter_key, device_id, block, level, object_name, panel)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(meter_key) DO UPDATE SET
                    device_id=excluded.device_id,
                    block=excluded.block,
                    level=excluded.level,
                    object_name=excluded.object_name,
                    panel=excluded.panel
                """,
                (meter_key, dev, blk, lvl, name, panel),
            )
    conn.commit()
    return result

# --------------------- FILTER HELPERS --------------

def build_filter_clause(
    block: Optional[str], level: Optional[str], device_id: Optional[str],
    panel: Optional[str]
) -> Tuple[str, Tuple]:
    clauses = []
    args: List[Any] = []
    if block:
        clauses.append("(COALESCE(mm.block, mh.block) = ?)")
        args.append(block)
    if level:
        clauses.append("(COALESCE(mm.level, 'UNK') = ?)")
        args.append(level)
    if device_id:
        clauses.append("(COALESCE(mm.device_id, mh.device_id) = ?)")
        args.append(device_id)
    if panel:
        clauses.append("(mm.panel = ?)")
        args.append(panel)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, tuple(args)

def build_filter_clause_daily(
    block: Optional[str], level: Optional[str], device_id: Optional[str],
    panel: Optional[str]
) -> Tuple[str, Tuple]:
    clauses = []
    args: List[Any] = []
    if block:
        clauses.append("(COALESCE(mm.block, md.block) = ?)")
        args.append(block)
    if level:
        clauses.append("(COALESCE(mm.level, 'UNK') = ?)")
        args.append(level)
    if device_id:
        # ---- FIXED: add missing parentheses here ----
        clauses.append("(COALESCE(mm.device_id, md.device_id) = ?)")
        args.append(device_id)
    if panel:
        clauses.append("(mm.panel = ?)")
        args.append(panel)
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, tuple(args)

def month_bounds(month: Optional[str]) -> Tuple[str, str, str]:
    """
    Resolve a month string 'YYYY-MM' (or None for current month) into
    [start_iso, end_iso_exclusive, label].
    """
    now = datetime.now()
    if month:
        try:
            start = datetime.strptime(month, "%Y-%m")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid month format (use YYYY-MM): {e}")
    else:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    _, days_in_month = monthrange(start.year, start.month)
    end = start.replace(day=days_in_month, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    start_iso = start.strftime("%Y-%m-%dT00:00:00")
    end_iso = end.strftime("%Y-%m-%dT00:00:00")
    label = start.strftime("%Y-%m")
    return start_iso, end_iso, label

def year_bounds(year: Optional[str]) -> Tuple[str, str, str]:
    """
    Resolve a year string 'YYYY' (or None for current year) into
    [start_iso, end_iso_exclusive, label].
    """
    now = datetime.now()
    if year:
        try:
            y = int(year)
            start = datetime(year=y, month=1, day=1)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid year (use YYYY): {e}")
    else:
        start = datetime(year=now.year, month=1, day=1)

    end = datetime(year=start.year + 1, month=1, day=1)

    start_iso = start.strftime("%Y-%m-%dT00:00:00")
    end_iso = end.strftime("%Y-%m-%dT00:00:00")
    label = start.strftime("%Y")
    return start_iso, end_iso, label

# --------------------- ENDPOINTS ------------------

@app.get("/health")
def health():
    conn = get_db()
    try:
        conn.execute("SELECT 1")
        return {"ok": True}
    finally:
        conn.close()

@app.get("/meta/status", response_model=MetaStatus)
def meta_status():
    conn = get_db()
    ensure_meta_table(conn)
    try:
        cur = conn.cursor()
        meters_in_db = cur.execute(
            "SELECT COUNT(DISTINCT meter_key) FROM meter_hourly"
        ).fetchone()[0] or 0
        meters_with_meta = cur.execute(
            "SELECT COUNT(*) FROM meter_meta"
        ).fetchone()[0] or 0
        unmapped = cur.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT DISTINCT mh.meter_key
              FROM meter_hourly mh
              LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
              WHERE mm.meter_key IS NULL
            )
            """
        ).fetchone()[0] or 0
        return MetaStatus(
            meters_in_db=int(meters_in_db),
            meters_with_meta=int(meters_with_meta),
            unmapped_meters=int(unmapped),
        )
    finally:
        conn.close()

@app.get("/meta/levels", response_model=LevelsResponse)
def meta_levels(format: Optional[str] = Query(None, description="csv for CSV export")):
    conn = get_db()
    ensure_meta_table(conn)
    try:
        rows = conn.execute(
            """
            SELECT block, level, COUNT(*) AS meters
            FROM meter_meta
            GROUP BY block, level
            ORDER BY block, level
            """
        ).fetchall()
        data = [
            {
                "block": r[0] or "UNK",
                "level": r[1] or "UNK",
                "meters": int(r[2] or 0),
            }
            for r in rows
        ]
        if format == "csv":
            return respond_csv(data, filename="levels.csv")
        return LevelsResponse(rows=data)
    finally:
        conn.close()

@app.get(
    "/meta/hierarchy",
    response_model=HierarchyResponse,
    summary="Block/Level → Panels hierarchy and reverse panel lookup",
)
def meta_hierarchy():
    conn = get_db()
    ensure_meta_table(conn)
    try:
        rows = conn.execute(
            """
            SELECT panel, block, level, device_id, object_name
            FROM meter_meta
            WHERE panel IS NOT NULL AND panel <> ''
            """
        ).fetchall()
        by_bl: Dict[str, Dict[str, List[str]]] = {}
        by_panel: Dict[str, Dict[str, Any]] = {}
        for panel, block, level, dev, objname in rows:
            block = block or "UNK"
            level = level or "UNK"
            by_bl.setdefault(block, {}).setdefault(level, [])
            if panel not in by_bl[block][level]:
                by_bl[block][level].append(panel)
            bp = by_panel.setdefault(
                panel,
                {
                    "block": block,
                    "level": level,
                    "device_ids": set(),
                    "object_names": set(),
                },
            )
            if dev:
                bp["device_ids"].add(dev)
            if objname:
                bp["object_names"].add(objname)
        for p in by_panel.values():
            p["device_ids"] = sorted(list(p["device_ids"]))
            p["object_names"] = sorted(list(p["object_names"]))
        return HierarchyResponse(by_block_level=by_bl, by_panel=by_panel)
    finally:
        conn.close()

@app.get("/availability/hours")
def availability_hours(days: int = Query(7, ge=1, le=365)):
    conn = get_db()
    try:
        cutoff = (
            datetime.now().replace(minute=0, second=0, microsecond=0)
            - timedelta(days=days)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        rows = conn.execute(
            "SELECT ts FROM device_hourly WHERE ts >= ? ORDER BY ts", (cutoff,)
        ).fetchall()
        return {"ts": [r[0] for r in rows]}
    finally:
        conn.close()

@app.post("/meta/refresh")
def meta_refresh():
    conn = get_db()
    try:
        info = build_or_refresh_meta(conn, DEVICE_IDS)
        return {"updated": len(info), "devices_scanned": len(DEVICE_IDS)}
    finally:
        conn.close()

# ---- Block + Level (disambiguated group/order) ----

@app.get("/power/block-level/hour", response_model=BlockLevelResponse)
def power_block_level_hour(
    ts: Optional[str] = Query(None),
    format: Optional[str] = Query(None, description="csv for CSV export"),
):
    """
    NOTE:
    - Excludes AHU/PAHU/etc. (anything with mm.panel NOT NULL/empty).
    - Only Excel-mapped / non-panel meters contribute.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(
                    status_code=404, detail="No hourly data available."
                )
        ts = parse_ts(ts)
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, mh.block) AS blk,
                   COALESCE(mm.level, 'UNK')    AS lvl,
                   SUM(mh.kwh)                  AS kwh,
                   COUNT(DISTINCT mh.meter_key) AS meters
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, mh.block), COALESCE(mm.level, 'UNK')
            ORDER BY COALESCE(mm.block, mh.block), COALESCE(mm.level, 'UNK')
            """,
            (ts,),
        ).fetchall()
        data = []
        total = 0.0
        for block, level, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "level": display_level(level),
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, filename="block_level_hour.csv")
        return BlockLevelResponse(
            window="hour",
            ts=ts,
            rows=[BlockLevelRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

@app.get("/power/block-level/day", response_model=BlockLevelResponse)
def power_block_level_day(
    date_: Optional[str] = Query(None, alias="date"),
    format: Optional[str] = Query(None, description="csv for CSV export"),
):
    """
    Day-level Block×Level for power (Excel-mapped).
    Excludes AHU/PAHU (mm.panel NOT NULL/empty).
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, md.block) AS blk,
                   COALESCE(mm.level, 'UNK')    AS lvl,
                   SUM(md.kwh)                  AS kwh,
                   COUNT(DISTINCT md.meter_key) AS meters
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            ORDER BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            """,
            (midnight,),
        ).fetchall()
        data = []
        total = 0.0
        for block, level, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "level": display_level(level),
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, filename="block_level_day.csv")
        return BlockLevelResponse(
            window="day",
            date=d,
            rows=[BlockLevelRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

# ---- Block only ----

@app.get("/power/block/hour", response_model=GenericListResponse)
def power_block_hour(
    ts: Optional[str] = Query(None),
    format: Optional[str] = Query(None),
):
    """
    Hourly by Block for power (Excel-mapped only).
    Excludes AHU/PAHU.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, mh.block) AS blk,
                   SUM(mh.kwh) AS kwh,
                   COUNT(DISTINCT mh.meter_key) AS meters
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, mh.block)
            ORDER BY COALESCE(mm.block, mh.block)
            """,
            (ts,),
        ).fetchall()
        data, total = [], 0.0
        for block, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, "block_hour.csv")
        return GenericListResponse(
            window="hour",
            ts=ts,
            rows=[BlockRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

@app.get("/power/block/day", response_model=GenericListResponse)
def power_block_day(
    date_: Optional[str] = Query(None, alias="date"),
    format: Optional[str] = Query(None),
):
    """
    Daily by Block for power (Excel-mapped only).
    Excludes AHU/PAHU.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, md.block) AS blk,
                   SUM(md.kwh) AS kwh,
                   COUNT(DISTINCT md.meter_key) AS meters
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, md.block)
            ORDER BY COALESCE(mm.block, md.block)
            """,
            (midnight,),
        ).fetchall()
        data, total = [], 0.0
        for block, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, "block_day.csv")
        return GenericListResponse(
            window="day",
            date=d,
            rows=[BlockRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

# ---- Device ----

@app.get("/power/device/hour", response_model=GenericListResponse)
def power_device_hour(
    ts: Optional[str] = Query(None),
    block: Optional[str] = None,
    level: Optional[str] = None,
    panel: Optional[str] = None,
    format: Optional[str] = None,
):
    """
    Hourly by Device for power.
    Excludes AHU/PAHU (panel meters) even if panel filter is passed.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        where, args = build_filter_clause(block, level, None, panel)
        rows = conn.execute(
            f"""
            SELECT COALESCE(mm.device_id, mh.device_id) AS dev,
                   SUM(mh.kwh) AS kwh,
                   COUNT(DISTINCT mh.meter_key) AS meters
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            {where}
            GROUP BY COALESCE(mm.device_id, mh.device_id)
            ORDER BY COALESCE(mm.device_id, mh.device_id)
            """,
            (ts, *args),
        ).fetchall()
        data, total = [], 0.0
        for device_id, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "device_id": device_id,
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, "device_hour.csv")
        return GenericListResponse(
            window="hour",
            ts=ts,
            rows=[DeviceRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

@app.get("/power/device/day", response_model=GenericListResponse)
def power_device_day(
    date_: Optional[str] = Query(None, alias="date"),
    block: Optional[str] = None,
    level: Optional[str] = None,
    panel: Optional[str] = None,
    format: Optional[str] = None,
):
    """
    Daily by Device for power (Excel-mapped only).
    Excludes AHU/PAHU.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        where, args = build_filter_clause_daily(block, level, None, panel)
        rows = conn.execute(
            f"""
            SELECT COALESCE(mm.device_id, md.device_id) AS dev,
                   SUM(md.kwh) AS kwh,
                   COUNT(DISTINCT md.meter_key) AS meters
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            {where}
            GROUP BY COALESCE(mm.device_id, md.device_id)
            ORDER BY COALESCE(mm.device_id, md.device_id)
            """,
            (midnight, *args),
        ).fetchall()
        data, total = [], 0.0
        for device_id, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "device_id": device_id,
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "meters": int(meters or 0),
                }
            )
        if format == "csv":
            return respond_csv(data, "device_day.csv")
        return GenericListResponse(
            window="day",
            date=d,
            rows=[DeviceRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

# ---- Meter (panel-capable drill-down) ----

@app.get("/power/meter/hour", response_model=GenericListResponse)
def power_meter_hour(
    ts: Optional[str] = Query(None),
    block: Optional[str] = None,
    level: Optional[str] = None,
    device_id: Optional[str] = None,
    panel: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    format: Optional[str] = None,
):
    """
    Meter-level hourly for power (Excel-mapped meters only).
    AHU/PAHU panel meters are excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        where, args = build_filter_clause(block, level, device_id, panel)
        rows = conn.execute(
            f"""
            SELECT mh.meter_key,
                   COALESCE(mm.device_id, mh.device_id) AS dev,
                   COALESCE(mm.block, mh.block) AS blk,
                   COALESCE(mm.level, 'UNK') AS lvl,
                   mm.panel,
                   mh.kwh,
                   mm.object_name
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            {where}
            ORDER BY mh.kwh DESC, mh.meter_key
            LIMIT ? OFFSET ?
            """,
            (ts, *args, limit, offset),
        ).fetchall()
        data = []
        total = 0.0
        for meter_key, dev, block, level_, pnl, kwh, objname in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "meter_key": meter_key,
                    "device_id": dev,
                    "block": block or "UNK",
                    "level": display_level(level_),
                    "panel": pnl,
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "object_name": objname,
                }
            )
        if format == "csv":
            return respond_csv(data, "meter_hour.csv")
        return GenericListResponse(
            window="hour",
            ts=ts,
            rows=[MeterRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

@app.get("/power/meter/day", response_model=GenericListResponse)
def power_meter_day(
    date_: Optional[str] = Query(None, alias="date"),
    block: Optional[str] = None,
    level: Optional[str] = None,
    device_id: Optional[str] = None,
    panel: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    format: Optional[str] = None,
):
    """
    Meter-level daily for power (Excel-mapped meters only).
    AHU/PAHU panel meters are excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        where, args = build_filter_clause_daily(block, level, device_id, panel)
        rows = conn.execute(
            f"""
            SELECT md.meter_key,
                   COALESCE(mm.device_id, md.device_id) AS dev,
                   COALESCE(mm.block, md.block) AS blk,
                   COALESCE(mm.level, 'UNK') AS lvl,
                   mm.panel,
                   md.kwh,
                   mm.object_name
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ?
              AND (mm.panel IS NULL OR mm.panel = '')
            {where}
            ORDER BY md.kwh DESC, md.meter_key
            LIMIT ? OFFSET ?
            """,
            (midnight, *args, limit, offset),
        ).fetchall()
        data = []
        total = 0.0
        for meter_key, dev, block, level_, pnl, kwh, objname in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "meter_key": meter_key,
                    "device_id": dev,
                    "block": block or "UNK",
                    "level": display_level(level_),
                    "panel": pnl,
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "object_name": objname,
                }
            )
        if format == "csv":
            return respond_csv(data, "meter_day.csv")
        return GenericListResponse(
            window="day",
            date=d,
            rows=[MeterRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

# ---- Panel endpoints (AHU / airside remain here) ----

@app.get("/power/panel/hour", summary="Aggregate a single panel in an hour")
def power_panel_hour(
    panel: str = Query(..., description="Panel label (e.g., LCP-AHU-C-7-1-PM)"),
    ts: Optional[str] = Query(None),
):
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        rows = conn.execute(
            """
            SELECT SUM(mh.kwh) AS kwh
            FROM meter_hourly mh
            JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ? AND mm.panel = ?
            """,
            (ts, panel),
        ).fetchone()
        total = float(rows[0] or 0.0) if rows else 0.0
        meters = conn.execute(
            """
            SELECT mh.meter_key, COALESCE(mm.device_id, mh.device_id) AS dev, mh.kwh, mm.object_name
            FROM meter_hourly mh
            JOIN meter_meta mm ON mm.meter_key = mh.meter_key
            WHERE mh.ts = ? AND mm.panel = ?
            ORDER BY mh.kwh DESC, mh.meter_key
            """,
            (ts, panel),
        ).fetchall()
        return {
            "panel": panel,
            "ts": ts,
            "total_kwh": round(total, 6),
            "meters": [
                {
                    "meter_key": m[0],
                    "device_id": m[1],
                    "kwh": round(float(m[2] or 0.0), 6),
                    "object_name": m[3],
                }
                for m in meters
            ],
        }
    finally:
        conn.close()

@app.get("/power/panel/day", summary="Aggregate a single panel in a day")
def power_panel_day(
    panel: str = Query(..., description="Panel label (e.g., LCP-AHU-C-7-1-PM)"),
    date_: Optional[str] = Query(None, alias="date"),
):
    conn = get_db()
    ensure_meta_table(conn)
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        rows = conn.execute(
            """
            SELECT SUM(md.kwh) AS kwh
            FROM meter_daily md
            JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ? AND mm.panel = ?
            """,
            (midnight, panel),
        ).fetchone()
        total = float(rows[0] or 0.0) if rows else 0.0
        meters = conn.execute(
            """
            SELECT md.meter_key, COALESCE(mm.device_id, md.device_id) AS dev, md.kwh, mm.object_name
            FROM meter_daily md
            JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts = ? AND mm.panel = ?
            ORDER BY md.kwh DESC, md.meter_key
            """,
            (midnight, panel),
        ).fetchall()
        return {
            "panel": panel,
            "date": d,
            "total_kwh": round(total, 6),
            "meters": [
                {
                    "meter_key": m[0],
                    "device_id": m[1],
                    "kwh": round(float(m[2] or 0.0), 6),
                    "object_name": m[3],
                }
                for m in meters
            ],
        }
    finally:
        conn.close()

# ---- Top-N ----

@app.get("/power/top")
def power_top(
    window: str = Query("hour", regex="^(hour|day)$"),
    ts: Optional[str] = None,
    date_: Optional[str] = Query(None, alias="date"),
    n: int = Query(10, ge=1, le=1000),
    block: Optional[str] = None,
    level: Optional[str] = None,
    panel: Optional[str] = None,
    format: Optional[str] = None,
):
    """
    Top-N meters by kWh for power (Excel-mapped meters only).
    AHU/PAHU excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        if window == "hour":
            if ts is None:
                ts = latest_hour_ts(conn)
                if not ts:
                    raise HTTPException(status_code=404, detail="No hourly data.")
            ts = parse_ts(ts)
            where, args = build_filter_clause(block, level, None, panel)
            rows = conn.execute(
                f"""
                SELECT mh.meter_key, COALESCE(mm.device_id, mh.device_id) AS dev,
                       COALESCE(mm.block, mh.block) AS blk,
                       COALESCE(mm.level, 'UNK') AS lvl,
                       mh.kwh, mm.object_name
                FROM meter_hourly mh
                LEFT JOIN meter_meta mm ON mm.meter_key = mh.meter_key
                WHERE mh.ts = ?
                  AND (mm.panel IS NULL OR mm.panel = '')
                {where}
                ORDER BY mh.kwh DESC, mh.meter_key
                LIMIT ?
                """,
                (ts, *args, n),
            ).fetchall()
            title = f"top_hour_{ts}.csv"
            d = None
        else:
            d = day_string_or_today(date_)
            midnight = f"{d}T00:00:00"
            where, args = build_filter_clause_daily(block, level, None, panel)
            rows = conn.execute(
                f"""
                SELECT md.meter_key, COALESCE(mm.device_id, md.device_id) AS dev,
                       COALESCE(mm.block, md.block) AS blk,
                       COALESCE(mm.level, 'UNK') AS lvl,
                       md.kwh, mm.object_name
                FROM meter_daily md
                LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
                WHERE md.ts = ?
                  AND (mm.panel IS NULL OR mm.panel = '')
                {where}
                ORDER BY md.kwh DESC, md.meter_key
                LIMIT ?
                """,
                (midnight, *args, n),
            ).fetchall()
            title = f"top_day_{d}.csv"

        data = []
        for i, (meter_key, dev, blk, lvl, kwh, objname) in enumerate(rows, start=1):
            kwh = float(kwh or 0.0)
            data.append(
                {
                    "rank": i,
                    "meter_key": meter_key,
                    "device_id": dev,
                    "block": blk or "UNK",
                    "level": display_level(lvl),
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),
                    "object_name": objname,
                }
            )
        if format == "csv":
            return respond_csv(data, title)
        return {
            "window": window,
            "ts": ts if window == "hour" else None,
            "date": d if window == "day" else None,
            "rows": [TopRow(**r) for r in data],
        }
    finally:
        conn.close()

# ---- Trends ----

@app.get("/power/blocks/trend", response_model=TrendResponse)
def power_blocks_trend(
    start: str = Query(...),
    end: str = Query(...),
    format: Optional[str] = None,
):
    """
    Block-level trend for power (Excel-mapped meters only).
    AHU/PAHU excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        start, end = parse_range(start, end)
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, mh.block) AS blk,
                   mh.ts,
                   SUM(mh.kwh) AS kwh
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mh.meter_key = mm.meter_key
            WHERE mh.ts >= ? AND mh.ts < ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, mh.block), mh.ts
            ORDER BY COALESCE(mm.block, mh.block), mh.ts
            """,
            (start, end),
        ).fetchall()
        series_map: Dict[str, List[TrendPoint]] = {}
        for block, ts_, kwh in rows:
            block = block or "UNK"
            series_map.setdefault(block, []).append(
                TrendPoint(ts=ts_, kwh=round(float(kwh or 0.0), 6))
            )
        series = [TrendSeries(key=b, points=p) for b, p in series_map.items()]
        if format == "csv":
            flat = []
            for b, pts in series_map.items():
                for p in pts:
                    flat.append({"block": b, "ts": p.ts, "kwh": p.kwh})
            return respond_csv(flat, "blocks_trend.csv")
        return TrendResponse(start=start, end=end, series=series)
    finally:
        conn.close()

@app.get("/power/block-level/trend", response_model=TrendResponse)
def power_block_level_trend(
    block: str = Query(...),
    level: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    format: Optional[str] = None,
):
    """
    Block×Level trend for power (Excel-mapped meters only).
    AHU/PAHU excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        start, end = parse_range(start, end)
        rows = conn.execute(
            """
            SELECT mh.ts, SUM(mh.kwh) AS kwh
            FROM meter_hourly mh
            LEFT JOIN meter_meta mm ON mh.meter_key = mm.meter_key
            WHERE COALESCE(mm.block, mh.block) = ?
              AND COALESCE(mm.level, 'UNK') = ?
              AND mh.ts >= ? AND mh.ts < ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY mh.ts
            ORDER BY mh.ts
            """,
            (block, level, start, end),
        ).fetchall()
        points = [
            TrendPoint(ts=ts_, kwh=round(float(kwh or 0.0), 6)) for ts_, kwh in rows
        ]
        if format == "csv":
            return respond_csv(
                [{"ts": p.ts, "kwh": p.kwh} for p in points], "block_level_trend.csv"
            )
        return TrendResponse(
            start=start,
            end=end,
            series=[
                TrendSeries(key=f"{block}.{display_level(level)}", points=points)
            ],
        )
    finally:
        conn.close()

# ---- Plant & Efficiency (UNCHANGED) ----

@app.get("/plant/hour", response_model=PlantHour)
def plant_hour(ts: Optional[str] = Query(None)):
    conn = get_db()
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        row = conn.execute(
            "SELECT ts, rt, flow_ls, dt_C, plant_kw FROM plant_hourly WHERE ts = ?",
            (ts,),
        ).fetchone()
        if not row:
            return PlantHour(ts=ts, rt=None, flow_ls=None, dt_C=None, plant_kw=None)
        return PlantHour(
            ts=row[0], rt=row[1], flow_ls=row[2], dt_C=row[3], plant_kw=row[4]
        )
    finally:
        conn.close()

@app.get("/plant/day", response_model=PlantDay)
def plant_day(date_: Optional[str] = Query(None, alias="date")):
    conn = get_db()
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        row = conn.execute(
            """
            SELECT ts, ton_hours, rt_avg, rt_peak, flow_ls_avg, dt_C_avg
            FROM plant_daily WHERE ts = ?
            """,
            (midnight,),
        ).fetchone()
        if not row:
            return PlantDay(
                date=d,
                ton_hours=0.0,
                rt_avg=0.0,
                rt_peak=0.0,
                flow_ls_avg=0.0,
                dt_C_avg=0.0,
            )
        return PlantDay(
            date=d,
            ton_hours=row[1] or 0.0,
            rt_avg=row[2] or 0.0,
            rt_peak=row[3] or 0.0,
            flow_ls_avg=row[4] or 0.0,
            dt_C_avg=row[5] or 0.0,
        )
    finally:
        conn.close()

@app.get("/efficiency/hour", response_model=EfficiencyHour)
def efficiency_hour(ts: Optional[str] = Query(None)):
    conn = get_db()
    try:
        if ts is None:
            ts = latest_hour_ts(conn)
            if not ts:
                raise HTTPException(status_code=404, detail="No hourly data.")
        ts = parse_ts(ts)
        row = conn.execute(
            "SELECT ts, airside_kw, rt, kw_per_rt FROM efficiency_hourly WHERE ts = ?",
            (ts,),
        ).fetchone()
        if not row:
            return EfficiencyHour(ts=ts, airside_kw=None, rt=None, kw_per_rt=None)
        return EfficiencyHour(
            ts=row[0], airside_kw=row[1], rt=row[2], kw_per_rt=row[3]
        )
    finally:
        conn.close()

@app.get("/efficiency/day", response_model=EfficiencyDay)
def efficiency_day(date_: Optional[str] = Query(None, alias="date")):
    conn = get_db()
    try:
        d = day_string_or_today(date_)
        midnight = f"{d}T00:00:00"
        row = conn.execute(
            "SELECT ts, airside_kwh, ton_hours, kw_per_rt FROM efficiency_daily WHERE ts = ?",
            (midnight,),
        ).fetchone()
        if not row:
            return EfficiencyDay(
                date=d, airside_kwh=0.0, ton_hours=0.0, kw_per_rt=None
            )
        return EfficiencyDay(
            date=d,
            airside_kwh=row[1] or 0.0,
            ton_hours=row[2] or 0.0,
            kw_per_rt=row[3],
        )
    finally:
        conn.close()

# ---- Monthly / Yearly Block×Level ----

@app.get("/power/block-level/month", response_model=BlockLevelResponse)
def power_block_level_month(
    month: Optional[str] = Query(
        None, description="YYYY-MM (defaults to current month)"
    ),
    format: Optional[str] = Query(None, description="csv for CSV export"),
):
    """
    Monthly Block×Level for power (Excel-mapped meters only).
    Aggregates meter_daily across the month window.
    AHU/PAHU excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        start_iso, end_iso, label = month_bounds(month)
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, md.block) AS blk,
                   COALESCE(mm.level, 'UNK')    AS lvl,
                   SUM(md.kwh)                  AS kwh,
                   COUNT(DISTINCT md.meter_key) AS meters
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts >= ? AND md.ts < ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            ORDER BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            """,
            (start_iso, end_iso),
        ).fetchall()

        data, total = [], 0.0
        for block, level, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "level": display_level(level),
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),  # same value; monthly energy
                    "meters": int(meters or 0),
                }
            )

        if format == "csv":
            return respond_csv(data, filename=f"block_level_month_{label}.csv")

        return BlockLevelResponse(
            window="month",
            date=label,  # e.g., '2025-10'
            rows=[BlockLevelRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()

@app.get("/power/block-level/year", response_model=BlockLevelResponse)
def power_block_level_year(
    year: Optional[str] = Query(
        None, description="YYYY (defaults to current year)"
    ),
    format: Optional[str] = Query(None, description="csv for CSV export"),
):
    """
    Yearly Block×Level for power (Excel-mapped meters only).
    Aggregates meter_daily across the year window.
    AHU/PAHU excluded.
    """
    conn = get_db()
    ensure_meta_table(conn)
    try:
        start_iso, end_iso, label = year_bounds(year)
        rows = conn.execute(
            """
            SELECT COALESCE(mm.block, md.block) AS blk,
                   COALESCE(mm.level, 'UNK')    AS lvl,
                   SUM(md.kwh)                  AS kwh,
                   COUNT(DISTINCT md.meter_key) AS meters
            FROM meter_daily md
            LEFT JOIN meter_meta mm ON mm.meter_key = md.meter_key
            WHERE md.ts >= ? AND md.ts < ?
              AND (mm.panel IS NULL OR mm.panel = '')
            GROUP BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            ORDER BY COALESCE(mm.block, md.block), COALESCE(mm.level, 'UNK')
            """,
            (start_iso, end_iso),
        ).fetchall()

        data, total = [], 0.0
        for block, level, kwh, meters in rows:
            kwh = float(kwh or 0.0)
            total += kwh
            data.append(
                {
                    "block": block or "UNK",
                    "level": display_level(level),
                    "kwh": round(kwh, 6),
                    "kw": round(kwh, 6),  # yearly energy
                    "meters": int(meters or 0),
                }
            )

        if format == "csv":
            return respond_csv(data, filename=f"block_level_year_{label}.csv")

        return BlockLevelResponse(
            window="year",
            date=label,  # e.g., '2025'
            rows=[BlockLevelRow(**r) for r in data],
            total_kwh=round(total, 6),
            total_kw=round(total, 6),
        )
    finally:
        conn.close()
