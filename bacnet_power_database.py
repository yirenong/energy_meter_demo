#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Electrical energy + plant RT collector (continuous)

- AHU energy: Uses "Active Energy" cumulative (kWh) -> hourly usage:
      hourly_kWh = max(curr_kWh - prev_kWh, 0)
- Plant load (device 1210):
      RT = 1.19 × Flow(L/s) × (CHWR °C − CHWS °C)
      plant_kW = 4.186 × Flow(L/s) × ΔT(°C)
- Efficiency each hour:
      airside_kW = sum of hourly_kWh across AHU meters (== avg kW over that hour)
      kW_per_RT  = airside_kW / RT

Writes SQLite:
  meter_hourly(meter_key, ts, kwh, device_id, block, PRIMARY KEY(meter_key, ts))
  device_hourly(device_id, ts, kwh, PRIMARY KEY(device_id, ts))
  block_hourly(block, ts, kwh, PRIMARY KEY(block, ts))
  meter_daily(meter_key, ts, kwh, device_id, block, PRIMARY KEY(meter_key, ts))
  device_daily(device_id, ts, kwh, PRIMARY KEY(device_id, ts))
  block_daily(block, ts, kwh, PRIMARY KEY(block, ts))
  meter_last(meter_key PRIMARY KEY, last_kwh, last_ts)

  plant_hourly(ts PRIMARY KEY, rt, flow_ls, dt_C, plant_kw)
  plant_daily(ts PRIMARY KEY, ton_hours, rt_avg, rt_peak, flow_ls_avg, dt_C_avg)

  efficiency_hourly(ts PRIMARY KEY, airside_kw, rt, kw_per_rt)
  efficiency_daily(ts PRIMARY KEY, airside_kwh, ton_hours, kw_per_rt)

Env vars (optional):
  WACNET_BASE       (default http://localhost:47800/api/v1/bacnet)
  DB_PATH           (default energy_meters.sqlite)
  LOG_LEVEL         (DEBUG|INFO|WARN, default INFO)
  REQUEST_TIMEOUT   (default 60)
  PAGE_LIMIT        (default 900)

  DEVICE_IDS_CSV    (override AHU/panel device list — these are your AIRSIDE devices too)
  PLANT_DEVICE_ID   (default "1210")

  # Exact object-names on 1210; change if needed
  HDR_FLOW_NAME     (default "HDR_CHWR_Flow(H)")
  HDR_CHWR_NAME     (default "HDR_CHWR_Temp(H)")
  HDR_CHWS_NAME     (default "HDR_CHWS_Temp(H)")

  FLOW_MIN_LS       (default 0.5)  # noise gate
  DELTA_T_MIN_C     (default 0.3)  # noise gate for near-zero ΔT

  # Excel mapping
  MAPPING_XLSX      (default "Mapping v1.xlsx")
"""

import os
import re
import signal
import sqlite3
import time
import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from opcua import Client as OPCUAClient

import requests

# Excel mapping requires pandas
try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None

OPC_ENDPOINT = "opc.tcp://172.30.153.138:49320"
OPC_USER = "OPC-UA-USER"
OPC_PASS = "Metasys-OPC-UA-client"

# --------------------- CONFIG ---------------------

WACNET_BASE = os.environ.get("WACNET_BASE", "http://localhost:47800/api/v1/bacnet")
DB_PATH = os.environ.get("DB_PATH", "energy_meters.sqlite")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
PAGE_LIMIT = int(os.environ.get("PAGE_LIMIT", "900"))

# AHU/panel devices providing Active Energy (kWh)
# These are also used for AIRSIDE calculation (old logic).
DEVICE_IDS: List[str] = [
    "4582","1563","1121","2121","6151","1058","9141","6582","1024","4131","7502","1124",
    "8241","3582","1258","5582","3151","2342","1342","9582","1784","1158","7231","5131"
]
if os.environ.get("DEVICE_IDS_CSV"):
    DEVICE_IDS = [x.strip() for x in os.environ["DEVICE_IDS_CSV"].split(",") if x.strip()]

# Plant device (HDR flow/temps)
PLANT_DEVICE_ID = os.environ.get("PLANT_DEVICE_ID", "1210")

# Exact object-names (case-sensitive) on 1210
HDR_FLOW_NAME = os.environ.get("HDR_FLOW_NAME", "HDR_CHWR_Flow(H)")
HDR_CHWR_NAME = os.environ.get("HDR_CHWR_NAME", "HDR_CHWR_Temp(H)")
HDR_CHWS_NAME = os.environ.get("HDR_CHWS_NAME", "HDR_CHWS_Temp(H)")

# Noise gates
FLOW_MIN_LS = float(os.environ.get("FLOW_MIN_LS", "0.5"))     # L/s below this -> treat as 0
DELTA_T_MIN_C = float(os.environ.get("DELTA_T_MIN_C", "0.3")) # °C below this -> treat as 0

# Excel mapping file
MAPPING_XLSX = os.environ.get("MAPPING_XLSX", "Mapping v1.xlsx")

# Mapping structures
# MAPPING_BY_DEVICE[device_id] = list of { "point_name", "meter_key", "block" }
MAPPING_BY_DEVICE: Dict[str, List[Dict[str, str]]] = {}
# Extra devices from Excel (may not be in DEVICE_IDS)
EXCEL_DEVICE_IDS: List[str] = []

# Final polling list: old DEVICE_IDS (AHUs) + EXCEL_DEVICE_IDS
POLL_DEVICE_IDS: List[str] = []

_shutdown = False


# --------------------- OPC UA ----------------------

def opc_read_points(points: List[str]) -> Dict[str, float]:
    """
    Synchronous version — works with your existing collector.
    Reads OPC UA NodeIds and returns {nodeId: value}.
    """
    out = {}

    try:
        client = OPCUAClient(OPC_ENDPOINT)
        client.set_user(OPC_USER)
        client.set_password(OPC_PASS)
        client.connect()

        for p in points:
            try:
                node = client.get_node(p)
                val = node.get_value()
                out[p] = float(val)
            except Exception as e:
                print(f"[OPC ERROR] Failed reading {p}: {e}")

        client.disconnect()

    except Exception as e:
        print(f"[OPC ERROR] Connection failed: {e}")

    return out

# --------------------- LOGGING --------------------

def log(level: str, msg: str) -> None:
    wanted = ["DEBUG", "INFO", "WARN", "ERROR"]
    if level not in wanted:
        level = "INFO"
    if wanted.index(level) >= wanted.index(LOG_LEVEL):
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {level}: {msg}", flush=True)


# --------------------- DB -------------------------

def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # Existing tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_hourly(
          meter_key TEXT,
          ts TEXT,
          kwh REAL,
          device_id TEXT,
          block TEXT,
          PRIMARY KEY(meter_key, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_hourly(
          device_id TEXT,
          ts TEXT,
          kwh REAL,
          PRIMARY KEY(device_id, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS block_hourly(
          block TEXT,
          ts TEXT,
          kwh REAL,
          PRIMARY KEY(block, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_daily(
          meter_key TEXT,
          ts TEXT,
          kwh REAL,
          device_id TEXT,
          block TEXT,
          PRIMARY KEY(meter_key, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_daily(
          device_id TEXT,
          ts TEXT,
          kwh REAL,
          PRIMARY KEY(device_id, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS block_daily(
          block TEXT,
          ts TEXT,
          kwh REAL,
          PRIMARY KEY(block, ts)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meter_last(
          meter_key TEXT PRIMARY KEY,
          last_kwh REAL,
          last_ts TEXT
        )
    """)

    # NEW: plant + efficiency tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plant_hourly(
          ts TEXT PRIMARY KEY,
          rt REAL,
          flow_ls REAL,
          dt_C REAL,
          plant_kw REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plant_daily(
          ts TEXT PRIMARY KEY,
          ton_hours REAL,
          rt_avg REAL,
          rt_peak REAL,
          flow_ls_avg REAL,
          dt_C_avg REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS efficiency_hourly(
          ts TEXT PRIMARY KEY,
          airside_kw REAL,
          rt REAL,
          kw_per_rt REAL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS efficiency_daily(
          ts TEXT PRIMARY KEY,
          airside_kwh REAL,
          ton_hours REAL,
          kw_per_rt REAL
        )
    """)

    # helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meter_hourly_ts ON meter_hourly(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_device_hourly_ts ON device_hourly(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_block_hourly_ts ON block_hourly(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_meter_daily_ts ON meter_daily(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_device_daily_ts ON device_daily(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_block_daily_ts ON block_daily(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_plant_hourly_ts ON plant_hourly(ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_eff_hourly_ts ON efficiency_hourly(ts)")
    conn.commit()


def upsert(conn: sqlite3.Connection, sql: str, args: Tuple) -> None:
    conn.execute(sql, args)


def get_last(conn: sqlite3.Connection, meter_key: str) -> Tuple[Optional[float], Optional[str]]:
    row = conn.execute("SELECT last_kwh, last_ts FROM meter_last WHERE meter_key=?", (meter_key,)).fetchone()
    if not row:
        return None, None
    try:
        return float(row[0]), row[1]
    except (TypeError, ValueError):
        return None, row[1]


def set_last(conn: sqlite3.Connection, meter_key: str, kwh: float, ts: str) -> None:
    conn.execute("""
        INSERT INTO meter_last(meter_key, last_kwh, last_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(meter_key) DO UPDATE SET last_kwh=excluded.last_kwh, last_ts=excluded.last_ts
    """, (meter_key, kwh, ts))


# --------------------- HELPERS --------------------

_active_energy_re = re.compile(r"active energy", re.IGNORECASE)
_block_re = re.compile(r"BLK-([A-Z])")

def is_active_energy(obj: dict) -> bool:
    name = (obj.get("object-name") or "")
    units = (obj.get("units") or "").lower()
    return bool(_active_energy_re.search(name)) or units == "kilowatt hours"


def parse_meter_and_block(object_name: str, device_id: str) -> Tuple[str, str]:
    """
    OLD logic: meter_key based on object-name; block extracted with BLK-<X>.
    Used as fallback when no Excel mapping for that device.
    """
    parts = (object_name or "").split(".")
    meter_label = parts[-2] if len(parts) >= 2 else object_name or f"dev{device_id}"
    meter_key = f"{device_id}:{meter_label}"
    m = _block_re.search(object_name or "")
    block = m.group(1) if m else "UNK"
    return meter_key, block


# -------- Excel Mapping --------

def extract_block_code_from_block_name(block_name: str) -> str:
    """
    Examples:
      'Blk A-Admin MSGB' -> 'A'
      'BLK BD MSB'       -> 'BD'
      'Blk C MSG'        -> 'C'
      'HT INC 1'         -> 'HT'  (no 'Blk', so first word)
    """
    s = str(block_name).strip()
    if not s:
        return "UNK"

    # Prefer text after 'Blk'
    m = re.search(r"\bblk\b", s, flags=re.IGNORECASE)
    if m:
        i = m.end()
        while i < len(s) and s[i].isspace():
            i += 1
        start = i
        while i < len(s) and not s[i].isspace() and s[i] != "-":
            i += 1
        code = s[start:i].strip()
    else:
        parts = s.split()
        code = parts[0] if parts else ""

    j = 0
    while j < len(code) and code[j].isalnum():
        j += 1
    code = code[:j]
    return code.upper() if code else "UNK"


def load_mapping_from_excel() -> None:
    """
    Read Mapping v1.xlsx and populate:
      - MAPPING_BY_DEVICE[device_id] = list of {point_name, meter_key, block}
      - EXCEL_DEVICE_IDS
      - POLL_DEVICE_IDS = DEVICE_IDS ∪ EXCEL_DEVICE_IDS

    Expected columns (case-insensitive search):
      - 'device id'
      - 'Point name'
      - 'Block name'

    Rows with blank device id are ignored.
    """
    global MAPPING_BY_DEVICE, EXCEL_DEVICE_IDS, POLL_DEVICE_IDS

    # Start from old logic: poll list is DEVICE_IDS
    POLL_DEVICE_IDS = DEVICE_IDS.copy()

    if not pd:
        log("WARN", "pandas not installed; Excel mapping disabled. Using old code logic only.")
        return

    if not os.path.exists(MAPPING_XLSX):
        log("INFO", f"Mapping file '{MAPPING_XLSX}' not found; using old code logic with DEVICE_IDS only.")
        return

    try:
        raw = pd.read_excel(MAPPING_XLSX, sheet_name=0, header=None, dtype=str)
    except Exception as e:
        log("ERROR", f"Failed to read mapping Excel '{MAPPING_XLSX}': {e}")
        return

    header_idx = None
    for i, row in raw.iterrows():
        if any(isinstance(v, str) and v.strip().lower() == "device id" for v in row):
            header_idx = i
            break

    if header_idx is None:
        log("ERROR", f"Mapping Excel '{MAPPING_XLSX}' does not contain a 'device id' header row.")
        return

    header = [(v or "").strip() for v in raw.iloc[header_idx].tolist()]
    df = raw.iloc[header_idx + 1:].copy()
    df.columns = header

    cols_lower = {c.lower(): c for c in df.columns}
    dev_col = next((cols_lower[k] for k in cols_lower if "device" in k and "id" in k), None)
    point_col = next((cols_lower[k] for k in cols_lower if "point" in k and "name" in k), None)
    block_col = next((cols_lower[k] for k in cols_lower if "block" in k and "name" in k), None)

    if not (dev_col and point_col and block_col):
        log("ERROR", "Mapping Excel is missing 'device id', 'Point name', or 'Block name' columns.")
        return

    df = df[[dev_col, point_col, block_col]].rename(columns={
        dev_col:   "device_id",
        point_col: "point_name",
        block_col: "block_name",
    })

    df = df.dropna(subset=["device_id", "point_name", "block_name"])
    df["device_id"]  = df["device_id"].astype(str).str.strip()
    df["point_name"] = df["point_name"].astype(str).str.strip()
    df["block_name"] = df["block_name"].astype(str).str.strip()
    df = df[df["device_id"] != ""]

    if df.empty:
        log("WARN", f"Mapping Excel '{MAPPING_XLSX}' had no usable rows after cleaning.")
        return

    df["block"] = df["block_name"].apply(extract_block_code_from_block_name)
    df["meter_key"] = df["block_name"]

    MAPPING_BY_DEVICE = {}
    for _, row in df.iterrows():
        dev = row["device_id"]
        MAPPING_BY_DEVICE.setdefault(dev, []).append({
            "point_name": row["point_name"],
            "meter_key": row["meter_key"],
            "block": row["block"],
            "opc_node": row["point_name"],
        })

    EXCEL_DEVICE_IDS = sorted(MAPPING_BY_DEVICE.keys())

    # Final poll list = old AHU list + Excel devices
    POLL_DEVICE_IDS = sorted(set(DEVICE_IDS).union(EXCEL_DEVICE_IDS))

    log("INFO", f"Loaded Excel mapping from '{MAPPING_XLSX}' for {len(EXCEL_DEVICE_IDS)} devices, {len(df)} mapped points.")
    log("INFO", f"Poll devices: {POLL_DEVICE_IDS}")
    log("INFO", f"AIRSIDE will use only DEVICE_IDS (old AHU list): {DEVICE_IDS}")


# --- Object finders ---

def find_obj_exact(objects: List[dict], exact_name: str) -> Optional[dict]:
    if not exact_name:
        return None
    for obj in objects:
        if (obj.get("object-name") or "") == exact_name:
            return obj
    return None


def find_obj_substr(objects: List[dict], name_hint: str) -> Optional[dict]:
    if not name_hint:
        return None
    key = name_hint.lower()
    for obj in objects:
        name = (obj.get("object-name") or "").lower()
        if key in name:
            return obj
    return None


# --- Unit unifiers ---

def unify_flow_to_ls(value: float, units: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    u = (units or "").strip().lower()
    if ("liter" in u and "second" in u) or "l/s" in u or "lps" in u:
        return v
    if "cubic" in u and "meter" in u and "hour" in u:
        return v * 0.2777777778
    if "m3/h" in u or "m³/h" in u or "m^3/h" in u or "m3 per hour" in u or "m³ per hour" in u:
        return v * 0.2777777778
    if "gpm" in u or ("gallon" in u and "minute" in u) or "gal/min" in u:
        return v * 0.0630901964
    return None


def unify_temp_to_C(value: float, units: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    u = (units or "").strip().lower()
    if "celsius" in u or "degc" in u or "°c" in u or u == "c":
        return v
    if "fahrenheit" in u or "degf" in u or "°f" in u or u == "f":
        return (v - 32.0) * 5.0 / 9.0
    return None


# --------------------- WACNET ---------------------

def fetch_device_objects(device_id: str) -> List[dict]:
    """
    Fetch ALL objects for a device, following pagination.
    """
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


def normalise_energy_value_to_kwh(obj: dict, device_id: str, name: str) -> Optional[float]:
    """
    Convert BACnet present-value to kWh using units.
    - 'kilowatt hours' / 'kwh' -> kWh as-is
    - 'watt hours' / 'wh'      -> value / 1000
    - else: assume already kWh, but log a warning
    """
    raw_val = obj.get("present-value")
    units = (obj.get("units") or "").strip().lower()

    try:
        v = float(raw_val)
    except (TypeError, ValueError):
        log("WARN", f"[dev {device_id}] Active Energy '{name}' has non-numeric present-value='{raw_val}'; skipping.")
        return None

    if "kilowatt hours" in units or units == "kwh":
        return v
    elif "watt hours" in units or units == "wh":
        return v / 1000.0
    else:
        if units:
            log("WARN", f"[dev {device_id}] Active Energy '{name}' has unexpected units='{units}'; assuming kWh.")
        else:
            log("WARN", f"[dev {device_id}] Active Energy '{name}' has no units; assuming kWh.")
        return v


def extract_active_energy_kwh_auto(objects: List[dict], device_id: str) -> Dict[str, Dict[str, object]]:
    """
    OLD logic, but with unit normalisation to kWh.
    """
    out: Dict[str, Dict[str, object]] = {}
    for obj in objects:
        if not is_active_energy(obj):
            continue
        name = obj.get("object-name") or ""
        kwh = normalise_energy_value_to_kwh(obj, device_id, name)
        if kwh is None:
            continue
        meter_key, block = parse_meter_and_block(name, device_id)
        out[meter_key] = {"device_id": device_id, "block": block, "kwh": kwh}
    return out


def extract_active_energy_kwh_mapped(objects: List[dict], device_id: str) -> Dict[str, Dict[str, object]]:
    """
    Excel mapping logic:
      - Find objects by 'Point name'
      - meter_key = 'Block name' string
      - block     = parsed code from block name
    """
    mapping_rows = MAPPING_BY_DEVICE.get(device_id)
    if not mapping_rows:
        return extract_active_energy_kwh_auto(objects, device_id)

    out: Dict[str, Dict[str, object]] = {}
    by_name = { (o.get("object-name") or ""): o for o in objects }
    by_name_lower = { (o.get("object-name") or "").lower(): o for o in objects }

    for row in mapping_rows:
        pt_name = row["point_name"]
        meter_key = row["meter_key"]
        block = row["block"]

        obj = by_name.get(pt_name) or by_name_lower.get(pt_name.lower())
        if not obj:
            log("WARN", f"[dev {device_id}] Mapped point '{pt_name}' not found in BACnet objects.")
            continue

        kwh = normalise_energy_value_to_kwh(obj, device_id, pt_name)
        if kwh is None:
            continue

        out[meter_key] = {"device_id": device_id, "block": block, "kwh": kwh}

    if not out:
        log("WARN", f"[dev {device_id}] No Active Energy values extracted using Excel mapping.")
    return out


def fetch_all_current_readings(device_ids: List[str]) -> Dict[str, Dict[str, object]]:
    combined: Dict[str, Dict[str, object]] = {}

    for dev in device_ids:
        # ---- NEW OPC UA MODE ----
        if dev == "OPC":
            try:
                if dev == "OPC":
                    per_dev = extract_opc_active_energy(dev)
                    combined.update(per_dev)
                    continue
                combined.update(per_dev)
            except Exception as e:
                log("ERROR", f"[OPC] Error: {e}")
            continue

        # ---- EXISTING BACNET MODE ----
        try:
            objs = fetch_device_objects(dev)
        except requests.HTTPError as e:
            log("ERROR", f"[dev {dev}] HTTP error: {e}")
            continue
        except Exception as e:
            log("ERROR", f"[dev {dev}] Unexpected fetch error: {e}")
            continue

        if dev in MAPPING_BY_DEVICE:
            per_dev = extract_active_energy_kwh_mapped(objs, dev)
        else:
            per_dev = extract_active_energy_kwh_auto(objs, dev)

        combined.update(per_dev)

    return combined



# --------------------- PLANT (HDR) ----------------

def fetch_hdr_snapshot(plant_device_id: str) -> Optional[Tuple[float, float, float]]:
    try:
        objs = fetch_device_objects(plant_device_id)
    except Exception as e:
        log("ERROR", f"[plant {plant_device_id}] fetch failed: {e}")
        return None

    flow_obj = find_obj_exact(objs, HDR_FLOW_NAME)
    chwr_obj = find_obj_exact(objs, HDR_CHWR_NAME)
    chws_obj = find_obj_exact(objs, HDR_CHWS_NAME)
    exact_mode = bool(flow_obj and chwr_obj and chws_obj)

    if not flow_obj:
        flow_obj = find_obj_substr(objs, HDR_FLOW_NAME)
    if not chwr_obj:
        chwr_obj = find_obj_substr(objs, HDR_CHWR_NAME)
    if not chws_obj:
        chws_obj = find_obj_substr(objs, HDR_CHWS_NAME)

    if not (flow_obj and chwr_obj and chws_obj):
        log("WARN", f"[plant {plant_device_id}] Missing HDR points (Flow/CHWR/CHWS).")
        return None

    flow_ls = unify_flow_to_ls(flow_obj.get("present-value"), flow_obj.get("units"))
    chwr_C  = unify_temp_to_C(chwr_obj.get("present-value"), chwr_obj.get("units"))
    chws_C  = unify_temp_to_C(chws_obj.get("present-value"), chws_obj.get("units"))

    def as_float(x):
        try:
            return float(x)
        except Exception:
            return None

    if exact_mode and (flow_ls is None or chwr_C is None or chws_C is None):
        if flow_ls is None:
            flow_ls = as_float(flow_obj.get("present-value"))
        if chwr_C is None:
            chwr_C = as_float(chwr_obj.get("present-value"))
        if chws_C is None:
            chws_C = as_float(chws_obj.get("present-value"))

    if flow_ls is None or chwr_C is None or chws_C is None:
        log("WARN", f"[plant {plant_device_id}] Unit conversion failed: "
                    f"flow_units={flow_obj.get('units')}, "
                    f"CHWR_units={chwr_obj.get('units')}, "
                    f"CHWS_units={chws_obj.get('units')}")
        return None

    log("DEBUG", f"[plant {plant_device_id}] Using "
                f"{flow_obj.get('object-name')} ({flow_obj.get('units')}), "
                f"{chwr_obj.get('object-name')} ({chwr_obj.get('units')}), "
                f"{chws_obj.get('object-name')} ({chws_obj.get('units')})")

    return (flow_ls, chwr_C, chws_C)


def compute_rt_from_hdr(flow_ls: float, chwr_C: float, chws_C: float) -> Tuple[float, float, float]:
    dt_C = chwr_C - chws_C
    if dt_C < DELTA_T_MIN_C:
        dt_C = 0.0
    if flow_ls < FLOW_MIN_LS:
        flow_ls = 0.0
    plant_kw = 4.186 * flow_ls * dt_C
    rt = 1.19 * flow_ls * dt_C
    return (round(rt, 6), round(dt_C, 6), round(plant_kw, 6))


# --------------------- DAILY AGG -------------------

def midnight_ts(date_str: str) -> str:
    return f"{date_str}T00:00:00"


def refresh_daily(conn: sqlite3.Connection, date_str: str) -> None:
    # meter_daily
    rows = conn.execute("""
        SELECT meter_key, device_id, block, SUM(kwh)
        FROM meter_hourly
        WHERE substr(ts,1,10)=?
        GROUP BY meter_key, device_id, block
    """, (date_str,)).fetchall()
    for meter_key, device_id, block, total in rows:
        upsert(conn, """
            INSERT INTO meter_daily(meter_key, ts, kwh, device_id, block)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(meter_key, ts) DO UPDATE SET kwh=excluded.kwh, device_id=excluded.device_id, block=excluded.block
        """, (meter_key, midnight_ts(date_str), round(total or 0.0, 6), device_id, block))

    # device_daily
    rows = conn.execute("""
        SELECT device_id, SUM(kwh)
        FROM meter_hourly
        WHERE substr(ts,1,10)=?
        GROUP BY device_id
    """, (date_str,)).fetchall()
    for device_id, total in rows:
        upsert(conn, """
            INSERT INTO device_daily(device_id, ts, kwh)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id, ts) DO UPDATE SET kwh=excluded.kwh
        """, (device_id, midnight_ts(date_str), round(total or 0.0, 6)))

    # block_daily
    rows = conn.execute("""
        SELECT block, SUM(kwh)
        FROM meter_hourly
        WHERE substr(ts,1,10)=?
        GROUP BY block
    """, (date_str,)).fetchall()
    for block, total in rows:
        upsert(conn, """
            INSERT INTO block_daily(block, ts, kwh)
            VALUES (?, ?, ?)
            ON CONFLICT(block, ts) DO UPDATE SET kwh=excluded.kwh
        """, (block, midnight_ts(date_str), round(total or 0.0, 6)))

    # plant_daily
    rows = conn.execute("""
        SELECT AVG(rt), MAX(rt), SUM(rt), AVG(flow_ls), AVG(dt_C)
        FROM plant_hourly
        WHERE substr(ts,1,10)=?
    """, (date_str,)).fetchone()
    if rows and any(r is not None for r in rows):
        rt_avg, rt_peak, ton_hours, flow_ls_avg, dt_C_avg = rows
        upsert(conn, """
            INSERT INTO plant_daily(ts, ton_hours, rt_avg, rt_peak, flow_ls_avg, dt_C_avg)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(ts) DO UPDATE SET ton_hours=excluded.ton_hours, rt_avg=excluded.rt_avg,
                rt_peak=excluded.rt_peak, flow_ls_avg=excluded.flow_ls_avg, dt_C_avg=excluded.dt_C_avg
        """, (midnight_ts(date_str),
              round(ton_hours or 0.0, 6),
              round(rt_avg or 0.0, 6),
              round(rt_peak or 0.0, 6),
              round(flow_ls_avg or 0.0, 6),
              round(dt_C_avg or 0.0, 6)))

    # efficiency_daily: airside_kwh(total) and ton_hours(total) -> kW/RT (daily)
    rows = conn.execute("""
        SELECT (SELECT SUM(airside_kw) FROM efficiency_hourly WHERE substr(ts,1,10)=?),
               (SELECT SUM(rt)        FROM efficiency_hourly WHERE substr(ts,1,10)=?)
    """, (date_str, date_str)).fetchone()
    if rows:
        airside_kwh, ton_hours = rows
        kw_per_rt = (airside_kwh / ton_hours) if (airside_kwh and ton_hours and ton_hours > 0) else None
        upsert(conn, """
            INSERT INTO efficiency_daily(ts, airside_kwh, ton_hours, kw_per_rt)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ts) DO UPDATE SET airside_kwh=excluded.airside_kwh, ton_hours=excluded.ton_hours,
                kw_per_rt=excluded.kw_per_rt
        """, (midnight_ts(date_str),
              round(airside_kwh or 0.0, 6),
              round(ton_hours or 0.0, 6),
              None if kw_per_rt is None else round(kw_per_rt, 6)))

    conn.commit()


# --------------------- CORE -----------------------

def seed_if_needed(conn: sqlite3.Connection, readings: Dict[str, Dict[str, object]], ts_hour: str) -> bool:
    cnt = conn.execute("SELECT COUNT(*) FROM meter_last").fetchone()[0]
    if cnt == 0:
        log("INFO", f"Seeding meter_last for {len(readings)} meters…")
        for meter_key, r in readings.items():
            kwh = float(r["kwh"])
            set_last(conn, meter_key, kwh, ts_hour)
            log("DEBUG", f"[SEED] {meter_key}: kWh={kwh:.6f} @ {ts_hour}")
        conn.commit()
        log("INFO", "Seeding done. Will compute usage next hour.")
        return True
    return False


def compute_and_store(conn: sqlite3.Connection, readings: Dict[str, Dict[str, object]], ts_hour: str) -> None:
    device_totals: Dict[str, float] = {}
    block_totals: Dict[str, float] = {}

    for meter_key, r in readings.items():
        device_id = str(r["device_id"])
        block = str(r["block"])
        curr_kwh = float(r["kwh"])

        prev_kwh, prev_ts = get_last(conn, meter_key)
        if prev_kwh is None:
            set_last(conn, meter_key, curr_kwh, ts_hour)
            log("WARN", f"[{meter_key}] No previous kWh; seeding with curr={curr_kwh:.6f} @ {ts_hour}. Skipping hourly this time.")
            continue

        raw_delta = curr_kwh - prev_kwh
        hourly = raw_delta if raw_delta >= 0 else 0.0
        if raw_delta < 0:
            log("WARN", f"[{meter_key}] Counter reset/decrease: prev={prev_kwh:.6f} @ {prev_ts}, curr={curr_kwh:.6f} @ {ts_hour}, delta={raw_delta:.6f} -> using 0.000000")

        log("DEBUG", f"[{meter_key}] block={block} dev={device_id} prev={prev_kwh:.6f} @ {prev_ts}, curr={curr_kwh:.6f} @ {ts_hour} -> hourly_kWh={hourly:.6f}")

        upsert(conn, """
            INSERT INTO meter_hourly(meter_key, ts, kwh, device_id, block)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(meter_key, ts) DO UPDATE SET kwh=excluded.kwh, device_id=excluded.device_id, block=excluded.block
        """, (meter_key, ts_hour, round(hourly, 6), device_id, block))

        device_totals[device_id] = device_totals.get(device_id, 0.0) + hourly
        block_totals[block] = block_totals.get(block, 0.0) + hourly

        set_last(conn, meter_key, curr_kwh, ts_hour)

    for dev, total in device_totals.items():
        upsert(conn, """
            INSERT INTO device_hourly(device_id, ts, kwh)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id, ts) DO UPDATE SET kwh=excluded.kwh
        """, (dev, ts_hour, round(total, 6)))

    for blk, total in block_totals.items():
        upsert(conn, """
            INSERT INTO block_hourly(block, ts, kwh)
            VALUES (?, ?, ?)
            ON CONFLICT(block, ts) DO UPDATE SET kwh=excluded.kwh
        """, (blk, ts_hour, round(total, 6)))

    conn.commit()


def total_airside_for_hour(conn: sqlite3.Connection, ts_hour: str) -> float:
    """
    OLD logic: airside_kW = sum of device_hourly for your AHU DEVICE_IDS only.
    (Excel devices do not affect airside.)
    """
    if DEVICE_IDS:
        placeholders = ",".join("?" for _ in DEVICE_IDS)
        params = [ts_hour] + DEVICE_IDS
        row = conn.execute(
            f"SELECT SUM(kwh) FROM device_hourly WHERE ts=? AND device_id IN ({placeholders})",
            params
        ).fetchone()
    else:
        row = conn.execute("SELECT SUM(kwh) FROM device_hourly WHERE ts=?", (ts_hour,)).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def one_hour_cycle(conn: sqlite3.Connection) -> None:
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    ts_hour = now.strftime("%Y-%m-%dT%H:00:00")
    today = ts_hour[:10]

    # --- energy (Active Energy deltas) ---
    readings = fetch_all_current_readings(POLL_DEVICE_IDS or DEVICE_IDS)
    if seed_if_needed(conn, readings, ts_hour):
        return
    compute_and_store(conn, readings, ts_hour)

    # airside = AHU devices only (old logic)
    airside_kw = total_airside_for_hour(conn, ts_hour)

    # --- Plant RT from HDR on device 1210 ---
    hdr = fetch_hdr_snapshot(PLANT_DEVICE_ID)
    if hdr:
        flow_ls, chwr_C, chws_C = hdr
        rt, dt_C, plant_kw = compute_rt_from_hdr(flow_ls, chwr_C, chws_C)

        upsert(conn, """
            INSERT INTO plant_hourly(ts, rt, flow_ls, dt_C, plant_kw)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ts) DO UPDATE SET rt=excluded.rt, flow_ls=excluded.flow_ls, dt_C=excluded.dt_C, plant_kw=excluded.plant_kw
        """, (ts_hour, rt, round(flow_ls, 6), dt_C, plant_kw))

        kw_per_rt = (airside_kw / rt) if rt > 0 else None
        upsert(conn, """
            INSERT INTO efficiency_hourly(ts, airside_kw, rt, kw_per_rt)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ts) DO UPDATE SET airside_kw=excluded.airside_kw, rt=excluded.rt, kw_per_rt=excluded.kw_per_rt
        """, (ts_hour, round(airside_kw, 6), rt, None if kw_per_rt is None else round(kw_per_rt, 6)))

        kprt_str = f"{kw_per_rt:.3f}" if kw_per_rt is not None else "nan"
        log("INFO", f"[EFF] ts={ts_hour} airside_kW={airside_kw:.3f} RT={rt:.3f} -> kW/RT={kprt_str}")
        log("DEBUG", f"[PLANT] flow_ls={flow_ls:.3f} dt_C={dt_C:.3f} plant_kW={plant_kw:.3f}")
    else:
        log("WARN", f"[plant {PLANT_DEVICE_ID}] HDR snapshot unavailable; skipping plant/efficiency rows for {ts_hour}")

    # --- Daily aggregates ---
    refresh_daily(conn, today)
    if now.hour == 0:
        yday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        refresh_daily(conn, yday)


def seconds_until_next_hour() -> int:
    now = datetime.now()
    nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    secs = (nxt - now).total_seconds()
    return min(3600, max(1, int(math.ceil(secs))))


def handle_signal(signum, _frame):
    global _shutdown
    _shutdown = True
    log("WARN", f"Received signal {signum}; shutting down after current cycle…")

def extract_opc_active_energy(device_id: str) -> Dict[str, Dict[str, object]]:
    """
    Uses Excel mapping.
    For device_id == 'OPC', read OPC UA values instead of BACnet.
    """
    mapping_rows = MAPPING_BY_DEVICE.get(device_id, [])
    if not mapping_rows:
        print("[OPC] No mapping found for device 'OPC'")
        return {}

    opc_points = [row["opc_node"] for row in mapping_rows]
    values = opc_read_points(opc_points)

    result = {}
    for row in mapping_rows:
        node = row["opc_node"]
        if node not in values:
            continue

        meter_key = row["meter_key"]
        block = row["block"]
        kwh = float(values[node])

        result[meter_key] = {
            "device_id": device_id,
            "block": block,
            "kwh": kwh
        }

    return result



def main():
    # Load Excel mapping (if available); otherwise falls back to old logic
    load_mapping_from_excel()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    log("INFO", f"Energy + RT collector started. "
               f"Poll devices: {POLL_DEVICE_IDS or DEVICE_IDS}; "
               f"AIRSIDE (AHU) devices: {DEVICE_IDS}; "
               f"Plant device: {PLANT_DEVICE_ID}; DB: {DB_PATH}")
    if MAPPING_BY_DEVICE:
        log("INFO", f"Excel mapping active from '{MAPPING_XLSX}'.")

    try:
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    except Exception:
        pass  # Windows services / some envs

    while not _shutdown:
        try:
            one_hour_cycle(conn)
        except requests.HTTPError as e:
            log("ERROR", f"Wacnet HTTP error: {e}")
        except Exception as e:
            log("ERROR", f"Unexpected error: {e}")

        if _shutdown:
            break

        sleep_s = seconds_until_next_hour()
        log("INFO", f"Sleeping {sleep_s} seconds until next hour…")
        while sleep_s > 0 and not _shutdown:
            chunk = min(sleep_s, 30)
            time.sleep(chunk)
            sleep_s -= chunk

    conn.close()
    log("INFO", "Collector stopped.")


if __name__ == "__main__":
    main()
