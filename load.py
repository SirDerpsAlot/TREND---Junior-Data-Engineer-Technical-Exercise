#!/usr/bin/env python3
from datetime import datetime
import json
import sqlite3
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# DB will be created at "<repo_root>/data/spacex.db"
DB_SUBDIR = "data"
DB_FILENAME = "spacex.db"
# =========================

ROCKETS_URL  = "https://api.spacexdata.com/v4/rockets"
LAUNCHES_URL = "https://api.spacexdata.com/v5/launches"
PAYLOADS_URL = "https://api.spacexdata.com/v4/payloads"

# -------- path helpers --------
def repo_root() -> Path:
    return Path(__file__).resolve().parent

def db_path() -> Path:
    p = repo_root() / DB_SUBDIR
    p.mkdir(parents=True, exist_ok=True)
    return p / DB_FILENAME

# Utility functions
def bool_to_int(val):
    if val is None:
        return None
    return 1 if bool(val) else 0

def dumps_array(arr):
    return json.dumps(arr or [])

def dumps_object(obj):
    return json.dumps(obj or {})

def fetch_data(url: str) -> List[Dict[str, Any]]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def init_schema(conn: sqlite3.Connection, schema_file: Path):
    conn.executescript(schema_file.read_text(encoding="utf-8"))


def split_date_parts(date_utc: Optional[str], precision: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    try:
       dt = datetime.fromisoformat(date_utc.replace("Z", "+00:00"))
    except Exception:
        return (None, None, None)
    
    precision = (precision or "").lower()
    year = dt.year
    month = None
    day = None
    
    if precision in ("month", "day", "hour"):
        month = dt.month
    if precision in ("day", "hour"):
        day = dt.day
    return (year, month, day)
    
# -------- inserts --------
def insert_rocket(cur: sqlite3.Cursor, r: Dict[str, Any]):
    height   = r.get("height")   or {}
    diameter = r.get("diameter") or {}
    mass     = r.get("mass")     or {}

    row = (
        r.get("id"),
        r.get("name"),
        r.get("type"),
        bool_to_int(r.get("active")),
        r.get("stages"),
        r.get("boosters"),
        r.get("cost_per_launch"),
        r.get("success_rate_pct"),
        r.get("first_flight"),
        r.get("country"),
        r.get("company"),
        height.get("meters"),
        diameter.get("meters"),
        mass.get("kg"),
        r.get("description"),
    )

    cur.execute("""
        INSERT OR REPLACE INTO rockets (
            id, name, type, active, stages, boosters, cost_per_launch,
            success_rate_pct, first_flight, country, company,
            height_meters, diameter_meters, mass_kg, description
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, row)

def insert_launch(cur: sqlite3.Cursor, L: Dict[str, Any]):
    fair = L.get("fairings") or {}
    date_key = insert_launch_date(cur, L)
    year, month, day = split_date_parts(L.get("date_utc"), L.get("date_precision"))
    row = (
        L.get("id"),
        L.get("flight_number"),
        L.get("name"),
        year, month, day,
        L.get("date_unix"),
        date_key,
        bool_to_int(L.get("tbd")),
        bool_to_int(L.get("net")),
        L.get("window"),
        L.get("rocket"),
        bool_to_int(L.get("success")),
        L.get("details"),
        bool_to_int(fair.get("reused")),
        bool_to_int(fair.get("recovery_attempt")),
        bool_to_int(fair.get("recovered")),
        dumps_array(fair.get("ships")),
        dumps_array(L.get("failures")),
        dumps_array(L.get("crew")),
        dumps_array(L.get("ships")),
        dumps_array(L.get("capsules")),
        L.get("launchpad"),
        bool_to_int(L.get("upcoming")),
        bool_to_int(L.get("auto_update", True)),
    )
    cur.execute("""
        INSERT OR REPLACE INTO launches (
          id, flight_number, name,
          year, month, day, date_unix, date_key,
          tbd, net, "window",
          rocket, success, details,
          fairings_reused, fairings_recovery_attempt, fairings_recovered,
          fairings_ships_json, failures_json, crew_json, ships_json, capsules_json,
          launchpad, upcoming, auto_update
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, row)

def insert_launch_date(cur: sqlite3.Cursor, L: Dict[str, Any]) -> str:
    launch_id = L.get("id")
    row = (
        launch_id,
        L.get("date_utc"),
        L.get("date_local"),
        L.get("date_precision"),
        L.get("static_fire_date_utc"),
        L.get("static_fire_date_unix"),
    )
    cur.execute("""
        INSERT OR REPLACE INTO launch_dates (
          launch_id, date_utc, date_local, date_precision,
          static_fire_date_utc, static_fire_date_unix
        ) VALUES (?,?,?,?,?,?)
    """, row)
    return launch_id

def insert_launch_cores(cur: sqlite3.Cursor, launch_id: str, L: Dict[str, Any]):
    cur.execute("DELETE FROM launch_cores WHERE launch_id = ?", (launch_id,))

    for c in L.get("cores") or []:
        c = c or {}
        row = (
            launch_id,
            c.get("core"),
            c.get("flight"),
            bool_to_int(c.get("gridfins")),
            bool_to_int(c.get("legs")),
            bool_to_int(c.get("reused")),
            bool_to_int(c.get("landing_attempt")),
            bool_to_int(c.get("landing_success")),
            c.get("landing_type"),
            c.get("landpad"),
        )

        cur.execute("""
            INSERT INTO launch_cores (
                launch_id, core_id, flight, gridfins, legs, reused,
                landing_attempt, landing_success, landing_type, landpad
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, row)

def insert_payload(cur: sqlite3.Cursor, p: Dict[str, Any]):
    payload_id = p.get("id")
    launch_id  = p.get("launch")

    # Skip if launch_id is NULL/empty
    if not launch_id:
        print(f"[SKIP] payload {payload_id} has NULL launch_id", flush=True)
        return

    # Ensure parent launch exists
    cur.execute("SELECT 1 FROM launches WHERE id = ? LIMIT 1", (launch_id,))
    if cur.fetchone() is None:
        print(f"[SKIP] payload {payload_id} references missing launch {launch_id}", flush=True)
        return

    row = (
        payload_id,
        p.get("name"),
        p.get("type"),
        bool_to_int(p.get("reused")),
        launch_id,
        dumps_array(p.get("customers")),
        dumps_array(p.get("manufacturers")),
        dumps_array(p.get("nationalities")),
        dumps_array(p.get("norad_ids")),
        p.get("mass_kg"),
        p.get("mass_lbs"),
        p.get("orbit"),
        p.get("reference_system"),
        p.get("regime"),
        p.get("apoapsis_km"),
        p.get("periapsis_km"),
        p.get("inclination_deg"),
        p.get("lifespan_years"),
        dumps_object(p.get("dragon")),
    )
    # Parent exists → insert/replace
    cur.execute("""
        INSERT OR REPLACE INTO payloads (
          id, name, type, reused, launch_id,
          customers_json, manufacturers_json, nationalities_json, norad_ids_json,
          mass_kg, mass_lbs, orbit, reference_system, regime,
          apoapsis_km, periapsis_km, inclination_deg, lifespan_years,
          dragon_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (row))


def main():
    db = db_path()
    schema = repo_root() / "schema.sql"

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")

    init_schema(conn, schema)

    # Fetch data
    rockets  = fetch_data(ROCKETS_URL)
    launches = fetch_data(LAUNCHES_URL)
    payloads = fetch_data(PAYLOADS_URL)

    with conn:
        cur = conn.cursor()

        # dimensions first: rockets
        for r in rockets:
            insert_rocket(cur, r)

        # 2) fact: launches (
        for L in launches:
            insert_launch(cur, L)
            insert_launch_cores(cur, L.get("id"), L)

        # 3) dimension: payloads (references launches)
        for p in payloads:
            insert_payload(cur, p)

    conn.close()
    print(f"Loaded rockets={len(rockets)}, launches={len(launches)}, payloads={len(payloads)} into {db}")

if __name__ == "__main__":
    main()
