import os, sqlite3
from pathlib import Path
from typing import List, Tuple, Sequence
DB_SUBDIR = "data"
DB_FILENAME = "spacex.db"

def repo_root() -> Path:
    return Path(__file__).resolve().parent

def db_path() -> Path:
    p = repo_root() / DB_SUBDIR
    p.mkdir(parents=True, exist_ok=True)
    return p / DB_FILENAME

SQL_FAILURE_RATE_BY_YEAR = """
SELECT
  year,
  SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END)               AS failures,
  SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END)               AS successes,
  COUNT(*)                                                   AS total,
  ROUND(100.0 * SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END)
             / COUNT(*), 2)                                  AS failure_rate_pct
FROM launches
WHERE upcoming = 0
  AND success IS NOT NULL
GROUP BY year
ORDER BY year;
"""

SQL_AVG_DAYS_BETWEEN_CORE_REUSES = """
WITH diffs AS (
  SELECT
    (l.date_unix - LAG(l.date_unix) OVER (
       PARTITION BY lc.core_id ORDER BY l.date_unix
    )) / 86400.0 AS days_between
  FROM launch_cores AS lc
  JOIN launches AS l
    ON l.id = lc.launch_id
)
SELECT
  ROUND(AVG(days_between), 2) AS average_days_between_flights
FROM diffs
WHERE days_between IS NOT NULL;
 """

def run_query(con: sqlite3.Connection, query: str) -> Tuple[List[str], List[Tuple]]:
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    return headers, rows


def print_results(headers: List[str], rows: List[Tuple]):
    print(" | ".join(headers))
    print("-" * 60)
    for row in rows:
        print(" | ".join(str(val) for val in row))
    
def main():
    if not os.path.exists(db_path()):
        raise SystemExit(f"Database not found at: {db_path()}. Check load.py for errors")
    
    conn = sqlite3.connect(db_path(), timeout=60)
    conn.execute("PRAGMA foreign_keys = ON")
    
    try:
        with conn:
            headers, rows = run_query(conn, SQL_AVG_DAYS_BETWEEN_CORE_REUSES)
            print_results(headers, rows)
    finally:
        conn.close()
        
if __name__ == "__main__":
    main()