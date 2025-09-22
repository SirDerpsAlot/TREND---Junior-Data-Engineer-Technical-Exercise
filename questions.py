import os, sqlite3
from pathlib import Path
from typing import List, Tuple
from matplotlib import pyplot as plt
DB_SUBDIR = "data"
DB_FILENAME = "spacex.db"


def repo_root() -> Path:
    """Returns the path to repository root (parent of this file)."""
    return Path(__file__).resolve().parent

def db_path() -> Path:
    """Ensures project data directory exists and returns full path to DB file."""
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

SQL_TOP_MANUFACTURERS = """
 WITH exploded AS (
  SELECT
    TRIM(LOWER(j.value)) AS manufacturer
  FROM payloads p
  JOIN json_each(p.manufacturers_json) AS j
  WHERE p.manufacturers_json IS NOT NULL
    AND json_valid(p.manufacturers_json)
    AND j.value IS NOT NULL
    AND TRIM(j.value) <> ''
)
SELECT manufacturer, COUNT(*) AS payload_count
FROM exploded
GROUP BY manufacturer
ORDER BY payload_count DESC;
"""

SQL_TOP_CUSTOMERS = """
WITH exploded AS (
  SELECT
    TRIM(LOWER(j.value)) AS customer
  FROM payloads p
  JOIN json_each(p.customers_json) AS j
  WHERE p.customers_json IS NOT NULL
    AND json_valid(p.customers_json)
    AND j.value IS NOT NULL
    AND TRIM(j.value) <> ''
)
SELECT customer, COUNT(*) AS payload_count
FROM exploded
GROUP BY customer
ORDER BY payload_count DESC;
"""
def pie_chart(labels, values, title: str, top_n: int = 8, outfile: Path | None = None):
  """Save a simple pie chart (top N plus 'other') for the provided labels/values to outfile."""

  labels = list(labels)
  values = list(values)
  if len(values) > top_n:
    labels_top = labels[:top_n] + ["other"]
    values_top = values[:top_n] + [sum(values[top_n:])]
  else:
    labels_top = labels
    values_top = values

  fig = plt.figure()
  plt.pie(values_top, labels=labels_top, autopct="%1.1f%%", startangle=90)
  plt.title(title)
  if outfile:
    plt.savefig(outfile, bbox_inches="tight")
  plt.close(fig)
  
  
def run_query(con: sqlite3.Connection, query: str) -> Tuple[List[str], List[Tuple]]:
    """Execute a SQL query and return (column_names, rows)."""
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    return headers, rows


def print_results(headers: List[str], rows: List[Tuple]):
    """Preinst tabular results to stdout."""
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
            # Q1) Failure rate by year
            print("\n[Q1] Failure rate by year")
            headers, rows = run_query(conn, SQL_FAILURE_RATE_BY_YEAR)
            print_results(headers, rows)

            # Q2) Average days between core reuses
            print("\n[Q2] Average days between core reuses")
            headers, rows = run_query(conn, SQL_AVG_DAYS_BETWEEN_CORE_REUSES)
            print_results(headers, rows)

            # Q3) Top payload manufacturers
            print("\n[Q3] Top payload manufacturers (by payload count)")
            headers, rows = run_query(conn, SQL_TOP_MANUFACTURERS)
            print_results(headers, rows[:25])

            labels = [r[0] for r in rows]
            values = [r[1] for r in rows]
            out_mfg = repo_root() / "payload_manufacturers_share.png"
            pie_chart(labels, values, title="Payload Manufacturers Share", top_n=8, outfile=out_mfg)
            print(f"Saved pie chart: {out_mfg}")

            # Q4) Top customers 
            print("\n[Q4] Top customers (by payload count)")
            headers, rows = run_query(conn, SQL_TOP_CUSTOMERS)
            print_results(headers, rows[:25])

            labels = [r[0] for r in rows]
            values = [r[1] for r in rows]
            out_cust = repo_root() / "payload_customers_share.png"
            pie_chart(labels, values, title="Payload Customers Share", top_n=8, outfile=out_cust)
            print(f"Saved pie chart: {out_cust}")
    finally:
        conn.close()
if __name__ == "__main__":
    main()