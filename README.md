# SpaceX Launches — ETL & Analysis (SQLite + Python)

This project ingests data from the public SpaceX API, models it into a normalized SQLite database, and answers business-style questions with SQL and Python-generated charts.

**Dataset:** SpaceX API (v4/v5) — rockets, launches, payloads 
 Docs: https://github.com/r-spacex/SpaceX-API/blob/master/README.md
 
**Why this dataset?** I think space and rockets in general are neat so it was a dataset that I found personally interesting to work with and look at. It is also reasonably sized allowing for quick loads and transforms for the project


## Repository layout
```
load.py          # ETL: fetch -> transform -> load into SQLite (idempotent writes)
questions.py     # Runs SQL analyses and saves charts
run_project.py   # Orchestrates ETL + analysis with one command
schema.sql       # SQLite DDL (tables + indexes)
data/            # created on first run; holds spacex.db
README.md        # this file
```


## Quickstart
```bash
# 1) Create & activate a virtual environment
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows (PowerShell):
# .venv\Scripts\Activate.ps1

# 2) Install dependencies
pip install -r requirements.txt

# 3) Run the pipeline end-to-end (ETL → analysis)
python run_project.py
```

**Where to see results**
- **Console:** Each question prints a tabular result to stdout.  
- **Files:** Charts are saved in the repo root:
  - `payload_manufacturers_share.png`
  - `payload_customers_share.png`  
- **Database:** The SQLite file is written to `data/spacex.db`.

---

## How it works

### ETL (`load.py`)
1. **Fetch** JSON from SpaceX endpoints for **rockets**, **launches**, and **payloads**.  
2. **Transform** fields (normalize booleans; flatten/serialize arrays; split date precision fields).  
3. **Load** into SQLite using `INSERT OR REPLACE` for **idempotent** runs.  
Additional behavior:
- Ensures the `data/` directory exists.
- Applies `schema.sql` to create tables and indexes before inserts.
- Populates dimension tables (`rockets`, `payloads`) and fact tables (`launches`, `launch_cores`, optionally helper date tables).

### Analysis (`questions.py`)
- **Q1 (SQL): Failure rate by year** — completed launches only.
	Year-over-year reliability conveys maturity to non-technical stakeholders at a glance 
	Overall failure rate was 2.69% across all launches in DB with a 0% failure rate since 2018. 
- **Q2 (SQL): Average days between core reuses** — window functions over each core’s flight history. 
	Average days between re-use of cores shows 103.89 days. An average near four months shows steady fleet cadence
- **Q3 (SQL→Python): Top payload manufacturers**
	**spacex** — 105  
 	**thales alenia space** — 14  
	**ssl** — 13  
 	**boeing** — 10 
	
- **Q4 (SQL→Python): Top payload customers**
	**spacex** — 67  
	**nasa (crs)** — 25  
 	**nasa** — 10  
	**iridium communications** — 8  
 	**nasa (cctcap)** — 8 



