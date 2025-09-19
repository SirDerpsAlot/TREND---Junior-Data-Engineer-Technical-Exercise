
BEGIN;

-- Fact: Launches
CREATE TABLE launches (
  id               TEXT PRIMARY KEY,                          
  flight_number    INTEGER NOT NULL,
  name             TEXT    NOT NULL UNIQUE,
  date_utc         TEXT    NOT NULL,
  date_unix        INTEGER NOT NULL,
  date_local       TEXT    NOT NULL,
  date_precision   TEXT    NOT NULL CHECK (date_precision IN ('half','quarter','year','month','day','hour')),
  static_fire_date_utc  TEXT,
  static_fire_date_unix INTEGER,
  tbd              INTEGER CHECK (tbd IN (0,1) OR tbd IS NULL),
  net              INTEGER CHECK (net IN (0,1) OR net IS NULL),
  window           INTEGER,
  rocket           TEXT,     
  success          INTEGER CHECK (success IN (0,1) OR success IS NULL),   
  details          TEXT,
  fairings_reused          INTEGER CHECK (fairings_reused IN (0,1) OR fairings_reused IS NULL),
  fairings_recovery_attempt INTEGER CHECK (fairings_recovery_attempt IN (0,1) OR fairings_recovery_attempt IS NULL),
  fairings_recovered       INTEGER CHECK (fairings_recovered IN (0,1) OR fairings_recovered IS NULL),
  fairings_ships_json      TEXT,
  failures_json             TEXT,
  crew_json                 TEXT,
  ships_json                TEXT,
  capsules_json             TEXT,
  launchpad        TEXT,
  upcoming         INTEGER NOT NULL CHECK (upcoming IN (0,1)),
  auto_update      INTEGER CHECK (auto_update IN (0,1) OR auto_update IS NULL)
);
CREATE INDEX IF NOT EXISTS idx_launches_date_unix ON launches(date_unix);
CREATE INDEX IF NOT EXISTS idx_launches_success   ON launches(success);
CREATE INDEX IF NOT EXISTS idx_launches_rocket    ON launches(rocket);


-- Cores used per launch (1 launch : many cores)
CREATE TABLE IF NOT EXISTS launch_cores (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  launch_id        TEXT NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
  core_id          TEXT,                          
  flight           INTEGER,
  gridfins         INTEGER CHECK (gridfins IN (0,1)),
  legs             INTEGER CHECK (legs IN (0,1)),
  reused           INTEGER CHECK (reused IN (0,1)),
  landing_attempt  INTEGER CHECK (landing_attempt IN (0,1)),
  landing_success  INTEGER CHECK (landing_success IN (0,1)),
  landing_type     TEXT,
  landpad          TEXT
);
CREATE INDEX IF NOT EXISTS idx_lc_launch ON launch_cores(launch_id);
CREATE INDEX IF NOT EXISTS idx_lc_core   ON launch_cores(core_id);

-- Dimension: Rockets
CREATE TABLE IF NOT EXISTS rockets (
  id                TEXT PRIMARY KEY,                 
  name              TEXT NOT NULL UNIQUE,
  type              TEXT,
  active            INTEGER CHECK (active IN (0,1)),
  stages            INTEGER,
  boosters          INTEGER,
  cost_per_launch   INTEGER,
  success_rate_pct  INTEGER,
  first_flight      TEXT,                             
  country           TEXT,
  company           TEXT,
  height_meters     REAL,
  diameter_meters   REAL,
  mass_kg           REAL,
  description       TEXT
);
CREATE INDEX IF NOT EXISTS idx_rockets_active ON rockets(active);

-- Dimension: Payloads 
CREATE TABLE IF NOT EXISTS payloads (
  id                 TEXT PRIMARY KEY,
  name               TEXT,
  type               TEXT,
  reused             INTEGER CHECK (reused IN (0,1)),
  launch_id          TEXT REFERENCES launches(id) ON DELETE SET NULL,
  customers_json       TEXT,
  manufacturers_json   TEXT,
  nationalities_json   TEXT,
  norad_ids_json       TEXT,
  mass_kg            REAL,
  mass_lbs           REAL,
  orbit              TEXT,
  reference_system   TEXT,
  regime             TEXT,
  apoapsis_km        REAL,
  periapsis_km       REAL,
  inclination_deg    REAL,
  lifespan_years     REAL,
  dragon_json        TEXT
);
CREATE INDEX IF NOT EXISTS idx_payloads_launch ON payloads(launch_id);
CREATE INDEX IF NOT EXISTS idx_payloads_type   ON payloads(type);
CREATE INDEX IF NOT EXISTS idx_payloads_orbit  ON payloads(orbit);

COMMIT;
