# CDM Stats Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a data pipeline that ingests CDL match CSVs into SQLite, computes pick/avoidance/Elo metrics, and exports formatted Excel workbooks for coaching staff.

**Architecture:** CLI-driven pipeline. CSV → SQLite (with derived fields computed on ingest) → metrics computed at query time → Excel/chart output. All DB writes use transactions. No ORM — raw parameterized SQL throughout.

**Tech Stack:** Python >=3.12, uv, sqlite3, openpyxl, matplotlib, pytest

**Spec:** `docs/superpowers/specs/2026-03-14-cdm-stats-design.md`

---

## Chunk 1: Project Setup, DB Schema, and Seed Data

### Task 1: Project scaffolding and dependencies

**Files:**
- Modify: `pyproject.toml`
- Create: `src/cdm_stats/__init__.py`
- Create: `src/cdm_stats/db/__init__.py`
- Create: `src/cdm_stats/ingestion/__init__.py`
- Create: `src/cdm_stats/metrics/__init__.py`
- Create: `src/cdm_stats/export/__init__.py`
- Create: `src/cdm_stats/charts/__init__.py`
- Create: `.gitignore`

- [ ] **Step 1: Update pyproject.toml with dependencies and package config**

```toml
[project]
name = "cdm-stats"
version = "0.1.0"
description = "CDL match analytics pipeline for coaching staff"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "openpyxl>=3.1",
    "matplotlib>=3.8",
    "numpy>=1.26",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.backends"

[tool.hatch.build.targets.wheel]
packages = ["src/cdm_stats"]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.pytest.ini_options]
pythonpath = ["src"]
```

- [ ] **Step 2: Create directory structure with __init__.py files**

Create empty `__init__.py` files at:
- `src/cdm_stats/__init__.py`
- `src/cdm_stats/db/__init__.py`
- `src/cdm_stats/ingestion/__init__.py`
- `src/cdm_stats/metrics/__init__.py`
- `src/cdm_stats/export/__init__.py`
- `src/cdm_stats/charts/__init__.py`

- [ ] **Step 3: Create tests/conftest.py with shared fixtures**

```python
# tests/conftest.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    """Fresh in-memory DB with schema and seed data."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    """DB with one ingested match (ATL 3-1 LAT)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id
```

Note: Individual test files can import and use these fixtures directly (pytest auto-discovers `conftest.py`). Test files that need custom fixtures or CSV data can define their own alongside these shared ones.

- [ ] **Step 4: Create .gitignore**

```gitignore
__pycache__/
*.pyc
.venv/
data/cdl.db
output/
*.egg-info/
dist/
```

- [ ] **Step 5: Install dependencies**

Run: `uv sync --all-extras`
Expected: Dependencies installed, `.venv` created.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .gitignore .python-version src/ tests/conftest.py
git commit -m "chore: scaffold project structure and dependencies"
```

---

### Task 2: Database schema

**Files:**
- Create: `src/cdm_stats/db/schema.py`
- Create: `tests/test_schema.py`

- [ ] **Step 1: Write the failing test for schema creation**

```python
# tests/test_schema.py
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_create_tables_creates_all_tables(db):
    create_tables(db)
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]


def test_create_tables_is_idempotent(db):
    create_tables(db)
    create_tables(db)  # should not raise
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cdm_stats'`

- [ ] **Step 3: Write schema.py**

```python
# src/cdm_stats/db/schema.py
import sqlite3

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS teams (
        team_id      INTEGER PRIMARY KEY,
        team_name    TEXT NOT NULL,
        abbreviation TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS maps (
        map_id   INTEGER PRIMARY KEY,
        map_name TEXT NOT NULL,
        mode     TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS matches (
        match_id            INTEGER PRIMARY KEY,
        match_date          DATE NOT NULL,
        team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
        team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
        two_v_two_winner_id INTEGER NOT NULL REFERENCES teams(team_id),
        series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
        CHECK(team1_id != team2_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS map_results (
        result_id              INTEGER PRIMARY KEY,
        match_id               INTEGER NOT NULL REFERENCES matches(match_id),
        slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 5),
        map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
        picked_by_team_id      INTEGER REFERENCES teams(team_id),
        winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
        picking_team_score     INTEGER NOT NULL,
        non_picking_team_score INTEGER NOT NULL,
        team1_score_before     INTEGER NOT NULL,
        team2_score_before     INTEGER NOT NULL,
        pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                   'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss'
                               )),
        UNIQUE(match_id, slot)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_elo (
        elo_id     INTEGER PRIMARY KEY,
        team_id    INTEGER NOT NULL REFERENCES teams(team_id),
        match_id   INTEGER NOT NULL REFERENCES matches(match_id),
        elo_after  REAL NOT NULL,
        match_date DATE NOT NULL,
        UNIQUE(team_id, match_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_map_notes (
        note_id    INTEGER PRIMARY KEY,
        team_id    INTEGER NOT NULL REFERENCES teams(team_id),
        map_id     INTEGER NOT NULL REFERENCES maps(map_id),
        note       TEXT NOT NULL,
        created_at DATE NOT NULL
    )
    """,
]


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    for ddl in TABLES:
        conn.execute(ddl)
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_schema.py -v`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/schema.py tests/test_schema.py
git commit -m "feat: add database schema creation"
```

---

### Task 3: Seed data (teams and maps)

**Files:**
- Create: `src/cdm_stats/ingestion/seed.py`
- Create: `tests/test_seed.py`

- [ ] **Step 1: Write the failing test for seed data**

```python
# tests/test_seed.py
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps, TEAMS, MAPS


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


def test_seed_teams_inserts_14_teams(db):
    seed_teams(db)
    cursor = db.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 14


def test_seed_teams_abbreviations_are_unique(db):
    seed_teams(db)
    cursor = db.execute("SELECT abbreviation FROM teams ORDER BY abbreviation")
    abbrs = [row[0] for row in cursor.fetchall()]
    assert len(abbrs) == len(set(abbrs))


def test_seed_maps_inserts_13_maps(db):
    seed_maps(db)
    cursor = db.execute("SELECT COUNT(*) FROM maps")
    assert cursor.fetchone()[0] == 13


def test_seed_maps_correct_mode_counts(db):
    seed_maps(db)
    cursor = db.execute("SELECT mode, COUNT(*) FROM maps GROUP BY mode ORDER BY mode")
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    assert counts == {"Control": 3, "HP": 5, "SnD": 5}


def test_seed_is_idempotent(db):
    seed_teams(db)
    seed_teams(db)  # should not raise or duplicate
    cursor = db.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 14
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_seed.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write seed.py with placeholder team/map names**

Note: Use real CDL team names and abbreviations. Map names are placeholders — the user will confirm or update these. The seed function uses `INSERT OR IGNORE` for idempotency.

```python
# src/cdm_stats/ingestion/seed.py
import sqlite3

# 14 CDL teams for the current season
TEAMS = [
    ("Atlanta FaZe", "ATL"),
    ("Boston Breach", "BOS"),
    ("Carolina Royal Ravens", "CAR"),
    ("Las Vegas Legion", "LV"),
    ("Los Angeles Guerrillas", "LAG"),
    ("Los Angeles Thieves", "LAT"),
    ("Miami Heretics", "MIA"),
    ("Minnesota ROKKR", "MIN"),
    ("New York Subliners", "NYSL"),
    ("OpTic Texas", "OPT"),
    ("Seattle Surge", "SEA"),
    ("Toronto Ultra", "TOR"),
    ("Cloud9 New York", "C9"),
    ("Tampa Bay Mutineers", "TB"),
]

# 13 maps: 5 SnD, 5 HP, 3 Control
# TODO: Replace with actual season map pool once confirmed
MAPS = [
    ("Invasion", "SnD"),
    ("Karachi", "SnD"),
    ("Rio", "SnD"),
    ("Skidrow", "SnD"),
    ("Terminal", "SnD"),
    ("Highrise", "HP"),
    ("Invasion", "HP"),
    ("Karachi", "HP"),
    ("Rio", "HP"),
    ("Sub Base", "HP"),
    ("Highrise", "Control"),
    ("Invasion", "Control"),
    ("Karachi", "Control"),
]


def seed_teams(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO teams (team_name, abbreviation) VALUES (?, ?)",
        TEAMS,
    )
    conn.commit()


def seed_maps(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO maps (map_name, mode) VALUES (?, ?)",
        MAPS,
    )
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_seed.py -v`
Expected: 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/seed.py tests/test_seed.py
git commit -m "feat: add team and map seed data"
```

---

### Task 4: SQL query functions (foundation)

**Files:**
- Create: `src/cdm_stats/db/queries.py`
- Create: `tests/test_queries.py`

- [ ] **Step 1: Write the failing test for core query helpers**

```python
# tests/test_queries.py
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


def test_get_team_id_by_abbr(db):
    team_id = get_team_id_by_abbr(db, "ATL")
    assert team_id is not None
    assert isinstance(team_id, int)


def test_get_team_id_by_abbr_invalid(db):
    assert get_team_id_by_abbr(db, "INVALID") is None


def test_get_map_id(db):
    map_id = get_map_id(db, "Terminal", "SnD")
    assert map_id is not None


def test_get_map_id_wrong_mode(db):
    assert get_map_id(db, "Terminal", "HP") is None


def test_get_mode_for_slot():
    assert get_mode_for_slot(1) == "SnD"
    assert get_mode_for_slot(2) == "HP"
    assert get_mode_for_slot(3) == "Control"
    assert get_mode_for_slot(4) == "SnD"
    assert get_mode_for_slot(5) == "HP"


def test_insert_match(db):
    atl = get_team_id_by_abbr(db, "ATL")
    lat = get_team_id_by_abbr(db, "LAT")
    match_id = insert_match(db, "2026-01-15", atl, lat, atl, atl)
    assert match_id is not None
    row = db.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
    assert row is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_queries.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write queries.py with core functions**

```python
# src/cdm_stats/db/queries.py
import sqlite3

SLOT_MODES = {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"}


def get_mode_for_slot(slot: int) -> str:
    return SLOT_MODES[slot]


def get_team_id_by_abbr(conn: sqlite3.Connection, abbr: str) -> int | None:
    row = conn.execute(
        "SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)
    ).fetchone()
    return row[0] if row else None


def get_map_id(conn: sqlite3.Connection, map_name: str, mode: str) -> int | None:
    row = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = ? AND mode = ?", (map_name, mode)
    ).fetchone()
    return row[0] if row else None


def insert_match(
    conn: sqlite3.Connection,
    match_date: str,
    team1_id: int,
    team2_id: int,
    two_v_two_winner_id: int,
    series_winner_id: int,
) -> int:
    cursor = conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id)
           VALUES (?, ?, ?, ?, ?)""",
        (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id),
    )
    return cursor.lastrowid


def insert_map_result(
    conn: sqlite3.Connection,
    match_id: int,
    slot: int,
    map_id: int,
    picked_by_team_id: int | None,
    winner_team_id: int,
    picking_team_score: int,
    non_picking_team_score: int,
    team1_score_before: int,
    team2_score_before: int,
    pick_context: str,
) -> int:
    cursor = conn.execute(
        """INSERT INTO map_results
           (match_id, slot, map_id, picked_by_team_id, winner_team_id,
            picking_team_score, non_picking_team_score,
            team1_score_before, team2_score_before, pick_context)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, slot, map_id, picked_by_team_id, winner_team_id,
         picking_team_score, non_picking_team_score,
         team1_score_before, team2_score_before, pick_context),
    )
    return cursor.lastrowid
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_queries.py -v`
Expected: 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/queries.py tests/test_queries.py
git commit -m "feat: add core SQL query functions"
```

---

## Chunk 2: CSV Ingestion and Derivation Pipeline

### Task 5: Pick context derivation

**Files:**
- Create: `src/cdm_stats/ingestion/csv_loader.py` (initial — derivation helpers only)
- Create: `tests/test_pick_context.py`

- [ ] **Step 1: Write the failing tests for pick_context derivation**

```python
# tests/test_pick_context.py
from cdm_stats.ingestion.csv_loader import derive_pick_context


def test_slot_5_is_coin_toss():
    assert derive_pick_context(slot=5, picker_score=1, opponent_score=2) == "Coin-Toss"


def test_slot_1_is_opener():
    assert derive_pick_context(slot=1, picker_score=0, opponent_score=0) == "Opener"


def test_must_win_down_0_2():
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=2) == "Must-Win"


def test_must_win_down_1_2():
    assert derive_pick_context(slot=4, picker_score=1, opponent_score=2) == "Must-Win"


def test_close_out_up_2_0():
    assert derive_pick_context(slot=3, picker_score=2, opponent_score=0) == "Close-Out"


def test_close_out_up_2_1():
    assert derive_pick_context(slot=4, picker_score=2, opponent_score=1) == "Close-Out"


def test_neutral_1_0():
    assert derive_pick_context(slot=2, picker_score=1, opponent_score=0) == "Neutral"


def test_neutral_1_1():
    assert derive_pick_context(slot=3, picker_score=1, opponent_score=1) == "Neutral"


def test_neutral_0_1():
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=1) == "Neutral"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_pick_context.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write derive_pick_context in csv_loader.py**

```python
# src/cdm_stats/ingestion/csv_loader.py


def derive_pick_context(slot: int, picker_score: int, opponent_score: int) -> str:
    if slot == 5:
        return "Coin-Toss"
    if slot == 1:
        return "Opener"
    if opponent_score == 2 and picker_score < 2:
        return "Must-Win"
    if picker_score == 2 and opponent_score < 2:
        return "Close-Out"
    return "Neutral"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_pick_context.py -v`
Expected: 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/csv_loader.py tests/test_pick_context.py
git commit -m "feat: add pick_context derivation logic"
```

---

### Task 6: Full CSV ingestion pipeline

**Files:**
- Modify: `src/cdm_stats/ingestion/csv_loader.py`
- Create: `tests/test_csv_loader.py`

- [ ] **Step 1: Write the failing tests for CSV loading**

```python
# tests/test_csv_loader.py
import sqlite3
import csv
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


FOUR_MAP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""

SWEEP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-14,MIN,TOR,MIN,1,Terminal,MIN,6,2
2026-01-14,MIN,TOR,MIN,2,Highrise,MIN,250,180
2026-01-14,MIN,TOR,MIN,3,Karachi,MIN,3,0"""

FIVE_MAP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-16,BOS,SEA,BOS,1,Invasion,BOS,6,4
2026-01-16,BOS,SEA,BOS,2,Karachi,SEA,250,200
2026-01-16,BOS,SEA,BOS,3,Highrise,BOS,3,2
2026-01-16,BOS,SEA,BOS,4,Skidrow,SEA,6,3
2026-01-16,BOS,SEA,BOS,5,Invasion,BOS,250,230"""

INVALID_TEAM_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-17,FAKE,LAT,FAKE,1,Terminal,FAKE,6,3
2026-01-17,FAKE,LAT,FAKE,2,Highrise,LAT,250,220
2026-01-17,FAKE,LAT,FAKE,3,Karachi,FAKE,3,1"""

INVALID_NO_WINNER_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-17,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-17,ATL,LAT,ATL,2,Highrise,LAT,250,220"""


def test_ingest_sweep_3_0_creates_3_map_results(db):
    """A 3-0 sweep should produce exactly 3 map_results rows."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 3


def test_ingest_sweep_3_0_winner_has_one_pick(db):
    """In a 3-0 sweep, the sweeping team (2v2 winner) has exactly 1 pick (slot 1)."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    min_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'MIN'").fetchone()[0]
    picks = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id = ?", (min_id,)
    ).fetchone()[0]
    assert picks == 1


def test_ingest_sweep_3_0_swept_team_has_two_picks(db):
    """In a 3-0 where MIN won 2v2 and all 3 maps, TOR (loser of each map) picks slots 2 and 3."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    tor_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'TOR'").fetchone()[0]
    picks = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id = ?", (tor_id,)
    ).fetchone()[0]
    assert picks == 2


def test_ingest_sweep_3_0_series_winner_correct(db):
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    min_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'MIN'").fetchone()[0]
    winner = db.execute("SELECT series_winner_id FROM matches").fetchone()[0]
    assert winner == min_id


def test_ingest_invalid_team_returns_error(db):
    results = ingest_csv(db, io.StringIO(INVALID_TEAM_CSV))
    assert results[0]["status"] == "error"
    assert any("FAKE" in e for e in results[0]["errors"])


def test_ingest_no_winner_returns_error(db):
    """Only 2 maps played, no team reaches 3 wins."""
    results = ingest_csv(db, io.StringIO(INVALID_NO_WINNER_CSV))
    assert results[0]["status"] == "error"
    assert any("3 wins" in e for e in results[0]["errors"])


def test_ingest_sweep_creates_match(db):
    results = ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    assert len(results) == 1
    match = db.execute("SELECT * FROM matches").fetchone()
    assert match is not None


def test_ingest_sweep_creates_4_map_results(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 4


def test_ingest_sweep_series_winner_is_correct(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    match = db.execute("SELECT series_winner_id FROM matches").fetchone()
    atl_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'ATL'"
    ).fetchone()[0]
    assert match[0] == atl_id


def test_ingest_sweep_slot1_pick_context_is_opener(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    ctx = db.execute(
        "SELECT pick_context FROM map_results WHERE slot = 1"
    ).fetchone()[0]
    assert ctx == "Opener"


def test_ingest_sweep_picked_by_slot1_is_2v2_winner(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    atl_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'ATL'"
    ).fetchone()[0]
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 1"
    ).fetchone()[0]
    assert picker == atl_id


def test_ingest_sweep_slot2_picked_by_loser_of_slot1(db):
    """ATL won slot 1, so LAT (loser) picks slot 2."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    lat_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'LAT'"
    ).fetchone()[0]
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 2"
    ).fetchone()[0]
    assert picker == lat_id


def test_ingest_sweep_series_scores_accumulate(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    rows = db.execute(
        "SELECT slot, team1_score_before, team2_score_before FROM map_results ORDER BY slot"
    ).fetchall()
    # Slot 1: 0-0, Slot 2: 1-0 (ATL won s1), Slot 3: 1-1 (LAT won s2), Slot 4: 2-1 (ATL won s3)
    assert rows[0][1:] == (0, 0)  # slot 1
    assert rows[1][1:] == (1, 0)  # slot 2
    assert rows[2][1:] == (1, 1)  # slot 3
    assert rows[3][1:] == (2, 1)  # slot 4


def test_ingest_five_map_slot5_picked_by_is_null(db):
    ingest_csv(db, io.StringIO(FIVE_MAP_CSV))
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 5"
    ).fetchone()[0]
    assert picker is None


def test_ingest_five_map_slot5_is_coin_toss(db):
    ingest_csv(db, io.StringIO(FIVE_MAP_CSV))
    ctx = db.execute(
        "SELECT pick_context FROM map_results WHERE slot = 5"
    ).fetchone()[0]
    assert ctx == "Coin-Toss"


def test_ingest_duplicate_match_is_rejected(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    results = ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    assert len(results) == 1
    assert results[0]["status"] == "skipped"
    count = db.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    assert count == 1


def test_ingest_picking_team_score_when_picker_wins(db):
    """Slot 1: ATL picked, ATL won 6-3. picking_team_score=6, non=3."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (6, 3)


def test_ingest_picking_team_score_when_picker_loses(db):
    """Slot 2: LAT picked, LAT won 250-220. picking_team_score=250, non=220."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 2"
    ).fetchone()
    assert row == (250, 220)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_csv_loader.py -v`
Expected: FAIL — `ImportError: cannot import name 'ingest_csv'`

- [ ] **Step 3: Write the ingest_csv function**

Add to `src/cdm_stats/ingestion/csv_loader.py`:

```python
import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
)


def derive_pick_context(slot: int, picker_score: int, opponent_score: int) -> str:
    if slot == 5:
        return "Coin-Toss"
    if slot == 1:
        return "Opener"
    if opponent_score == 2 and picker_score < 2:
        return "Must-Win"
    if picker_score == 2 and opponent_score < 2:
        return "Close-Out"
    return "Neutral"


def _group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["team1"], row["team2"])
        matches.setdefault(key, []).append(row)
    return matches


def _validate_match(
    conn: sqlite3.Connection, key: tuple, rows: list[dict]
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr = key

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    two_v_two = rows[0]["two_v_two_winner"]
    if two_v_two not in (team1_abbr, team2_abbr):
        errors.append(f"2v2 winner '{two_v_two}' is not one of the teams")

    slots = [int(r["slot"]) for r in rows]
    expected_slots = list(range(1, len(rows) + 1))
    if slots != expected_slots:
        errors.append(f"Slots are not sequential: {slots}")

    for row in rows:
        slot = int(row["slot"])
        mode = get_mode_for_slot(slot)
        if get_map_id(conn, row["map_name"], mode) is None:
            errors.append(f"Unknown map '{row['map_name']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    # Check exactly one team reaches 3 wins
    t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
    t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
    if max(t1_wins, t2_wins) != 3:
        errors.append(f"No team reached 3 wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}")

    return errors


def _is_duplicate_match(conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int) -> bool:
    row = conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return row is not None


def ingest_csv(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    grouped = _group_rows_by_match(reader)
    results = []

    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate_match(conn, date, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_match(conn, key, rows)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        two_v_two_id = get_team_id_by_abbr(conn, rows[0]["two_v_two_winner"])

        # Walk slots to derive fields
        t1_series = 0
        t2_series = 0
        prev_loser_id = None
        map_result_data = []

        for row in sorted(rows, key=lambda r: int(r["slot"])):
            slot = int(row["slot"])
            mode = get_mode_for_slot(slot)
            map_id = get_map_id(conn, row["map_name"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            winner_score = int(row["winner_score"])
            loser_score = int(row["loser_score"])

            # Derive picker
            if slot == 1:
                picker_id = two_v_two_id
            elif slot == 5:
                picker_id = None
            else:
                picker_id = prev_loser_id

            # Derive scores relative to picker
            if picker_id is None:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            elif picker_id == winner_id:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            else:
                picking_team_score = loser_score
                non_picking_team_score = winner_score

            # Derive pick context
            if picker_id is None:
                pick_context = derive_pick_context(slot, 0, 0)
            else:
                if picker_id == team1_id:
                    pick_context = derive_pick_context(slot, t1_series, t2_series)
                else:
                    pick_context = derive_pick_context(slot, t2_series, t1_series)

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context,
            ))

            # Update running scores and prev loser
            if winner_id == team1_id:
                t1_series += 1
                prev_loser_id = team2_id
            else:
                t2_series += 1
                prev_loser_id = team1_id

        # Derive series winner
        series_winner_id = team1_id if t1_series == 3 else team2_id

        # Atomic insert
        try:
            match_id = insert_match(conn, date, team1_id, team2_id, two_v_two_id, series_winner_id)
            for data in map_result_data:
                insert_map_result(conn, match_id, *data)
            conn.commit()
            results.append({"match": key, "status": "ok", "match_id": match_id})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_csv_loader.py -v`
Expected: 19 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/csv_loader.py tests/test_csv_loader.py
git commit -m "feat: add CSV ingestion with derivation pipeline"
```

---

## Chunk 3: Metrics (Elo, Avoidance, Margin)

### Task 7: Elo calculation

**Files:**
- Create: `src/cdm_stats/metrics/elo.py`
- Create: `tests/test_elo.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_elo.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo, get_elo_history

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id


def test_update_elo_inserts_two_rows(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 2


def test_elo_winner_goes_up_loser_goes_down(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    atl_elo = get_current_elo(db, atl_id)
    lat_elo = get_current_elo(db, lat_id)
    assert atl_elo > 1000  # winner
    assert lat_elo < 1000  # loser


def test_elo_changes_sum_to_zero(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    atl_elo = get_current_elo(db, atl_id)
    lat_elo = get_current_elo(db, lat_id)
    assert abs((atl_elo - 1000) + (lat_elo - 1000)) < 0.001


def test_get_current_elo_no_matches(db):
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    assert get_current_elo(db, atl_id) == 1000.0


def test_get_elo_history(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    history = get_elo_history(db, atl_id)
    assert len(history) == 1
    assert history[0]["elo_after"] > 1000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_elo.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write elo.py**

```python
# src/cdm_stats/metrics/elo.py
import sqlite3

K_FACTOR = 32
SEED_ELO = 1000.0
LOW_CONFIDENCE_THRESHOLD = 7


def get_current_elo(conn: sqlite3.Connection, team_id: int) -> float:
    row = conn.execute(
        "SELECT elo_after FROM team_elo WHERE team_id = ? ORDER BY match_date DESC, elo_id DESC LIMIT 1",
        (team_id,),
    ).fetchone()
    return row[0] if row else SEED_ELO


def get_elo_history(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT elo_after, match_date, match_id
           FROM team_elo WHERE team_id = ?
           ORDER BY match_date, elo_id""",
        (team_id,),
    ).fetchall()
    return [{"elo_after": r[0], "match_date": r[1], "match_id": r[2]} for r in rows]


def is_low_confidence(conn: sqlite3.Connection, team_id: int) -> bool:
    count = conn.execute(
        "SELECT COUNT(*) FROM team_elo WHERE team_id = ?", (team_id,)
    ).fetchone()[0]
    return count < LOW_CONFIDENCE_THRESHOLD


def update_elo(conn: sqlite3.Connection, match_id: int) -> None:
    match = conn.execute(
        "SELECT team1_id, team2_id, series_winner_id, match_date FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    team1_id, team2_id, winner_id, match_date = match

    elo1 = get_current_elo(conn, team1_id)
    elo2 = get_current_elo(conn, team2_id)

    expected1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))
    expected2 = 1 - expected1

    result1 = 1.0 if winner_id == team1_id else 0.0
    result2 = 1.0 - result1

    new_elo1 = elo1 + K_FACTOR * (result1 - expected1)
    new_elo2 = elo2 + K_FACTOR * (result2 - expected2)

    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team1_id, match_id, new_elo1, match_date),
    )
    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team2_id, match_id, new_elo2, match_date),
    )
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_elo.py -v`
Expected: 5 tests PASS

- [ ] **Step 5: Integrate Elo into ingestion pipeline**

Now that `elo.py` exists, add the Elo update call to `ingest_csv`. Modify `src/cdm_stats/ingestion/csv_loader.py` — add this after `conn.commit()` inside the try block:

```python
            from cdm_stats.metrics.elo import update_elo
            update_elo(conn, match_id)
```

The import is inside the function body to keep the module-level import clean (elo is a downstream consumer of ingestion data, not a core dependency).

- [ ] **Step 6: Verify existing ingestion tests still pass with Elo integrated**

Run: `uv run pytest tests/test_csv_loader.py tests/test_elo.py -v`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/cdm_stats/metrics/elo.py src/cdm_stats/ingestion/csv_loader.py tests/test_elo.py
git commit -m "feat: add Elo calculation and integrate into ingestion"
```

---

### Task 8: Avoidance and target index metrics

**Files:**
- Create: `src/cdm_stats/metrics/avoidance.py`
- Create: `tests/test_avoidance.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_avoidance.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.avoidance import (
    pick_win_loss,
    defend_win_loss,
    avoidance_index,
    target_index,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# ATL picks Terminal (slot 1, SnD), wins 6-3
# LAT picks Highrise (slot 2, HP), LAT wins 250-220
# ATL picks Karachi Control (slot 3), ATL wins 3-1
# LAT picks Karachi SnD (slot 4), ATL wins 6-2
MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


def _get_ids(db):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    terminal = db.execute("SELECT map_id FROM maps WHERE map_name = 'Terminal' AND mode = 'SnD'").fetchone()[0]
    karachi_snd = db.execute("SELECT map_id FROM maps WHERE map_name = 'Karachi' AND mode = 'SnD'").fetchone()[0]
    highrise_hp = db.execute("SELECT map_id FROM maps WHERE map_name = 'Highrise' AND mode = 'HP'").fetchone()[0]
    return atl, lat, terminal, karachi_snd, highrise_hp


def test_pick_win_loss_atl_terminal(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, terminal, _, _ = _get_ids(db)
    result = pick_win_loss(db, atl, terminal)
    assert result == {"wins": 1, "losses": 0}


def test_defend_win_loss_lat_terminal(db):
    """LAT didn't pick Terminal, ATL did. LAT's defend record on Terminal."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    _, lat, terminal, _, _ = _get_ids(db)
    result = defend_win_loss(db, lat, terminal)
    assert result == {"wins": 0, "losses": 1}


def test_pick_win_loss_no_data(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, _, highrise_hp = _get_ids(db)
    result = pick_win_loss(db, atl, highrise_hp)
    assert result == {"wins": 0, "losses": 0}


def test_avoidance_index_basic(db):
    """ATL had 1 SnD pick opportunity (slot 1) and picked Terminal, not Karachi SnD.
    So ATL's avoidance of Karachi SnD = 1/1 = 100%."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, karachi_snd, _ = _get_ids(db)
    result = avoidance_index(db, atl, karachi_snd)
    assert result["ratio"] == 1.0
    assert result["opportunities"] == 1


def test_target_index_basic(db):
    """LAT is the opponent. LAT had 1 SnD pick (slot 4) against ATL and chose Karachi SnD.
    So target index for ATL on Karachi SnD = 0/1 = 0% (opponents DO pick it against ATL)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, karachi_snd, _ = _get_ids(db)
    result = target_index(db, atl, karachi_snd)
    assert result["ratio"] == 0.0
    assert result["opportunities"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_avoidance.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write avoidance.py**

```python
# src/cdm_stats/metrics/avoidance.py
import sqlite3
from cdm_stats.db.queries import get_mode_for_slot, SLOT_MODES


def pick_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND slot != 5""",
        (team_id, team_id, team_id, map_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def defend_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE picked_by_team_id IS NOT NULL
             AND picked_by_team_id != ?
             AND map_id = ?
             AND slot != 5
             AND (m.team1_id = ? OR m.team2_id = ?)""",
        (team_id, team_id, team_id, map_id, team_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def _get_pick_opportunities(conn: sqlite3.Connection, team_id: int, mode: str) -> list[dict]:
    """Get all slots where team_id had pick priority for the given mode.
    Walk map_results per match in slot order to find actual pick opportunities."""
    valid_slots = [s for s, m in SLOT_MODES.items() if m == mode and s != 5]

    rows = conn.execute(
        """SELECT mr.match_id, mr.slot, mr.picked_by_team_id, mr.map_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE (m.team1_id = ? OR m.team2_id = ?)
             AND mr.slot IN ({})
             AND mr.slot != 5
           ORDER BY mr.match_id, mr.slot""".format(",".join("?" * len(valid_slots))),
        (team_id, team_id, *valid_slots),
    ).fetchall()

    opportunities = []
    for row in rows:
        match_id, slot, picker_id, map_id = row
        if picker_id == team_id:
            opportunities.append({"match_id": match_id, "slot": slot, "map_id": map_id})
    return opportunities


def avoidance_index(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    """How often team avoids this map when they have pick priority on its mode."""
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]
    opportunities = _get_pick_opportunities(conn, team_id, mode)
    if not opportunities:
        return {"ratio": 0.0, "opportunities": 0}

    avoided = sum(1 for opp in opportunities if opp["map_id"] != map_id)
    return {"ratio": avoided / len(opportunities), "opportunities": len(opportunities)}


def target_index(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    """How often opponents avoid this map when they have pick priority against this team.
    A HIGH value means opponents consistently choose OTHER maps (they don't want to give
    this team this map). A LOW value means opponents are happy to play this map against
    this team. Compare with avoidance_index for full picture."""
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]
    valid_slots = [s for s, m in SLOT_MODES.items() if m == mode and s != 5]

    rows = conn.execute(
        """SELECT mr.match_id, mr.slot, mr.picked_by_team_id, mr.map_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE (m.team1_id = ? OR m.team2_id = ?)
             AND mr.picked_by_team_id IS NOT NULL
             AND mr.picked_by_team_id != ?
             AND mr.slot IN ({})
             AND mr.slot != 5
           ORDER BY mr.match_id, mr.slot""".format(",".join("?" * len(valid_slots))),
        (team_id, team_id, team_id, *valid_slots),
    ).fetchall()

    if not rows:
        return {"ratio": 0.0, "opportunities": 0}

    avoided = sum(1 for r in rows if r[3] != map_id)
    return {"ratio": avoided / len(rows), "opportunities": len(rows)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_avoidance.py -v`
Expected: 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/avoidance.py tests/test_avoidance.py
git commit -m "feat: add avoidance and target index metrics"
```

---

### Task 9: Score margin and dominance flags

**Files:**
- Create: `src/cdm_stats/metrics/margin.py`
- Create: `tests/test_margin.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_margin.py
from cdm_stats.metrics.margin import dominance_flag, score_margins
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


def test_snd_dominant():
    assert dominance_flag("SnD", 6, 2) == "Dominant"
    assert dominance_flag("SnD", 6, 3) == "Dominant"


def test_snd_contested():
    assert dominance_flag("SnD", 6, 5) == "Contested"


def test_snd_normal():
    assert dominance_flag("SnD", 6, 4) is None


def test_hp_dominant():
    assert dominance_flag("HP", 250, 170) == "Dominant"
    assert dominance_flag("HP", 250, 180) == "Dominant"


def test_hp_contested():
    assert dominance_flag("HP", 250, 230) == "Contested"


def test_hp_normal():
    assert dominance_flag("HP", 250, 200) is None


def test_control_dominant():
    assert dominance_flag("Control", 3, 1) == "Dominant"
    assert dominance_flag("Control", 3, 0) == "Dominant"


def test_control_contested():
    assert dominance_flag("Control", 3, 2) == "Contested"


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    yield conn
    conn.close()


def test_score_margins_returns_list(db):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    terminal = db.execute("SELECT map_id FROM maps WHERE map_name = 'Terminal' AND mode = 'SnD'").fetchone()[0]
    margins = score_margins(db, atl, terminal)
    assert len(margins) == 1
    assert margins[0]["margin"] == 3  # 6 - 3
    assert margins[0]["dominance"] == "Dominant"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_margin.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write margin.py**

```python
# src/cdm_stats/metrics/margin.py
import sqlite3


def dominance_flag(mode: str, winner_score: int, loser_score: int) -> str | None:
    margin = winner_score - loser_score
    if mode == "SnD":
        if margin >= 3:
            return "Dominant"
        if margin == 1:
            return "Contested"
    elif mode == "HP":
        if margin >= 70:
            return "Dominant"
        if margin < 25:
            return "Contested"
    elif mode == "Control":
        if margin >= 2:
            return "Dominant"
        if margin == 1:
            return "Contested"
    return None


def score_margins(conn: sqlite3.Connection, team_id: int, map_id: int) -> list[dict]:
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]

    rows = conn.execute(
        """SELECT mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score,
                  mr.match_id, mr.slot
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date""",
        (map_id, team_id, team_id),
    ).fetchall()

    results = []
    for winner_id, pick_score, non_pick_score, match_id, slot in rows:
        winner_score = max(pick_score, non_pick_score)
        loser_score = min(pick_score, non_pick_score)
        margin = winner_score - loser_score
        if winner_id != team_id:
            margin = -margin
        results.append({
            "match_id": match_id,
            "slot": slot,
            "margin": margin,
            "won": winner_id == team_id,
            "dominance": dominance_flag(mode, winner_score, loser_score) if winner_id == team_id else None,
        })
    return results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_margin.py -v`
Expected: 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/margin.py tests/test_margin.py
git commit -m "feat: add score margin and dominance flag metrics"
```

---

### Task 10: Pick context distribution

**Files:**
- Modify: `src/cdm_stats/metrics/avoidance.py`
- Modify: `tests/test_avoidance.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_avoidance.py`:

```python
from cdm_stats.metrics.avoidance import pick_context_distribution


def test_pick_context_distribution(db):
    """ATL picked Terminal in slot 1 (Opener context)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, terminal, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, atl, terminal)
    assert dist["Opener"] == 1
    assert dist.get("Neutral", 0) == 0
    assert dist.get("Must-Win", 0) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_avoidance.py::test_pick_context_distribution -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write pick_context_distribution function**

Add to `src/cdm_stats/metrics/avoidance.py`:

```python
def pick_context_distribution(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict[str, int]:
    """Breakdown of how often a team picks this map in each context.
    Returns dict like {"Opener": 2, "Must-Win": 1, "Neutral": 0, "Close-Out": 0}."""
    rows = conn.execute(
        """SELECT pick_context, COUNT(*)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND slot != 5
           GROUP BY pick_context""",
        (team_id, map_id),
    ).fetchall()
    result = {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
    for ctx, count in rows:
        if ctx in result:
            result[ctx] = count
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_avoidance.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/avoidance.py tests/test_avoidance.py
git commit -m "feat: add pick context distribution metric"
```

---

## Chunk 4: Export, Charts, CLI, and Backfill

### Task 11: Excel export — Map Matrix

**Files:**
- Create: `src/cdm_stats/export/excel.py`
- Create: `tests/test_excel.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_excel.py
import sqlite3
import io
import os
import pytest
from openpyxl import load_workbook
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.export.excel import export_map_matrix


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    yield conn
    conn.close()


def test_export_map_matrix_creates_file(db, tmp_path):
    output_path = tmp_path / "matrix.xlsx"
    export_map_matrix(db, str(output_path))
    assert output_path.exists()


def test_export_map_matrix_has_correct_sheet(db, tmp_path):
    output_path = tmp_path / "matrix.xlsx"
    export_map_matrix(db, str(output_path))
    wb = load_workbook(str(output_path))
    assert "Map Matrix" in wb.sheetnames


def test_export_map_matrix_has_team_rows(db, tmp_path):
    output_path = tmp_path / "matrix.xlsx"
    export_map_matrix(db, str(output_path))
    wb = load_workbook(str(output_path))
    ws = wb["Map Matrix"]
    # Row 1 is header, rows 2-15 are teams
    team_cells = [ws.cell(row=r, column=1).value for r in range(2, 16)]
    assert all(t is not None for t in team_cells)
    assert len(team_cells) == 14
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_excel.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write excel.py with map matrix export**

```python
# src/cdm_stats/export/excel.py
import sqlite3
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from cdm_stats.metrics.avoidance import pick_win_loss, defend_win_loss, avoidance_index, target_index

GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
YELLOW_FILL = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
LOW_SAMPLE_THRESHOLD = 4


def _get_all_teams(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    return conn.execute("SELECT team_id, abbreviation FROM teams ORDER BY abbreviation").fetchall()


def _get_all_maps(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()


def _cell_color(pick_wl: dict, defend_wl: dict, avoid: dict, tgt: dict) -> PatternFill | None:
    total_sample = (pick_wl["wins"] + pick_wl["losses"] +
                    defend_wl["wins"] + defend_wl["losses"])
    if total_sample == 0:
        return YELLOW_FILL
    if avoid.get("opportunities", 0) < LOW_SAMPLE_THRESHOLD:
        return YELLOW_FILL

    pick_total = pick_wl["wins"] + pick_wl["losses"]
    defend_total = defend_wl["wins"] + defend_wl["losses"]
    pick_rate = pick_wl["wins"] / pick_total if pick_total else 0
    defend_rate = defend_wl["wins"] / defend_total if defend_total else 0

    if pick_rate >= 0.6 and defend_rate >= 0.6:
        return GREEN_FILL
    if defend_rate <= 0.4 or avoid.get("ratio", 0) >= 0.7:
        return RED_FILL
    return None


def export_map_matrix(conn: sqlite3.Connection, output_path: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Map Matrix"

    teams = _get_all_teams(conn)
    maps = _get_all_maps(conn)

    # Header row
    ws.cell(row=1, column=1, value="Team").fill = HEADER_FILL
    ws.cell(row=1, column=1).font = HEADER_FONT
    for col_idx, (_, map_name, mode) in enumerate(maps, start=2):
        cell = ws.cell(row=1, column=col_idx, value=f"{map_name} ({mode})")
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center")

    # Data rows
    for row_idx, (team_id, abbr) in enumerate(teams, start=2):
        ws.cell(row=row_idx, column=1, value=abbr)
        for col_idx, (map_id, _, _) in enumerate(maps, start=2):
            pwl = pick_win_loss(conn, team_id, map_id)
            dwl = defend_win_loss(conn, team_id, map_id)
            avoid = avoidance_index(conn, team_id, map_id)
            tgt = target_index(conn, team_id, map_id)

            text = (
                f"P:{pwl['wins']}-{pwl['losses']} | "
                f"D:{dwl['wins']}-{dwl['losses']} | "
                f"Av:{avoid['ratio']:.0%}(n={avoid['opportunities']}) | "
                f"Tg:{tgt['ratio']:.0%}(n={tgt['opportunities']})"
            )
            cell = ws.cell(row=row_idx, column=col_idx, value=text)
            cell.alignment = Alignment(horizontal="center", wrap_text=True)
            fill = _cell_color(pwl, dwl, avoid, tgt)
            if fill:
                cell.fill = fill

    # Auto-width columns
    for col in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)

    wb.save(output_path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_excel.py -v`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/export/excel.py tests/test_excel.py
git commit -m "feat: add Excel Map Matrix export"
```

---

### Task 11: Excel export — Match-Up Prep

**Files:**
- Modify: `src/cdm_stats/export/excel.py`
- Modify: `tests/test_excel.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_excel.py`:

```python
from cdm_stats.export.excel import export_matchup_prep


def test_export_matchup_creates_file(db, tmp_path):
    output_path = tmp_path / "matchup_ATL_vs_LAT.xlsx"
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    export_matchup_prep(db, atl, lat, str(output_path))
    assert output_path.exists()


def test_export_matchup_has_correct_sheet(db, tmp_path):
    output_path = tmp_path / "matchup_ATL_vs_LAT.xlsx"
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    export_matchup_prep(db, atl, lat, str(output_path))
    wb = load_workbook(str(output_path))
    assert "Match-Up Prep" in wb.sheetnames
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_excel.py::test_export_matchup_creates_file -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write export_matchup_prep function**

Add to `src/cdm_stats/export/excel.py`:

```python
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.metrics.margin import score_margins, dominance_flag


def export_matchup_prep(
    conn: sqlite3.Connection, your_team_id: int, opp_team_id: int, output_path: str
) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Match-Up Prep"

    maps = _get_all_maps(conn)

    your_abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (your_team_id,)).fetchone()[0]
    opp_abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (opp_team_id,)).fetchone()[0]

    # Header
    headers = ["Map (Mode)", f"{your_abbr} Pick W-L", f"{your_abbr} Defend W-L",
               f"{opp_abbr} Avoid%", f"{opp_abbr} Avoid n", f"{opp_abbr} Target%",
               f"{opp_abbr} Target n", "Dominance"]
    for col_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=h)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT

    # Data rows — one per map
    for row_idx, (map_id, map_name, mode) in enumerate(maps, start=2):
        ws.cell(row=row_idx, column=1, value=f"{map_name} ({mode})")

        your_pwl = pick_win_loss(conn, your_team_id, map_id)
        your_dwl = defend_win_loss(conn, your_team_id, map_id)
        opp_avoid = avoidance_index(conn, opp_team_id, map_id)
        opp_tgt = target_index(conn, opp_team_id, map_id)
        margins = score_margins(conn, your_team_id, map_id)

        ws.cell(row=row_idx, column=2, value=f"{your_pwl['wins']}-{your_pwl['losses']}")
        ws.cell(row=row_idx, column=3, value=f"{your_dwl['wins']}-{your_dwl['losses']}")
        ws.cell(row=row_idx, column=4, value=f"{opp_avoid['ratio']:.0%}")
        ws.cell(row=row_idx, column=5, value=opp_avoid["opportunities"])
        ws.cell(row=row_idx, column=6, value=f"{opp_tgt['ratio']:.0%}")
        ws.cell(row=row_idx, column=7, value=opp_tgt["opportunities"])

        dom_counts = {}
        for m in margins:
            if m["dominance"]:
                dom_counts[m["dominance"]] = dom_counts.get(m["dominance"], 0) + 1
        dom_str = ", ".join(f"{k}:{v}" for k, v in dom_counts.items()) if dom_counts else "-"
        ws.cell(row=row_idx, column=8, value=dom_str)

        # Yellow fill for low sample
        if opp_avoid["opportunities"] < LOW_SAMPLE_THRESHOLD:
            for c in range(4, 8):
                ws.cell(row=row_idx, column=c).fill = YELLOW_FILL

    # Footer — Elo ratings
    footer_row = len(maps) + 3
    your_elo = get_current_elo(conn, your_team_id)
    opp_elo = get_current_elo(conn, opp_team_id)
    your_lc = " (LOW CONFIDENCE)" if is_low_confidence(conn, your_team_id) else ""
    opp_lc = " (LOW CONFIDENCE)" if is_low_confidence(conn, opp_team_id) else ""

    ws.cell(row=footer_row, column=1, value="Elo Ratings").font = Font(bold=True)
    ws.cell(row=footer_row + 1, column=1, value=f"{your_abbr}: {your_elo:.0f}{your_lc}")
    ws.cell(row=footer_row + 1, column=3, value=f"{opp_abbr}: {opp_elo:.0f}{opp_lc}")

    for col in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 30)

    wb.save(output_path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_excel.py -v`
Expected: 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/export/excel.py tests/test_excel.py
git commit -m "feat: add Match-Up Prep Excel export"
```

---

### Task 12: Charts (heatmap + Elo trajectory)

**Files:**
- Create: `src/cdm_stats/charts/heatmap.py`
- Create: `tests/test_charts.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_charts.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo
from cdm_stats.charts.heatmap import chart_avoidance_target, chart_elo_trajectory


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_chart_avoidance_target_creates_file(db, tmp_path):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    output_path = tmp_path / "heatmap_ATL.png"
    chart_avoidance_target(db, atl, str(output_path))
    assert output_path.exists()


def test_chart_elo_trajectory_creates_file(db, tmp_path):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    output_path = tmp_path / "elo_ATL.png"
    chart_elo_trajectory(db, atl, str(output_path))
    assert output_path.exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_charts.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write heatmap.py**

```python
# src/cdm_stats/charts/heatmap.py
import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from cdm_stats.metrics.avoidance import avoidance_index, target_index
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO


def chart_avoidance_target(conn: sqlite3.Connection, team_id: int, output_path: str) -> None:
    maps = conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()
    abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]

    labels = [f"{m[1]} ({m[2]})" for m in maps]
    avoid_vals = []
    target_vals = []
    for map_id, _, _ in maps:
        av = avoidance_index(conn, team_id, map_id)
        tg = target_index(conn, team_id, map_id)
        avoid_vals.append(av["ratio"])
        target_vals.append(tg["ratio"])

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(labels))
    width = 0.35

    ax.bar(x - width / 2, avoid_vals, width, label="Avoidance Index", color="#FF6B6B")
    ax.bar(x + width / 2, target_vals, width, label="Target Index", color="#4ECDC4")

    ax.set_ylabel("Index")
    ax.set_title(f"{abbr} — Avoidance vs Target Index by Map")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend()
    ax.set_ylim(0, 1.0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def chart_elo_trajectory(conn: sqlite3.Connection, team_id: int, output_path: str) -> None:
    abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]
    history = get_elo_history(conn, team_id)

    dates = [h["match_date"] for h in history]
    elos = [h["elo_after"] for h in history]

    # Prepend seed
    if dates:
        dates = ["Start"] + dates
        elos = [SEED_ELO] + elos

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(len(dates)), elos, marker="o", linewidth=2, color="#4472C4")
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha="right")
    ax.set_ylabel("Elo Rating")
    ax.set_title(f"{abbr} — Elo Trajectory")
    ax.axhline(y=SEED_ELO, color="gray", linestyle="--", alpha=0.5, label="Seed (1000)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_charts.py -v`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/charts/heatmap.py tests/test_charts.py
git commit -m "feat: add avoidance/target heatmap and Elo trajectory charts"
```

---

### Task 13: Backfill (Elo recalculation)

**Files:**
- Create: `src/cdm_stats/ingestion/backfill.py`
- Create: `tests/test_backfill.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_backfill.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo
from cdm_stats.ingestion.backfill import backfill_elo

TWO_MATCHES_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2
2026-01-16,BOS,SEA,BOS,1,Invasion,BOS,6,4
2026-01-16,BOS,SEA,BOS,2,Karachi,SEA,250,200
2026-01-16,BOS,SEA,BOS,3,Highrise,BOS,3,2
2026-01-16,BOS,SEA,BOS,4,Skidrow,SEA,6,3
2026-01-16,BOS,SEA,BOS,5,Invasion,BOS,250,230"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(TWO_MATCHES_CSV))
    yield conn
    conn.close()


def test_backfill_elo_populates_all_teams(db):
    backfill_elo(db)
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 4  # 2 matches × 2 teams each


def test_backfill_elo_is_idempotent(db):
    backfill_elo(db)
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    elo_first = get_current_elo(db, atl)
    backfill_elo(db)
    elo_second = get_current_elo(db, atl)
    assert elo_first == elo_second


def test_backfill_elo_chronological_order(db):
    backfill_elo(db)
    rows = db.execute("SELECT match_date FROM team_elo ORDER BY elo_id").fetchall()
    dates = [r[0] for r in rows]
    assert dates == sorted(dates)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backfill.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Write backfill.py**

```python
# src/cdm_stats/ingestion/backfill.py
import sqlite3
from cdm_stats.metrics.elo import update_elo


def backfill_elo(conn: sqlite3.Connection) -> int:
    """Wipe all Elo history and recalculate from matches in chronological order.
    Returns the number of matches processed."""
    conn.execute("DELETE FROM team_elo")
    conn.commit()

    matches = conn.execute(
        "SELECT match_id FROM matches ORDER BY match_date, match_id"
    ).fetchall()

    for (match_id,) in matches:
        update_elo(conn, match_id)

    return len(matches)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_backfill.py -v`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/backfill.py tests/test_backfill.py
git commit -m "feat: add Elo backfill from existing match data"
```

---

### Task 14: CLI entry point (main.py)

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Write main.py with all CLI commands**

```python
# main.py
import argparse
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cdl.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def cmd_init(_args: argparse.Namespace) -> None:
    from cdm_stats.db.schema import create_tables
    from cdm_stats.ingestion.seed import seed_teams, seed_maps

    conn = get_db()
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    conn.close()
    print("Database initialized and seeded.")


def cmd_ingest(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.csv_loader import ingest_csv

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_csv(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['match']}")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()


def cmd_export_matrix(_args: argparse.Namespace) -> None:
    from cdm_stats.export.excel import export_map_matrix

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    path = os.path.join(OUTPUT_DIR, "map_matrix.xlsx")
    export_map_matrix(conn, path)
    conn.close()
    print(f"Map Matrix exported to {path}")


def cmd_export_matchup(args: argparse.Namespace) -> None:
    from cdm_stats.export.excel import export_matchup_prep
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    your_id = get_team_id_by_abbr(conn, args.your_team)
    opp_id = get_team_id_by_abbr(conn, args.opponent)
    if not your_id or not opp_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"matchup_{args.your_team}_vs_{args.opponent}.xlsx")
    export_matchup_prep(conn, your_id, opp_id, path)
    conn.close()
    print(f"Match-Up Prep exported to {path}")


def cmd_chart_heatmap(args: argparse.Namespace) -> None:
    from cdm_stats.charts.heatmap import chart_avoidance_target
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    team_id = get_team_id_by_abbr(conn, args.team)
    if not team_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"heatmap_{args.team}.png")
    chart_avoidance_target(conn, team_id, path)
    conn.close()
    print(f"Heatmap exported to {path}")


def cmd_chart_elo(args: argparse.Namespace) -> None:
    from cdm_stats.charts.heatmap import chart_elo_trajectory
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    team_id = get_team_id_by_abbr(conn, args.team)
    if not team_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"elo_{args.team}.png")
    chart_elo_trajectory(conn, team_id, path)
    conn.close()
    print(f"Elo trajectory exported to {path}")


def cmd_backfill(_args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.backfill import backfill_elo

    conn = get_db()
    count = backfill_elo(conn)
    conn.close()
    print(f"Backfill complete: {count} matches reprocessed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="CDM Stats — CDL Analytics Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Create DB and seed teams/maps")

    p_ingest = sub.add_parser("ingest", help="Ingest match data from CSV")
    p_ingest.add_argument("csv_file", help="Path to CSV file")

    sub_export = sub.add_parser("export", help="Export data to Excel")
    export_sub = sub_export.add_subparsers(dest="export_type", required=True)
    export_sub.add_parser("matrix", help="Export Map Matrix")
    p_matchup = export_sub.add_parser("matchup", help="Export Match-Up Prep")
    p_matchup.add_argument("your_team", help="Your team abbreviation")
    p_matchup.add_argument("opponent", help="Opponent team abbreviation")

    sub_chart = sub.add_parser("chart", help="Generate charts")
    chart_sub = sub_chart.add_subparsers(dest="chart_type", required=True)
    p_heatmap = chart_sub.add_parser("heatmap", help="Avoidance vs Target heatmap")
    p_heatmap.add_argument("team", help="Team abbreviation")
    p_elo = chart_sub.add_parser("elo", help="Elo trajectory")
    p_elo.add_argument("team", help="Team abbreviation")

    sub.add_parser("backfill", help="Wipe and recalculate Elo")

    args = parser.parse_args()

    commands = {
        "init": cmd_init,
        "ingest": cmd_ingest,
        "backfill": cmd_backfill,
    }

    if args.command in commands:
        commands[args.command](args)
    elif args.command == "export":
        if args.export_type == "matrix":
            cmd_export_matrix(args)
        elif args.export_type == "matchup":
            cmd_export_matchup(args)
    elif args.command == "chart":
        if args.chart_type == "heatmap":
            cmd_chart_heatmap(args)
        elif args.chart_type == "elo":
            cmd_chart_elo(args)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Smoke test the CLI**

Run: `uv run python main.py init`
Expected: `Database initialized and seeded.`

Run: `uv run python main.py --help`
Expected: Shows all subcommands.

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "feat: add CLI entry point with all commands"
```

---

### Task 15: Run full test suite and final cleanup

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 2: Add output/ and data/ to .gitignore if not already**

Verify `.gitignore` contains `output/` and `data/cdl.db`.

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "chore: final cleanup and verify all tests pass"
```
