# Tournament Data Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support tournament and CDL playoff match formats with bans, different mode orders, and seed-based picks alongside the existing regular season pipeline.

**Architecture:** Extend the existing schema with format-aware tables (match_format column, map_bans table, wider slot range). Add a tournament loader alongside the existing CSV loader. Generalize pick_context derivation and slot-mode mapping to be format-dependent. Add ban summary queries for the matchup prep export.

**Tech Stack:** Python 3.12, SQLite3, pytest, openpyxl

**Spec:** `docs/superpowers/specs/2026-03-18-tournament-data-integration-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/cdm_stats/db/schema.py` | Modify | Add migration logic, map_bans table, updated constraints |
| `src/cdm_stats/db/queries.py` | Modify | Replace SLOT_MODES with FORMAT_SLOT_MODES, add ban queries, update insert_match |
| `src/cdm_stats/ingestion/csv_loader.py` | Modify | Generalize derive_pick_context with win_threshold/last_slot params |
| `src/cdm_stats/ingestion/tournament_loader.py` | Create | Tournament + playoff CSV ingestion |
| `src/cdm_stats/export/excel.py` | Modify | Add ban summary section to matchup prep |
| `main.py` | Modify | Add ingest-tournament CLI command |
| `tests/conftest.py` | Modify | Update db fixture for migrated schema |
| `tests/test_schema.py` | Modify | Test migration and new tables |
| `tests/test_pick_context.py` | Modify | Test generalized derive_pick_context |
| `tests/test_tournament_loader.py` | Create | Test tournament and playoff ingestion |
| `tests/test_ban_queries.py` | Create | Test ban summary queries |

---

### Task 1: Schema Migration — Rebuild tables with new constraints

**Files:**
- Modify: `src/cdm_stats/db/schema.py`
- Modify: `tests/test_schema.py`

- [ ] **Step 1: Write failing test for migration — matches table gets match_format column**

```python
# tests/test_schema.py — add to existing file
import sqlite3
from cdm_stats.db.schema import create_tables, migrate


@pytest.fixture
def raw_db():
    """Fresh in-memory DB with OLD schema (pre-migration) — no seeds, no migrate."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


def test_migrate_adds_match_format_column(raw_db):
    """After migration, matches table has match_format column."""
    migrate(raw_db)
    row = raw_db.execute("PRAGMA table_info(matches)").fetchall()
    col_names = [r[1] for r in row]
    assert "match_format" in col_names


def test_migrate_adds_map_bans_table(raw_db):
    """After migration, map_bans table exists."""
    migrate(raw_db)
    tables = [r[0] for r in raw_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    assert "map_bans" in tables


def test_migrate_allows_slot_7(raw_db):
    """After migration, map_results accepts slot values up to 7."""
    migrate(raw_db)
    info = raw_db.execute("SELECT sql FROM sqlite_master WHERE name='map_results'").fetchone()[0]
    assert "BETWEEN 1 AND 7" in info


def test_migrate_allows_nullable_two_v_two_winner(raw_db):
    """After migration, two_v_two_winner_id can be NULL."""
    migrate(raw_db)
    info = raw_db.execute("PRAGMA table_info(matches)").fetchall()
    two_v_two_col = [r for r in info if r[1] == "two_v_two_winner_id"][0]
    assert two_v_two_col[3] == 0  # notnull = 0 means nullable


def test_migrate_preserves_existing_data(raw_db):
    """Migration preserves existing match and map_results rows."""
    from cdm_stats.ingestion.seed import seed_teams, seed_maps
    seed_teams(raw_db)
    seed_maps(raw_db)
    raw_db.execute(
        "INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id) "
        "VALUES ('2026-01-01', 1, 2, 1, 1)"
    )
    raw_db.execute(
        "INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id, "
        "picking_team_score, non_picking_team_score, team1_score_before, team2_score_before, pick_context) "
        "VALUES (1, 1, 1, 1, 1, 6, 3, 0, 0, 'Opener')"
    )
    raw_db.commit()
    migrate(raw_db)
    match = raw_db.execute("SELECT match_format FROM matches WHERE match_id = 1").fetchone()
    assert match[0] == "CDL_BO5"
    mr = raw_db.execute("SELECT slot FROM map_results WHERE match_id = 1").fetchone()
    assert mr[0] == 1


def test_migrate_is_idempotent(raw_db):
    """Running migrate twice does not error or duplicate data."""
    migrate(raw_db)
    migrate(raw_db)  # should not raise
```

Note: The `raw_db` fixture creates the OLD schema (via `create_tables` before the update) without calling `migrate()`, so the migration tests exercise the actual migration path on a pre-migration schema. After Task 7 updates the shared `db` fixture to include `migrate()`, these tests remain independent.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_schema.py -v`
Expected: FAIL — `migrate` does not exist

- [ ] **Step 3: Implement schema migration**

Update `src/cdm_stats/db/schema.py`:

1. Update the `TABLES` list so that `create_tables()` creates the NEW table definitions (with `match_format`, slot 1-7, `Unknown` pick_context, nullable `two_v_two_winner_id`, and `map_bans`). This ensures fresh databases get the right schema.

2. Add a `migrate()` function that:
   - Uses `PRAGMA user_version` to track schema version (0 = old, 1 = migrated)
   - If `user_version == 0` AND tables exist with old constraints, performs table rebuild migrations:
     - Rebuild `matches` with `match_format` column (default `'CDL_BO5'`) and nullable `two_v_two_winner_id`
     - Rebuild `map_results` with `slot BETWEEN 1 AND 7` and `'Unknown'` in pick_context CHECK
     - Create `map_bans` table
   - Sets `PRAGMA user_version = 1`
   - If `user_version >= 1`, no-op (idempotent)
   - All within a transaction

```python
SCHEMA_VERSION = 1

MAP_BANS_TABLE = """
CREATE TABLE IF NOT EXISTS map_bans (
    ban_id    INTEGER PRIMARY KEY,
    match_id  INTEGER NOT NULL REFERENCES matches(match_id),
    team_id   INTEGER NOT NULL REFERENCES teams(team_id),
    map_id    INTEGER NOT NULL REFERENCES maps(map_id),
    UNIQUE(match_id, team_id, map_id)
)
"""

def migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= SCHEMA_VERSION:
        return

    conn.execute("PRAGMA foreign_keys = OFF")

    # Rebuild matches table
    conn.execute("""CREATE TABLE matches_new (
        match_id            INTEGER PRIMARY KEY,
        match_date          DATE NOT NULL,
        team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
        team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
        two_v_two_winner_id INTEGER REFERENCES teams(team_id),
        series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
        match_format        TEXT NOT NULL DEFAULT 'CDL_BO5'
            CHECK(match_format IN ('CDL_BO5', 'CDL_PLAYOFF_BO5', 'CDL_PLAYOFF_BO7',
                                    'TOURNAMENT_BO5', 'TOURNAMENT_BO7')),
        CHECK(team1_id != team2_id)
    )""")
    conn.execute("""INSERT INTO matches_new
        (match_id, match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, match_format)
        SELECT match_id, match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, 'CDL_BO5'
        FROM matches""")
    conn.execute("DROP TABLE matches")
    conn.execute("ALTER TABLE matches_new RENAME TO matches")

    # Rebuild map_results table
    conn.execute("""CREATE TABLE map_results_new (
        result_id              INTEGER PRIMARY KEY,
        match_id               INTEGER NOT NULL REFERENCES matches(match_id),
        slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 7),
        map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
        picked_by_team_id      INTEGER REFERENCES teams(team_id),
        winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
        picking_team_score     INTEGER NOT NULL,
        non_picking_team_score INTEGER NOT NULL,
        team1_score_before     INTEGER NOT NULL,
        team2_score_before     INTEGER NOT NULL,
        pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                   'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss', 'Unknown'
                               )),
        UNIQUE(match_id, slot)
    )""")
    conn.execute("""INSERT INTO map_results_new
        SELECT * FROM map_results""")
    conn.execute("DROP TABLE map_results")
    conn.execute("ALTER TABLE map_results_new RENAME TO map_results")

    # Create map_bans table
    conn.execute(MAP_BANS_TABLE)

    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
```

Also update the TABLES list in-place so `create_tables()` creates the new schema for fresh DBs:
- `matches`: add `match_format`, make `two_v_two_winner_id` nullable
- `map_results`: slot `BETWEEN 1 AND 7`, add `'Unknown'` to pick_context
- Add `MAP_BANS_TABLE` to the TABLES list
- Set `PRAGMA user_version = 1` at end of `create_tables()`

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_schema.py -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to check nothing broke**

Run: `uv run pytest -v`
Expected: All existing tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/cdm_stats/db/schema.py tests/test_schema.py
git commit -m "feat: add schema migration for tournament format support"
```

---

### Task 2: Format-dependent slot-mode mapping and generalized pick_context

**Files:**
- Modify: `src/cdm_stats/db/queries.py`
- Modify: `src/cdm_stats/ingestion/csv_loader.py`
- Modify: `tests/test_pick_context.py`

- [ ] **Step 1: Write failing tests for generalized derive_pick_context**

Add to `tests/test_pick_context.py`:

```python
# Test BO7 coin-toss slot is 7, not 5
def test_bo7_slot_7_is_coin_toss():
    assert derive_pick_context(slot=7, picker_score=3, opponent_score=3,
                               win_threshold=4, last_slot=7) == "Coin-Toss"

def test_bo7_slot_5_is_not_coin_toss():
    """In BO7, slot 5 is a regular slot, not a coin toss."""
    result = derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                                 win_threshold=4, last_slot=7)
    assert result == "Neutral"

def test_bo7_must_win_when_opponent_has_3():
    """In BO7, Must-Win when opponent needs 1 more win (has 3) and picker has less."""
    assert derive_pick_context(slot=5, picker_score=1, opponent_score=3,
                               win_threshold=4, last_slot=7) == "Must-Win"

def test_bo7_close_out_when_picker_has_3():
    """In BO7, Close-Out when picker needs 1 more win (has 3) and opponent has less."""
    assert derive_pick_context(slot=5, picker_score=3, opponent_score=1,
                               win_threshold=4, last_slot=7) == "Close-Out"

def test_bo7_neutral_at_2_2():
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=4, last_slot=7) == "Neutral"

# Ensure existing BO5 behavior still works with explicit params
def test_bo5_explicit_params_slot5_coin_toss():
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=3, last_slot=5) == "Coin-Toss"

def test_bo5_defaults_backward_compatible():
    """Calling without new params still works for BO5."""
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2) == "Coin-Toss"
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=2) == "Must-Win"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pick_context.py -v`
Expected: FAIL — derive_pick_context does not accept win_threshold/last_slot

- [ ] **Step 3: Update derive_pick_context to accept win_threshold and last_slot**

In `src/cdm_stats/ingestion/csv_loader.py`, update the function signature:

```python
def derive_pick_context(
    slot: int, picker_score: int, opponent_score: int,
    *, win_threshold: int = 3, last_slot: int = 5
) -> str:
    if slot == last_slot:
        return "Coin-Toss"
    if slot == 1:
        return "Opener"
    if opponent_score == win_threshold - 1 and picker_score < win_threshold - 1:
        return "Must-Win"
    if picker_score == win_threshold - 1 and opponent_score < win_threshold - 1:
        return "Close-Out"
    return "Neutral"
```

The existing callers pass positional args without the new params, so defaults preserve backward compatibility.

- [ ] **Step 4: Run pick_context tests**

Run: `uv run pytest tests/test_pick_context.py -v`
Expected: PASS

- [ ] **Step 5: Replace SLOT_MODES with FORMAT_SLOT_MODES in queries.py**

In `src/cdm_stats/db/queries.py`:

```python
FORMAT_SLOT_MODES = {
    "CDL_BO5":         {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO5": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO7": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP", 6: "Control", 7: "SnD"},
    "TOURNAMENT_BO5":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD"},
    "TOURNAMENT_BO7":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD", 6: "Control", 7: "SnD"},
}

# Keep SLOT_MODES as alias for CDL_BO5 for backward compatibility
SLOT_MODES = FORMAT_SLOT_MODES["CDL_BO5"]


def get_mode_for_slot(slot: int, match_format: str = "CDL_BO5") -> str:
    return FORMAT_SLOT_MODES[match_format][slot]
```

Keep the `SLOT_MODES` alias so existing imports in `avoidance.py` and `csv_loader.py` continue to work unchanged. The avoidance module already filters on `picked_by_team_id IS NOT NULL` and `slot != 5`, which naturally excludes tournament rows.

- [ ] **Step 6: Update insert_match to accept match_format and nullable two_v_two_winner_id**

In `src/cdm_stats/db/queries.py`:

```python
def insert_match(
    conn: sqlite3.Connection,
    match_date: str,
    team1_id: int,
    team2_id: int,
    two_v_two_winner_id: int | None,
    series_winner_id: int,
    match_format: str = "CDL_BO5",
) -> int:
    cursor = conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, match_format),
    )
    return cursor.lastrowid
```

- [ ] **Step 7: Add insert_map_ban query**

In `src/cdm_stats/db/queries.py`:

```python
def insert_map_ban(
    conn: sqlite3.Connection,
    match_id: int,
    team_id: int,
    map_id: int,
) -> int:
    cursor = conn.execute(
        "INSERT INTO map_bans (match_id, team_id, map_id) VALUES (?, ?, ?)",
        (match_id, team_id, map_id),
    )
    return cursor.lastrowid


def get_ban_summary(
    conn: sqlite3.Connection, team_id: int, opponent_id: int
) -> list[dict]:
    """Get ban frequency for team_id in matches against opponent_id."""
    rows = conn.execute(
        """SELECT mb.team_id, m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?) OR (m.team1_id = ? AND m.team2_id = ?))
           GROUP BY mb.team_id, m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        (team_id, team_id, opponent_id, opponent_id, team_id),
    ).fetchall()

    total_series = conn.execute(
        """SELECT COUNT(*) FROM matches
           WHERE match_format != 'CDL_BO5'
             AND ((team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?))""",
        (team_id, opponent_id, opponent_id, team_id),
    ).fetchone()[0]

    return [
        {"map_name": r[1], "mode": r[2], "ban_count": r[3], "total_series": total_series}
        for r in rows
    ]
```

- [ ] **Step 8: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS (SLOT_MODES alias keeps backward compat)

- [ ] **Step 9: Commit**

```bash
git add src/cdm_stats/db/queries.py src/cdm_stats/ingestion/csv_loader.py tests/test_pick_context.py
git commit -m "feat: generalize pick_context and slot-mode mapping for multi-format support"
```

---

### Task 3: Tournament loader — external tournament format (unknown picks)

**Files:**
- Create: `src/cdm_stats/ingestion/tournament_loader.py`
- Create: `tests/test_tournament_loader.py`

- [ ] **Step 1: Write failing tests for tournament map ingestion**

Create `tests/test_tournament_loader.py`:

```python
import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# ELV 3-2 ALU in TOURNAMENT_BO5 (HP -> SnD -> Control -> HP -> SnD)
TOURNAMENT_BO5_MAPS = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

TOURNAMENT_BO5_BANS = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""

# ALU 4-2 ELV in TOURNAMENT_BO7 (HP -> SnD -> Control -> HP -> SnD -> Control -> SnD)
TOURNAMENT_BO7_MAPS = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Summit,ALU,250,200
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Tunisia,ELV,6,3
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Raid,ALU,3,1
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Hacienda,ALU,250,180
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Firing Range,ELV,6,4
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Standoff,ALU,3,2"""

TOURNAMENT_BO7_BANS = """date,team1,team2,format,banned_by,map
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ELV,Hacienda
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ALU,Summit
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ELV,Tunisia
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ALU,Firing Range"""


def test_tournament_bo5_creates_match_with_format(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    match = db.execute("SELECT match_format, two_v_two_winner_id FROM matches").fetchone()
    assert match[0] == "TOURNAMENT_BO5"
    assert match[1] is None


def test_tournament_bo5_creates_5_map_results(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 5


def test_tournament_all_picks_are_null(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    non_null = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id IS NOT NULL"
    ).fetchone()[0]
    assert non_null == 0


def test_tournament_all_pick_context_unknown(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    contexts = db.execute(
        "SELECT DISTINCT pick_context FROM map_results"
    ).fetchall()
    assert contexts == [("Unknown",)]


def test_tournament_scores_are_team1_team2(db):
    """When pick is unknown, picking_team_score = team1's score, non_picking = team2's."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    # Slot 1: ELV (team1) 250, ALU (team2) 200
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (250, 200)


def test_tournament_bo5_creates_6_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 6


def test_tournament_bo7_creates_match(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    match = db.execute("SELECT match_format FROM matches").fetchone()
    assert match[0] == "TOURNAMENT_BO7"


def test_tournament_bo7_creates_6_map_results(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 6


def test_tournament_bo7_creates_4_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 4


def test_tournament_series_winner_correct(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    winner = db.execute("SELECT series_winner_id FROM matches").fetchone()[0]
    assert winner == elv_id


def test_tournament_elo_updated(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    elo_count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert elo_count == 2  # one row per team


def test_tournament_duplicate_match_skipped(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    results = ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    assert any(r["status"] == "skipped" for r in results)
    count = db.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    assert count == 1


def test_tournament_series_scores_tracked(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    rows = db.execute(
        "SELECT slot, team1_score_before, team2_score_before FROM map_results ORDER BY slot"
    ).fetchall()
    # ELV=team1, ALU=team2. Slot1: ELV wins -> 1-0. Slot2: ALU wins -> 1-1.
    # Slot3: ELV wins -> 2-1. Slot4: ALU wins -> 2-2. Slot5: ELV wins -> 3-2.
    assert rows[0][1:] == (0, 0)
    assert rows[1][1:] == (1, 0)
    assert rows[2][1:] == (1, 1)
    assert rows[3][1:] == (2, 1)
    assert rows[4][1:] == (2, 2)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tournament_loader.py -v`
Expected: FAIL — tournament_loader module does not exist

- [ ] **Step 3: Implement tournament_loader.py**

Create `src/cdm_stats/ingestion/tournament_loader.py`:

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
    insert_map_ban,
    FORMAT_SLOT_MODES,
)
from cdm_stats.ingestion.csv_loader import derive_pick_context


FORMAT_WIN_THRESHOLD = {
    "TOURNAMENT_BO5": 3,
    "TOURNAMENT_BO7": 4,
    "CDL_PLAYOFF_BO5": 3,
    "CDL_PLAYOFF_BO7": 4,
}

FORMAT_BAN_MODES = {
    "TOURNAMENT_BO5": {"HP", "SnD", "Control"},
    "TOURNAMENT_BO7": {"HP", "SnD"},
    "CDL_PLAYOFF_BO5": {"SnD", "HP"},
    "CDL_PLAYOFF_BO7": {"SnD", "HP"},
}

FORMAT_EXPECTED_BANS = {
    "TOURNAMENT_BO5": 6,
    "TOURNAMENT_BO7": 4,
    "CDL_PLAYOFF_BO5": 4,
    "CDL_PLAYOFF_BO7": 4,
}


def _group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["team1"], row["team2"])
        matches.setdefault(key, []).append(row)
    return matches


def _is_duplicate_match(conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int) -> bool:
    row = conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return row is not None


def _validate_tournament_match(
    conn: sqlite3.Connection, key: tuple, rows: list[dict], match_format: str
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr = key
    slot_modes = FORMAT_SLOT_MODES[match_format]
    win_threshold = FORMAT_WIN_THRESHOLD[match_format]

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    slots = [i + 1 for i in range(len(rows))]
    for i, row in enumerate(rows):
        slot = i + 1
        if slot not in slot_modes:
            errors.append(f"Slot {slot} not valid for format {match_format}")
            continue
        mode = slot_modes[slot]
        if get_map_id(conn, row["map"], mode) is None:
            errors.append(f"Unknown map '{row['map']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
    t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
    if max(t1_wins, t2_wins) != win_threshold:
        errors.append(
            f"No team reached {win_threshold} wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}"
        )

    return errors


def _validate_bans(
    conn: sqlite3.Connection, bans: list[dict], match_format: str,
    team1_abbr: str, team2_abbr: str
) -> list[str]:
    errors = []
    allowed_modes = FORMAT_BAN_MODES[match_format]
    expected_count = FORMAT_EXPECTED_BANS[match_format]

    if len(bans) != expected_count:
        errors.append(f"Expected {expected_count} bans for {match_format}, got {len(bans)}")

    for ban in bans:
        if ban["banned_by"] not in (team1_abbr, team2_abbr):
            errors.append(f"Ban by '{ban['banned_by']}' is not one of the teams")
        # Look up map in any mode to find its actual mode
        map_row = conn.execute(
            "SELECT mode FROM maps WHERE map_name = ?", (ban["map"],)
        ).fetchone()
        if map_row is None:
            errors.append(f"Unknown map: {ban['map']}")
        elif map_row[0] not in allowed_modes:
            errors.append(
                f"Map '{ban['map']}' is mode '{map_row[0]}', not allowed for {match_format} bans "
                f"(allowed: {allowed_modes})"
            )

    return errors


def _ingest_maps(
    conn: sqlite3.Connection, grouped: dict[tuple, list[dict]]
) -> list[dict]:
    results = []
    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr = key
        match_format = rows[0]["format"]
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate_match(conn, date, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_tournament_match(conn, key, rows, match_format)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        slot_modes = FORMAT_SLOT_MODES[match_format]
        win_threshold = FORMAT_WIN_THRESHOLD[match_format]
        is_playoff = match_format.startswith("CDL_PLAYOFF")
        higher_seed_id = None
        if is_playoff:
            higher_seed_abbr = rows[0].get("higher_seed")
            higher_seed_id = get_team_id_by_abbr(conn, higher_seed_abbr) if higher_seed_abbr else None

        t1_series = 0
        t2_series = 0
        map_result_data = []
        max_slot = max(slot_modes.keys())

        for i, row in enumerate(rows):
            slot = i + 1
            mode = slot_modes[slot]
            map_id = get_map_id(conn, row["map"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            team1_score = int(row["team1_score"])
            team2_score = int(row["team2_score"])

            if is_playoff and higher_seed_id:
                # Playoff: alternating picks by seed, last slot is coin toss
                if slot == max_slot:
                    picker_id = None
                elif slot % 2 == 1:
                    picker_id = higher_seed_id
                else:
                    other_id = team2_id if higher_seed_id == team1_id else team1_id
                    picker_id = other_id

                # Scores oriented by picker
                if picker_id is None:
                    picking_team_score = team1_score
                    non_picking_team_score = team2_score
                    pick_context = "Coin-Toss"
                else:
                    if picker_id == team1_id:
                        picking_team_score = team1_score
                        non_picking_team_score = team2_score
                        pick_context = derive_pick_context(
                            slot, t1_series, t2_series,
                            win_threshold=win_threshold, last_slot=max_slot
                        )
                    else:
                        picking_team_score = team2_score
                        non_picking_team_score = team1_score
                        pick_context = derive_pick_context(
                            slot, t2_series, t1_series,
                            win_threshold=win_threshold, last_slot=max_slot
                        )
            else:
                # Tournament: no picks known
                picker_id = None
                picking_team_score = team1_score
                non_picking_team_score = team2_score
                pick_context = "Unknown"

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context,
            ))

            if winner_id == team1_id:
                t1_series += 1
            else:
                t2_series += 1

        series_winner_id = team1_id if t1_series == win_threshold else team2_id

        try:
            match_id = insert_match(
                conn, date, team1_id, team2_id,
                higher_seed_id,  # None for tournament, higher seed for playoff
                series_winner_id,
                match_format,
            )
            for data in map_result_data:
                insert_map_result(conn, match_id, *data)
            conn.commit()
            from cdm_stats.metrics.elo import update_elo
            update_elo(conn, match_id)
            results.append({"match": key, "status": "ok", "match_id": match_id})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results


def _ingest_bans(
    conn: sqlite3.Connection, grouped: dict[tuple, list[dict]]
) -> list[dict]:
    results = []
    for key, bans in grouped.items():
        date, team1_abbr, team2_abbr = key
        match_format = bans[0]["format"]
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if not team1_id or not team2_id:
            results.append({"match": key, "status": "error", "errors": ["Unknown team"]})
            continue

        # Find the match
        match = conn.execute(
            """SELECT match_id FROM matches
               WHERE match_date = ? AND (
                   (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
               )""",
            (date, team1_id, team2_id, team2_id, team1_id),
        ).fetchone()

        if not match:
            results.append({"match": key, "status": "error", "errors": ["Match not found — ingest maps first"]})
            continue

        match_id = match[0]

        # Check if bans already exist for this match
        existing = conn.execute(
            "SELECT COUNT(*) FROM map_bans WHERE match_id = ?", (match_id,)
        ).fetchone()[0]
        if existing > 0:
            results.append({"match": key, "status": "skipped", "reason": "bans already exist"})
            continue

        errors = _validate_bans(conn, bans, match_format, team1_abbr, team2_abbr)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        try:
            for ban in bans:
                ban_team_id = get_team_id_by_abbr(conn, ban["banned_by"])
                map_row = conn.execute(
                    "SELECT map_id FROM maps WHERE map_name = ?", (ban["map"],)
                ).fetchone()
                insert_map_ban(conn, match_id, ban_team_id, map_row[0])
            conn.commit()
            results.append({"match": key, "status": "ok", "bans": len(bans)})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results


def ingest_tournament(
    conn: sqlite3.Connection, maps_file: IO[str], bans_file: IO[str]
) -> list[dict]:
    maps_reader = csv.DictReader(maps_file)
    maps_grouped = _group_rows_by_match(maps_reader)
    map_results = _ingest_maps(conn, maps_grouped)

    bans_reader = csv.DictReader(bans_file)
    bans_grouped = _group_rows_by_match(bans_reader)
    ban_results = _ingest_bans(conn, bans_grouped)

    return map_results + ban_results
```

Note: `derive_pick_context` is imported at module level (top of file). The `ingest_tournament` function does NOT need an inline import.

- [ ] **Step 4: Run tournament loader tests**

Run: `uv run pytest tests/test_tournament_loader.py -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/cdm_stats/ingestion/tournament_loader.py tests/test_tournament_loader.py
git commit -m "feat: add tournament loader for external tournament format ingestion"
```

---

### Task 4: Playoff ingestion (known picks + bans)

**Files:**
- Modify: `tests/test_tournament_loader.py`

- [ ] **Step 1: Write failing tests for playoff ingestion**

Add to `tests/test_tournament_loader.py`:

```python
# CDL Playoff BO5: SnD -> HP -> Control -> SnD -> HP
# ELV (higher seed) 3-1 ALU
PLAYOFF_BO5_MAPS = """date,team1,team2,format,higher_seed,map,winner,team1_score,team2_score
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Tunisia,ELV,6,3
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Summit,ALU,250,200
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Raid,ELV,3,1
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Slums,ELV,6,4"""

PLAYOFF_BO5_BANS = """date,team1,team2,format,banned_by,map
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Firing Range
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ALU,Meltdown
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Hacienda
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ALU,Takeoff"""


def test_playoff_bo5_creates_match_with_format(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    match = db.execute("SELECT match_format, two_v_two_winner_id FROM matches").fetchone()
    assert match[0] == "CDL_PLAYOFF_BO5"
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    assert match[1] == elv_id  # higher seed stored as two_v_two_winner_id


def test_playoff_bo5_picks_alternate_by_seed(db):
    """HS picks odd slots (1, 3), LS picks even slots (2, 4). No slot 5 played."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    alu_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ALU'").fetchone()[0]

    pickers = db.execute(
        "SELECT slot, picked_by_team_id FROM map_results ORDER BY slot"
    ).fetchall()
    assert pickers[0] == (1, elv_id)   # slot 1: HS
    assert pickers[1] == (2, alu_id)   # slot 2: LS
    assert pickers[2] == (3, elv_id)   # slot 3: HS
    assert pickers[3] == (4, alu_id)   # slot 4: LS


def test_playoff_bo5_pick_context_slot1_opener(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    ctx = db.execute("SELECT pick_context FROM map_results WHERE slot = 1").fetchone()[0]
    assert ctx == "Opener"


def test_playoff_bo5_scores_oriented_by_picker(db):
    """Slot 1: ELV picked, ELV won 6-3. picking=6, non=3."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (6, 3)


def test_playoff_bo5_scores_oriented_when_picker_loses(db):
    """Slot 2: ALU picked, ALU won 250-200. ELV=team1=200, ALU=team2=250.
    picker=ALU so picking_team_score=250, non=200."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 2"
    ).fetchone()
    assert row == (250, 200)


def test_playoff_bo5_creates_4_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 4
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tournament_loader.py::test_playoff_bo5_creates_match_with_format -v`
Expected: FAIL (playoff logic not yet in tournament_loader — but it was included in Task 3's implementation)

Actually, the playoff path IS in the Task 3 implementation already. Let's verify.

- [ ] **Step 3: Run all playoff tests**

Run: `uv run pytest tests/test_tournament_loader.py -v -k playoff`
Expected: PASS if Task 3 implementation is correct. If any fail, debug and fix.

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_tournament_loader.py
git commit -m "test: add playoff ingestion tests for seed-based alternating picks"
```

---

### Task 5: Ban summary queries

**Files:**
- Create: `tests/test_ban_queries.py`

- [ ] **Step 1: Write failing tests for ban summary**

Create `tests/test_ban_queries.py`:

```python
import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.tournament_loader import ingest_tournament
from cdm_stats.db.queries import get_ban_summary, get_team_id_by_abbr


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


MAPS_CSV = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

BANS_CSV = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""


def test_ban_summary_returns_bans_for_team(db):
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    elv_id = get_team_id_by_abbr(db, "ELV")
    alu_id = get_team_id_by_abbr(db, "ALU")
    summary = get_ban_summary(db, elv_id, alu_id)
    assert len(summary) == 3  # ELV banned 3 maps
    assert all(s["total_series"] >= 1 for s in summary)


def test_ban_summary_correct_counts(db):
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    alu_id = get_team_id_by_abbr(db, "ALU")
    elv_id = get_team_id_by_abbr(db, "ELV")
    summary = get_ban_summary(db, alu_id, elv_id)
    # ALU banned Summit, Firing Range, Standoff — each once
    ban_maps = {s["map_name"] for s in summary}
    assert ban_maps == {"Summit", "Firing Range", "Standoff"}
    assert all(s["ban_count"] == 1 for s in summary)


def test_ban_summary_empty_for_no_bans(db):
    """No bans between teams that haven't played tournament matches."""
    elv_id = get_team_id_by_abbr(db, "ELV")
    dvs_id = get_team_id_by_abbr(db, "DVS")
    summary = get_ban_summary(db, elv_id, dvs_id)
    assert summary == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ban_queries.py -v`
Expected: FAIL — get_ban_summary may not exist yet (it was defined in Task 2 step 7)

If the function was added in Task 2, tests should pass. If not, implement `get_ban_summary` in `src/cdm_stats/db/queries.py` now.

- [ ] **Step 3: Run tests to verify they pass**

Run: `uv run pytest tests/test_ban_queries.py -v`
Expected: PASS

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_ban_queries.py
git commit -m "test: add ban summary query tests"
```

---

### Task 6: Matchup prep export — add ban summary section

**Files:**
- Modify: `src/cdm_stats/export/excel.py`
- Modify: `tests/test_excel.py`

- [ ] **Step 1: Read current test_excel.py to understand test patterns**

Read `tests/test_excel.py` to understand existing patterns.

- [ ] **Step 2: Write failing test for ban summary in matchup prep**

Add to `tests/test_excel.py`:

```python
def test_matchup_prep_includes_ban_section_when_bans_exist(db_with_tournament_match):
    """When tournament/playoff bans exist between two teams, matchup prep includes ban summary."""
    conn = db_with_tournament_match
    from cdm_stats.db.queries import get_team_id_by_abbr
    from cdm_stats.export.excel import export_matchup_prep
    import tempfile, os
    from openpyxl import load_workbook

    elv_id = get_team_id_by_abbr(conn, "ELV")
    alu_id = get_team_id_by_abbr(conn, "ALU")

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        path = f.name
    try:
        export_matchup_prep(conn, elv_id, alu_id, path)
        wb = load_workbook(path)
        ws = wb.active
        # Find "Ban Summary" or "Head-to-Head" text in any cell
        all_values = [cell.value for row in ws.iter_rows() for cell in row if cell.value]
        assert any("Ban" in str(v) for v in all_values), "Expected ban summary section in matchup prep"
    finally:
        os.unlink(path)
```

This requires a `db_with_tournament_match` fixture. Add it to `tests/conftest.py`:

```python
@pytest.fixture
def db_with_tournament_match(db):
    """DB with schema migrated and one tournament match (ELV vs ALU) with bans."""
    import io
    from cdm_stats.db.schema import migrate
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    migrate(db)

    maps_csv = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

    bans_csv = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""

    ingest_tournament(db, io.StringIO(maps_csv), io.StringIO(bans_csv))
    return db
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_excel.py::test_matchup_prep_includes_ban_section_when_bans_exist -v`
Expected: FAIL — ban section not in export

- [ ] **Step 4: Add ban summary section to export_matchup_prep**

In `src/cdm_stats/export/excel.py`, at the end of `export_matchup_prep()`, before `wb.save()`:

1. Import `get_ban_summary` from queries
2. After the Elo footer, add a "Head-to-Head Tournament Data" section:

```python
from cdm_stats.db.queries import get_ban_summary

# ... at end of export_matchup_prep, after Elo section:

# Ban summary section
your_bans = get_ban_summary(conn, your_team_id, opp_team_id)
opp_bans = get_ban_summary(conn, opp_team_id, your_team_id)

if your_bans or opp_bans:
    ban_row = footer_row + 3
    ws.cell(row=ban_row, column=1, value="Head-to-Head Ban Data").font = Font(bold=True)
    ban_row += 1

    if your_bans:
        ws.cell(row=ban_row, column=1, value=f"{your_abbr} bans vs {opp_abbr}:")
        ban_strs = [f"{b['map_name']} {b['mode']} ({b['ban_count']}/{b['total_series']})"
                    for b in your_bans]
        ws.cell(row=ban_row, column=2, value=", ".join(ban_strs))
        ban_row += 1

    if opp_bans:
        ws.cell(row=ban_row, column=1, value=f"{opp_abbr} bans vs {your_abbr}:")
        ban_strs = [f"{b['map_name']} {b['mode']} ({b['ban_count']}/{b['total_series']})"
                    for b in opp_bans]
        ws.cell(row=ban_row, column=2, value=", ".join(ban_strs))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_excel.py -v`
Expected: PASS

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/cdm_stats/export/excel.py tests/test_excel.py tests/conftest.py
git commit -m "feat: add ban summary section to matchup prep export"
```

---

### Task 7: CLI command and conftest updates

**Files:**
- Modify: `main.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Add ingest-tournament CLI command**

In `main.py`, add:

```python
def cmd_ingest_tournament(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    from cdm_stats.db.schema import migrate

    conn = get_db()
    migrate(conn)
    with open(args.maps_csv) as mf, open(args.bans_csv) as bf:
        results = ingest_tournament(conn, mf, bf)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED: {r['match']} ({r.get('reason', '')})")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()
```

Add the subparser:

```python
p_ingest_t = sub.add_parser("ingest-tournament", help="Ingest tournament/playoff data from CSVs")
p_ingest_t.add_argument("maps_csv", help="Path to maps CSV file")
p_ingest_t.add_argument("bans_csv", help="Path to bans CSV file")
```

Add to the commands dict:

```python
"ingest-tournament": cmd_ingest_tournament,
```

- [ ] **Step 2: Update cmd_init to run migrate after create_tables**

In `main.py`, update `cmd_init`:

```python
def cmd_init(_args: argparse.Namespace) -> None:
    from cdm_stats.db.schema import create_tables, migrate
    from cdm_stats.ingestion.seed import seed_teams, seed_maps

    conn = get_db()
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    conn.close()
    print("Database initialized and seeded.")
```

- [ ] **Step 3: Update conftest.py db fixture to include migration**

In `tests/conftest.py`, update the `db` fixture:

```python
from cdm_stats.db.schema import create_tables, migrate

@pytest.fixture
def db():
    """Fresh in-memory DB with schema, migration, and seed data."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()
```

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add main.py tests/conftest.py
git commit -m "feat: add ingest-tournament CLI command and migrate on init"
```

---

### Task 8: Migrate existing database

**Files:**
- No code changes — runtime operation

- [ ] **Step 1: Back up the existing database**

```bash
cp data/cdl.db data/cdl.db.backup
```

- [ ] **Step 2: Run migration on existing database**

```bash
uv run python -c "
from main import get_db
from cdm_stats.db.schema import migrate
conn = get_db()
migrate(conn)
print('Migration complete')
conn.close()
"
```

- [ ] **Step 3: Verify migration**

```bash
uv run python -c "
from main import get_db
conn = get_db()
# Check schema version
v = conn.execute('PRAGMA user_version').fetchone()[0]
print(f'Schema version: {v}')
# Check match_format column
info = conn.execute('PRAGMA table_info(matches)').fetchall()
cols = [r[1] for r in info]
print(f'matches columns: {cols}')
# Check map_bans table exists
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print(f'Tables: {tables}')
# Check existing data preserved
count = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
print(f'Existing matches: {count}')
conn.close()
"
```

Expected: Schema version 1, match_format in columns, map_bans table exists, existing match count unchanged.

- [ ] **Step 4: Commit**

```bash
git add data/cdl.db
git commit -m "chore: migrate existing database to v1 schema"
```
