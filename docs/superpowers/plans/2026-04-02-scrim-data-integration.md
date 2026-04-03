# Scrim Data Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add scrim match results and player stats to the CDL analytics platform with separate tables, CSV ingestion, and two new dashboard tabs.

**Architecture:** Two new independent SQLite tables (`scrim_maps`, `scrim_player_stats`) with no schema changes to existing tournament tables. New `scrim_loader.py` for CSV ingestion, `queries_scrim.py` for aggregation queries, and two new Dash tabs (Scrim Performance, Player Stats). `opponent_id` FKs into the existing `teams` table.

**Tech Stack:** Python 3.11, SQLite, Dash + Plotly + dash-bootstrap-components (existing stack)

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/cdm_stats/db/schema.py` | Add `scrim_maps` and `scrim_player_stats` CREATE TABLE statements, bump schema version |
| Create | `src/cdm_stats/ingestion/scrim_loader.py` | CSV parsing and DB insertion for scrim team + player data |
| Create | `src/cdm_stats/db/queries_scrim.py` | All scrim-related SQL queries (win/loss, map breakdown, player stats, weekly trends) |
| Create | `src/cdm_stats/dashboard/tabs/scrim_performance.py` | Scrim Performance dashboard tab |
| Create | `src/cdm_stats/dashboard/tabs/player_stats.py` | Player Stats dashboard tab |
| Modify | `src/cdm_stats/dashboard/app.py` | Register two new tabs and their callbacks |
| Modify | `main.py` | Add `ingest-scrims-team` and `ingest-scrims-players` CLI commands |
| Create | `tests/test_scrim_loader.py` | Tests for scrim CSV ingestion |
| Create | `tests/test_queries_scrim.py` | Tests for scrim query functions |
| Create | `tests/test_dashboard_scrim.py` | Tests for both new dashboard tabs |

---

### Task 1: Schema — Add Scrim Tables

**Files:**
- Modify: `src/cdm_stats/db/schema.py`
- Test: `tests/test_schema.py`

- [ ] **Step 1: Write failing test for scrim_maps table creation**

Add to `tests/test_schema.py`:

```python
def test_scrim_maps_table_exists(db):
    """scrim_maps table should be created by create_tables."""
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='scrim_maps'"
    ).fetchall()
    assert len(rows) == 1


def test_scrim_player_stats_table_exists(db):
    """scrim_player_stats table should be created by create_tables."""
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='scrim_player_stats'"
    ).fetchall()
    assert len(rows) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_schema.py::test_scrim_maps_table_exists tests/test_schema.py::test_scrim_player_stats_table_exists -v`
Expected: FAIL — tables don't exist yet

- [ ] **Step 3: Add scrim tables to schema.py**

In `src/cdm_stats/db/schema.py`, bump version and append two new DDL strings to the `TABLES` list:

Change `SCHEMA_VERSION = 2` to `SCHEMA_VERSION = 3`.

Append to `TABLES`:

```python
    """
    CREATE TABLE IF NOT EXISTS scrim_maps (
        scrim_map_id   INTEGER PRIMARY KEY,
        scrim_date     DATE NOT NULL,
        week           INTEGER NOT NULL,
        opponent_id    INTEGER NOT NULL REFERENCES teams(team_id),
        map_name       TEXT NOT NULL,
        mode           TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
        game_number    INTEGER NOT NULL DEFAULT 1,
        our_score      INTEGER NOT NULL,
        opponent_score INTEGER NOT NULL,
        result         TEXT NOT NULL CHECK(result IN ('W', 'L'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scrim_player_stats (
        stat_id      INTEGER PRIMARY KEY,
        scrim_map_id INTEGER NOT NULL REFERENCES scrim_maps(scrim_map_id),
        player_name  TEXT NOT NULL,
        kills        INTEGER NOT NULL,
        deaths       INTEGER NOT NULL,
        assists      INTEGER NOT NULL,
        UNIQUE(scrim_map_id, player_name)
    )
    """,
```

Add migration logic in `migrate()` — after the `if version < 2:` block, add:

```python
    if version < 3:
        conn.execute("""CREATE TABLE IF NOT EXISTS scrim_maps (
            scrim_map_id   INTEGER PRIMARY KEY,
            scrim_date     DATE NOT NULL,
            week           INTEGER NOT NULL,
            opponent_id    INTEGER NOT NULL REFERENCES teams(team_id),
            map_name       TEXT NOT NULL,
            mode           TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
            game_number    INTEGER NOT NULL DEFAULT 1,
            our_score      INTEGER NOT NULL,
            opponent_score INTEGER NOT NULL,
            result         TEXT NOT NULL CHECK(result IN ('W', 'L'))
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scrim_player_stats (
            stat_id      INTEGER PRIMARY KEY,
            scrim_map_id INTEGER NOT NULL REFERENCES scrim_maps(scrim_map_id),
            player_name  TEXT NOT NULL,
            kills        INTEGER NOT NULL,
            deaths       INTEGER NOT NULL,
            assists      INTEGER NOT NULL,
            UNIQUE(scrim_map_id, player_name)
        )""")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_schema.py -v`
Expected: ALL PASS (including the two new tests)

- [ ] **Step 5: Run full test suite to check nothing broke**

Run: `uv run pytest -v`
Expected: ALL 134+ tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/cdm_stats/db/schema.py tests/test_schema.py
git commit -m "feat: add scrim_maps and scrim_player_stats tables to schema"
```

---

### Task 2: Scrim Team CSV Ingestion

**Files:**
- Create: `src/cdm_stats/ingestion/scrim_loader.py`
- Create: `tests/test_scrim_loader.py`

- [ ] **Step 1: Write failing tests for team CSV ingestion**

Create `tests/test_scrim_loader.py`:

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


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-10,1,DVS,Raid,Control,3-1,W
2026-03-10,1,OUG,Hacienda,HP,180-250,L"""


def test_ingest_scrims_team_basic(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    results = ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    assert len(results) == 4
    assert all(r["status"] == "ok" for r in results)

    rows = db.execute("SELECT * FROM scrim_maps").fetchall()
    assert len(rows) == 4


def test_ingest_scrims_team_scores(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))

    row = db.execute(
        "SELECT our_score, opponent_score, result FROM scrim_maps WHERE map_name = 'Tunisia'"
    ).fetchone()
    assert row == (6, 3, "W")

    row = db.execute(
        "SELECT our_score, opponent_score, result FROM scrim_maps WHERE map_name = 'Hacienda'"
    ).fetchone()
    assert row == (180, 250, "L")


def test_ingest_scrims_team_opponent_fk(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))

    row = db.execute(
        """SELECT t.abbreviation FROM scrim_maps s
           JOIN teams t ON s.opponent_id = t.team_id
           WHERE s.map_name = 'Tunisia'"""
    ).fetchone()
    assert row[0] == "DVS"


def test_ingest_scrims_team_idempotent(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    results = ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    assert all(r["status"] == "skipped" for r in results)

    rows = db.execute("SELECT * FROM scrim_maps").fetchall()
    assert len(rows) == 4


def test_ingest_scrims_team_game_number(db):
    """Same map+mode+opponent+date played twice gets sequential game_numbers."""
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Tunisia,SnD,4-6,L"""
    ingest_scrims_team(db, io.StringIO(csv))

    rows = db.execute(
        "SELECT game_number, our_score, result FROM scrim_maps ORDER BY game_number"
    ).fetchall()
    assert rows == [(1, 6, "W"), (2, 4, "L")]


def test_ingest_scrims_team_bad_opponent(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,BADTEAM,Tunisia,SnD,6-3,W"""
    results = ingest_scrims_team(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert "opponent" in results[0]["errors"].lower()


def test_ingest_scrims_team_score_result_mismatch(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,3-6,W"""
    results = ingest_scrims_team(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert "result" in results[0]["errors"].lower() or "score" in results[0]["errors"].lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scrim_loader.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement ingest_scrims_team**

Create `src/cdm_stats/ingestion/scrim_loader.py`:

```python
import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import get_team_id_by_abbr


def _parse_score(score_str: str) -> tuple[int, int]:
    """Parse 'X-Y' score string into (our_score, opponent_score)."""
    parts = score_str.strip().split("-")
    return int(parts[0]), int(parts[1])


def _validate_result(our_score: int, opp_score: int, result: str) -> bool:
    """Check that result matches scores."""
    if result == "W":
        return our_score > opp_score
    return our_score < opp_score


def ingest_scrims_team(conn: sqlite3.Connection, file: IO) -> list[dict]:
    """Ingest scrim team-level CSV. Returns list of result dicts per row."""
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) group
    game_counts: dict[tuple, int] = {}

    rows_to_insert = []
    for row in reader:
        date = row["Date"].strip()
        week = int(row["Week"].strip())
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        score_str = row["Score"].strip()
        result = row["Result"].strip()

        desc = f"{date} vs {opponent_abbr} {map_name} {mode}"

        # Validate opponent
        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if not opponent_id:
            results.append({"status": "error", "row": desc, "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        # Parse and validate score
        try:
            our_score, opp_score = _parse_score(score_str)
        except (ValueError, IndexError):
            results.append({"status": "error", "row": desc, "errors": f"Invalid score format: {score_str}"})
            continue

        if not _validate_result(our_score, opp_score, result):
            results.append({"status": "error", "row": desc, "errors": f"Score {score_str} does not match result {result}"})
            continue

        if mode not in ("SnD", "HP", "Control"):
            results.append({"status": "error", "row": desc, "errors": f"Invalid mode: {mode}"})
            continue

        # Assign game_number
        key = (date, opponent_id, map_name, mode)
        game_counts[key] = game_counts.get(key, 0) + 1
        game_number = game_counts[key]

        rows_to_insert.append({
            "date": date, "week": week, "opponent_id": opponent_id,
            "map_name": map_name, "mode": mode, "game_number": game_number,
            "our_score": our_score, "opponent_score": opp_score,
            "result": result, "desc": desc,
        })

    for r in rows_to_insert:
        # Duplicate check
        existing = conn.execute(
            """SELECT scrim_map_id FROM scrim_maps
               WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?
                 AND mode = ? AND game_number = ?""",
            (r["date"], r["opponent_id"], r["map_name"], r["mode"], r["game_number"]),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": r["desc"]})
            continue

        conn.execute(
            """INSERT INTO scrim_maps
               (scrim_date, week, opponent_id, map_name, mode, game_number,
                our_score, opponent_score, result)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["date"], r["week"], r["opponent_id"], r["map_name"], r["mode"],
             r["game_number"], r["our_score"], r["opponent_score"], r["result"]),
        )
        results.append({"status": "ok", "row": r["desc"]})

    conn.commit()
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_scrim_loader.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/scrim_loader.py tests/test_scrim_loader.py
git commit -m "feat: add scrim team CSV ingestion"
```

---

### Task 3: Scrim Player CSV Ingestion

**Files:**
- Modify: `src/cdm_stats/ingestion/scrim_loader.py`
- Modify: `tests/test_scrim_loader.py`

- [ ] **Step 1: Write failing tests for player CSV ingestion**

Append to `tests/test_scrim_loader.py`:

```python
PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Player1,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Player2,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Player3,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Player4,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Player5,12,20,4"""


def test_ingest_scrims_players_basic(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    results = ingest_scrims_players(db, io.StringIO(PLAYER_CSV))
    assert len(results) == 5
    assert all(r["status"] == "ok" for r in results)

    rows = db.execute("SELECT * FROM scrim_player_stats").fetchall()
    assert len(rows) == 5


def test_ingest_scrims_players_values(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    ingest_scrims_players(db, io.StringIO(PLAYER_CSV))

    row = db.execute(
        "SELECT kills, deaths, assists FROM scrim_player_stats WHERE player_name = 'Player1'"
    ).fetchone()
    assert row == (20, 15, 5)


def test_ingest_scrims_players_idempotent(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    ingest_scrims_players(db, io.StringIO(PLAYER_CSV))
    results = ingest_scrims_players(db, io.StringIO(PLAYER_CSV))
    assert all(r["status"] == "skipped" for r in results)

    rows = db.execute("SELECT * FROM scrim_player_stats").fetchall()
    assert len(rows) == 5


def test_ingest_scrims_players_no_matching_map(db):
    """Player CSV row with no matching scrim_maps row should error."""
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_players
    csv = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Player1,20,15,5"""
    results = ingest_scrims_players(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert "no matching" in results[0]["errors"].lower()


def test_ingest_scrims_players_game_number(db):
    """Player stats link to correct game when same map played twice."""
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players
    team_csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Tunisia,SnD,4-6,L"""
    ingest_scrims_team(db, io.StringIO(team_csv))

    player_csv = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Player1,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Player2,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Player3,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Player4,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Player5,12,20,4
2026-03-10,1,DVS,Tunisia,SnD,Player1,10,20,3
2026-03-10,1,DVS,Tunisia,SnD,Player2,14,16,5
2026-03-10,1,DVS,Tunisia,SnD,Player3,8,22,2
2026-03-10,1,DVS,Tunisia,SnD,Player4,16,14,7
2026-03-10,1,DVS,Tunisia,SnD,Player5,9,18,1"""
    ingest_scrims_players(db, io.StringIO(player_csv))

    # Game 1 player1 should have 20 kills
    game1_id = db.execute(
        "SELECT scrim_map_id FROM scrim_maps WHERE game_number = 1"
    ).fetchone()[0]
    row = db.execute(
        "SELECT kills FROM scrim_player_stats WHERE scrim_map_id = ? AND player_name = 'Player1'",
        (game1_id,),
    ).fetchone()
    assert row[0] == 20

    # Game 2 player1 should have 10 kills
    game2_id = db.execute(
        "SELECT scrim_map_id FROM scrim_maps WHERE game_number = 2"
    ).fetchone()[0]
    row = db.execute(
        "SELECT kills FROM scrim_player_stats WHERE scrim_map_id = ? AND player_name = 'Player1'",
        (game2_id,),
    ).fetchone()
    assert row[0] == 10
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scrim_loader.py::test_ingest_scrims_players_basic -v`
Expected: FAIL — `ingest_scrims_players` doesn't exist

- [ ] **Step 3: Implement ingest_scrims_players**

Append to `src/cdm_stats/ingestion/scrim_loader.py`:

```python
def ingest_scrims_players(conn: sqlite3.Connection, file: IO) -> list[dict]:
    """Ingest scrim player-level CSV. Team CSV must be ingested first."""
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) to match team CSV ordering
    game_counts: dict[tuple, int] = {}
    # Track which (scrim_map_id, player) combos we've seen in this batch
    seen_in_batch: dict[tuple, int] = {}

    for row in reader:
        date = row["Date"].strip()
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())

        desc = f"{date} {map_name} {mode} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if not opponent_id:
            results.append({"status": "error", "row": desc, "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        # Determine game_number — same logic as team CSV: sequential per group
        key = (date, opponent_id, map_name, mode)
        player_key = (date, opponent_id, map_name, mode, player_name)

        # If we've already seen this player for this key at the current game_number,
        # that means we've moved to the next game
        current_game = game_counts.get(key, 1)
        batch_key = (key, current_game, player_name)
        if batch_key in seen_in_batch:
            game_counts[key] = current_game + 1
            current_game = game_counts[key]

        game_number = current_game
        seen_in_batch[(key, game_number, player_name)] = True

        # Find matching scrim_maps row
        scrim_map = conn.execute(
            """SELECT scrim_map_id FROM scrim_maps
               WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?
                 AND mode = ? AND game_number = ?""",
            (date, opponent_id, map_name, mode, game_number),
        ).fetchone()

        if not scrim_map:
            results.append({"status": "error", "row": desc, "errors": "No matching scrim map found"})
            continue

        scrim_map_id = scrim_map[0]

        # Duplicate check
        existing = conn.execute(
            "SELECT stat_id FROM scrim_player_stats WHERE scrim_map_id = ? AND player_name = ?",
            (scrim_map_id, player_name),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": desc})
            continue

        conn.execute(
            """INSERT INTO scrim_player_stats
               (scrim_map_id, player_name, kills, deaths, assists)
               VALUES (?, ?, ?, ?, ?)""",
            (scrim_map_id, player_name, kills, deaths, assists),
        )
        results.append({"status": "ok", "row": desc})

    conn.commit()
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_scrim_loader.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/scrim_loader.py tests/test_scrim_loader.py
git commit -m "feat: add scrim player CSV ingestion"
```

---

### Task 4: CLI Commands

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Add CLI commands for scrim ingestion**

In `main.py`, add two new command handler functions:

```python
def cmd_ingest_scrims_team(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_scrims_team(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['row']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['row']}")
        else:
            print(f"  ERROR: {r['row']}: {r['errors']}")

    conn.close()


def cmd_ingest_scrims_players(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_players

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_scrims_players(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['row']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['row']}")
        else:
            print(f"  ERROR: {r['row']}: {r['errors']}")

    conn.close()
```

In the `main()` function, add parser entries after the existing `sub.add_parser("backfill", ...)`:

```python
    p_scrim_team = sub.add_parser("ingest-scrims-team", help="Ingest scrim team-level CSV")
    p_scrim_team.add_argument("csv_file", help="Path to scrim team CSV file")

    p_scrim_players = sub.add_parser("ingest-scrims-players", help="Ingest scrim player-level CSV")
    p_scrim_players.add_argument("csv_file", help="Path to scrim player CSV file")
```

Add to the `commands` dict:

```python
    commands = {
        "init": cmd_init,
        "ingest": cmd_ingest,
        "ingest-tournament": cmd_ingest_tournament,
        "ingest-scrims-team": cmd_ingest_scrims_team,
        "ingest-scrims-players": cmd_ingest_scrims_players,
        "backfill": cmd_backfill,
    }
```

- [ ] **Step 2: Verify CLI help works**

Run: `uv run python main.py ingest-scrims-team --help`
Expected: Shows help with `csv_file` argument

Run: `uv run python main.py ingest-scrims-players --help`
Expected: Shows help with `csv_file` argument

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "feat: add CLI commands for scrim data ingestion"
```

---

### Task 5: Scrim Query Functions

**Files:**
- Create: `src/cdm_stats/db/queries_scrim.py`
- Create: `tests/test_queries_scrim.py`

- [ ] **Step 1: Write failing tests for scrim queries**

Create `tests/test_queries_scrim.py`:

```python
import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-10,1,DVS,Raid,Control,3-1,W
2026-03-17,2,OUG,Tunisia,SnD,4-6,L
2026-03-17,2,OUG,Summit,HP,230-250,L
2026-03-17,2,OUG,Hacienda,HP,250-180,W"""


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Alpha,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Bravo,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Charlie,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Delta,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Echo,12,20,4
2026-03-17,2,OUG,Tunisia,SnD,Alpha,10,20,3
2026-03-17,2,OUG,Tunisia,SnD,Bravo,14,16,5
2026-03-17,2,OUG,Tunisia,SnD,Charlie,8,22,2
2026-03-17,2,OUG,Tunisia,SnD,Delta,16,14,7
2026-03-17,2,OUG,Tunisia,SnD,Echo,9,18,1"""


@pytest.fixture
def scrim_db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_scrims_team(conn, io.StringIO(TEAM_CSV))
    ingest_scrims_players(conn, io.StringIO(PLAYER_CSV))
    yield conn
    conn.close()


def test_scrim_win_loss_all(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db)
    assert result["wins"] == 4
    assert result["losses"] == 2
    assert result["win_pct"] == pytest.approx(66.67, abs=0.01)


def test_scrim_win_loss_by_mode(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db, mode="SnD")
    assert result["wins"] == 1
    assert result["losses"] == 1


def test_scrim_win_loss_by_week(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db, week_range=(1, 1))
    assert result["wins"] == 3
    assert result["losses"] == 0


def test_scrim_map_breakdown(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_map_breakdown
    rows = scrim_map_breakdown(scrim_db)
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 1
    assert tunisia["played"] == 2


def test_scrim_map_breakdown_by_mode(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_map_breakdown
    rows = scrim_map_breakdown(scrim_db, mode="HP")
    assert len(rows) == 2  # Summit and Hacienda
    summit = next(r for r in rows if r["map_name"] == "Summit")
    assert summit["wins"] == 1
    assert summit["losses"] == 1


def test_scrim_weekly_trend(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_weekly_trend
    rows = scrim_weekly_trend(scrim_db)
    assert len(rows) == 2
    week1 = next(r for r in rows if r["week"] == 1)
    assert week1["win_pct"] == 100.0
    week2 = next(r for r in rows if r["week"] == 2)
    assert week2["win_pct"] == pytest.approx(33.33, abs=0.01)


def test_player_summary_all(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    rows = player_summary(scrim_db)
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 30  # 20 + 10
    assert alpha["deaths"] == 35  # 15 + 20
    assert alpha["assists"] == 8  # 5 + 3
    assert alpha["kd"] == pytest.approx(30 / 35, abs=0.01)


def test_player_summary_filtered(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    rows = player_summary(scrim_db, player="Alpha", week_range=(1, 1))
    assert len(rows) == 1
    assert rows[0]["kills"] == 20


def test_player_weekly_trend(scrim_db):
    from cdm_stats.db.queries_scrim import player_weekly_trend
    rows = player_weekly_trend(scrim_db, player="Alpha")
    assert len(rows) == 2
    week1 = next(r for r in rows if r["week"] == 1)
    assert week1["kd"] == pytest.approx(20 / 15, abs=0.01)


def test_player_map_breakdown(scrim_db):
    from cdm_stats.db.queries_scrim import player_map_breakdown
    rows = player_map_breakdown(scrim_db, player="Alpha")
    assert len(rows) == 1  # Alpha only played Tunisia
    assert rows[0]["map_name"] == "Tunisia"
    assert rows[0]["games"] == 2
    assert rows[0]["avg_kills"] == 15.0  # (20+10)/2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_queries_scrim.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement scrim query functions**

Create `src/cdm_stats/db/queries_scrim.py`:

```python
import sqlite3


def scrim_win_loss(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> dict:
    """Return W, L, Win% for scrims with optional filters."""
    conditions = []
    params: list = []

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("map_name = ?")
        params.append(map_name)
    if week_range:
        conditions.append("week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    row = conn.execute(
        f"""SELECT
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                COUNT(*) as total
            FROM scrim_maps{where}""",
        params,
    ).fetchone()

    wins, losses, total = row[0] or 0, row[1] or 0, row[2] or 0
    win_pct = round(wins / total * 100, 2) if total > 0 else 0.0
    return {"wins": wins, "losses": losses, "total": total, "win_pct": win_pct}


def scrim_map_breakdown(
    conn: sqlite3.Connection,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return per-map: played, W, L, Win%, avg scores."""
    conditions = []
    params: list = []

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT map_name, mode,
                   COUNT(*) as played,
                   SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                   ROUND(AVG(our_score), 1) as avg_our,
                   ROUND(AVG(opponent_score), 1) as avg_opp
            FROM scrim_maps{where}
            GROUP BY map_name, mode
            ORDER BY mode, map_name""",
        params,
    ).fetchall()

    return [
        {
            "map_name": r[0], "mode": r[1], "played": r[2],
            "wins": r[3], "losses": r[4],
            "win_pct": round(r[3] / r[2] * 100, 2) if r[2] > 0 else 0.0,
            "avg_our_score": r[5], "avg_opp_score": r[6],
        }
        for r in rows
    ]


def scrim_weekly_trend(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
) -> list[dict]:
    """Return per-week win rate for trend chart."""
    conditions = []
    params: list = []

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("map_name = ?")
        params.append(map_name)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT week,
                   COUNT(*) as played,
                   SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins
            FROM scrim_maps{where}
            GROUP BY week
            ORDER BY week""",
        params,
    ).fetchall()

    return [
        {
            "week": r[0], "played": r[1], "wins": r[2],
            "win_pct": round(r[2] / r[1] * 100, 2) if r[1] > 0 else 0.0,
        }
        for r in rows
    ]


def player_summary(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return per-player totals: kills, deaths, assists, K/D."""
    conditions = []
    params: list = []

    if player:
        conditions.append("sp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("sm.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("sm.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT sp.player_name,
                   SUM(sp.kills) as kills,
                   SUM(sp.deaths) as deaths,
                   SUM(sp.assists) as assists,
                   COUNT(*) as games
            FROM scrim_player_stats sp
            JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
            {where}
            GROUP BY sp.player_name
            ORDER BY sp.player_name""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "kills": r[1], "deaths": r[2], "assists": r[3],
            "games": r[4],
            "kd": round(r[1] / r[2], 2) if r[2] > 0 else 0.0,
        }
        for r in rows
    ]


def player_weekly_trend(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
) -> list[dict]:
    """Return per-week K/D per player for trend chart."""
    conditions = []
    params: list = []

    if player:
        conditions.append("sp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("sm.mode = ?")
        params.append(mode)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT sp.player_name, sm.week,
                   SUM(sp.kills) as kills,
                   SUM(sp.deaths) as deaths
            FROM scrim_player_stats sp
            JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
            {where}
            GROUP BY sp.player_name, sm.week
            ORDER BY sp.player_name, sm.week""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "week": r[1],
            "kills": r[2], "deaths": r[3],
            "kd": round(r[2] / r[3], 2) if r[3] > 0 else 0.0,
        }
        for r in rows
    ]


def player_map_breakdown(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return per-map player averages."""
    conditions = []
    params: list = []

    if player:
        conditions.append("sp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("sm.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("sm.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT sm.map_name, sm.mode,
                   COUNT(*) as games,
                   ROUND(AVG(sp.kills), 1) as avg_kills,
                   ROUND(AVG(sp.deaths), 1) as avg_deaths,
                   ROUND(AVG(sp.assists), 1) as avg_assists,
                   ROUND(AVG(CAST(sp.kills AS REAL) / NULLIF(sp.deaths, 0)), 2) as avg_kd,
                   ROUND(AVG(CAST(sp.kills AS REAL) / NULLIF(sp.kills + sp.deaths + sp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM scrim_player_stats sp
            JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
            {where}
            GROUP BY sm.map_name, sm.mode
            ORDER BY sm.mode, sm.map_name""",
        params,
    ).fetchall()

    return [
        {
            "map_name": r[0], "mode": r[1], "games": r[2],
            "avg_kills": r[3], "avg_deaths": r[4], "avg_assists": r[5],
            "avg_kd": r[6] or 0.0, "avg_pos_eng_pct": r[7] or 0.0,
        }
        for r in rows
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_queries_scrim.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/queries_scrim.py tests/test_queries_scrim.py
git commit -m "feat: add scrim query functions"
```

---

### Task 6: Scrim Performance Dashboard Tab

**Files:**
- Create: `src/cdm_stats/dashboard/tabs/scrim_performance.py`
- Create: `tests/test_dashboard_scrim.py`

- [ ] **Step 1: Write failing test for scrim performance tab**

Create `tests/test_dashboard_scrim.py`:

```python
import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-17,2,OUG,Tunisia,SnD,4-6,L"""


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Alpha,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Bravo,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Charlie,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Delta,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Echo,12,20,4"""


@pytest.fixture
def scrim_db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_scrims_team(conn, io.StringIO(TEAM_CSV))
    ingest_scrims_players(conn, io.StringIO(PLAYER_CSV))
    yield conn
    conn.close()


def test_scrim_performance_build_summary(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_summary_data
    data = _build_summary_data(scrim_db)
    assert data["overall"]["wins"] == 2
    assert data["overall"]["losses"] == 1
    assert "SnD" in data["by_mode"]
    assert "HP" in data["by_mode"]


def test_scrim_performance_build_map_table(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_map_table_data
    rows = _build_map_table_data(scrim_db)
    assert len(rows) == 2  # Tunisia and Summit
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 1


def test_scrim_performance_build_trend(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_trend_data
    rows = _build_trend_data(scrim_db)
    assert len(rows) == 2
    assert rows[0]["week"] == 1
    assert rows[0]["win_pct"] == 100.0


def test_scrim_performance_layout():
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    result = layout()
    assert result is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_scrim.py::test_scrim_performance_build_summary -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement scrim performance tab**

Create `src/cdm_stats/dashboard/tabs/scrim_performance.py`:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, MODE_COLORS
from cdm_stats.db.queries_scrim import (
    scrim_win_loss, scrim_map_breakdown, scrim_weekly_trend,
)


def _build_summary_data(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> dict:
    """Build summary cards data."""
    overall = scrim_win_loss(conn, mode=mode, map_name=map_name, week_range=week_range)
    by_mode = {}
    for m in ("SnD", "HP", "Control"):
        result = scrim_win_loss(conn, mode=m, map_name=map_name, week_range=week_range)
        if result["total"] > 0:
            by_mode[m] = result
    return {"overall": overall, "by_mode": by_mode}


def _build_map_table_data(
    conn: sqlite3.Connection,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Build map breakdown table data."""
    return scrim_map_breakdown(conn, mode=mode, week_range=week_range)


def _build_trend_data(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
) -> list[dict]:
    """Build weekly trend data."""
    return scrim_weekly_trend(conn, mode=mode, map_name=map_name)


def _summary_card(title: str, wl: dict, color: str) -> dbc.Card:
    total = wl["wins"] + wl["losses"]
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, style={"color": COLORS["muted"]}),
            html.H3(
                f"{wl['win_pct']:.0f}%",
                style={"color": color, "marginBottom": "0"},
            ),
            html.Small(
                f"{wl['wins']}W – {wl['losses']}L ({total} maps)",
                style={"color": COLORS["text"]},
            ),
        ]),
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
    )


def _trend_figure(trend_data: list[dict]) -> go.Figure:
    fig = go.Figure()
    if trend_data:
        fig.add_trace(go.Scatter(
            x=[f"W{d['week']}" for d in trend_data],
            y=[d["win_pct"] for d in trend_data],
            mode="lines+markers",
            name="Win %",
            marker={"size": 8, "color": COLORS["win"]},
            line={"width": 2, "color": COLORS["win"]},
            text=[f"W{d['week']}: {d['win_pct']:.0f}% ({d['wins']}/{d['played']})" for d in trend_data],
            hovertemplate="%{text}<extra></extra>",
        ))
    fig.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.4)
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 50, "r": 20, "t": 30, "b": 50},
        height=350,
        yaxis={"title": "Win %", "range": [0, 105], "gridcolor": COLORS["border"]},
        xaxis={"title": "Week", "gridcolor": COLORS["border"]},
        showlegend=False,
    )
    return fig


def _get_available_weeks(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute("SELECT DISTINCT week FROM scrim_maps ORDER BY week").fetchall()
    return [r[0] for r in rows]


def _get_available_maps(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT map_name FROM scrim_maps ORDER BY map_name"
    ).fetchall()
    return [r[0] for r in rows]


def layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.Label("Mode", style={"color": COLORS["text"]}),
                dcc.Dropdown(
                    id="scrim-mode-filter",
                    options=[{"label": "All", "value": "All"}]
                        + [{"label": m, "value": m} for m in ("SnD", "HP", "Control")],
                    value="All",
                    clearable=False,
                    style={"backgroundColor": COLORS["card_bg"]},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Map", style={"color": COLORS["text"]}),
                dcc.Dropdown(
                    id="scrim-map-filter",
                    options=[{"label": "All", "value": "All"}],
                    value="All",
                    clearable=False,
                    style={"backgroundColor": COLORS["card_bg"]},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Weeks", style={"color": COLORS["text"]}),
                dcc.RangeSlider(
                    id="scrim-week-slider",
                    min=1, max=13, step=1, value=[1, 13],
                    marks={i: f"W{i}" for i in range(1, 14)},
                ),
            ], width=8),
        ], className="mb-3"),
        html.Div(id="scrim-summary-cards"),
        html.H5("Map Breakdown", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        html.Div(id="scrim-map-table"),
        html.H5("Weekly Trend", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        dcc.Graph(id="scrim-trend-chart"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("scrim-map-filter", "options"),
        Input("scrim-mode-filter", "value"),
    )
    def update_map_options(mode):
        conn = get_db()
        if mode and mode != "All":
            rows = conn.execute(
                "SELECT DISTINCT map_name FROM scrim_maps WHERE mode = ? ORDER BY map_name",
                (mode,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT map_name FROM scrim_maps ORDER BY map_name"
            ).fetchall()
        conn.close()
        return [{"label": "All", "value": "All"}] + [{"label": r[0], "value": r[0]} for r in rows]

    @app.callback(
        Output("scrim-week-slider", "min"),
        Output("scrim-week-slider", "max"),
        Output("scrim-week-slider", "marks"),
        Output("scrim-week-slider", "value"),
        Input("scrim-mode-filter", "value"),
    )
    def update_week_slider(_mode):
        conn = get_db()
        weeks = _get_available_weeks(conn)
        conn.close()
        if not weeks:
            return 1, 1, {1: "W1"}, [1, 1]
        mn, mx = min(weeks), max(weeks)
        marks = {w: f"W{w}" for w in weeks}
        return mn, mx, marks, [mn, mx]

    @app.callback(
        Output("scrim-summary-cards", "children"),
        Output("scrim-map-table", "children"),
        Output("scrim-trend-chart", "figure"),
        Input("scrim-mode-filter", "value"),
        Input("scrim-map-filter", "value"),
        Input("scrim-week-slider", "value"),
    )
    def update_scrim_tab(mode, map_name, week_range):
        conn = get_db()
        mode_val = mode if mode != "All" else None
        map_val = map_name if map_name != "All" else None
        wr = tuple(week_range) if week_range else None

        # Summary cards
        summary = _build_summary_data(conn, mode=mode_val, map_name=map_val, week_range=wr)
        cards = [
            dbc.Col(_summary_card(
                "Overall", summary["overall"],
                COLORS["win"] if summary["overall"]["win_pct"] >= 50 else COLORS["loss"],
            ), width=3),
        ]
        for m, wl in summary["by_mode"].items():
            cards.append(dbc.Col(_summary_card(
                m, wl, MODE_COLORS.get(m, COLORS["text"]),
            ), width=3))
        card_row = dbc.Row(cards)

        # Map table
        map_data = _build_map_table_data(conn, mode=mode_val, week_range=wr)
        if map_data:
            header = html.Thead(html.Tr([
                html.Th("Map"), html.Th("Mode"), html.Th("Played"),
                html.Th("W"), html.Th("L"), html.Th("Win%"),
                html.Th("Avg Score"), html.Th("Avg Opp Score"),
            ]))
            body_rows = []
            for d in map_data:
                win_color = COLORS["win"] if d["win_pct"] >= 60 else (COLORS["loss"] if d["win_pct"] <= 40 else COLORS["neutral"])
                body_rows.append(html.Tr([
                    html.Td(d["map_name"]),
                    html.Td(d["mode"], style={"color": MODE_COLORS.get(d["mode"], COLORS["text"])}),
                    html.Td(str(d["played"])),
                    html.Td(str(d["wins"])),
                    html.Td(str(d["losses"])),
                    html.Td(f"{d['win_pct']:.0f}%", style={"color": win_color, "fontWeight": "600"}),
                    html.Td(f"{d['avg_our_score']:.0f}"),
                    html.Td(f"{d['avg_opp_score']:.0f}"),
                ]))
            table = dbc.Table(
                [header, html.Tbody(body_rows)],
                bordered=True, dark=True, hover=True, size="sm",
                style={"backgroundColor": COLORS["card_bg"]},
            )
        else:
            table = html.P("No scrim data found.", style={"color": COLORS["muted"]})

        # Trend chart
        trend_data = _build_trend_data(conn, mode=mode_val, map_name=map_val)
        fig = _trend_figure(trend_data)

        conn.close()
        return card_row, table, fig
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dashboard_scrim.py::test_scrim_performance_build_summary tests/test_dashboard_scrim.py::test_scrim_performance_build_map_table tests/test_dashboard_scrim.py::test_scrim_performance_build_trend tests/test_dashboard_scrim.py::test_scrim_performance_layout -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/scrim_performance.py tests/test_dashboard_scrim.py
git commit -m "feat: add Scrim Performance dashboard tab"
```

---

### Task 7: Player Stats Dashboard Tab

**Files:**
- Create: `src/cdm_stats/dashboard/tabs/player_stats.py`
- Modify: `tests/test_dashboard_scrim.py`

- [ ] **Step 1: Write failing tests for player stats tab**

Append to `tests/test_dashboard_scrim.py`:

```python
def test_player_stats_build_summary(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_player_cards_data
    data = _build_player_cards_data(scrim_db)
    assert len(data) == 5
    alpha = next(d for d in data if d["player_name"] == "Alpha")
    assert alpha["kills"] == 20
    assert alpha["deaths"] == 15


def test_player_stats_build_trend(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_kd_trend_data
    rows = _build_kd_trend_data(scrim_db)
    assert len(rows) >= 1
    alpha_w1 = next(r for r in rows if r["player_name"] == "Alpha" and r["week"] == 1)
    assert alpha_w1["kd"] == pytest.approx(20 / 15, abs=0.01)


def test_player_stats_build_map_table(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_player_map_data
    rows = _build_player_map_data(scrim_db, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["map_name"] == "Tunisia"


def test_player_stats_layout():
    from cdm_stats.dashboard.tabs.player_stats import layout
    result = layout()
    assert result is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_scrim.py::test_player_stats_build_summary -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement player stats tab**

Create `src/cdm_stats/dashboard/tabs/player_stats.py`:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, MODE_COLORS
from cdm_stats.db.queries_scrim import (
    player_summary, player_weekly_trend, player_map_breakdown,
)

PLAYER_COLORS = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
]


def _build_player_cards_data(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Build per-player summary card data."""
    return player_summary(conn, player=player, mode=mode, week_range=week_range)


def _build_kd_trend_data(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
) -> list[dict]:
    """Build weekly K/D trend data."""
    return player_weekly_trend(conn, player=player, mode=mode)


def _build_player_map_data(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Build per-map breakdown for a player."""
    return player_map_breakdown(conn, player=player, mode=mode, week_range=week_range)


def _player_card(data: dict, color: str) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.H5(data["player_name"], style={"color": color, "marginBottom": "4px"}),
            html.H3(
                f"{data['kd']:.2f}",
                style={"color": COLORS["text"], "marginBottom": "0"},
            ),
            html.Small("K/D", style={"color": COLORS["muted"]}),
            html.Div([
                html.Span(f"{data['kills']}K ", style={"color": COLORS["win"]}),
                html.Span(f"{data['deaths']}D ", style={"color": COLORS["loss"]}),
                html.Span(f"{data['assists']}A", style={"color": COLORS["neutral"]}),
            ], className="mt-1"),
            html.Small(f"{data['games']} maps", style={"color": COLORS["muted"]}),
        ]),
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
    )


def _kd_trend_figure(trend_data: list[dict]) -> go.Figure:
    fig = go.Figure()
    players = sorted(set(d["player_name"] for d in trend_data))
    for i, p in enumerate(players):
        pdata = [d for d in trend_data if d["player_name"] == p]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        fig.add_trace(go.Scatter(
            x=[f"W{d['week']}" for d in pdata],
            y=[d["kd"] for d in pdata],
            mode="lines+markers",
            name=p,
            marker={"size": 6, "color": color},
            line={"width": 2, "color": color},
            hovertemplate=f"{p}: %{{y:.2f}} K/D<extra></extra>",
        ))
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.4,
                  annotation_text="1.0 K/D", annotation_position="bottom right")
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 50, "r": 20, "t": 30, "b": 50},
        height=400,
        yaxis={"title": "K/D", "gridcolor": COLORS["border"]},
        xaxis={"title": "Week", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def _get_available_players(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT player_name FROM scrim_player_stats ORDER BY player_name"
    ).fetchall()
    return [r[0] for r in rows]


def layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.Label("Player", style={"color": COLORS["text"]}),
                dcc.Dropdown(
                    id="player-filter",
                    options=[{"label": "All", "value": "All"}],
                    value="All",
                    clearable=False,
                    style={"backgroundColor": COLORS["card_bg"]},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Mode", style={"color": COLORS["text"]}),
                dcc.Dropdown(
                    id="player-mode-filter",
                    options=[{"label": "All", "value": "All"}]
                        + [{"label": m, "value": m} for m in ("SnD", "HP", "Control")],
                    value="All",
                    clearable=False,
                    style={"backgroundColor": COLORS["card_bg"]},
                ),
            ], width=2),
            dbc.Col([
                html.Label("Weeks", style={"color": COLORS["text"]}),
                dcc.RangeSlider(
                    id="player-week-slider",
                    min=1, max=13, step=1, value=[1, 13],
                    marks={i: f"W{i}" for i in range(1, 14)},
                ),
            ], width=8),
        ], className="mb-3"),
        html.Div(id="player-summary-cards"),
        html.H5("K/D Trend", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        dcc.Graph(id="player-kd-chart"),
        html.H5("Per-Map Breakdown", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        html.Div(id="player-map-table"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("player-filter", "options"),
        Input("player-filter", "id"),
    )
    def populate_players(_):
        conn = get_db()
        players = _get_available_players(conn)
        conn.close()
        return [{"label": "All", "value": "All"}] + [{"label": p, "value": p} for p in players]

    @app.callback(
        Output("player-week-slider", "min"),
        Output("player-week-slider", "max"),
        Output("player-week-slider", "marks"),
        Output("player-week-slider", "value"),
        Input("player-filter", "id"),
    )
    def update_player_week_slider(_):
        conn = get_db()
        rows = conn.execute("SELECT DISTINCT week FROM scrim_maps ORDER BY week").fetchall()
        conn.close()
        weeks = [r[0] for r in rows]
        if not weeks:
            return 1, 1, {1: "W1"}, [1, 1]
        mn, mx = min(weeks), max(weeks)
        marks = {w: f"W{w}" for w in weeks}
        return mn, mx, marks, [mn, mx]

    @app.callback(
        Output("player-summary-cards", "children"),
        Output("player-kd-chart", "figure"),
        Output("player-map-table", "children"),
        Input("player-filter", "value"),
        Input("player-mode-filter", "value"),
        Input("player-week-slider", "value"),
    )
    def update_player_tab(player, mode, week_range):
        conn = get_db()
        player_val = player if player != "All" else None
        mode_val = mode if mode != "All" else None
        wr = tuple(week_range) if week_range else None

        # Player cards
        card_data = _build_player_cards_data(conn, player=player_val, mode=mode_val, week_range=wr)
        cards = []
        for i, d in enumerate(card_data):
            color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
            cards.append(dbc.Col(_player_card(d, color), width=True))
        card_row = dbc.Row(cards) if cards else html.P("No player data found.", style={"color": COLORS["muted"]})

        # K/D trend
        trend_data = _build_kd_trend_data(conn, player=player_val, mode=mode_val)
        fig = _kd_trend_figure(trend_data)

        # Map table
        map_data = _build_player_map_data(conn, player=player_val, mode=mode_val, week_range=wr)
        if map_data:
            header = html.Thead(html.Tr([
                html.Th("Map"), html.Th("Mode"), html.Th("Games"),
                html.Th("Avg K/D"), html.Th("Avg Kills"), html.Th("Avg Deaths"),
                html.Th("Avg Assists"), html.Th("Pos Eng %"),
            ]))
            body_rows = []
            for d in map_data:
                kd_color = COLORS["win"] if d["avg_kd"] >= 1.0 else COLORS["loss"]
                body_rows.append(html.Tr([
                    html.Td(d["map_name"]),
                    html.Td(d["mode"], style={"color": MODE_COLORS.get(d["mode"], COLORS["text"])}),
                    html.Td(str(d["games"])),
                    html.Td(f"{d['avg_kd']:.2f}", style={"color": kd_color, "fontWeight": "600"}),
                    html.Td(f"{d['avg_kills']:.1f}"),
                    html.Td(f"{d['avg_deaths']:.1f}"),
                    html.Td(f"{d['avg_assists']:.1f}"),
                    html.Td(f"{d['avg_pos_eng_pct']:.1f}%"),
                ]))
            table = dbc.Table(
                [header, html.Tbody(body_rows)],
                bordered=True, dark=True, hover=True, size="sm",
                style={"backgroundColor": COLORS["card_bg"]},
            )
        else:
            table = html.P("No player data found.", style={"color": COLORS["muted"]})

        conn.close()
        return card_row, fig, table
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dashboard_scrim.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/player_stats.py tests/test_dashboard_scrim.py
git commit -m "feat: add Player Stats dashboard tab"
```

---

### Task 8: Wire New Tabs Into Dashboard App

**Files:**
- Modify: `src/cdm_stats/dashboard/app.py`

- [ ] **Step 1: Add new tabs to app layout**

In `src/cdm_stats/dashboard/app.py`, add two new `dbc.Tab` entries in the `dbc.Tabs` children list, after the Elo Tracker tab:

```python
        dbc.Tab(label="Scrim Performance", tab_id="scrim-performance"),
        dbc.Tab(label="Player Stats", tab_id="player-stats"),
```

- [ ] **Step 2: Add tab rendering in render_tab callback**

In the `render_tab` callback, add imports and elif branches:

```python
def render_tab(active_tab: str):
    from cdm_stats.dashboard.tabs import team_profile, map_matrix, matchup_prep, elo_tracker
    from cdm_stats.dashboard.tabs import scrim_performance, player_stats
    if active_tab == "team-profile":
        return team_profile.layout()
    elif active_tab == "map-matrix":
        return map_matrix.layout()
    elif active_tab == "matchup-prep":
        return matchup_prep.layout()
    elif active_tab == "elo-tracker":
        return elo_tracker.layout()
    elif active_tab == "scrim-performance":
        return scrim_performance.layout()
    elif active_tab == "player-stats":
        return player_stats.layout()
    return html.Div("Select a tab")
```

- [ ] **Step 3: Register callbacks in register_all_callbacks**

```python
def register_all_callbacks():
    from cdm_stats.dashboard.tabs import team_profile, map_matrix, matchup_prep, elo_tracker
    from cdm_stats.dashboard.tabs import scrim_performance, player_stats
    team_profile.register_callbacks(app)
    map_matrix.register_callbacks(app)
    matchup_prep.register_callbacks(app)
    elo_tracker.register_callbacks(app)
    scrim_performance.register_callbacks(app)
    player_stats.register_callbacks(app)
```

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: ALL tests PASS (existing + new)

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/app.py
git commit -m "feat: wire Scrim Performance and Player Stats tabs into dashboard"
```

---

### Task 9: Final Integration Test

**Files:**
- No new files — verify everything works end to end

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v --tb=short`
Expected: ALL tests PASS

- [ ] **Step 2: Verify CLI commands work**

Run: `uv run python main.py --help`
Expected: Shows `ingest-scrims-team` and `ingest-scrims-players` in the command list

- [ ] **Step 3: Commit all remaining changes (if any)**

```bash
git status
```

If clean, no commit needed. If any unstaged changes remain, add and commit.
