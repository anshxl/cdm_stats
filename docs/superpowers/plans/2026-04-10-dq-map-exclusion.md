# DQ Map Exclusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `dq` flag to `map_results` so DQ'd maps are excluded from every map-level metric (Elo margin, pick/defend W-L, pick context, map strength, per-map W-L) while the face-value row is preserved for audit.

**Architecture:** One new SQLite column + one migration step. Every aggregation query that touches `map_results` gains `AND dq = 0`. CSV ingestion reads an optional `dq` column. Finally, the existing GL vs OUG Standoff row (2026-03-20) is flagged and Elo is recalculated.

**Tech Stack:** Python 3.11, SQLite, pytest

**Spec:** `docs/superpowers/specs/2026-04-10-dq-map-exclusion-design.md`

---

### Task 1: Schema — add `dq` column and v5 migration

**Files:**
- Modify: `src/cdm_stats/db/schema.py`
- Modify: `tests/test_schema.py`

- [ ] **Step 1: Update failing tests for schema v5**

In `tests/test_schema.py`, replace `test_schema_version_is_4` (around line 179) with:

```python
def test_schema_version_is_5():
    import sqlite3
    from cdm_stats.db.schema import create_tables, SCHEMA_VERSION

    assert SCHEMA_VERSION == 5
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 5
    conn.close()


def test_map_results_has_dq_column():
    import sqlite3
    from cdm_stats.db.schema import create_tables

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
    assert "dq" in cols
    conn.close()


def test_migration_v4_to_v5_adds_dq_column():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    conn.execute("ALTER TABLE map_results DROP COLUMN dq")
    conn.execute("PRAGMA user_version = 4")
    conn.commit()

    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
    assert "dq" in cols
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
    conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_schema.py -v -k "version_is_5 or dq_column or v4_to_v5"`
Expected: FAIL — `SCHEMA_VERSION == 4` assertion fails, `dq` column doesn't exist.

- [ ] **Step 3: Bump SCHEMA_VERSION and add `dq` to CREATE**

In `src/cdm_stats/db/schema.py`, change line 3:

```python
SCHEMA_VERSION = 5
```

In the `CREATE TABLE IF NOT EXISTS map_results` block (around line 37), add the `dq` column right before the `UNIQUE(match_id, slot)` line:

```sql
        pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                   'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss', 'Unknown'
                               )),
        dq                     INTEGER NOT NULL DEFAULT 0 CHECK(dq IN (0, 1)),
        UNIQUE(match_id, slot)
```

- [ ] **Step 4: Add v4 → v5 migration branch**

In `src/cdm_stats/db/schema.py`, add a new branch in `migrate()` after the `if version < 4:` block (around line 232, before the final `PRAGMA user_version` line):

```python
    if version < 5:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
        if "dq" not in cols:
            conn.execute(
                "ALTER TABLE map_results ADD COLUMN dq INTEGER NOT NULL DEFAULT 0 CHECK(dq IN (0, 1))"
            )
```

- [ ] **Step 5: Run full schema test suite to verify it passes**

Run: `uv run pytest tests/test_schema.py -v`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/cdm_stats/db/schema.py tests/test_schema.py
git commit -m "feat(schema): add dq column to map_results (v5)"
```

---

### Task 2: `insert_map_result` — accept `dq` parameter

**Files:**
- Modify: `src/cdm_stats/db/queries.py:52-75`
- Modify: `tests/test_queries.py`

- [ ] **Step 1: Write failing test for dq persistence**

Append to `tests/test_queries.py`:

```python
def test_insert_map_result_stores_dq(db):
    from cdm_stats.db.queries import insert_match, insert_map_result
    dvs = get_team_id_by_abbr(db, "DVS")
    oug = get_team_id_by_abbr(db, "OUG")
    tunisia = get_map_id(db, "Tunisia", "SnD")

    match_id = insert_match(db, "2026-01-15", dvs, oug, dvs, dvs)
    insert_map_result(db, match_id, 1, tunisia, dvs, dvs, 6, 3, 0, 0, "Opener", dq=1)

    row = db.execute(
        "SELECT dq FROM map_results WHERE match_id = ? AND slot = 1", (match_id,)
    ).fetchone()
    assert row[0] == 1


def test_insert_map_result_dq_defaults_to_zero(db):
    from cdm_stats.db.queries import insert_match, insert_map_result
    dvs = get_team_id_by_abbr(db, "DVS")
    oug = get_team_id_by_abbr(db, "OUG")
    tunisia = get_map_id(db, "Tunisia", "SnD")

    match_id = insert_match(db, "2026-01-15", dvs, oug, dvs, dvs)
    insert_map_result(db, match_id, 1, tunisia, dvs, dvs, 6, 3, 0, 0, "Opener")

    row = db.execute(
        "SELECT dq FROM map_results WHERE match_id = ? AND slot = 1", (match_id,)
    ).fetchone()
    assert row[0] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_queries.py -v -k "insert_map_result_stores_dq or dq_defaults"`
Expected: FAIL — `insert_map_result` rejects unexpected `dq` keyword.

- [ ] **Step 3: Update `insert_map_result` signature and INSERT**

In `src/cdm_stats/db/queries.py`, replace the entire `insert_map_result` function (lines 52-75) with:

```python
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
    dq: int = 0,
) -> int:
    cursor = conn.execute(
        """INSERT INTO map_results
           (match_id, slot, map_id, picked_by_team_id, winner_team_id,
            picking_team_score, non_picking_team_score,
            team1_score_before, team2_score_before, pick_context, dq)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, slot, map_id, picked_by_team_id, winner_team_id,
         picking_team_score, non_picking_team_score,
         team1_score_before, team2_score_before, pick_context, dq),
    )
    return cursor.lastrowid
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_queries.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/queries.py tests/test_queries.py
git commit -m "feat(queries): add dq parameter to insert_map_result"
```

---

### Task 3: CSV loader reads optional `dq` column

**Files:**
- Modify: `src/cdm_stats/ingestion/csv_loader.py:134-175`
- Modify: `tests/test_csv_loader.py`

- [ ] **Step 1: Write failing tests for `dq` column support**

Append to `tests/test_csv_loader.py`:

```python
DQ_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3,,,
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,80,,,1
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1,,,
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2,,,"""


def test_csv_loader_reads_dq_flag(db):
    ingest_csv(db, io.StringIO(DQ_CSV))
    rows = db.execute(
        "SELECT slot, dq FROM map_results ORDER BY slot"
    ).fetchall()
    assert rows == [(1, 0), (2, 1), (3, 0), (4, 0)]


def test_csv_loader_blank_dq_defaults_to_zero(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))  # no dq column at all
    rows = db.execute("SELECT dq FROM map_results").fetchall()
    assert all(r[0] == 0 for r in rows)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_csv_loader.py -v -k "reads_dq_flag or blank_dq"`
Expected: FAIL — loader ignores `dq` column; `dq` stays 0 on the DQ'd row.

- [ ] **Step 3: Update loader to read `dq`**

In `src/cdm_stats/ingestion/csv_loader.py`, inside the `for row in sorted(...)` loop (around line 134), add after `loser_score = int(row["loser_score"])` (line 140):

```python
            dq_val = (row.get("dq") or "").strip()
            dq = 1 if dq_val == "1" else 0
```

Then in the tuple appended to `map_result_data` (around line 171), add `dq` as the final element:

```python
            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context, dq,
            ))
```

The call site a few lines below passes the tuple via `*data` to `insert_map_result`, which now accepts `dq` as its last positional parameter. No change needed at that call site.

- [ ] **Step 4: Run full csv_loader test suite to verify it passes**

Run: `uv run pytest tests/test_csv_loader.py -v`
Expected: All tests PASS. Old CSVs without a `dq` column still load (backwards compatible via `row.get("dq")`).

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/csv_loader.py tests/test_csv_loader.py
git commit -m "feat(ingest): read optional dq column from matches CSV"
```

---

### Task 4: Elo — exclude DQ'd maps from margin average

**Files:**
- Modify: `src/cdm_stats/metrics/elo.py:61-68`
- Modify: `tests/test_elo.py`

- [ ] **Step 1: Write failing test for DQ exclusion**

Append to `tests/test_elo.py`:

```python
DQ_MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-03-20,DVS,OUG,DVS,1,Tunisia,DVS,6,4,,,
2026-03-20,DVS,OUG,DVS,2,Summit,OUG,250,80,,,1
2026-03-20,DVS,OUG,DVS,3,Raid,DVS,4,2,,,
2026-03-20,DVS,OUG,DVS,4,Slums,DVS,6,3,,,"""

FACE_VALUE_MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-03-20,DVS,OUG,DVS,1,Tunisia,DVS,6,4,,,
2026-03-20,DVS,OUG,DVS,2,Summit,OUG,250,80,,,
2026-03-20,DVS,OUG,DVS,3,Raid,DVS,4,2,,,
2026-03-20,DVS,OUG,DVS,4,Slums,DVS,6,3,,,"""


def _fresh_db():
    from cdm_stats.db.schema import create_tables
    from cdm_stats.ingestion.seed import seed_teams, seed_maps
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    return conn


def test_dq_map_excluded_from_elo_margin():
    """A DQ'd map's margin must not drag down the series winner's Elo gain."""
    conn_dq = _fresh_db()
    ingest_csv(conn_dq, io.StringIO(DQ_MATCH_CSV))
    dvs_id = conn_dq.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    dvs_elo_dq = get_current_elo(conn_dq, dvs_id)
    conn_dq.close()

    conn_face = _fresh_db()
    ingest_csv(conn_face, io.StringIO(FACE_VALUE_MATCH_CSV))
    dvs_elo_face = get_current_elo(conn_face, dvs_id)
    conn_face.close()

    # With DQ excluded, DVS's dominance score drops the huge negative margin
    # from Summit (OUG 250-80), so DVS should gain meaningfully more Elo.
    assert dvs_elo_dq > dvs_elo_face
    assert dvs_elo_dq - dvs_elo_face > 2.0  # sanity: the delta is non-trivial
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_elo.py::test_dq_map_excluded_from_elo_margin -v`
Expected: FAIL — current `update_elo` includes the DQ'd map, so both Elo values are identical.

- [ ] **Step 3: Filter DQ'd rows in `update_elo`**

In `src/cdm_stats/metrics/elo.py`, update the `map_rows` query in `update_elo` (lines 61-68) to add `AND mr.dq = 0`:

```python
    map_rows = conn.execute(
        """SELECT mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score, m.mode
           FROM map_results mr
           JOIN maps m ON mr.map_id = m.map_id
           WHERE mr.match_id = ? AND mr.dq = 0
           ORDER BY mr.slot""",
        (match_id,),
    ).fetchall()
```

- [ ] **Step 4: Run full elo test suite to verify it passes**

Run: `uv run pytest tests/test_elo.py -v`
Expected: All tests PASS (including the existing structural tests — they use non-DQ matches so behavior is unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/elo.py tests/test_elo.py
git commit -m "feat(elo): exclude DQ'd maps from margin-weighted dominance"
```

---

### Task 5: Avoidance metrics — filter DQ'd maps

**Files:**
- Modify: `src/cdm_stats/metrics/avoidance.py`
- Modify: `tests/test_avoidance.py`

- [ ] **Step 1: Write failing tests for DQ exclusion in all three functions**

Append to `tests/test_avoidance.py`:

```python
# DVS picks Tunisia (slot 1) at face value, DVS "wins" 6-3, but the map is DQ'd.
# OUG picks Summit (slot 2, HP), OUG wins 250-220.
# DVS picks Raid (slot 3, Control), DVS wins 3-1.
# OUG picks Slums (slot 4, SnD), DVS wins 6-2.
DQ_PICK_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,1
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220,,,
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1,,,
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2,,,"""


def test_pick_win_loss_excludes_dq(db):
    """DVS's DQ'd Tunisia pick-win must not count toward pick W-L."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = pick_win_loss(db, dvs, tunisia)
    assert result == {"wins": 0, "losses": 0}


def test_defend_win_loss_excludes_dq(db):
    """OUG's DQ'd Tunisia defend-loss must not count toward defend W-L."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    _, oug, tunisia, _, _ = _get_ids(db)
    result = defend_win_loss(db, oug, tunisia)
    assert result == {"wins": 0, "losses": 0}


def test_pick_context_distribution_excludes_dq(db):
    """DVS's DQ'd Opener pick on Tunisia must not appear in the context distribution."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, dvs, tunisia)
    assert dist == {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_avoidance.py -v -k "excludes_dq"`
Expected: FAIL — all three functions currently include DQ'd rows.

- [ ] **Step 3: Add `dq = 0` filter to all three queries**

In `src/cdm_stats/metrics/avoidance.py`:

Replace `pick_win_loss` (lines 4-13) with:

```python
def pick_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND dq = 0""",
        (team_id, team_id, team_id, map_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}
```

Replace `defend_win_loss` (lines 16-29) with:

```python
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
             AND mr.dq = 0
             AND (m.team1_id = ? OR m.team2_id = ?)""",
        (team_id, team_id, team_id, map_id, team_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}
```

Replace `pick_context_distribution` (lines 33-46) with:

```python
def pick_context_distribution(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict[str, int]:
    """Breakdown of how often a team picks this map in each context."""
    rows = conn.execute(
        """SELECT pick_context, COUNT(*)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND dq = 0
           GROUP BY pick_context""",
        (team_id, map_id),
    ).fetchall()
    result = {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
    for ctx, count in rows:
        if ctx in result:
            result[ctx] = count
    return result
```

- [ ] **Step 4: Run avoidance test suite to verify all pass**

Run: `uv run pytest tests/test_avoidance.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/avoidance.py tests/test_avoidance.py
git commit -m "feat(avoidance): exclude DQ'd maps from pick/defend/context metrics"
```

---

### Task 6: Map strength — filter DQ'd maps

**Files:**
- Modify: `src/cdm_stats/metrics/map_strength.py:73-81`
- Modify: `tests/test_map_strength.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_map_strength.py`:

```python
DQ_STRENGTH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-02-01,DVS,OUG,DVS,1,Tunisia,OUG,6,3,DVS,,1
2026-02-01,DVS,OUG,DVS,2,Summit,DVS,250,100,,,
2026-02-01,DVS,OUG,DVS,3,Raid,DVS,3,1,,,
2026-02-01,DVS,OUG,DVS,4,Slums,DVS,6,2,,,"""


def test_map_strength_excludes_dq():
    from cdm_stats.db.schema import create_tables
    from cdm_stats.ingestion.seed import seed_teams, seed_maps
    from cdm_stats.metrics.map_strength import map_strength

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(DQ_STRENGTH_CSV))

    dvs = conn.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    tunisia = conn.execute(
        "SELECT map_id FROM maps WHERE map_name='Tunisia' AND mode='SnD'"
    ).fetchone()[0]

    result = map_strength(conn, dvs, tunisia)
    # DVS's only Tunisia result was DQ'd → no data
    assert result["total_played"] == 0
    assert result["rating"] is None
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_map_strength.py::test_map_strength_excludes_dq -v`
Expected: FAIL — `total_played` is 1, not 0.

- [ ] **Step 3: Add `dq = 0` filter to `map_strength` query**

In `src/cdm_stats/metrics/map_strength.py`, update the rows query in `map_strength` (lines 73-81):

```python
    rows = conn.execute(
        """SELECT mr.match_id, mr.winner_team_id, mr.pick_context, mr.slot
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND mr.dq = 0
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date""",
        (map_id, team_id, team_id),
    ).fetchall()
```

- [ ] **Step 4: Run test suite to verify it passes**

Run: `uv run pytest tests/test_map_strength.py -v`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/map_strength.py tests/test_map_strength.py
git commit -m "feat(map_strength): exclude DQ'd maps from rating computation"
```

---

### Task 7: `get_team_map_wl` — filter DQ'd maps

**Files:**
- Modify: `src/cdm_stats/db/queries.py:120-152`
- Modify: `tests/test_queries.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_queries.py`:

```python
def test_get_team_map_wl_excludes_dq(db):
    import io
    from cdm_stats.ingestion.csv_loader import ingest_csv
    from cdm_stats.db.queries import get_team_map_wl

    dq_csv = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-02-01,DVS,OUG,DVS,1,Tunisia,OUG,6,3,DVS,,1
2026-02-01,DVS,OUG,DVS,2,Summit,DVS,250,100,,,
2026-02-01,DVS,OUG,DVS,3,Raid,DVS,3,1,,,
2026-02-01,DVS,OUG,DVS,4,Slums,DVS,6,2,,,"""
    ingest_csv(db, io.StringIO(dq_csv))

    dvs = get_team_id_by_abbr(db, "DVS")
    rows = get_team_map_wl(db, dvs)
    tunisia_row = next((r for r in rows if r["map_name"] == "Tunisia"), None)
    # DVS's only Tunisia result was DQ'd → Tunisia should not appear at all
    assert tunisia_row is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_queries.py::test_get_team_map_wl_excludes_dq -v`
Expected: FAIL — Tunisia row present with the DQ'd loss.

- [ ] **Step 3: Add `dq = 0` filter to both branches**

In `src/cdm_stats/db/queries.py`, update `get_team_map_wl` (lines 120-152). In the format-filtered branch, change the WHERE clause to include `AND mr.dq = 0`:

```python
    if format_filter:
        rows = conn.execute(
            """SELECT m2.map_name, m2.mode,
                      SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END) as losses
               FROM map_results mr
               JOIN maps m2 ON mr.map_id = m2.map_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE (m.team1_id = ? OR m.team2_id = ?)
                 AND m.match_format LIKE ? || '%'
                 AND mr.dq = 0
               GROUP BY m2.map_name, m2.mode
               ORDER BY m2.mode, wins DESC""",
            (team_id, team_id, team_id, team_id, format_filter),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT m2.map_name, m2.mode,
                      SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END) as losses
               FROM map_results mr
               JOIN maps m2 ON mr.map_id = m2.map_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE (m.team1_id = ? OR m.team2_id = ?)
                 AND mr.dq = 0
               GROUP BY m2.map_name, m2.mode
               ORDER BY m2.mode, wins DESC""",
            (team_id, team_id, team_id, team_id),
        ).fetchall()
```

- [ ] **Step 4: Run full test suite to verify everything still passes**

Run: `uv run pytest -v`
Expected: All tests PASS. No regressions in other modules.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/queries.py tests/test_queries.py
git commit -m "feat(queries): exclude DQ'd maps from get_team_map_wl"
```

---

### Task 8: Data correction — flag GL vs OUG Standoff and recalculate Elo

**Files:**
- Modify: `data/matches.csv`
- Modify: `data/cdl.db`

- [ ] **Step 1: Add `dq` column header to `matches.csv`**

Edit `data/matches.csv` header line (line 1):

Before:
```
date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by
```

After:
```
date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
```

Existing rows do not need trailing commas added — Python's `csv.DictReader` treats missing fields as `None`, and the loader uses `row.get("dq") or ""` which handles both missing and blank.

- [ ] **Step 2: Flag the Standoff row with `dq=1`**

Find the row in `data/matches.csv` for `2026-03-20,GL,OUG,GL,3,Standoff,OUG,4,1,`. Append `,,1` so the line reads:

```
2026-03-20,GL,OUG,GL,3,Standoff,OUG,4,1,,,1
```

(The two empty fields before `1` are `series_winner` and `picked_by`. The GL vs OUG match must already have a `series_winner` override of `GL` on one of its rows — verify that's still present. If not, move the override to the Coastal row or another non-DQ'd row.)

- [ ] **Step 3: Verify the CSV parses cleanly**

Run:
```bash
uv run python -c "
import csv
with open('data/matches.csv') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    dq_rows = [r for r in rows if (r.get('dq') or '').strip() == '1']
    print(f'Total rows: {len(rows)}')
    print(f'DQ rows: {len(dq_rows)}')
    for r in dq_rows:
        print(f\"  {r['date']} {r['team1']} vs {r['team2']} slot {r['slot']} ({r['map_name']})\")
"
```
Expected: `Total rows: <N>`, `DQ rows: 1`, followed by the GL vs OUG Standoff line.

- [ ] **Step 4: Apply the `dq` flag to the live DB**

Run:
```bash
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/cdl.db')

# Ensure schema is migrated to v5 first
from cdm_stats.db.schema import migrate
migrate(conn)

cur = conn.execute('''
    UPDATE map_results
       SET dq = 1
     WHERE slot = 3
       AND match_id = (
           SELECT match_id FROM matches
            WHERE match_date = '2026-03-20'
              AND team1_id = (SELECT team_id FROM teams WHERE abbreviation = 'GL')
              AND team2_id = (SELECT team_id FROM teams WHERE abbreviation = 'OUG')
       )
''')
print(f'Rows updated: {cur.rowcount}')
assert cur.rowcount == 1, f'Expected exactly 1 row, got {cur.rowcount}'
conn.commit()
conn.close()
"
```
Expected: `Rows updated: 1`.

- [ ] **Step 5: Recalculate Elo for the entire history**

Run:
```bash
uv run python -c "
import sqlite3
from cdm_stats.metrics.elo import recalculate_all_elo, get_current_elo
conn = sqlite3.connect('data/cdl.db')
count = recalculate_all_elo(conn)
print(f'Recalculated Elo for {count} matches')

gl = conn.execute(\"SELECT team_id FROM teams WHERE abbreviation='GL'\").fetchone()[0]
oug = conn.execute(\"SELECT team_id FROM teams WHERE abbreviation='OUG'\").fetchone()[0]
print(f'GL current Elo: {get_current_elo(conn, gl):.1f}')
print(f'OUG current Elo: {get_current_elo(conn, oug):.1f}')
conn.close()
"
```
Expected: `Recalculated Elo for <N> matches`, followed by the new GL and OUG Elo values. GL should be meaningfully higher and OUG meaningfully lower than before the fix (compare against values you see in the current DB before running this step).

- [ ] **Step 6: Run the full test suite one more time**

Run: `uv run pytest -v`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add data/matches.csv data/cdl.db
git commit -m "chore: flag GL vs OUG Standoff as DQ and recalculate Elo"
```
