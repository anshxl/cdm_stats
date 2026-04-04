# Dashboard Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorder dashboard tabs (removing Map Matrix), replace week `RangeSlider` with pill buttons, and add a Tournament/Scrim source toggle to Player Stats backed by a new `tournament_player_stats` table.

**Architecture:** Three parallel changes. (1) Pure UI: tab reorder + delete. (2) New shared Dash component for week selection, swapped into two existing tabs. (3) New SQLite table + queries module + CSV loader mirroring the scrim player schema, wired into Player Stats via a Source radio.

**Tech Stack:** Python 3.11, Dash + dash-bootstrap-components, SQLite (stdlib `sqlite3`), pytest.

**Spec:** `docs/superpowers/specs/2026-04-04-dashboard-improvements-design.md`

---

## File Structure

**New files:**
- `src/cdm_stats/dashboard/components/__init__.py`
- `src/cdm_stats/dashboard/components/week_pills.py` — `week_pills()` factory + `pill_value_to_range()` converter
- `src/cdm_stats/db/queries_tournament_player.py` — 3 query functions mirroring queries_scrim signatures
- `src/cdm_stats/ingestion/tournament_player_loader.py` — `ingest_tournament_players()` function
- `tests/test_week_pills.py`
- `tests/test_queries_tournament_player.py`
- `tests/test_tournament_player_loader.py`

**Modified files:**
- `src/cdm_stats/dashboard/app.py` — tab order, default active_tab, drop Map Matrix
- `src/cdm_stats/db/schema.py` — SCHEMA_VERSION → 4, add `tournament_player_stats` table + migration
- `src/cdm_stats/dashboard/tabs/scrim_performance.py` — swap RangeSlider for week_pills
- `src/cdm_stats/dashboard/tabs/player_stats.py` — swap RangeSlider, add Source toggle, dispatch
- `tests/test_dashboard_app.py` — update tab-list assertion

**Deleted files:**
- `src/cdm_stats/dashboard/tabs/map_matrix.py`
- `tests/test_dashboard_map_matrix.py` (obsolete along with the tab)

---

## Task 1: Reorder Tabs + Remove Map Matrix

**Files:**
- Modify: `src/cdm_stats/dashboard/app.py`
- Modify: `tests/test_dashboard_app.py`
- Delete: `src/cdm_stats/dashboard/tabs/map_matrix.py`
- Delete: `tests/test_dashboard_map_matrix.py`

- [ ] **Step 1: Update tab-render test to expect new order without map-matrix**

Replace contents of `tests/test_dashboard_app.py`:

```python
def test_app_imports_and_initializes():
    """Verify the Dash app can be imported and all callbacks registered."""
    from cdm_stats.dashboard.app import app, register_all_callbacks
    register_all_callbacks()
    assert app.layout is not None


def test_render_tab_returns_content():
    """Verify each tab renders without error."""
    from cdm_stats.dashboard.app import render_tab
    for tab in ["matchup-prep", "team-profile", "player-stats", "scrim-performance", "elo-tracker"]:
        result = render_tab(tab)
        assert result is not None


def test_map_matrix_tab_removed():
    """map-matrix is no longer a valid tab id."""
    from cdm_stats.dashboard.app import render_tab
    from dash import html
    result = render_tab("map-matrix")
    assert isinstance(result, html.Div)
    # Falls through to "Select a tab" placeholder
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dashboard_app.py -v`
Expected: FAIL — `render_tab("map-matrix")` still returns real content from `map_matrix.layout()`.

- [ ] **Step 3: Update `app.py` — new tab order, remove map-matrix**

Replace the `dbc.Tabs(...)` call and `render_tab` / `register_all_callbacks` functions in `src/cdm_stats/dashboard/app.py`:

```python
app.layout = dbc.Container([
    dbc.NavbarSimple(
        brand="CDM Stats",
        brand_style={"fontSize": "1.3rem", "fontWeight": "600"},
        color="#16213e",
        dark=True,
        className="mb-0",
    ),
    dbc.Tabs(id="main-tabs", active_tab="matchup-prep", className="mt-0", children=[
        dbc.Tab(label="Match-Up Prep", tab_id="matchup-prep"),
        dbc.Tab(label="Team Profile", tab_id="team-profile"),
        dbc.Tab(label="Player Stats", tab_id="player-stats"),
        dbc.Tab(label="Scrim Performance", tab_id="scrim-performance"),
        dbc.Tab(label="Elo Tracker", tab_id="elo-tracker"),
    ]),
    html.Div(id="tab-content", className="mt-3"),
], fluid=True, className="px-0")


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@app.callback(Output("tab-content", "children"), Input("main-tabs", "active_tab"))
def render_tab(active_tab: str):
    from cdm_stats.dashboard.tabs import team_profile, matchup_prep, elo_tracker
    from cdm_stats.dashboard.tabs import scrim_performance, player_stats
    if active_tab == "matchup-prep":
        return matchup_prep.layout()
    elif active_tab == "team-profile":
        return team_profile.layout()
    elif active_tab == "player-stats":
        return player_stats.layout()
    elif active_tab == "scrim-performance":
        return scrim_performance.layout()
    elif active_tab == "elo-tracker":
        return elo_tracker.layout()
    return html.Div("Select a tab")


def register_all_callbacks():
    from cdm_stats.dashboard.tabs import team_profile, matchup_prep, elo_tracker
    from cdm_stats.dashboard.tabs import scrim_performance, player_stats
    team_profile.register_callbacks(app)
    matchup_prep.register_callbacks(app)
    elo_tracker.register_callbacks(app)
    scrim_performance.register_callbacks(app)
    player_stats.register_callbacks(app)
```

- [ ] **Step 4: Delete Map Matrix tab file and its test**

Run:
```bash
rm src/cdm_stats/dashboard/tabs/map_matrix.py
rm tests/test_dashboard_map_matrix.py
```

- [ ] **Step 5: Run all tests to verify pass**

Run: `uv run pytest tests/test_dashboard_app.py -v`
Expected: 3 tests PASS.

Run: `uv run pytest -v`
Expected: all tests pass (no references to `map_matrix` module remain in other test files).

- [ ] **Step 6: Commit**

```bash
git add src/cdm_stats/dashboard/app.py tests/test_dashboard_app.py
git rm src/cdm_stats/dashboard/tabs/map_matrix.py tests/test_dashboard_map_matrix.py
git commit -m "feat(dashboard): reorder tabs and remove Map Matrix"
```

---

## Task 2: Schema — `tournament_player_stats` table + migration to v4

**Files:**
- Modify: `src/cdm_stats/db/schema.py`
- Test: `tests/test_schema.py`

- [ ] **Step 1: Write failing test for new table**

Append to `tests/test_schema.py`:

```python
def test_tournament_player_stats_table_exists():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(tournament_player_stats)").fetchall()]
    assert cols == ["stat_id", "result_id", "week", "player_name", "kills", "deaths", "assists"]

    # Unique (result_id, player_name)
    idx_rows = conn.execute("PRAGMA index_list(tournament_player_stats)").fetchall()
    unique_indexes = [r for r in idx_rows if r[2] == 1]
    assert len(unique_indexes) >= 1
    conn.close()


def test_schema_version_is_4():
    import sqlite3
    from cdm_stats.db.schema import create_tables, SCHEMA_VERSION

    assert SCHEMA_VERSION == 4
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 4
    conn.close()


def test_migration_v3_to_v4_adds_tournament_player_stats():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    # Simulate old DB at v3
    conn.execute("DROP TABLE tournament_player_stats")
    conn.execute("PRAGMA user_version = 3")
    conn.commit()

    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(tournament_player_stats)").fetchall()]
    assert "result_id" in cols
    assert "week" in cols
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_schema.py -v`
Expected: FAIL — table doesn't exist and `SCHEMA_VERSION` is 3.

- [ ] **Step 3: Add table to `TABLES` list and bump SCHEMA_VERSION**

In `src/cdm_stats/db/schema.py`:

- Change `SCHEMA_VERSION = 3` to `SCHEMA_VERSION = 4`.
- Append to the `TABLES` list (after `scrim_player_stats`):

```python
    """
    CREATE TABLE IF NOT EXISTS tournament_player_stats (
        stat_id      INTEGER PRIMARY KEY,
        result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
        week         INTEGER NOT NULL,
        player_name  TEXT NOT NULL,
        kills        INTEGER NOT NULL,
        deaths       INTEGER NOT NULL,
        assists      INTEGER NOT NULL,
        UNIQUE(result_id, player_name)
    )
    """,
```

- Add a `version < 4` block inside `migrate()` (before the final `PRAGMA user_version` line):

```python
    if version < 4:
        conn.execute("""CREATE TABLE IF NOT EXISTS tournament_player_stats (
            stat_id      INTEGER PRIMARY KEY,
            result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
            week         INTEGER NOT NULL,
            player_name  TEXT NOT NULL,
            kills        INTEGER NOT NULL,
            deaths       INTEGER NOT NULL,
            assists      INTEGER NOT NULL,
            UNIQUE(result_id, player_name)
        )""")
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_schema.py -v`
Expected: all schema tests PASS including the three new ones.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/schema.py tests/test_schema.py
git commit -m "feat(db): add tournament_player_stats table (schema v4)"
```

---

## Task 3: Tournament Player Queries Module

**Files:**
- Create: `src/cdm_stats/db/queries_tournament_player.py`
- Test: `tests/test_queries_tournament_player.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_queries_tournament_player.py`:

```python
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


@pytest.fixture
def db_with_tournament_players():
    """DB with one match, 2 map_results, and player stats for 2 players over 2 maps."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)

    # Insert a match: DVS vs OUG on 2026-02-15
    dvs_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number)
           VALUES ('2026-02-15', ?, ?, ?, ?, 'CDL_BO5', 1)""",
        (dvs_id, oug_id, dvs_id, dvs_id),
    )
    match_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    tunisia_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'"
    ).fetchone()[0]
    summit_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'"
    ).fetchone()[0]

    # Two map_results
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 1, ?, ?, ?, 6, 3, 0, 0, 'Opener')""",
        (match_id, tunisia_id, dvs_id, dvs_id),
    )
    tunisia_result_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 2, ?, ?, ?, 250, 200, 1, 0, 'Neutral')""",
        (match_id, summit_id, oug_id, dvs_id),
    )
    summit_result_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Player stats: Alpha + Bravo on both maps, week 1
    stats = [
        (tunisia_result_id, 1, "Alpha", 20, 15, 5),
        (tunisia_result_id, 1, "Bravo", 18, 12, 8),
        (summit_result_id, 1, "Alpha", 30, 25, 10),
        (summit_result_id, 1, "Bravo", 28, 20, 12),
    ]
    conn.executemany(
        """INSERT INTO tournament_player_stats
           (result_id, week, player_name, kills, deaths, assists)
           VALUES (?, ?, ?, ?, ?, ?)""",
        stats,
    )
    conn.commit()
    yield conn
    conn.close()


def test_player_summary_all(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players)
    assert len(rows) == 2
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 50
    assert alpha["deaths"] == 40
    assert alpha["games"] == 2
    assert alpha["kd"] == pytest.approx(50 / 40, abs=0.01)


def test_player_summary_filter_by_player(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["player_name"] == "Alpha"


def test_player_summary_filter_by_mode(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, mode="SnD")
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 20


def test_player_summary_filter_by_week(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, week_range=(1, 1))
    assert len(rows) == 2
    rows_empty = player_summary(db_with_tournament_players, week_range=(2, 2))
    assert rows_empty == []


def test_player_weekly_trend(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_weekly_trend
    rows = player_weekly_trend(db_with_tournament_players, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["week"] == 1
    assert rows[0]["kd"] == pytest.approx(50 / 40, abs=0.01)


def test_player_map_breakdown(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_map_breakdown
    rows = player_map_breakdown(db_with_tournament_players, player="Alpha")
    assert len(rows) == 2
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["games"] == 1
    assert tunisia["avg_kills"] == 20.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_queries_tournament_player.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement queries module**

Create `src/cdm_stats/db/queries_tournament_player.py`:

```python
import sqlite3


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
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT tp.player_name,
                   SUM(tp.kills) as kills,
                   SUM(tp.deaths) as deaths,
                   SUM(tp.assists) as assists,
                   COUNT(*) as games,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.kills + tp.deaths + tp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY tp.player_name
            ORDER BY tp.player_name""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "kills": r[1], "deaths": r[2], "assists": r[3],
            "games": r[4],
            "kd": round(r[1] / r[2], 2) if r[2] > 0 else 0.0,
            "avg_pos_eng_pct": r[5] or 0.0,
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
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT tp.player_name, tp.week,
                   SUM(tp.kills) as kills,
                   SUM(tp.deaths) as deaths
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY tp.player_name, tp.week
            ORDER BY tp.player_name, tp.week""",
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
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT m.map_name, m.mode,
                   COUNT(DISTINCT tp.result_id) as games,
                   ROUND(AVG(tp.kills), 1) as avg_kills,
                   ROUND(AVG(tp.deaths), 1) as avg_deaths,
                   ROUND(AVG(tp.assists), 1) as avg_assists,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.deaths, 0)), 2) as avg_kd,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.kills + tp.deaths + tp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY m.map_name, m.mode
            ORDER BY m.mode, m.map_name""",
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

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_queries_tournament_player.py -v`
Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/db/queries_tournament_player.py tests/test_queries_tournament_player.py
git commit -m "feat(db): add tournament player queries module"
```

---

## Task 4: Tournament Player CSV Loader

**Files:**
- Create: `src/cdm_stats/ingestion/tournament_player_loader.py`
- Test: `tests/test_tournament_player_loader.py`

The loader reads CSV columns `Date, Week, Opponent, Map, Mode, Player, Kills, Deaths, Assists` (mirrors `scrims_players.csv`). It resolves each row to a `map_results.result_id` via `(match_date, opponent appears on either side of the match, map_id)` — this combination is unique in practice since a team can't play the same map in two different series on the same day. If more than one row matches, the loader errors on that row.

- [ ] **Step 1: Write failing test**

Create `tests/test_tournament_player_loader.py`:

```python
import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-02-15,1,OUG,Tunisia,SnD,Alpha,20,15,5
2026-02-15,1,OUG,Tunisia,SnD,Bravo,18,12,8
2026-02-15,1,OUG,Summit,HP,Alpha,30,25,10
2026-02-15,1,OUG,Summit,HP,Bravo,28,20,12"""


@pytest.fixture
def db_with_match():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)

    dvs_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number)
           VALUES ('2026-02-15', ?, ?, ?, ?, 'CDL_BO5', 1)""",
        (dvs_id, oug_id, dvs_id, dvs_id),
    )
    match_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    tunisia_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'"
    ).fetchone()[0]
    summit_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'"
    ).fetchone()[0]

    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 1, ?, ?, ?, 6, 3, 0, 0, 'Opener')""",
        (match_id, tunisia_id, dvs_id, dvs_id),
    )
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 2, ?, ?, ?, 250, 200, 1, 0, 'Neutral')""",
        (match_id, summit_id, oug_id, dvs_id),
    )
    conn.commit()
    yield conn
    conn.close()


def test_ingest_tournament_players_inserts_rows(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    results = ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    ok = [r for r in results if r["status"] == "ok"]
    assert len(ok) == 4

    count = db_with_match.execute(
        "SELECT COUNT(*) FROM tournament_player_stats"
    ).fetchone()[0]
    assert count == 4

    row = db_with_match.execute(
        """SELECT week, player_name, kills, deaths, assists
           FROM tournament_player_stats
           WHERE player_name = 'Alpha'
           ORDER BY stat_id"""
    ).fetchall()
    assert row[0] == (1, "Alpha", 20, 15, 5)
    assert row[1] == (1, "Alpha", 30, 25, 10)


def test_ingest_tournament_players_skips_duplicates(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    results = ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    skipped = [r for r in results if r["status"] == "skipped"]
    assert len(skipped) == 4

    count = db_with_match.execute(
        "SELECT COUNT(*) FROM tournament_player_stats"
    ).fetchone()[0]
    assert count == 4


def test_ingest_tournament_players_errors_on_unknown_opponent(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    bad_csv = "Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists\n2026-02-15,1,ZZZ,Tunisia,SnD,Alpha,20,15,5"
    results = ingest_tournament_players(db_with_match, io.StringIO(bad_csv))
    assert len(results) == 1
    assert results[0]["status"] == "error"
    assert "ZZZ" in results[0]["errors"]


def test_ingest_tournament_players_errors_on_missing_match(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    bad_csv = "Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists\n2099-01-01,1,OUG,Tunisia,SnD,Alpha,20,15,5"
    results = ingest_tournament_players(db_with_match, io.StringIO(bad_csv))
    assert len(results) == 1
    assert results[0]["status"] == "error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tournament_player_loader.py -v`
Expected: FAIL — loader module doesn't exist.

- [ ] **Step 3: Implement loader**

Create `src/cdm_stats/ingestion/tournament_player_loader.py`:

```python
import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr, get_map_id


def ingest_tournament_players(
    conn: sqlite3.Connection,
    file: IO,
) -> list[dict]:
    """Ingest tournament player-level CSV.

    CSV columns: Date, Week, Opponent, Map, Mode, Player, Kills, Deaths, Assists.
    Matches are resolved by (match_date, opponent_id appears on either side,
    map_id) — unique in practice because a team can't play the same map in two
    different series on the same day. Matches must already be ingested.
    """
    reader = csv.DictReader(file)
    results: list[dict] = []

    for row in reader:
        date = row["Date"].strip()
        week = int(row["Week"].strip())
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())

        desc = f"{date} {map_name} {mode} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if opponent_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        map_id = get_map_id(conn, map_name, mode)
        if map_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown map: {map_name} ({mode})"})
            continue

        # Find the map_result: match on date where opponent is on either side,
        # then the specific map in that match. Unique in practice.
        result_rows = conn.execute(
            """SELECT mr.result_id
               FROM map_results mr
               JOIN matches mt ON mr.match_id = mt.match_id
               WHERE mt.match_date = ?
                 AND (mt.team1_id = ? OR mt.team2_id = ?)
                 AND mr.map_id = ?""",
            (date, opponent_id, opponent_id, map_id),
        ).fetchall()

        if not result_rows:
            results.append({"status": "error", "row": desc,
                            "errors": "No matching map_result found"})
            continue
        if len(result_rows) > 1:
            results.append({"status": "error", "row": desc,
                            "errors": "Multiple matching map_results — ambiguous"})
            continue

        result_id = result_rows[0][0]

        existing = conn.execute(
            "SELECT stat_id FROM tournament_player_stats WHERE result_id = ? AND player_name = ?",
            (result_id, player_name),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": desc})
            continue

        conn.execute(
            """INSERT INTO tournament_player_stats
               (result_id, week, player_name, kills, deaths, assists)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (result_id, week, player_name, kills, deaths, assists),
        )
        results.append({"status": "ok", "row": desc})

    conn.commit()
    return results
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_tournament_player_loader.py -v`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/ingestion/tournament_player_loader.py tests/test_tournament_player_loader.py
git commit -m "feat(ingestion): add tournament player CSV loader"
```

---

## Task 5: Shared Week Pills Component

**Files:**
- Create: `src/cdm_stats/dashboard/components/__init__.py`
- Create: `src/cdm_stats/dashboard/components/week_pills.py`
- Test: `tests/test_week_pills.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_week_pills.py`:

```python
def test_week_pills_builds_options():
    from cdm_stats.dashboard.components.week_pills import week_pills
    component = week_pills("test-pills", [1, 2, 3])
    assert component.id == "test-pills"
    values = [opt["value"] for opt in component.options]
    labels = [opt["label"] for opt in component.options]
    assert values == ["all", 1, 2, 3]
    assert labels == ["All", "W1", "W2", "W3"]
    assert component.value == "all"


def test_week_pills_empty_weeks():
    from cdm_stats.dashboard.components.week_pills import week_pills
    component = week_pills("test-pills", [])
    values = [opt["value"] for opt in component.options]
    assert values == ["all"]
    assert component.value == "all"


def test_pill_value_to_range_all():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range("all") is None


def test_pill_value_to_range_int():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range(3) == (3, 3)


def test_pill_value_to_range_none_defaults_to_all():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range(None) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_week_pills.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Create the components package and week_pills module**

Create `src/cdm_stats/dashboard/components/__init__.py` (empty file):

```python
```

Create `src/cdm_stats/dashboard/components/week_pills.py`:

```python
"""Shared week selector rendered as a Bootstrap button-pill radio group.

Value contract:
    - "all" means no week filter (None downstream)
    - integer N means that specific week, mapped to (N, N) downstream

Use pill_value_to_range() to translate to the week_range tuple that the
query layer expects.
"""
import dash_bootstrap_components as dbc


def week_pills(component_id: str, weeks: list[int]) -> dbc.RadioItems:
    """Return a RadioItems rendered as a btn-check pill group.

    Args:
        component_id: Dash component id.
        weeks: Sorted list of available week numbers (may be empty).
    """
    options = [{"label": "All", "value": "all"}]
    options.extend({"label": f"W{w}", "value": w} for w in weeks)
    return dbc.RadioItems(
        id=component_id,
        options=options,
        value="all",
        inline=True,
        inputClassName="btn-check",
        labelClassName="btn btn-outline-primary btn-sm me-1",
        labelCheckedClassName="active",
        className="mb-0",
    )


def pill_value_to_range(value) -> tuple[int, int] | None:
    """Convert a week-pill value to a (low, high) week range.

    "all" or None -> None (no filter)
    integer w     -> (w, w)
    """
    if value is None or value == "all":
        return None
    return (int(value), int(value))
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_week_pills.py -v`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/components/__init__.py src/cdm_stats/dashboard/components/week_pills.py tests/test_week_pills.py
git commit -m "feat(dashboard): add shared week_pills component"
```

---

## Task 6: Wire Week Pills into Scrim Performance Tab

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/scrim_performance.py`
- Test: `tests/test_dashboard_scrim.py`

- [ ] **Step 1: Add failing layout test asserting pill presence**

Append to `tests/test_dashboard_scrim.py`:

```python
def test_scrim_performance_layout_uses_week_pills():
    """Scrim layout renders the week_pills component, not a RangeSlider."""
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    import json
    result = layout()
    serialized = json.dumps(result.to_plotly_json(), default=str)
    assert "scrim-week-pills" in serialized
    assert "scrim-week-slider" not in serialized
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dashboard_scrim.py::test_scrim_performance_layout_uses_week_pills -v`
Expected: FAIL — the layout still uses `scrim-week-slider`.

- [ ] **Step 3: Update `scrim_performance.py` to use week_pills**

Edit `src/cdm_stats/dashboard/tabs/scrim_performance.py`:

Add import near the top (with the other cdm_stats imports):

```python
from cdm_stats.dashboard.components.week_pills import week_pills, pill_value_to_range
```

In `layout()`, replace the third `dbc.Col` (the one containing `dcc.RangeSlider(id="scrim-week-slider", ...)`) with:

```python
            dbc.Col([
                html.Label("Weeks", style={"color": COLORS["text"]}),
                html.Div(id="scrim-week-pills-container"),
            ], width=8),
```

Replace the `update_week_slider` callback (the one with four Outputs on `scrim-week-slider`) with a single-Output callback that renders the pill component:

```python
    @app.callback(
        Output("scrim-week-pills-container", "children"),
        Input("scrim-mode-filter", "value"),
    )
    def render_scrim_week_pills(_mode):
        conn = get_db()
        weeks = _get_available_weeks(conn)
        conn.close()
        return week_pills("scrim-week-pills", weeks)
```

Update the main `update_scrim_tab` callback: change the `Input("scrim-week-slider", "value")` to `Input("scrim-week-pills", "value")`, rename the callback parameter from `week_range` to `week_value`, and replace the `wr = tuple(week_range) if week_range else None` line with:

```python
        wr = pill_value_to_range(week_value)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_dashboard_scrim.py -v`
Expected: all scrim dashboard tests PASS (including the new layout test).

Run: `uv run pytest tests/test_dashboard_app.py -v`
Expected: app-level tests still PASS (layout still renders).

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/scrim_performance.py tests/test_dashboard_scrim.py
git commit -m "feat(dashboard): replace scrim week slider with pills"
```

---

## Task 7: Player Stats — Week Pills + Source Toggle

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/player_stats.py`
- Test: `tests/test_dashboard_scrim.py`

The Player Stats tab gains a Source radio (`Tournament | Scrim`, default Tournament). When Source=Scrim, queries dispatch to `queries_scrim`. When Source=Tournament, they dispatch to `queries_tournament_player`. The player dropdown, cards, trend, and map table all re-query from the active source. Week pills replace the range slider.

- [ ] **Step 1: Add failing tests for layout + dispatch**

Append to `tests/test_dashboard_scrim.py`:

```python
def test_player_stats_layout_has_pills_and_source_toggle():
    from cdm_stats.dashboard.tabs.player_stats import layout
    import json
    result = layout()
    serialized = json.dumps(result.to_plotly_json(), default=str)
    assert "player-week-pills" in serialized
    assert "player-week-slider" not in serialized
    assert "player-source-filter" in serialized


def test_player_stats_dispatch_scrim(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_player_cards_data
    data = _build_player_cards_data(scrim_db, source="scrim")
    assert len(data) == 5


def test_player_stats_dispatch_tournament_empty(scrim_db):
    """Tournament source with no tournament data returns empty."""
    from cdm_stats.dashboard.tabs.player_stats import _build_player_cards_data
    data = _build_player_cards_data(scrim_db, source="tournament")
    assert data == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_scrim.py -v -k "player_stats_layout_has_pills or dispatch"`
Expected: FAIL — layout lacks the new ids; `_build_player_cards_data` doesn't accept `source`.

- [ ] **Step 3: Rewrite `player_stats.py` tab with source dispatch + pills**

Replace the contents of `src/cdm_stats/dashboard/tabs/player_stats.py`:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.components.week_pills import week_pills, pill_value_to_range
from cdm_stats.dashboard.helpers import COLORS, MODE_COLORS
from cdm_stats.db import queries_scrim, queries_tournament_player

PLAYER_COLORS = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
]


def _queries_for(source: str):
    """Pick the query module for the selected source."""
    if source == "scrim":
        return queries_scrim
    return queries_tournament_player


def _build_player_cards_data(
    conn: sqlite3.Connection,
    source: str = "tournament",
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    return _queries_for(source).player_summary(
        conn, player=player, mode=mode, week_range=week_range,
    )


def _build_kd_trend_data(
    conn: sqlite3.Connection,
    source: str = "tournament",
    player: str | None = None,
    mode: str | None = None,
) -> list[dict]:
    return _queries_for(source).player_weekly_trend(
        conn, player=player, mode=mode,
    )


def _build_player_map_data(
    conn: sqlite3.Connection,
    source: str = "tournament",
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    return _queries_for(source).player_map_breakdown(
        conn, player=player, mode=mode, week_range=week_range,
    )


def _player_card(data: dict, color: str) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.H5(data["player_name"], style={"color": color, "marginBottom": "4px"}),
            html.H3(
                f"{data['kd']:.2f}",
                style={"color": COLORS["text"], "marginBottom": "0"},
            ),
            html.Small("K/D", style={"color": COLORS["muted"]}),
            html.Div(
                f"{data['avg_pos_eng_pct']:.1f}% Pos Eng",
                style={"color": COLORS["text"], "fontSize": "0.95rem", "marginTop": "4px"},
            ),
            html.Small(
                f"{data['kills']}K / {data['deaths']}D / {data['assists']}A  ·  {data['games']} maps",
                style={"color": COLORS["muted"], "fontSize": "0.75rem"},
            ),
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


def _get_available_players(conn: sqlite3.Connection, source: str) -> list[str]:
    table = "tournament_player_stats" if source == "tournament" else "scrim_player_stats"
    rows = conn.execute(
        f"SELECT DISTINCT player_name FROM {table} ORDER BY player_name"
    ).fetchall()
    return [r[0] for r in rows]


def _get_available_weeks(conn: sqlite3.Connection, source: str) -> list[int]:
    if source == "tournament":
        rows = conn.execute(
            "SELECT DISTINCT week FROM tournament_player_stats ORDER BY week"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT week FROM scrim_maps ORDER BY week"
        ).fetchall()
    return [r[0] for r in rows]


def layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.Label("Source", style={"color": COLORS["text"]}),
                dbc.RadioItems(
                    id="player-source-filter",
                    options=[
                        {"label": "Tournament", "value": "tournament"},
                        {"label": "Scrim", "value": "scrim"},
                    ],
                    value="tournament",
                    inline=True,
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-info btn-sm me-1",
                    labelCheckedClassName="active",
                ),
            ], width=3),
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
                html.Div(id="player-week-pills-container"),
            ], width=5),
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
        Input("player-source-filter", "value"),
    )
    def populate_players(source):
        conn = get_db()
        players = _get_available_players(conn, source)
        conn.close()
        return [{"label": "All", "value": "All"}] + [{"label": p, "value": p} for p in players]

    @app.callback(
        Output("player-week-pills-container", "children"),
        Input("player-source-filter", "value"),
    )
    def render_player_week_pills(source):
        conn = get_db()
        weeks = _get_available_weeks(conn, source)
        conn.close()
        return week_pills("player-week-pills", weeks)

    @app.callback(
        Output("player-summary-cards", "children"),
        Output("player-kd-chart", "figure"),
        Output("player-map-table", "children"),
        Input("player-source-filter", "value"),
        Input("player-filter", "value"),
        Input("player-mode-filter", "value"),
        Input("player-week-pills", "value"),
    )
    def update_player_tab(source, player, mode, week_value):
        conn = get_db()
        player_val = player if player != "All" else None
        mode_val = mode if mode != "All" else None
        wr = pill_value_to_range(week_value)

        card_data = _build_player_cards_data(
            conn, source=source, player=player_val, mode=mode_val, week_range=wr,
        )
        if not card_data and source == "tournament":
            card_row = dbc.Alert(
                "No tournament player data ingested yet.",
                color="info",
            )
        elif not card_data:
            card_row = html.P("No player data found.", style={"color": COLORS["muted"]})
        else:
            cards = []
            for i, d in enumerate(card_data):
                color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
                cards.append(dbc.Col(_player_card(d, color), width=True))
            card_row = dbc.Row(cards)

        trend_data = _build_kd_trend_data(
            conn, source=source, player=player_val, mode=mode_val,
        )
        fig = _kd_trend_figure(trend_data)

        map_data = _build_player_map_data(
            conn, source=source, player=player_val, mode=mode_val, week_range=wr,
        )
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
                bordered=True, hover=True, size="sm",
                style={"backgroundColor": COLORS["card_bg"]},
            )
        else:
            table = html.P("No player data found.", style={"color": COLORS["muted"]})

        conn.close()
        return card_row, fig, table
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/test_dashboard_scrim.py -v`
Expected: all scrim + player tests PASS (including the three new ones).

Run: `uv run pytest -v`
Expected: full test suite passes.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/player_stats.py tests/test_dashboard_scrim.py
git commit -m "feat(dashboard): add tournament/scrim source toggle and week pills to Player Stats"
```

---

## Verification Checklist

After all tasks are committed:

- [ ] `uv run pytest -v` — full suite green
- [ ] `uv run python -m cdm_stats.dashboard` — dashboard boots; tab order is Match-Up Prep → Team Profile → Player Stats → Scrim Performance → Elo Tracker
- [ ] Scrim Performance tab: click week pills, verify single-week + All work and update tables/charts
- [ ] Player Stats tab: flip Source between Tournament and Scrim, verify the player list, cards, trend chart, and map table update. With no tournament data ingested, Tournament source shows the info alert.
- [ ] Check `data/cdl.db`: `tournament_player_stats` table exists with correct columns (the migration should self-apply on next dashboard boot since `schema.create_tables/migrate` is called — verify by re-running or by inspecting via sqlite CLI).
