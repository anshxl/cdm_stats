# Map Strength Rating & Dashboard Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace misleading avoidance/target indices with a context-weighted, Elo-adjusted Map Strength Rating and restructure all three dashboard tabs to surface it.

**Architecture:** New `map_strength.py` module computes weighted win rates per team per map. Context weights (Must-Win 3.0, Close-Out 2.0, Neutral 1.0, Opener 0.5) amplify high-stakes signal. Opponent quality (`opponent_elo / league_avg_elo`) adjusts for schedule strength. Dashboard tabs replace avoidance/target with Map Strength throughout.

**Tech Stack:** Python 3.11, SQLite, Plotly/Dash, pytest

**Spec:** `docs/superpowers/specs/2026-04-02-elo-margin-weighting-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/cdm_stats/metrics/map_strength.py` | Map Strength Rating calculation |
| Create | `tests/test_map_strength.py` | Tests for map_strength module |
| Modify | `src/cdm_stats/metrics/avoidance.py` | Remove avoidance_index, target_index, _get_pick_opportunities; keep pick_win_loss, defend_win_loss, pick_context_distribution |
| Modify | `tests/test_avoidance.py` | Remove avoidance/target tests; keep pick_wl, defend_wl, context distribution tests |
| Modify | `src/cdm_stats/dashboard/tabs/map_matrix.py` | Use Map Strength for heatmap colors/tooltips |
| Modify | `tests/test_dashboard_map_matrix.py` | Update assertions for new data shape |
| Modify | `src/cdm_stats/dashboard/tabs/team_profile.py` | Replace avoidance/target card with Map Strength cards + expandable results table |
| Modify | `tests/test_dashboard_team_profile.py` | Update for new data builders |
| Modify | `src/cdm_stats/dashboard/tabs/matchup_prep.py` | Side-by-side Map Strength with delta, remove avoidance/target |
| Modify | `tests/test_dashboard_matchup_prep.py` | Update for new data shape |

---

### Task 1: Create map_strength module with core calculation

**Files:**
- Create: `src/cdm_stats/metrics/map_strength.py`
- Create: `tests/test_map_strength.py`

- [ ] **Step 1: Write failing tests for map_strength**

Create `tests/test_map_strength.py`:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def _get_ids(db):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    tunisia = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'").fetchone()[0]
    summit_hp = db.execute("SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'").fetchone()[0]
    raid = db.execute("SELECT map_id FROM maps WHERE map_name = 'Raid' AND mode = 'Control'").fetchone()[0]
    return dvs, oug, tunisia, summit_hp, raid


def test_map_strength_returns_dict_keys(db):
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert "rating" in result
    assert "weighted_sample" in result
    assert "total_played" in result
    assert "low_confidence" in result


def test_map_strength_win_is_positive(db):
    """DVS won on Tunisia — rating should be > 0.5."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert result["rating"] > 0.5
    assert result["total_played"] == 1


def test_map_strength_loss_is_below_half(db):
    """OUG lost on Tunisia — rating should be < 0.5."""
    from cdm_stats.metrics.map_strength import map_strength
    _, oug, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, oug, tunisia)
    assert result["rating"] < 0.5
    assert result["total_played"] == 1


def test_map_strength_no_data_returns_none_rating(db):
    """Team with no results on a map should get rating=None."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, _, _, _ = _get_ids(db)
    # DVS never played Hacienda HP
    hacienda = db.execute("SELECT map_id FROM maps WHERE map_name = 'Hacienda' AND mode = 'HP'").fetchone()[0]
    result = map_strength(db, dvs, hacienda)
    assert result["rating"] is None
    assert result["total_played"] == 0


def test_map_strength_low_confidence_under_3(db):
    """With only 1 game played, low_confidence should be True."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert result["low_confidence"] is True


def test_map_strength_opener_weighted_less(db):
    """Tunisia was played as Opener (slot 1, weight 0.5). Verify weighted_sample reflects this."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    # Opener weight = 0.5, opponent_quality ~1.0 (both start at 1000)
    # weighted_sample should be approximately 0.5
    assert result["weighted_sample"] < 1.0


def test_context_weights_constant():
    from cdm_stats.metrics.map_strength import CONTEXT_WEIGHTS
    assert CONTEXT_WEIGHTS["Must-Win"] == 3.0
    assert CONTEXT_WEIGHTS["Close-Out"] == 2.0
    assert CONTEXT_WEIGHTS["Neutral"] == 1.0
    assert CONTEXT_WEIGHTS["Opener"] == 0.5
    assert CONTEXT_WEIGHTS["Coin-Toss"] == 1.0
    assert CONTEXT_WEIGHTS["Unknown"] == 0.5


def test_all_team_map_strengths(db):
    """Bulk calculation should return dict keyed by (team_id, map_id)."""
    from cdm_stats.metrics.map_strength import all_team_map_strengths
    strengths = all_team_map_strengths(db)
    assert isinstance(strengths, dict)
    dvs, _, tunisia, _, _ = _get_ids(db)
    assert (dvs, tunisia) in strengths
    assert strengths[(dvs, tunisia)]["rating"] is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_map_strength.py -v`
Expected: FAIL with ModuleNotFoundError (map_strength module doesn't exist)

- [ ] **Step 3: Implement map_strength module**

Create `src/cdm_stats/metrics/map_strength.py`:

```python
import sqlite3

from cdm_stats.metrics.elo import get_current_elo, SEED_ELO

CONTEXT_WEIGHTS = {
    "Opener": 0.5,
    "Neutral": 1.0,
    "Must-Win": 3.0,
    "Close-Out": 2.0,
    "Coin-Toss": 1.0,
    "Unknown": 0.5,
}

LOW_SAMPLE_THRESHOLD = 3


def _get_league_avg_elo(conn: sqlite3.Connection, match_id: int) -> float:
    """Get the average Elo of all teams at the time of a match.

    Uses each team's most recent Elo before or at this match.
    Falls back to SEED_ELO for teams with no history yet.
    """
    match_date = conn.execute(
        "SELECT match_date FROM matches WHERE match_id = ?", (match_id,)
    ).fetchone()[0]

    team_ids = [r[0] for r in conn.execute("SELECT team_id FROM teams").fetchall()]
    total = 0.0
    for tid in team_ids:
        row = conn.execute(
            """SELECT elo_after FROM team_elo
               WHERE team_id = ? AND match_date <= ?
               ORDER BY match_date DESC, elo_id DESC LIMIT 1""",
            (tid, match_date),
        ).fetchone()
        total += row[0] if row else SEED_ELO
    return total / len(team_ids) if team_ids else SEED_ELO


def _get_opponent_elo_at_match(
    conn: sqlite3.Connection, team_id: int, match_id: int
) -> float:
    """Get the opponent's Elo just before a specific match."""
    match = conn.execute(
        "SELECT team1_id, team2_id, match_date FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    team1_id, team2_id, match_date = match
    opp_id = team2_id if team_id == team1_id else team1_id

    row = conn.execute(
        """SELECT elo_after FROM team_elo
           WHERE team_id = ? AND match_date < ?
           ORDER BY match_date DESC, elo_id DESC LIMIT 1""",
        (opp_id, match_date),
    ).fetchone()
    return row[0] if row else SEED_ELO


def map_strength(
    conn: sqlite3.Connection, team_id: int, map_id: int
) -> dict:
    """Compute the Map Strength Rating for a team on a specific map.

    Returns:
        {
            "rating": float | None (0.0-1.0, None if no data),
            "weighted_sample": float,
            "total_played": int,
            "low_confidence": bool,
        }
    """
    rows = conn.execute(
        """SELECT mr.match_id, mr.winner_team_id, mr.pick_context, mr.slot
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date""",
        (map_id, team_id, team_id),
    ).fetchall()

    if not rows:
        return {
            "rating": None,
            "weighted_sample": 0.0,
            "total_played": 0,
            "low_confidence": True,
        }

    weighted_sum = 0.0
    weight_total = 0.0

    for match_id, winner_id, pick_context, slot in rows:
        result = 1.0 if winner_id == team_id else 0.0
        context_weight = CONTEXT_WEIGHTS.get(pick_context, 1.0)

        opp_elo = _get_opponent_elo_at_match(conn, team_id, match_id)
        league_avg = _get_league_avg_elo(conn, match_id)
        opponent_quality = opp_elo / league_avg if league_avg > 0 else 1.0

        weight = context_weight * opponent_quality
        weighted_sum += result * weight
        weight_total += weight

    rating = weighted_sum / weight_total if weight_total > 0 else None
    total_played = len(rows)

    return {
        "rating": rating,
        "weighted_sample": weight_total,
        "total_played": total_played,
        "low_confidence": total_played < LOW_SAMPLE_THRESHOLD,
    }


def all_team_map_strengths(
    conn: sqlite3.Connection,
) -> dict[tuple[int, int], dict]:
    """Compute Map Strength for every (team, map) pair.

    Returns dict keyed by (team_id, map_id) -> map_strength result dict.
    """
    teams = conn.execute("SELECT team_id FROM teams").fetchall()
    maps = conn.execute("SELECT map_id FROM maps").fetchall()

    result = {}
    for (team_id,) in teams:
        for (map_id,) in maps:
            result[(team_id, map_id)] = map_strength(conn, team_id, map_id)

    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_map_strength.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/map_strength.py tests/test_map_strength.py
git commit -m "feat: add map_strength module with context-weighted Elo-adjusted win rate"
```

---

### Task 2: Remove avoidance_index, target_index, and _get_pick_opportunities

**Files:**
- Modify: `src/cdm_stats/metrics/avoidance.py`
- Modify: `tests/test_avoidance.py`

- [ ] **Step 1: Remove avoidance/target tests from test_avoidance.py**

In `tests/test_avoidance.py`, remove the import of `avoidance_index` and `target_index` from the import statement (line 8). Also remove the two test functions `test_avoidance_index_basic` (lines 68-75) and `test_target_index_basic` (lines 78-85).

Updated import line:
```python
from cdm_stats.metrics.avoidance import (
    pick_win_loss,
    defend_win_loss,
    pick_context_distribution,
)
```

Remove the entire `test_avoidance_index_basic` function (lines 68-75) and `test_target_index_basic` function (lines 78-85).

- [ ] **Step 2: Run tests to verify remaining tests still pass**

Run: `uv run pytest tests/test_avoidance.py -v`
Expected: 4 tests PASS (pick_win_loss_dvs_tunisia, defend_win_loss_oug_tunisia, pick_win_loss_no_data, pick_context_distribution)

- [ ] **Step 3: Remove avoidance_index, target_index, _get_pick_opportunities from avoidance.py**

In `src/cdm_stats/metrics/avoidance.py`, remove the entire `_get_pick_opportunities` function (lines 33-52), the entire `avoidance_index` function (lines 55-63), and the entire `target_index` function (lines 66-87). Also remove the `SLOT_MODES` import since only `_get_pick_opportunities` used it.

Updated import line:
```python
import sqlite3
```

The file should contain only `pick_win_loss`, `defend_win_loss`, and `pick_context_distribution`.

- [ ] **Step 4: Run full test suite to check for broken imports**

Run: `uv run pytest -v`
Expected: Failures in `test_dashboard_map_matrix.py`, `test_dashboard_team_profile.py`, `test_dashboard_matchup_prep.py` (these still import avoidance_index/target_index through the dashboard tabs). This is expected — we'll fix these in Tasks 3-5.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/metrics/avoidance.py tests/test_avoidance.py
git commit -m "refactor: remove avoidance_index, target_index from avoidance module"
```

---

### Task 3: Update Map Matrix tab to use Map Strength

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/map_matrix.py`
- Modify: `tests/test_dashboard_map_matrix.py`

- [ ] **Step 1: Update test expectations for new data shape**

Replace the contents of `tests/test_dashboard_map_matrix.py` with:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_build_matrix_data_has_strength(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db)
    assert len(teams) > 0
    assert len(maps) > 0
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia_id = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia'").fetchone()[0]
    cell = matrix.get((dvs_id, tunisia_id))
    assert cell is not None
    assert cell["wins"] == 1
    assert cell["losses"] == 0
    assert "strength" in cell
    assert cell["strength"]["rating"] is not None
    assert cell["strength"]["rating"] > 0.5


def test_build_matrix_data_mode_filter(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db, mode_filter="SnD")
    for _, _, mode in maps:
        assert mode == "SnD"


def test_build_matrix_data_no_avoidance_keys(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db)
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia_id = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia'").fetchone()[0]
    cell = matrix.get((dvs_id, tunisia_id))
    assert "avoid" not in cell
    assert "target" not in cell
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_map_matrix.py -v`
Expected: FAIL (import errors from avoidance_index/target_index removal, or missing `strength` key)

- [ ] **Step 3: Update map_matrix.py**

Replace the entire contents of `src/cdm_stats/dashboard/tabs/map_matrix.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, dcc, html

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS,
    get_all_maps,
    get_all_teams,
)
from cdm_stats.metrics.avoidance import pick_win_loss, defend_win_loss
from cdm_stats.metrics.map_strength import map_strength


def _build_matrix_data(
    conn: sqlite3.Connection, mode_filter: str | None = None
) -> tuple[list[tuple[int, str]], list[tuple[int, str, str]], dict]:
    """Build the data structures for the map matrix heatmap.

    Returns:
        teams: list of (team_id, abbreviation)
        maps: list of (map_id, map_name, mode)
        matrix: dict of (team_id, map_id) -> cell data dict
    """
    teams = get_all_teams(conn)
    maps = get_all_maps(conn)

    if mode_filter and mode_filter != "All":
        maps = [(mid, name, mode) for mid, name, mode in maps if mode == mode_filter]

    matrix: dict[tuple[int, int], dict] = {}

    for team_id, abbr in teams:
        for map_id, map_name, mode in maps:
            pw = pick_win_loss(conn, team_id, map_id)
            dw = defend_win_loss(conn, team_id, map_id)
            ms = map_strength(conn, team_id, map_id)

            wins = pw["wins"] + dw["wins"]
            losses = pw["losses"] + dw["losses"]

            matrix[(team_id, map_id)] = {
                "wins": wins,
                "losses": losses,
                "pick_wl": pw,
                "defend_wl": dw,
                "strength": ms,
            }

    return teams, maps, matrix


def _build_heatmap_figure(
    teams: list[tuple[int, str]],
    maps: list[tuple[int, str, str]],
    matrix: dict,
) -> go.Figure:
    """Build a Plotly heatmap figure from the matrix data."""
    x_labels = [f"{name} ({mode})" for _, name, mode in maps]
    y_labels = [abbr for _, abbr in teams]

    z_values: list[list[float | None]] = []
    text_values: list[list[str]] = []
    hover_texts: list[list[str]] = []

    for team_id, abbr in teams:
        z_row: list[float | None] = []
        text_row: list[str] = []
        hover_row: list[str] = []

        for map_id, map_name, mode in maps:
            cell = matrix.get((team_id, map_id))
            if cell is None:
                z_row.append(None)
                text_row.append("")
                hover_row.append("")
                continue

            wins = cell["wins"]
            losses = cell["losses"]
            total = wins + losses
            ms = cell["strength"]

            # Use Map Strength for heatmap color
            z_row.append(ms["rating"])
            text_row.append(f"{wins}-{losses}")

            # Build hover text
            pw = cell["pick_wl"]
            dw = cell["defend_wl"]

            strength_pct = f"{ms['rating']:.0%}" if ms["rating"] is not None else "N/A"
            low_sample = "\u26a0\ufe0f Low sample" if ms["low_confidence"] else ""

            hover = (
                f"<b>{abbr}</b> on <b>{map_name}</b> ({mode})<br>"
                f"Map Strength: {strength_pct} (w={ms['weighted_sample']:.1f})<br>"
                f"Overall: {wins}-{losses}<br>"
                f"Pick: {pw['wins']}-{pw['losses']}<br>"
                f"Defend: {dw['wins']}-{dw['losses']}<br>"
                f"{low_sample}"
            )
            hover_row.append(hover)

        z_values.append(z_row)
        text_values.append(text_row)
        hover_texts.append(hover_row)

    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=x_labels,
            y=y_labels,
            text=text_values,
            texttemplate="%{text}",
            hovertext=hover_texts,
            hoverinfo="text",
            colorscale=[
                [0, "#f87171"],    # red (loss)
                [0.5, "#6b7280"],  # gray (neutral)
                [1, "#4ade80"],    # green (win)
            ],
            zmin=0,
            zmax=1,
            colorbar=dict(
                title="Map Strength",
                tickvals=[0, 0.25, 0.5, 0.75, 1],
                ticktext=["0%", "25%", "50%", "75%", "100%"],
            ),
        )
    )

    fig.update_layout(
        xaxis=dict(
            side="top",
            tickangle=-45,
            color=COLORS["text"],
        ),
        yaxis=dict(
            autorange="reversed",
            color=COLORS["text"],
        ),
        paper_bgcolor=COLORS["page_bg"],
        plot_bgcolor=COLORS["card_bg"],
        font=dict(color=COLORS["text"]),
        margin=dict(t=120, l=80, r=40, b=40),
        height=max(400, len(teams) * 35 + 200),
    )

    return fig


def layout():
    """Return the layout for the Map Matrix tab."""
    return html.Div(
        [
            dbc.Row(
                dbc.Col(
                    dcc.Dropdown(
                        id="map-matrix-mode-filter",
                        options=[
                            {"label": "All Modes", "value": "All"},
                            {"label": "SnD", "value": "SnD"},
                            {"label": "HP", "value": "HP"},
                            {"label": "Control", "value": "Control"},
                        ],
                        value="All",
                        clearable=False,
                        style={
                            "backgroundColor": COLORS["card_bg"],
                            "color": "#000",
                            "width": "200px",
                        },
                    ),
                    width=3,
                ),
                className="mb-3",
            ),
            dcc.Graph(id="map-matrix-heatmap"),
        ]
    )


def register_callbacks(app):
    """Register callbacks for the Map Matrix tab."""

    @app.callback(
        Output("map-matrix-heatmap", "figure"),
        Input("map-matrix-mode-filter", "value"),
    )
    def update_heatmap(mode_filter):
        conn = get_db()
        try:
            teams, maps, matrix = _build_matrix_data(conn, mode_filter)
            return _build_heatmap_figure(teams, maps, matrix)
        finally:
            conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dashboard_map_matrix.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/map_matrix.py tests/test_dashboard_map_matrix.py
git commit -m "feat: update Map Matrix tab to use Map Strength Rating for heatmap"
```

---

### Task 4: Update Team Profile tab — replace avoidance/target with Map Strength

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/team_profile.py`
- Modify: `tests/test_dashboard_team_profile.py`

- [ ] **Step 1: Update test expectations**

Replace the contents of `tests/test_dashboard_team_profile.py` with:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_build_map_record_data(db):
    from cdm_stats.dashboard.tabs.team_profile import _build_map_record_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    records = _build_map_record_data(db, dvs_id)
    played_maps = {r["map_name"] for r in records if r["wins"] + r["losses"] > 0}
    assert "Tunisia" in played_maps
    assert "Summit" in played_maps
    tunisia = next(r for r in records if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 0
    assert "pick_wins" in tunisia
    assert "defend_wins" in tunisia
    assert "strength" in tunisia
    assert tunisia["strength"]["rating"] is not None
    assert tunisia["strength"]["rating"] > 0.5


def test_build_map_results_detail(db):
    from cdm_stats.dashboard.tabs.team_profile import _build_map_results_detail
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia_id = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia'").fetchone()[0]
    detail = _build_map_results_detail(db, dvs_id, tunisia_id)
    assert len(detail) == 1
    row = detail[0]
    assert row["opponent"] == "OUG"
    assert row["result"] == "W"
    assert row["pick_context"] == "Opener"
    assert "score" in row
    assert "picked_by" in row
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_team_profile.py -v`
Expected: FAIL (import errors from avoidance_index/target_index, missing `strength` key, missing `_build_map_results_detail`)

- [ ] **Step 3: Update team_profile.py**

Replace the entire contents of `src/cdm_stats/dashboard/tabs/team_profile.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
from dash import html, callback_context, ALL
from dash.dependencies import Input, Output, State

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS, MODE_COLORS, LOW_SAMPLE_THRESHOLD,
    wl_color, team_dropdown_options, get_all_maps,
)
from cdm_stats.metrics.avoidance import (
    pick_win_loss, defend_win_loss, pick_context_distribution,
)
from cdm_stats.metrics.map_strength import map_strength
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import get_team_map_wl, get_team_ban_summary


# ---------------------------------------------------------------------------
# Data builders (tested directly)
# ---------------------------------------------------------------------------

def _build_map_record_data(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    """Build per-map W/L records enriched with pick/defend splits and Map Strength."""
    base = get_team_map_wl(conn, team_id)
    maps = get_all_maps(conn)
    map_lookup = {(m[1], m[2]): m[0] for m in maps}

    for entry in base:
        map_id = map_lookup.get((entry["map_name"], entry["mode"]))
        if map_id:
            pwl = pick_win_loss(conn, team_id, map_id)
            dwl = defend_win_loss(conn, team_id, map_id)
            ms = map_strength(conn, team_id, map_id)
            entry["map_id"] = map_id
            entry["pick_wins"] = pwl["wins"]
            entry["pick_losses"] = pwl["losses"]
            entry["defend_wins"] = dwl["wins"]
            entry["defend_losses"] = dwl["losses"]
            entry["strength"] = ms
        else:
            entry["map_id"] = None
            entry["pick_wins"] = entry["pick_losses"] = 0
            entry["defend_wins"] = entry["defend_losses"] = 0
            entry["strength"] = {"rating": None, "weighted_sample": 0, "total_played": 0, "low_confidence": True}
    return base


def _build_map_results_detail(
    conn: sqlite3.Connection, team_id: int, map_id: int
) -> list[dict]:
    """Build individual match results for a team on a specific map.

    Returns list of dicts with: opponent, score, pick_context, picked_by, result, match_date.
    Sorted by date descending.
    """
    rows = conn.execute(
        """SELECT m.match_date, m.team1_id, m.team2_id,
                  mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score,
                  mr.pick_context, mr.picked_by_team_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date DESC""",
        (map_id, team_id, team_id),
    ).fetchall()

    results = []
    for match_date, t1_id, t2_id, winner_id, pick_score, non_pick_score, pick_ctx, picker_id in rows:
        opp_id = t2_id if team_id == t1_id else t1_id
        opp_abbr = conn.execute(
            "SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)
        ).fetchone()[0]

        result_str = "W" if winner_id == team_id else "L"

        # Determine score display oriented to this team
        if picker_id == team_id:
            score = f"{pick_score}-{non_pick_score}"
        elif picker_id == opp_id:
            score = f"{non_pick_score}-{pick_score}"
        else:
            # No picker — show pick_score-non_pick_score
            score = f"{pick_score}-{non_pick_score}"

        # Determine who picked
        if picker_id == team_id:
            picked_by = "You"
        elif picker_id == opp_id:
            picked_by = opp_abbr
        else:
            picked_by = "N/A"

        results.append({
            "match_date": match_date,
            "opponent": opp_abbr,
            "score": score,
            "pick_context": pick_ctx,
            "picked_by": picked_by,
            "result": result_str,
        })

    return results


# ---------------------------------------------------------------------------
# UI card builders
# ---------------------------------------------------------------------------

def _strength_color(rating: float | None) -> str:
    """Return color based on Map Strength rating."""
    if rating is None:
        return COLORS["muted"]
    if rating >= 0.6:
        return COLORS["win"]
    if rating <= 0.4:
        return COLORS["loss"]
    return COLORS["neutral"]


def _map_strength_card(records: list[dict]) -> dbc.Card:
    """Render the MAP STRENGTH card with expandable rows showing pick/defend splits."""
    header = dbc.CardHeader(
        html.H5("Map Strength", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    rows = []
    for rec in records:
        total = rec["wins"] + rec["losses"]
        if total == 0:
            continue

        ms = rec["strength"]
        rating = ms["rating"]
        strength_color = _strength_color(rating)
        mode_color = MODE_COLORS.get(rec["mode"], COLORS["text"])
        wl_col = wl_color(rec["wins"], rec["losses"])

        rating_text = f"{rating:.0%}" if rating is not None else "N/A"
        low_badge = " *" if ms["low_confidence"] else ""

        # Main row (clickable)
        main_row = html.Div(
            [
                html.Span(rec["map_name"], style={"fontWeight": "600", "width": "140px", "display": "inline-block"}),
                html.Span(rec["mode"], style={"color": mode_color, "width": "80px", "display": "inline-block", "fontSize": "0.85rem"}),
                html.Span(
                    f"{rating_text}{low_badge}",
                    style={"color": strength_color, "fontWeight": "700", "width": "80px", "display": "inline-block", "fontSize": "1.1rem"},
                ),
                html.Span(
                    f"{rec['wins']}-{rec['losses']}",
                    style={"color": wl_col, "fontWeight": "600", "width": "60px", "display": "inline-block"},
                ),
            ],
            id={"type": "tp-map-row", "index": f"{rec['map_name']}-{rec['mode']}"},
            style={
                "cursor": "pointer",
                "padding": "8px 12px",
                "borderBottom": f"1px solid {COLORS['border']}",
                "display": "flex",
                "alignItems": "center",
                "opacity": "0.5" if ms["low_confidence"] else "1",
            },
        )

        # Expandable detail: pick/defend splits
        pick_color = wl_color(rec.get("pick_wins", 0), rec.get("pick_losses", 0))
        defend_color = wl_color(rec.get("defend_wins", 0), rec.get("defend_losses", 0))
        detail = html.Div(
            [
                html.Div(
                    [
                        html.Span("Pick: ", style={"color": COLORS["muted"], "fontSize": "0.8rem"}),
                        html.Span(
                            f"{rec.get('pick_wins', 0)}-{rec.get('pick_losses', 0)}",
                            style={"color": pick_color, "fontSize": "0.8rem", "marginRight": "16px"},
                        ),
                        html.Span("Defend: ", style={"color": COLORS["muted"], "fontSize": "0.8rem"}),
                        html.Span(
                            f"{rec.get('defend_wins', 0)}-{rec.get('defend_losses', 0)}",
                            style={"color": defend_color, "fontSize": "0.8rem"},
                        ),
                    ],
                    style={"paddingLeft": "24px"},
                ),
            ],
            id={"type": "tp-expand", "index": f"{rec['map_name']}-{rec['mode']}"},
            style={"display": "none", "padding": "4px 12px 8px", "backgroundColor": "#0d1525"},
        )

        rows.append(html.Div([main_row, detail]))

    if not rows:
        rows = [html.Div("No map data available", style={"color": COLORS["muted"], "padding": "12px"})]

    footer_note = html.Div(
        "* Low sample size — interpret with caution",
        style={"color": COLORS["muted"], "fontSize": "0.75rem", "padding": "6px 12px"},
    )

    body = dbc.CardBody(rows + [footer_note], style={"padding": "0"})
    return dbc.Card(
        [header, body],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


def _context_distribution_card(conn: sqlite3.Connection, team_id: int, records: list[dict]) -> dbc.Card:
    """Render PICK CONTEXT DISTRIBUTION card showing how maps are used under pressure."""
    header = dbc.CardHeader(
        html.H5("Pick Context Distribution", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    context_colors = {
        "Opener": "#60a5fa",
        "Neutral": "#a78bfa",
        "Must-Win": "#f87171",
        "Close-Out": "#4ade80",
    }

    rows = []
    for rec in records:
        map_id = rec.get("map_id")
        if not map_id:
            continue
        total_played = rec["wins"] + rec["losses"]
        if total_played == 0:
            continue

        dist = pick_context_distribution(conn, team_id, map_id)
        total_picks = sum(dist.values())
        if total_picks == 0:
            continue

        mode_color = MODE_COLORS.get(rec["mode"], COLORS["text"])
        bar_segments = []
        for ctx in ("Opener", "Neutral", "Must-Win", "Close-Out"):
            count = dist.get(ctx, 0)
            if count == 0:
                continue
            pct = count / total_picks * 100
            bar_segments.append(
                html.Div(
                    title=f"{ctx}: {count}",
                    style={
                        "width": f"{pct}%",
                        "height": "12px",
                        "backgroundColor": context_colors[ctx],
                        "display": "inline-block",
                    },
                )
            )

        row = html.Div(
            [
                html.Div(
                    [
                        html.Span(rec["map_name"], style={"fontWeight": "600", "width": "120px", "display": "inline-block"}),
                        html.Span(rec["mode"], style={"color": mode_color, "fontSize": "0.8rem", "width": "70px", "display": "inline-block"}),
                    ],
                    style={"display": "flex", "alignItems": "center", "minWidth": "200px"},
                ),
                html.Div(
                    bar_segments,
                    style={"flex": "1", "display": "flex", "borderRadius": "4px", "overflow": "hidden", "backgroundColor": "#2a2a4a"},
                ),
                html.Span(
                    f"n={total_picks}",
                    style={"fontSize": "0.75rem", "color": COLORS["muted"], "marginLeft": "8px", "minWidth": "40px"},
                ),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "padding": "6px 12px",
                "borderBottom": f"1px solid {COLORS['border']}",
                "gap": "12px",
            },
        )
        rows.append(row)

    if not rows:
        rows = [html.Div("No pick data available", style={"color": COLORS["muted"], "padding": "12px"})]

    # Legend
    legend = html.Div(
        [
            html.Span(
                [html.Span("\u25a0 ", style={"color": c}), ctx],
                style={"fontSize": "0.7rem", "color": COLORS["muted"], "marginRight": "12px"},
            )
            for ctx, c in context_colors.items()
        ],
        style={"padding": "6px 12px", "display": "flex"},
    )

    body = dbc.CardBody(rows + [legend], style={"padding": "0"})
    return dbc.Card(
        [header, body],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


def _ban_card(ban_data: dict, abbr: str) -> dbc.Card:
    """Render BAN TENDENCIES card."""
    header = dbc.CardHeader(
        html.H5("Ban Tendencies", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    sections = []

    # Team bans
    team_bans = ban_data.get("team_bans", [])
    total = ban_data.get("total_series", 0)
    sections.append(
        html.Div(
            html.Span(f"{abbr} Bans", style={"fontWeight": "600", "color": COLORS["ban"]}),
            style={"padding": "8px 12px", "borderBottom": f"1px solid {COLORS['border']}"},
        )
    )
    if team_bans:
        for b in team_bans:
            mode_color = MODE_COLORS.get(b["mode"], COLORS["text"])
            sections.append(
                html.Div(
                    [
                        html.Span(b["map_name"], style={"width": "120px", "display": "inline-block"}),
                        html.Span(b["mode"], style={"color": mode_color, "width": "70px", "display": "inline-block", "fontSize": "0.85rem"}),
                        html.Span(f"{b['ban_count']}/{total}", style={"color": COLORS["text"]}),
                    ],
                    style={"padding": "4px 12px 4px 24px", "display": "flex", "alignItems": "center"},
                )
            )
    else:
        sections.append(
            html.Div("No ban data", style={"color": COLORS["muted"], "padding": "4px 12px 4px 24px", "fontSize": "0.85rem"})
        )

    # Opponent bans
    opp_bans = ban_data.get("opponent_bans", [])
    sections.append(
        html.Div(
            html.Span(f"Opponent Bans vs {abbr}", style={"fontWeight": "600", "color": COLORS["loss"]}),
            style={"padding": "8px 12px", "borderBottom": f"1px solid {COLORS['border']}", "borderTop": f"1px solid {COLORS['border']}"},
        )
    )
    if opp_bans:
        for b in opp_bans:
            mode_color = MODE_COLORS.get(b["mode"], COLORS["text"])
            sections.append(
                html.Div(
                    [
                        html.Span(b["map_name"], style={"width": "120px", "display": "inline-block"}),
                        html.Span(b["mode"], style={"color": mode_color, "width": "70px", "display": "inline-block", "fontSize": "0.85rem"}),
                        html.Span(f"{b['ban_count']}/{total}", style={"color": COLORS["text"]}),
                    ],
                    style={"padding": "4px 12px 4px 24px", "display": "flex", "alignItems": "center"},
                )
            )
    else:
        sections.append(
            html.Div("No ban data", style={"color": COLORS["muted"], "padding": "4px 12px 4px 24px", "fontSize": "0.85rem"})
        )

    body = dbc.CardBody(sections, style={"padding": "0"})
    return dbc.Card(
        [header, body],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


def _elo_card(conn: sqlite3.Connection, team_id: int, abbr: str) -> dbc.Card:
    """Render ELO RATING card."""
    elo = get_current_elo(conn, team_id)
    low_conf = is_low_confidence(conn, team_id)

    elo_display = f"{elo:.0f}"
    badge = []
    if low_conf:
        badge = [
            html.Span(
                " LOW CONFIDENCE",
                style={"color": COLORS["neutral"], "fontSize": "0.75rem", "marginLeft": "8px"},
            )
        ]

    header = dbc.CardHeader(
        html.H5("Elo Rating", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    body = dbc.CardBody(
        html.Div(
            [
                html.Span(abbr, style={"fontWeight": "600", "marginRight": "12px"}),
                html.Span(elo_display, style={"fontSize": "1.5rem", "fontWeight": "700", "color": COLORS["your_team"]}),
            ] + badge,
            style={"display": "flex", "alignItems": "center"},
        )
    )

    return dbc.Card(
        [header, body],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


# ---------------------------------------------------------------------------
# Layout and callbacks
# ---------------------------------------------------------------------------

def layout():
    """Return the team profile tab layout with team selector and content area."""
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Label("Select Team", style={"color": COLORS["text"], "fontWeight": "600"}),
                dbc.Select(
                    id="tp-team-select",
                    options=[],
                    placeholder="Choose a team...",
                    style={"backgroundColor": COLORS["card_bg"], "color": COLORS["text"], "border": f"1px solid {COLORS['border']}"},
                ),
            ], width=3),
        ], className="mb-3 mt-2", style={"padding": "0 12px"}),
        html.Div(id="tp-content"),
    ])


def register_callbacks(app):
    """Register all callbacks for the team profile tab."""

    # Populate team dropdown when tab loads
    @app.callback(
        Output("tp-team-select", "options"),
        Input("tp-team-select", "id"),  # fires once on load
    )
    def populate_teams(_):
        conn = get_db()
        try:
            return team_dropdown_options(conn)
        finally:
            conn.close()

    # Update content when team is selected
    @app.callback(
        Output("tp-content", "children"),
        Input("tp-team-select", "value"),
        prevent_initial_call=True,
    )
    def update_content(team_id):
        if not team_id:
            return html.Div("Select a team to view profile", style={"color": COLORS["muted"], "padding": "20px"})

        team_id = int(team_id)
        conn = get_db()
        try:
            abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]

            records = _build_map_record_data(conn, team_id)
            ban_data = get_team_ban_summary(conn, team_id)

            return html.Div([
                dbc.Row([
                    dbc.Col(_map_strength_card(records), md=6),
                    dbc.Col([
                        _context_distribution_card(conn, team_id, records),
                        _elo_card(conn, team_id, abbr),
                    ], md=6),
                ]),
                dbc.Row([
                    dbc.Col(_ban_card(ban_data, abbr), md=6),
                ]),
            ])
        finally:
            conn.close()

    # Toggle expand/collapse on map rows
    @app.callback(
        Output({"type": "tp-expand", "index": ALL}, "style"),
        Input({"type": "tp-map-row", "index": ALL}, "n_clicks"),
        State({"type": "tp-expand", "index": ALL}, "style"),
        prevent_initial_call=True,
    )
    def toggle_expand(n_clicks_list, styles):
        ctx = callback_context
        if not ctx.triggered:
            return styles

        triggered_id = ctx.triggered[0]["prop_id"]
        new_styles = []
        for i, style in enumerate(styles):
            row_id = ctx.inputs_list[0][i]["id"]["index"]
            expand_id_str = f'{{"index":"{row_id}","type":"tp-map-row"}}.n_clicks'
            if triggered_id == expand_id_str:
                if style.get("display") == "none":
                    new_style = {**style, "display": "block"}
                else:
                    new_style = {**style, "display": "none"}
                new_styles.append(new_style)
            else:
                new_styles.append(style)

        return new_styles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dashboard_team_profile.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/team_profile.py tests/test_dashboard_team_profile.py
git commit -m "feat: update Team Profile tab with Map Strength cards and results detail"
```

---

### Task 5: Update Match-Up Prep tab — side-by-side Map Strength with delta

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/matchup_prep.py`
- Modify: `tests/test_dashboard_matchup_prep.py`

- [ ] **Step 1: Update test expectations**

Replace the contents of `tests/test_dashboard_matchup_prep.py` with:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_build_matchup_data(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    assert "SnD" in data
    assert "HP" in data
    assert "Control" in data
    snd_maps = data["SnD"]
    tunisia = next((m for m in snd_maps if m["map_name"] == "Tunisia"), None)
    assert tunisia is not None
    assert tunisia["h2h"]["wins"] == 1
    assert tunisia["h2h"]["losses"] == 0


def test_build_matchup_data_includes_strength_and_delta(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    for mode_maps in data.values():
        for m in mode_maps:
            assert "your_wl" in m
            assert "opp_wl" in m
            assert "your_strength" in m
            assert "opp_strength" in m
            assert "delta" in m
            assert "your_pick_wl" in m
            assert "your_defend_wl" in m
            assert "opp_pick_wl" in m
            assert "opp_defend_wl" in m


def test_build_matchup_data_no_avoidance_keys(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    for mode_maps in data.values():
        for m in mode_maps:
            assert "your_avoid" not in m
            assert "opp_avoid" not in m
            assert "your_target" not in m
            assert "opp_target" not in m


def test_build_matchup_data_delta_sign(db):
    """DVS won on Tunisia, OUG lost. DVS should have positive delta on Tunisia."""
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    tunisia = next(m for m in data["SnD"] if m["map_name"] == "Tunisia")
    # DVS won, so your_strength > opp_strength, delta > 0
    assert tunisia["delta"] is not None
    assert tunisia["delta"] > 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dashboard_matchup_prep.py -v`
Expected: FAIL (import errors from avoidance_index/target_index, missing `your_strength`/`delta` keys)

- [ ] **Step 3: Update matchup_prep.py**

Replace the entire contents of `src/cdm_stats/dashboard/tabs/matchup_prep.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
from dash import html, callback_context, ALL
from dash.dependencies import Input, Output, State

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS, MODE_COLORS, LOW_SAMPLE_THRESHOLD,
    wl_color, team_dropdown_options, get_all_maps,
)
from cdm_stats.metrics.avoidance import pick_win_loss, defend_win_loss
from cdm_stats.metrics.map_strength import map_strength
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import get_ban_summary


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _head_to_head(
    conn: sqlite3.Connection, team_id: int, opp_id: int, map_id: int
) -> dict:
    """W-L between two specific teams on a specific map."""
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?)
               OR (m.team1_id = ? AND m.team2_id = ?))""",
        (team_id, opp_id, map_id, team_id, opp_id, opp_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def _team_map_wl(
    conn: sqlite3.Connection, team_id: int, map_id: int
) -> dict:
    """Overall W-L for a team on a map (all opponents)."""
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?)""",
        (team_id, team_id, map_id, team_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def _build_matchup_data(
    conn: sqlite3.Connection, your_id: int, opp_id: int
) -> dict[str, list[dict]]:
    """Build per-mode map comparison data between two teams.

    Returns {"SnD": [...], "HP": [...], "Control": [...]} where each entry
    contains map_id, map_name, mode, h2h, your_wl, opp_wl, Map Strength
    for both teams, matchup delta, and pick/defend W-L.
    """
    maps = get_all_maps(conn)
    result: dict[str, list[dict]] = {"SnD": [], "HP": [], "Control": []}

    for map_id, map_name, mode in maps:
        h2h = _head_to_head(conn, your_id, opp_id, map_id)
        your_wl = _team_map_wl(conn, your_id, map_id)
        opp_wl = _team_map_wl(conn, opp_id, map_id)

        your_ms = map_strength(conn, your_id, map_id)
        opp_ms = map_strength(conn, opp_id, map_id)

        your_pwl = pick_win_loss(conn, your_id, map_id)
        your_dwl = defend_win_loss(conn, your_id, map_id)
        opp_pwl = pick_win_loss(conn, opp_id, map_id)
        opp_dwl = defend_win_loss(conn, opp_id, map_id)

        # Compute delta (positive = your advantage)
        if your_ms["rating"] is not None and opp_ms["rating"] is not None:
            delta = your_ms["rating"] - opp_ms["rating"]
        else:
            delta = None

        entry = {
            "map_id": map_id,
            "map_name": map_name,
            "mode": mode,
            "h2h": h2h,
            "your_wl": your_wl,
            "opp_wl": opp_wl,
            "your_strength": your_ms,
            "opp_strength": opp_ms,
            "delta": delta,
            "your_pick_wl": your_pwl,
            "your_defend_wl": your_dwl,
            "opp_pick_wl": opp_pwl,
            "opp_defend_wl": opp_dwl,
        }
        if mode in result:
            result[mode].append(entry)

    # Sort each mode by delta (largest advantage first)
    for mode in result:
        result[mode].sort(key=lambda m: m["delta"] if m["delta"] is not None else -999, reverse=True)

    return result


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------


def _hex_to_rgb(hex_color: str) -> str:
    """Convert '#4cc9f0' to '76,201,240'."""
    h = hex_color.lstrip("#")
    return ",".join(str(int(h[i : i + 2], 16)) for i in (0, 2, 4))


def _stat_block(label: str, wins: int, losses: int, tint: str) -> html.Div:
    """Single stat display block showing W-L."""
    color = wl_color(wins, losses)
    total = wins + losses
    pct = f" ({wins / total:.0%})" if total > 0 else ""
    return html.Div(
        [
            html.Div(label, style={"fontSize": "0.7rem", "color": COLORS["muted"]}),
            html.Span(
                f"{wins}-{losses}{pct}",
                style={"fontWeight": "600", "color": color, "fontSize": "0.85rem"},
            ),
        ],
        style={
            "padding": "4px 8px",
            "borderRadius": "4px",
            "backgroundColor": f"rgba({_hex_to_rgb(tint)}, 0.1)",
            "display": "inline-block",
            "marginRight": "8px",
            "marginBottom": "4px",
        },
    )


def _strength_block(label: str, ms: dict, tint: str) -> html.Div:
    """Map Strength display block."""
    rating = ms["rating"]
    if rating is None:
        display = "N/A"
        color = COLORS["muted"]
    elif rating >= 0.6:
        display = f"{rating:.0%}"
        color = COLORS["win"]
    elif rating <= 0.4:
        display = f"{rating:.0%}"
        color = COLORS["loss"]
    else:
        display = f"{rating:.0%}"
        color = COLORS["neutral"]

    low = " *" if ms["low_confidence"] else ""

    return html.Div(
        [
            html.Div(label, style={"fontSize": "0.7rem", "color": COLORS["muted"]}),
            html.Span(
                f"{display}{low}",
                style={"fontWeight": "700", "color": color, "fontSize": "0.95rem"},
            ),
        ],
        style={
            "padding": "4px 8px",
            "borderRadius": "4px",
            "backgroundColor": f"rgba({_hex_to_rgb(tint)}, 0.1)",
            "display": "inline-block",
            "marginRight": "8px",
            "marginBottom": "4px",
        },
    )


def _delta_badge(delta: float | None) -> html.Span:
    """Matchup delta badge: green positive, red negative."""
    if delta is None:
        return html.Span("N/A", style={"color": COLORS["muted"], "fontSize": "0.8rem"})

    if delta > 0.05:
        color = COLORS["win"]
        prefix = "+"
    elif delta < -0.05:
        color = COLORS["loss"]
        prefix = ""
    else:
        color = COLORS["neutral"]
        prefix = ""

    return html.Span(
        f"{prefix}{delta:+.0%}",
        style={"fontWeight": "700", "color": color, "fontSize": "0.9rem"},
    )


def _map_row(m: dict, row_idx: int) -> html.Div:
    """Single map row with Map Strength comparison and expandable pick/defend detail."""
    mode_color = MODE_COLORS.get(m["mode"], COLORS["text"])
    h2h_color = wl_color(m["h2h"]["wins"], m["h2h"]["losses"])

    # Collapsed summary row
    main_row = html.Div(
        [
            html.Span(
                m["map_name"],
                style={"fontWeight": "600", "width": "130px", "display": "inline-block"},
            ),
            _delta_badge(m["delta"]),
            html.Span(style={"width": "16px", "display": "inline-block"}),
            _strength_block("Your Str", m["your_strength"], COLORS["your_team"]),
            html.Span("vs", style={"color": COLORS["muted"], "fontSize": "0.8rem", "margin": "0 4px"}),
            _strength_block("Opp Str", m["opp_strength"], COLORS["opponent"]),
            html.Span(style={"width": "16px", "display": "inline-block"}),
            html.Span(
                f"H2H {m['h2h']['wins']}-{m['h2h']['losses']}",
                style={"color": h2h_color, "fontSize": "0.85rem"},
            ),
        ],
        id={"type": "mp-row", "index": row_idx},
        style={
            "cursor": "pointer",
            "padding": "8px 12px",
            "borderBottom": f"1px solid {COLORS['border']}",
            "display": "flex",
            "alignItems": "center",
            "flexWrap": "wrap",
        },
    )

    # Expandable detail: W-L and pick/defend breakdown
    detail = html.Div(
        [
            html.Div(
                [
                    html.Span("Your Team", style={"fontWeight": "600", "color": COLORS["your_team"], "marginRight": "12px", "width": "80px", "display": "inline-block"}),
                    _stat_block("Overall", m["your_wl"]["wins"], m["your_wl"]["losses"], COLORS["your_team"]),
                    _stat_block("Pick", m["your_pick_wl"]["wins"], m["your_pick_wl"]["losses"], COLORS["your_team"]),
                    _stat_block("Defend", m["your_defend_wl"]["wins"], m["your_defend_wl"]["losses"], COLORS["your_team"]),
                ],
                style={"display": "flex", "alignItems": "center", "marginBottom": "4px"},
            ),
            html.Div(
                [
                    html.Span("Opponent", style={"fontWeight": "600", "color": COLORS["opponent"], "marginRight": "12px", "width": "80px", "display": "inline-block"}),
                    _stat_block("Overall", m["opp_wl"]["wins"], m["opp_wl"]["losses"], COLORS["opponent"]),
                    _stat_block("Pick", m["opp_pick_wl"]["wins"], m["opp_pick_wl"]["losses"], COLORS["opponent"]),
                    _stat_block("Defend", m["opp_defend_wl"]["wins"], m["opp_defend_wl"]["losses"], COLORS["opponent"]),
                ],
                style={"display": "flex", "alignItems": "center"},
            ),
        ],
        id={"type": "mp-expand", "index": row_idx},
        style={"display": "none", "padding": "6px 12px 10px 24px", "backgroundColor": "#0d1525"},
    )

    return html.Div([main_row, detail])


def _ban_comparison(
    conn: sqlite3.Connection,
    your_id: int,
    opp_id: int,
    your_abbr: str,
    opp_abbr: str,
) -> html.Div:
    """Ban comparison section for both teams in head-to-head matches."""
    your_bans = get_ban_summary(conn, your_id, opp_id)
    opp_bans = get_ban_summary(conn, opp_id, your_id)

    def _ban_list(bans: list[dict], label: str, tint: str) -> html.Div:
        rows = []
        for b in bans:
            mode_color = MODE_COLORS.get(b["mode"], COLORS["text"])
            rows.append(
                html.Div(
                    [
                        html.Span(b["map_name"], style={"width": "120px", "display": "inline-block"}),
                        html.Span(b["mode"], style={"color": mode_color, "width": "70px", "display": "inline-block", "fontSize": "0.85rem"}),
                        html.Span(
                            f"{b['ban_count']}/{b['total_series']}",
                            style={"color": COLORS["text"]},
                        ),
                    ],
                    style={"padding": "4px 12px 4px 24px", "display": "flex", "alignItems": "center"},
                )
            )
        if not rows:
            rows = [html.Div("No ban data", style={"color": COLORS["muted"], "padding": "4px 24px", "fontSize": "0.85rem"})]

        return html.Div(
            [
                html.Div(
                    html.Span(label, style={"fontWeight": "600", "color": tint}),
                    style={"padding": "8px 12px", "borderBottom": f"1px solid {COLORS['border']}"},
                ),
            ] + rows
        )

    return dbc.Card(
        [
            dbc.CardHeader(
                html.H5("Ban Comparison", className="mb-0", style={"color": COLORS["text"]}),
                style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
            ),
            dbc.CardBody(
                [
                    _ban_list(your_bans, f"{your_abbr} Bans vs {opp_abbr}", COLORS["your_team"]),
                    html.Hr(style={"borderColor": COLORS["border"], "margin": "4px 0"}),
                    _ban_list(opp_bans, f"{opp_abbr} Bans vs {your_abbr}", COLORS["opponent"]),
                ],
                style={"padding": "0"},
            ),
        ],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


# ---------------------------------------------------------------------------
# Layout and callbacks
# ---------------------------------------------------------------------------


def layout():
    """Return the Match-Up Prep tab layout with two team selectors."""
    return html.Div([
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Your Team", style={"color": COLORS["your_team"], "fontWeight": "600"}),
                        dbc.Select(
                            id="mp-your-team",
                            options=[],
                            placeholder="Select your team...",
                            style={
                                "backgroundColor": COLORS["card_bg"],
                                "color": COLORS["text"],
                                "border": f"1px solid {COLORS['border']}",
                            },
                        ),
                    ],
                    width=3,
                ),
                dbc.Col(
                    html.Div(
                        "vs",
                        style={
                            "textAlign": "center",
                            "fontSize": "1.2rem",
                            "fontWeight": "700",
                            "color": COLORS["muted"],
                            "paddingTop": "28px",
                        },
                    ),
                    width=1,
                ),
                dbc.Col(
                    [
                        html.Label("Opponent", style={"color": COLORS["opponent"], "fontWeight": "600"}),
                        dbc.Select(
                            id="mp-opp-team",
                            options=[],
                            placeholder="Select opponent...",
                            style={
                                "backgroundColor": COLORS["card_bg"],
                                "color": COLORS["text"],
                                "border": f"1px solid {COLORS['border']}",
                            },
                        ),
                    ],
                    width=3,
                ),
                dbc.Col(
                    html.Div(id="mp-elo-badge"),
                    width=5,
                    style={"paddingTop": "20px"},
                ),
            ],
            className="mb-3 mt-2",
            style={"padding": "0 12px"},
        ),
        html.Div(id="mp-content"),
    ])


def register_callbacks(app):
    """Register all callbacks for the Match-Up Prep tab."""

    # Populate both team dropdowns on load
    @app.callback(
        Output("mp-your-team", "options"),
        Input("mp-your-team", "id"),
    )
    def populate_your_team(_):
        conn = get_db()
        try:
            return team_dropdown_options(conn)
        finally:
            conn.close()

    @app.callback(
        Output("mp-opp-team", "options"),
        Input("mp-opp-team", "id"),
    )
    def populate_opp_team(_):
        conn = get_db()
        try:
            return team_dropdown_options(conn)
        finally:
            conn.close()

    # Update content and Elo badge when either team changes
    @app.callback(
        [Output("mp-content", "children"), Output("mp-elo-badge", "children")],
        [Input("mp-your-team", "value"), Input("mp-opp-team", "value")],
        prevent_initial_call=True,
    )
    def update_matchup(your_team, opp_team):
        if not your_team or not opp_team:
            msg = "Select both teams to view match-up analysis"
            return (
                html.Div(msg, style={"color": COLORS["muted"], "padding": "20px"}),
                html.Div(),
            )

        your_id = int(your_team)
        opp_id = int(opp_team)

        if your_id == opp_id:
            return (
                html.Div("Please select two different teams", style={"color": COLORS["muted"], "padding": "20px"}),
                html.Div(),
            )

        conn = get_db()
        try:
            your_abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (your_id,)).fetchone()[0]
            opp_abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)).fetchone()[0]

            data = _build_matchup_data(conn, your_id, opp_id)

            # Elo badge
            your_elo = get_current_elo(conn, your_id)
            opp_elo = get_current_elo(conn, opp_id)
            your_low = is_low_confidence(conn, your_id)
            opp_low = is_low_confidence(conn, opp_id)

            elo_badge = html.Div(
                [
                    html.Span(
                        f"{your_abbr} {your_elo:.0f}",
                        style={"color": COLORS["your_team"], "fontWeight": "600", "marginRight": "6px"},
                    ),
                    html.Span(" LOW CONFIDENCE", style={"color": COLORS["neutral"], "fontSize": "0.7rem", "marginRight": "12px"})
                    if your_low else html.Span(style={"marginRight": "12px"}),
                    html.Span("vs ", style={"color": COLORS["muted"], "marginRight": "6px"}),
                    html.Span(
                        f"{opp_abbr} {opp_elo:.0f}",
                        style={"color": COLORS["opponent"], "fontWeight": "600", "marginRight": "6px"},
                    ),
                    html.Span(" LOW CONFIDENCE", style={"color": COLORS["neutral"], "fontSize": "0.7rem"})
                    if opp_low else html.Span(),
                ],
                style={"display": "flex", "alignItems": "center"},
            )

            # Build mode sections
            row_idx = 0
            sections = []
            for mode in ("SnD", "HP", "Control"):
                mode_maps = data.get(mode, [])
                mode_color = MODE_COLORS.get(mode, COLORS["text"])

                map_rows = []
                for m in mode_maps:
                    map_rows.append(_map_row(m, row_idx))
                    row_idx += 1

                section = dbc.Card(
                    [
                        dbc.CardHeader(
                            html.H5(mode, className="mb-0", style={"color": mode_color}),
                            style={
                                "backgroundColor": COLORS["card_bg"],
                                "borderBottom": f"2px solid {mode_color}",
                            },
                        ),
                        dbc.CardBody(map_rows, style={"padding": "0"}),
                    ],
                    style={
                        "backgroundColor": COLORS["card_bg"],
                        "border": f"1px solid {COLORS['border']}",
                    },
                    className="mb-3",
                )
                sections.append(section)

            # Ban comparison
            sections.append(_ban_comparison(conn, your_id, opp_id, your_abbr, opp_abbr))

            # Low sample note
            sections.append(
                html.Div(
                    "* Low sample size -- interpret with caution",
                    style={"color": COLORS["muted"], "fontSize": "0.75rem", "padding": "6px 12px"},
                )
            )

            content = html.Div(sections)
            return content, elo_badge
        finally:
            conn.close()

    # Expand/collapse pattern-matching callback
    @app.callback(
        Output({"type": "mp-expand", "index": ALL}, "style"),
        Input({"type": "mp-row", "index": ALL}, "n_clicks"),
        State({"type": "mp-expand", "index": ALL}, "style"),
        prevent_initial_call=True,
    )
    def toggle_expand(n_clicks_list, styles):
        ctx = callback_context
        if not ctx.triggered:
            return styles

        triggered_id = ctx.triggered[0]["prop_id"]
        new_styles = []
        for i, style in enumerate(styles):
            row_id = ctx.inputs_list[0][i]["id"]["index"]
            expand_id_str = f'{{"index":{row_id},"type":"mp-row"}}.n_clicks'
            if triggered_id == expand_id_str:
                if style.get("display") == "none":
                    new_style = {**style, "display": "block"}
                else:
                    new_style = {**style, "display": "none"}
                new_styles.append(new_style)
            else:
                new_styles.append(style)

        return new_styles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dashboard_matchup_prep.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/matchup_prep.py tests/test_dashboard_matchup_prep.py
git commit -m "feat: update Match-Up Prep tab with Map Strength comparison and delta sorting"
```

---

### Task 6: Run full test suite and verify everything works

**Files:**
- No new files

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS. If any test fails due to stale imports of `avoidance_index` or `target_index`, find and remove those imports.

- [ ] **Step 2: Check for any remaining references to avoidance_index or target_index**

Run: `grep -rn "avoidance_index\|target_index" src/ tests/`
Expected: No results. If any remain, remove them.

- [ ] **Step 3: Verify the dashboard loads**

Run: `uv run python -m cdm_stats.dashboard.app`
Expected: Dashboard starts on port 8050. Verify:
- Map Matrix tab shows heatmap colored by Map Strength
- Team Profile tab shows Map Strength cards with expandable pick/defend
- Match-Up Prep tab shows side-by-side Map Strength with delta sorting

- [ ] **Step 4: Commit any final cleanup**

```bash
git add -A
git commit -m "chore: final cleanup after Map Strength migration"
```
