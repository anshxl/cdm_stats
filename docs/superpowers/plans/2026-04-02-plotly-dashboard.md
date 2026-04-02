# Plotly Dash Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace clunky Excel exports with an interactive Plotly Dash dashboard for CDL map analytics, served locally at `http://localhost:8050`.

**Architecture:** Four-tab Dash app (`Team Profile`, `Map Matrix`, `Match-Up Prep`, `Elo Tracker`) using `dash-bootstrap-components` for a dark theme. Each tab is its own module with `layout()` and `register_callbacks(app)`. All data comes from existing `cdm_stats.metrics.*` and `cdm_stats.db.queries` functions — no new data layer.

**Tech Stack:** `dash`, `dash-bootstrap-components`, `plotly`, Python 3.12, SQLite.

**Out of scope for initial build:** Map Matrix click-to-navigate (cross-tab routing with state). Can be added as a follow-up.

---

### Task 1: Add dependencies and create the dashboard package skeleton

**Files:**
- Modify: `pyproject.toml`
- Create: `src/cdm_stats/dashboard/__init__.py`
- Create: `src/cdm_stats/dashboard/app.py`
- Create: `src/cdm_stats/dashboard/__main__.py`
- Create: `src/cdm_stats/dashboard/tabs/__init__.py`

- [ ] **Step 1: Add dash dependencies to pyproject.toml**

In `pyproject.toml`, add to the `dependencies` list:

```toml
dependencies = [
    "openpyxl>=3.1",
    "matplotlib>=3.8",
    "numpy>=1.26",
    "dash>=2.18",
    "dash-bootstrap-components>=1.6",
    "plotly>=5.24",
]
```

- [ ] **Step 2: Install dependencies**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv sync`
Expected: Dependencies install successfully.

- [ ] **Step 3: Create dashboard package with empty init files**

Create `src/cdm_stats/dashboard/__init__.py`:
```python
```

Create `src/cdm_stats/dashboard/tabs/__init__.py`:
```python
```

- [ ] **Step 4: Create app.py with minimal Dash app and tab routing**

Create `src/cdm_stats/dashboard/app.py`:

```python
import sqlite3
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output

DB_PATH = Path(__file__).resolve().parents[3] / "data" / "cdl.db"

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
)

app.layout = dbc.Container([
    dbc.NavbarSimple(
        brand="CDM Stats",
        brand_style={"fontSize": "1.3rem", "fontWeight": "600"},
        color="#16213e",
        dark=True,
        className="mb-0",
    ),
    dbc.Tabs(id="main-tabs", active_tab="team-profile", className="mt-0", children=[
        dbc.Tab(label="Team Profile", tab_id="team-profile"),
        dbc.Tab(label="Map Matrix", tab_id="map-matrix"),
        dbc.Tab(label="Match-Up Prep", tab_id="matchup-prep"),
        dbc.Tab(label="Elo Tracker", tab_id="elo-tracker"),
    ]),
    html.Div(id="tab-content", className="mt-3"),
], fluid=True, className="px-0")


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@app.callback(Output("tab-content", "children"), Input("main-tabs", "active_tab"))
def render_tab(active_tab: str):
    from cdm_stats.dashboard.tabs import team_profile, map_matrix, matchup_prep, elo_tracker
    if active_tab == "team-profile":
        return team_profile.layout()
    elif active_tab == "map-matrix":
        return map_matrix.layout()
    elif active_tab == "matchup-prep":
        return matchup_prep.layout()
    elif active_tab == "elo-tracker":
        return elo_tracker.layout()
    return html.Div("Select a tab")


def register_all_callbacks():
    from cdm_stats.dashboard.tabs import team_profile, map_matrix, matchup_prep, elo_tracker
    team_profile.register_callbacks(app)
    map_matrix.register_callbacks(app)
    matchup_prep.register_callbacks(app)
    elo_tracker.register_callbacks(app)


def main():
    register_all_callbacks()
    app.run(debug=True, port=8050)
```

- [ ] **Step 5: Create __main__.py for `python -m cdm_stats.dashboard`**

Create `src/cdm_stats/dashboard/__main__.py`:

```python
from cdm_stats.dashboard.app import main

main()
```

- [ ] **Step 6: Create placeholder tab modules**

Create `src/cdm_stats/dashboard/tabs/team_profile.py`:
```python
import dash_bootstrap_components as dbc
from dash import html


def layout():
    return html.Div("Team Profile — coming soon")


def register_callbacks(app):
    pass
```

Create `src/cdm_stats/dashboard/tabs/map_matrix.py`:
```python
import dash_bootstrap_components as dbc
from dash import html


def layout():
    return html.Div("Map Matrix — coming soon")


def register_callbacks(app):
    pass
```

Create `src/cdm_stats/dashboard/tabs/matchup_prep.py`:
```python
import dash_bootstrap_components as dbc
from dash import html


def layout():
    return html.Div("Match-Up Prep — coming soon")


def register_callbacks(app):
    pass
```

Create `src/cdm_stats/dashboard/tabs/elo_tracker.py`:
```python
import dash_bootstrap_components as dbc
from dash import html


def layout():
    return html.Div("Elo Tracker — coming soon")


def register_callbacks(app):
    pass
```

- [ ] **Step 7: Verify the app starts**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run python -m cdm_stats.dashboard &`
Wait 3 seconds, then: `curl -s http://localhost:8050 | head -5`
Expected: HTML response containing "CDM Stats". Kill the process after verifying.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/cdm_stats/dashboard/
git commit -m "feat: scaffold Plotly Dash dashboard with tab routing"
```

---

### Task 2: Shared helpers — color and team/map loaders

**Files:**
- Create: `src/cdm_stats/dashboard/helpers.py`

These helpers are used by multiple tabs, so we build them once before any tab implementation.

- [ ] **Step 1: Create helpers.py**

Create `src/cdm_stats/dashboard/helpers.py`:

```python
import sqlite3
from cdm_stats.dashboard.app import get_db

LOW_SAMPLE_THRESHOLD = 4

# Color constants matching the design spec
COLORS = {
    "win": "#4ade80",
    "neutral": "#fbbf24",
    "loss": "#f87171",
    "your_team": "#4cc9f0",
    "opponent": "#f87171",
    "ban": "#e879f9",
    "card_bg": "#16213e",
    "page_bg": "#1a1a2e",
    "border": "#2a2a4a",
    "muted": "#666",
    "text": "#e0e0e0",
}

MODE_COLORS = {
    "SnD": "#4cc9f0",
    "HP": "#e879f9",
    "Control": "#fbbf24",
}


def wl_color(wins: int, losses: int) -> str:
    """Return color string based on win rate."""
    total = wins + losses
    if total == 0:
        return COLORS["muted"]
    rate = wins / total
    if rate >= 0.6:
        return COLORS["win"]
    if rate <= 0.4:
        return COLORS["loss"]
    return COLORS["neutral"]


def get_all_teams(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """Return list of (team_id, abbreviation) sorted alphabetically."""
    return conn.execute(
        "SELECT team_id, abbreviation FROM teams ORDER BY abbreviation"
    ).fetchall()


def get_all_maps(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    """Return list of (map_id, map_name, mode) sorted by mode then name."""
    return conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()


def team_dropdown_options(conn: sqlite3.Connection) -> list[dict]:
    """Return dropdown options for team selector."""
    teams = get_all_teams(conn)
    return [{"label": abbr, "value": tid} for tid, abbr in teams]
```

- [ ] **Step 2: Commit**

```bash
git add src/cdm_stats/dashboard/helpers.py
git commit -m "feat: add dashboard helper utilities for colors and data loading"
```

---

### Task 3: Team Profile tab

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/team_profile.py`
- Create: `tests/test_dashboard_team_profile.py`

- [ ] **Step 1: Write test for team profile layout rendering**

Create `tests/test_dashboard_team_profile.py`:

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
    # Should have entries for maps that DVS played
    played_maps = {r["map_name"] for r in records if r["wins"] + r["losses"] > 0}
    assert "Tunisia" in played_maps
    assert "Summit" in played_maps
    # Tunisia: DVS won
    tunisia = next(r for r in records if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 0
    # Should also include pick/defend splits
    assert "pick_wins" in tunisia
    assert "defend_wins" in tunisia


def test_build_avoidance_target_data(db):
    from cdm_stats.dashboard.tabs.team_profile import _build_avoidance_target_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    data = _build_avoidance_target_data(db, dvs_id)
    # Should be a list of dicts with map_name, mode, avoid_ratio, target_ratio, etc.
    assert len(data) > 0
    assert "map_name" in data[0]
    assert "avoid_ratio" in data[0]
    assert "target_ratio" in data[0]
    assert "avoid_n" in data[0]
    assert "target_n" in data[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_team_profile.py -v`
Expected: FAIL — `_build_map_record_data` and `_build_avoidance_target_data` do not exist.

- [ ] **Step 3: Implement team_profile.py**

Replace `src/cdm_stats/dashboard/tabs/team_profile.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
from dash import html, callback_context
from dash.dependencies import Input, Output, State

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS, MODE_COLORS, LOW_SAMPLE_THRESHOLD,
    wl_color, team_dropdown_options, get_all_maps,
)
from cdm_stats.metrics.avoidance import (
    pick_win_loss, defend_win_loss, avoidance_index, target_index,
)
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import get_team_map_wl, get_team_ban_summary


def _build_map_record_data(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    """Build map W-L records for the team profile, including pick/defend splits."""
    base = get_team_map_wl(conn, team_id)
    maps = get_all_maps(conn)
    map_lookup = {(m[1], m[2]): m[0] for m in maps}  # (map_name, mode) -> map_id
    for entry in base:
        map_id = map_lookup.get((entry["map_name"], entry["mode"]))
        if map_id:
            pwl = pick_win_loss(conn, team_id, map_id)
            dwl = defend_win_loss(conn, team_id, map_id)
            entry["pick_wins"] = pwl["wins"]
            entry["pick_losses"] = pwl["losses"]
            entry["defend_wins"] = dwl["wins"]
            entry["defend_losses"] = dwl["losses"]
        else:
            entry["pick_wins"] = entry["pick_losses"] = 0
            entry["defend_wins"] = entry["defend_losses"] = 0
    return base


def _build_avoidance_target_data(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    """Build avoidance and target index data per map."""
    maps = get_all_maps(conn)
    results = []
    for map_id, map_name, mode in maps:
        avoid = avoidance_index(conn, team_id, map_id)
        tgt = target_index(conn, team_id, map_id)
        results.append({
            "map_id": map_id,
            "map_name": map_name,
            "mode": mode,
            "avoid_ratio": avoid["ratio"],
            "avoid_n": avoid["opportunities"],
            "target_ratio": tgt["ratio"],
            "target_n": tgt["opportunities"],
        })
    return results


def _map_record_card(records: list[dict]) -> dbc.Card:
    """Render the Map Record card with clickable rows that expand to show pick/defend."""
    rows = []
    total_w, total_l = 0, 0
    for idx, r in enumerate(records):
        w, l = r["wins"], r["losses"]
        total_w += w
        total_l += l
        total = w + l
        pct = f"{w / total:.0%}" if total else "-"
        color = wl_color(w, l)
        # Collapsed row
        collapsed = html.Div([
            html.Span(f"{r['map_name']} ({r['mode']})", style={"flex": "1"}),
            html.Span(f"{w}-{l} · {pct}", style={"fontWeight": "600", "color": color}),
            html.Span(" ▼", style={"fontSize": "8px", "color": COLORS["your_team"], "marginLeft": "8px"}),
        ], style={
            "display": "flex", "justifyContent": "space-between", "alignItems": "center",
            "padding": "8px 12px", "background": "rgba(76,201,240,0.08)",
            "borderRadius": "4px", "cursor": "pointer",
        })
        # Expanded detail (pick/defend)
        expanded = html.Div([
            html.Div([
                html.Div([
                    html.Div(f"{r.get('pick_wins', 0)}-{r.get('pick_losses', 0)}", style={
                        "fontSize": "16px", "fontWeight": "600",
                        "color": wl_color(r.get("pick_wins", 0), r.get("pick_losses", 0)),
                    }),
                    html.Div("ON PICK", style={"fontSize": "9px", "color": COLORS["muted"]}),
                ], style={"textAlign": "center", "padding": "6px 14px",
                          "background": "rgba(76,201,240,0.06)", "borderRadius": "5px"}),
                html.Div([
                    html.Div(f"{r.get('defend_wins', 0)}-{r.get('defend_losses', 0)}", style={
                        "fontSize": "16px", "fontWeight": "600",
                        "color": wl_color(r.get("defend_wins", 0), r.get("defend_losses", 0)),
                    }),
                    html.Div("ON DEFEND", style={"fontSize": "9px", "color": COLORS["muted"]}),
                ], style={"textAlign": "center", "padding": "6px 14px",
                          "background": "rgba(76,201,240,0.06)", "borderRadius": "5px"}),
            ], style={"display": "flex", "gap": "12px", "justifyContent": "center"}),
        ], id={"type": "tp-expand", "index": idx}, style={
            "display": "none", "padding": "8px 12px",
            "background": "rgba(76,201,240,0.03)", "borderRadius": "0 0 4px 4px",
        })
        rows.append(html.Div([collapsed, expanded],
                             id={"type": "tp-map-row", "index": idx},
                             style={"marginBottom": "4px"}))
    # Totals row
    total_all = total_w + total_l
    total_pct = f"{total_w / total_all:.0%}" if total_all else "-"
    rows.append(
        html.Div([
            html.Span("TOTAL", style={"flex": "1", "fontWeight": "700"}),
            html.Span(f"{total_w}-{total_l} · {total_pct}",
                       style={"fontWeight": "700", "color": wl_color(total_w, total_l)}),
        ], style={
            "display": "flex", "justifyContent": "space-between",
            "padding": "8px 12px", "borderTop": f"1px solid {COLORS['border']}",
            "marginTop": "8px",
        })
    )
    return dbc.Card([
        dbc.CardHeader("MAP RECORD", style={
            "color": COLORS["your_team"], "fontWeight": "600",
            "fontSize": "14px", "letterSpacing": "1px",
            "background": COLORS["card_bg"], "border": "none",
        }),
        dbc.CardBody(rows),
    ], style={"background": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"})


def _avoidance_target_card(data: list[dict]) -> dbc.Card:
    """Render the Avoidance & Target Index card."""
    rows = []
    for d in data:
        low_sample = d["avoid_n"] < LOW_SAMPLE_THRESHOLD and d["target_n"] < LOW_SAMPLE_THRESHOLD
        opacity = "0.4" if low_sample else "1"
        avoid_width = max(d["avoid_ratio"] * 100, 2)
        target_width = max(d["target_ratio"] * 100, 2)
        rows.append(
            html.Div([
                html.Div([
                    html.Span(f"{d['map_name']} ({d['mode']})"),
                    html.Span(f"n={max(d['avoid_n'], d['target_n'])}", style={"color": COLORS["muted"]}),
                ], style={"display": "flex", "justifyContent": "space-between", "fontSize": "12px", "marginBottom": "4px"}),
                html.Div([
                    html.Div(style={"width": f"{avoid_width}%", "background": COLORS["loss"],
                                    "height": "20px", "borderRadius": "3px 0 0 3px"}),
                    html.Div(style={"width": f"{target_width}%", "background": COLORS["your_team"],
                                    "height": "20px", "borderRadius": "0 3px 3px 0"}),
                ], style={"display": "flex", "gap": "4px"}),
                html.Div([
                    html.Span(f"Avoid {d['avoid_ratio']:.0%}", style={"fontSize": "10px", "color": COLORS["muted"]}),
                    html.Span(f"Target {d['target_ratio']:.0%}", style={"fontSize": "10px", "color": COLORS["muted"]}),
                ], style={"display": "flex", "justifyContent": "space-between", "marginTop": "2px"}),
            ], style={"marginBottom": "10px", "opacity": opacity})
        )
    return dbc.Card([
        dbc.CardHeader("AVOIDANCE & TARGET INDEX", style={
            "color": COLORS["your_team"], "fontWeight": "600",
            "fontSize": "14px", "letterSpacing": "1px",
            "background": COLORS["card_bg"], "border": "none",
        }),
        dbc.CardBody(rows),
    ], style={"background": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"})


def _ban_card(ban_data: dict, abbr: str) -> dbc.Card:
    """Render the Ban Tendencies card."""
    rows = []
    if ban_data["total_series"] == 0:
        rows.append(html.P("No ban data available", style={"color": COLORS["muted"]}))
    else:
        for section_label, bans_key, color in [
            (f"{abbr} Bans", "team_bans", COLORS["ban"]),
            (f"Opponent Bans vs {abbr}", "opponent_bans", COLORS["loss"]),
        ]:
            rows.append(html.Div(section_label, style={
                "fontSize": "12px", "fontWeight": "600", "color": color,
                "marginBottom": "8px", "marginTop": "12px",
            }))
            for b in ban_data[bans_key]:
                rate = b["ban_count"] / b["total_series"] if b["total_series"] else 0
                bar_width = max(rate * 100, 2)
                rows.append(html.Div([
                    html.Div(style={"width": "50%", "background": COLORS["border"],
                                    "borderRadius": "4px", "height": "18px", "overflow": "hidden"},
                             children=[
                                 html.Div(style={"width": f"{bar_width}%", "background": color,
                                                 "height": "100%", "borderRadius": "4px"})
                             ]),
                    html.Span(f"{b['map_name']} ({b['mode']}) — {b['ban_count']}/{b['total_series']}",
                              style={"fontSize": "12px", "marginLeft": "8px"}),
                ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}))

    return dbc.Card([
        dbc.CardHeader("BAN TENDENCIES", style={
            "color": COLORS["your_team"], "fontWeight": "600",
            "fontSize": "14px", "letterSpacing": "1px",
            "background": COLORS["card_bg"], "border": "none",
        }),
        dbc.CardBody(rows),
    ], style={"background": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"})


def layout():
    conn = get_db()
    options = team_dropdown_options(conn)
    conn.close()
    default_team = options[0]["value"] if options else None

    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Select(
                    id="tp-team-select",
                    options=[{"label": o["label"], "value": o["value"]} for o in options],
                    value=default_team,
                    style={"background": COLORS["card_bg"], "border": f"1px solid #0f3460",
                           "color": COLORS["your_team"]},
                ),
            ], width=3),
        ], className="mb-3"),
        html.Div(id="tp-content"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("tp-content", "children"),
        Input("tp-team-select", "value"),
    )
    def update_profile(team_id):
        if not team_id:
            return html.Div("Select a team")
        team_id = int(team_id)
        conn = get_db()

        abbr = conn.execute(
            "SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)
        ).fetchone()[0]

        records = _build_map_record_data(conn, team_id)
        avoid_target = _build_avoidance_target_data(conn, team_id)
        ban_data = get_team_ban_summary(conn, team_id)
        elo = get_current_elo(conn, team_id)
        low_conf = is_low_confidence(conn, team_id)
        conn.close()

        elo_text = f"Elo: {elo:.0f}"
        if low_conf:
            elo_text += " (LOW CONFIDENCE)"

        return dbc.Container([
            dbc.Row([
                dbc.Col(_map_record_card(records), md=6),
                dbc.Col(_avoidance_target_card(avoid_target), md=6),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col(_ban_card(ban_data, abbr), md=6),
                dbc.Col(
                    dbc.Card([
                        dbc.CardHeader("ELO RATING", style={
                            "color": COLORS["your_team"], "fontWeight": "600",
                            "fontSize": "14px", "letterSpacing": "1px",
                            "background": COLORS["card_bg"], "border": "none",
                        }),
                        dbc.CardBody([
                            html.Div(f"{elo:.0f}", style={
                                "fontSize": "48px", "fontWeight": "700",
                                "color": COLORS["your_team"], "textAlign": "center",
                            }),
                            html.Div(
                                "LOW CONFIDENCE" if low_conf else "",
                                style={"textAlign": "center", "color": COLORS["neutral"],
                                       "fontSize": "12px", "marginTop": "4px"},
                            ),
                        ]),
                    ], style={"background": COLORS["card_bg"],
                              "border": f"1px solid {COLORS['border']}"}),
                    md=6,
                ),
            ]),
        ], fluid=True)

    @app.callback(
        Output({"type": "tp-expand", "index": ALL}, "style"),
        Input({"type": "tp-map-row", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def toggle_map_expand(n_clicks_list):
        from dash import ctx
        from dash.exceptions import PreventUpdate
        triggered = ctx.triggered_id
        if not triggered:
            raise PreventUpdate
        styles = []
        for i, clicks in enumerate(n_clicks_list):
            base_style = {"padding": "8px 12px", "background": "rgba(76,201,240,0.03)",
                          "borderRadius": "0 0 4px 4px"}
            if triggered.get("index") == i and clicks and clicks % 2 == 1:
                base_style["display"] = "block"
            else:
                base_style["display"] = "none"
            styles.append(base_style)
        return styles
```

Note: add `from dash import ALL` at the top of the file imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_team_profile.py -v`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/team_profile.py tests/test_dashboard_team_profile.py
git commit -m "feat: implement Team Profile dashboard tab"
```

---

### Task 4: Map Matrix tab

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/map_matrix.py`
- Create: `tests/test_dashboard_map_matrix.py`

- [ ] **Step 1: Write test for map matrix data builder**

Create `tests/test_dashboard_map_matrix.py`:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


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
    yield conn
    conn.close()


def test_build_matrix_data(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db)
    assert len(teams) > 0
    assert len(maps) > 0
    # DVS should have a winning record on Tunisia
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia_id = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia'").fetchone()[0]
    cell = matrix.get((dvs_id, tunisia_id))
    assert cell is not None
    assert cell["wins"] == 1
    assert cell["losses"] == 0


def test_build_matrix_data_mode_filter(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db, mode_filter="SnD")
    # Only SnD maps should be in the maps list
    for _, _, mode in maps:
        assert mode == "SnD"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_map_matrix.py -v`
Expected: FAIL — `_build_matrix_data` does not exist.

- [ ] **Step 3: Implement map_matrix.py**

Replace `src/cdm_stats/dashboard/tabs/map_matrix.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS, LOW_SAMPLE_THRESHOLD, get_all_teams, get_all_maps,
)
from cdm_stats.metrics.avoidance import (
    pick_win_loss, defend_win_loss, avoidance_index, target_index,
)


def _build_matrix_data(
    conn: sqlite3.Connection, mode_filter: str | None = None,
) -> tuple[list[tuple[int, str]], list[tuple[int, str, str]], dict]:
    """Build the data for the map matrix heatmap.

    Returns:
        teams: list of (team_id, abbreviation)
        maps: list of (map_id, map_name, mode), filtered by mode if provided
        matrix: dict of (team_id, map_id) -> {wins, losses, pick_wl, defend_wl, avoid, target}
    """
    teams = get_all_teams(conn)
    all_maps = get_all_maps(conn)
    if mode_filter:
        all_maps = [(mid, name, mode) for mid, name, mode in all_maps if mode == mode_filter]

    matrix = {}
    for team_id, _ in teams:
        for map_id, _, _ in all_maps:
            pwl = pick_win_loss(conn, team_id, map_id)
            dwl = defend_win_loss(conn, team_id, map_id)
            avoid = avoidance_index(conn, team_id, map_id)
            tgt = target_index(conn, team_id, map_id)
            total_w = pwl["wins"] + dwl["wins"]
            total_l = pwl["losses"] + dwl["losses"]
            matrix[(team_id, map_id)] = {
                "wins": total_w,
                "losses": total_l,
                "pick_wl": pwl,
                "defend_wl": dwl,
                "avoid": avoid,
                "target": tgt,
            }

    return teams, all_maps, matrix


def _build_heatmap_figure(teams, maps, matrix) -> go.Figure:
    """Build a Plotly heatmap figure from matrix data."""
    team_labels = [abbr for _, abbr in teams]
    map_labels = [f"{name} ({mode})" for _, name, mode in maps]

    z = []
    hover_text = []
    for team_id, abbr in teams:
        row_z = []
        row_hover = []
        for map_id, map_name, mode in maps:
            cell = matrix.get((team_id, map_id))
            if cell is None:
                row_z.append(0.5)
                row_hover.append("No data")
                continue
            w, l = cell["wins"], cell["losses"]
            total = w + l
            rate = w / total if total else 0.5
            row_z.append(rate)

            pwl = cell["pick_wl"]
            dwl = cell["defend_wl"]
            avoid = cell["avoid"]
            tgt = cell["target"]
            low = " ⚠️" if total < LOW_SAMPLE_THRESHOLD else ""
            row_hover.append(
                f"<b>{abbr} — {map_name} ({mode})</b>{low}<br>"
                f"Overall: {w}-{l} ({rate:.0%})<br>"
                f"Pick: {pwl['wins']}-{pwl['losses']}<br>"
                f"Defend: {dwl['wins']}-{dwl['losses']}<br>"
                f"Avoid: {avoid['ratio']:.0%} (n={avoid['opportunities']})<br>"
                f"Target: {tgt['ratio']:.0%} (n={tgt['opportunities']})"
            )
        z.append(row_z)
        hover_text.append(row_hover)

    # Custom text to show in cells — just W-L
    cell_text = []
    for team_id, _ in teams:
        row_text = []
        for map_id, _, _ in maps:
            cell = matrix.get((team_id, map_id))
            if cell is None:
                row_text.append("-")
            else:
                row_text.append(f"{cell['wins']}-{cell['losses']}")
        cell_text.append(row_text)

    fig = go.Figure(data=go.Heatmap(
        z=z,
        x=map_labels,
        y=team_labels,
        text=cell_text,
        texttemplate="%{text}",
        textfont={"size": 12, "color": "white"},
        hovertext=hover_text,
        hovertemplate="%{hovertext}<extra></extra>",
        colorscale=[
            [0, "#991b1b"],
            [0.4, "#dc2626"],
            [0.5, "#78716c"],
            [0.6, "#16a34a"],
            [1, "#15803d"],
        ],
        showscale=False,
        zmin=0,
        zmax=1,
    ))

    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 80, "r": 20, "t": 20, "b": 80},
        height=max(400, len(teams) * 40 + 100),
        xaxis={"side": "top", "tickangle": -45},
        yaxis={"autorange": "reversed"},
    )

    return fig


def layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Select(
                    id="mm-mode-filter",
                    options=[
                        {"label": "All Modes", "value": ""},
                        {"label": "SnD", "value": "SnD"},
                        {"label": "HP", "value": "HP"},
                        {"label": "Control", "value": "Control"},
                    ],
                    value="",
                    style={"background": COLORS["card_bg"], "border": "1px solid #0f3460",
                           "color": COLORS["your_team"]},
                ),
            ], width=3),
        ], className="mb-3"),
        dcc.Graph(id="mm-heatmap"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("mm-heatmap", "figure"),
        Input("mm-mode-filter", "value"),
    )
    def update_matrix(mode_filter):
        conn = get_db()
        mode = mode_filter if mode_filter else None
        teams, maps, matrix = _build_matrix_data(conn, mode_filter=mode)
        conn.close()
        return _build_heatmap_figure(teams, maps, matrix)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_map_matrix.py -v`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/map_matrix.py tests/test_dashboard_map_matrix.py
git commit -m "feat: implement Map Matrix dashboard tab with heatmap"
```

---

### Task 5: Match-Up Prep tab

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/matchup_prep.py`
- Create: `tests/test_dashboard_matchup_prep.py`

- [ ] **Step 1: Write test for matchup data builder**

Create `tests/test_dashboard_matchup_prep.py`:

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
    # Should be grouped by mode
    assert "SnD" in data
    assert "HP" in data
    assert "Control" in data
    # Tunisia is SnD — DVS should have 1-0 h2h
    snd_maps = data["SnD"]
    tunisia = next((m for m in snd_maps if m["map_name"] == "Tunisia"), None)
    assert tunisia is not None
    assert tunisia["h2h"]["wins"] == 1
    assert tunisia["h2h"]["losses"] == 0


def test_build_matchup_data_includes_wl_and_avoid(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    # Check that each map entry has the expected keys
    for mode_maps in data.values():
        for m in mode_maps:
            assert "your_wl" in m
            assert "opp_wl" in m
            assert "your_avoid" in m
            assert "opp_avoid" in m
            assert "your_target" in m
            assert "opp_target" in m
            assert "your_pick_wl" in m
            assert "your_defend_wl" in m
            assert "opp_pick_wl" in m
            assert "opp_defend_wl" in m
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_matchup_prep.py -v`
Expected: FAIL — `_build_matchup_data` does not exist.

- [ ] **Step 3: Implement matchup_prep.py**

Replace `src/cdm_stats/dashboard/tabs/matchup_prep.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
from dash import html
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS, MODE_COLORS, LOW_SAMPLE_THRESHOLD,
    wl_color, team_dropdown_options, get_all_maps,
)
from cdm_stats.metrics.avoidance import (
    pick_win_loss, defend_win_loss, avoidance_index, target_index,
)
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import get_ban_summary


def _head_to_head(conn: sqlite3.Connection, team_id: int, opp_id: int, map_id: int) -> dict:
    """W-L record between two specific teams on a specific map."""
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?) OR (m.team1_id = ? AND m.team2_id = ?))""",
        (team_id, opp_id, map_id, team_id, opp_id, opp_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def _build_matchup_data(
    conn: sqlite3.Connection, your_id: int, opp_id: int,
) -> dict[str, list[dict]]:
    """Build matchup data grouped by mode.

    Returns dict with keys 'SnD', 'HP', 'Control', each containing a list of map dicts.
    """
    all_maps = get_all_maps(conn)
    by_mode: dict[str, list[dict]] = {"SnD": [], "HP": [], "Control": []}

    for map_id, map_name, mode in all_maps:
        h2h = _head_to_head(conn, your_id, opp_id, map_id)
        your_pwl = pick_win_loss(conn, your_id, map_id)
        your_dwl = defend_win_loss(conn, your_id, map_id)
        opp_pwl = pick_win_loss(conn, opp_id, map_id)
        opp_dwl = defend_win_loss(conn, opp_id, map_id)
        your_avoid = avoidance_index(conn, your_id, map_id)
        opp_avoid = avoidance_index(conn, opp_id, map_id)
        your_tgt = target_index(conn, your_id, map_id)
        opp_tgt = target_index(conn, opp_id, map_id)

        your_total_w = your_pwl["wins"] + your_dwl["wins"]
        your_total_l = your_pwl["losses"] + your_dwl["losses"]
        opp_total_w = opp_pwl["wins"] + opp_dwl["wins"]
        opp_total_l = opp_pwl["losses"] + opp_dwl["losses"]

        by_mode[mode].append({
            "map_id": map_id,
            "map_name": map_name,
            "mode": mode,
            "h2h": h2h,
            "your_wl": {"wins": your_total_w, "losses": your_total_l},
            "opp_wl": {"wins": opp_total_w, "losses": opp_total_l},
            "your_avoid": your_avoid,
            "opp_avoid": opp_avoid,
            "your_target": your_tgt,
            "opp_target": opp_tgt,
            "your_pick_wl": your_pwl,
            "your_defend_wl": your_dwl,
            "opp_pick_wl": opp_pwl,
            "opp_defend_wl": opp_dwl,
        })

    return by_mode


def _stat_block(label: str, wins: int, losses: int, tint: str, clickable: bool = False) -> html.Div:
    """A single stat display block (e.g., W-L, Pick, Defend)."""
    style = {
        "textAlign": "center", "padding": "8px 16px",
        "background": f"rgba({_hex_to_rgb(tint)},0.06)",
        "borderRadius": "6px",
    }
    if clickable:
        style["cursor"] = "pointer"
        style["border"] = f"1px solid rgba({_hex_to_rgb(tint)},0.15)"
    return html.Div([
        html.Div(f"{wins}-{losses}", style={
            "fontSize": "22px" if clickable else "16px",
            "fontWeight": "700" if clickable else "600",
            "color": wl_color(wins, losses),
        }),
        html.Div(label, style={"fontSize": "10px", "color": COLORS["muted"], "marginTop": "2px"}),
    ], style=style)


def _pct_block(label: str, ratio: float, n: int, tint: str) -> html.Div:
    """A percentage stat block for avoid/target."""
    low = n < LOW_SAMPLE_THRESHOLD
    color = COLORS["loss"] if ratio >= 0.6 else COLORS["muted"]
    return html.Div([
        html.Div(f"{ratio:.0%}", style={
            "fontSize": "18px", "fontWeight": "600",
            "color": color, "opacity": "0.5" if low else "1",
        }),
        html.Div([
            html.Span(label, style={"fontSize": "10px", "color": COLORS["muted"]}),
            html.Span(f" n={n}", style={"fontSize": "10px", "color": "#555"}),
        ], style={"marginTop": "2px"}),
    ], style={
        "textAlign": "center", "padding": "8px 16px",
        "background": f"rgba({_hex_to_rgb(tint)},0.06)",
        "borderRadius": "6px",
    })


def _hex_to_rgb(hex_color: str) -> str:
    """Convert '#4cc9f0' to '76,201,240'."""
    h = hex_color.lstrip("#")
    return ",".join(str(int(h[i:i+2], 16)) for i in (0, 2, 4))


def _map_row(m: dict, row_idx: int) -> html.Div:
    """Build a single map row (collapsed) with expand/collapse for pick/defend."""
    your_tint = COLORS["your_team"]
    opp_tint = COLORS["opponent"]

    collapsed = html.Div([
        # Map name + H2H
        html.Div([
            html.Div(m["map_name"], style={"fontWeight": "600", "fontSize": "15px"}),
            html.Div(f"H2H: {m['h2h']['wins']}-{m['h2h']['losses']}",
                     style={"fontSize": "11px", "color": COLORS["muted"], "marginTop": "2px"}),
        ], style={"width": "140px"}),
        # Your team stats
        html.Div([
            _stat_block("W-L", m["your_wl"]["wins"], m["your_wl"]["losses"], your_tint, clickable=True),
            _pct_block("AVOID", m["your_avoid"]["ratio"], m["your_avoid"]["opportunities"], your_tint),
            _pct_block("TARGET", m["your_target"]["ratio"], m["your_target"]["opportunities"], your_tint),
        ], style={"display": "flex", "gap": "12px", "justifyContent": "center", "flex": "1"}),
        # VS divider
        html.Div("VS", style={
            "textAlign": "center", "color": "#333", "fontSize": "12px",
            "fontWeight": "700", "width": "50px",
        }),
        # Opponent stats
        html.Div([
            _stat_block("W-L", m["opp_wl"]["wins"], m["opp_wl"]["losses"], opp_tint, clickable=True),
            _pct_block("AVOID", m["opp_avoid"]["ratio"], m["opp_avoid"]["opportunities"], opp_tint),
            _pct_block("TARGET", m["opp_target"]["ratio"], m["opp_target"]["opportunities"], opp_tint),
        ], style={"display": "flex", "gap": "12px", "justifyContent": "center", "flex": "1"}),
    ], style={
        "display": "flex", "alignItems": "center", "gap": "12px",
        "padding": "14px 16px",
    })

    # Expanded detail (pick/defend breakdown) — hidden by default via Dash callback
    expanded = html.Div([
        html.Div("Breakdown", style={
            "fontSize": "11px", "color": "#555", "textTransform": "uppercase",
            "letterSpacing": "0.5px", "width": "140px",
        }),
        html.Div([
            _stat_block("ON PICK", m["your_pick_wl"]["wins"], m["your_pick_wl"]["losses"], your_tint),
            _stat_block("ON DEFEND", m["your_defend_wl"]["wins"], m["your_defend_wl"]["losses"], your_tint),
        ], style={"display": "flex", "gap": "12px", "justifyContent": "center", "flex": "1"}),
        html.Div(style={"width": "50px"}),
        html.Div([
            _stat_block("ON PICK", m["opp_pick_wl"]["wins"], m["opp_pick_wl"]["losses"], opp_tint),
            _stat_block("ON DEFEND", m["opp_defend_wl"]["wins"], m["opp_defend_wl"]["losses"], opp_tint),
        ], style={"display": "flex", "gap": "12px", "justifyContent": "center", "flex": "1"}),
    ], id={"type": "mp-expand", "index": row_idx}, style={
        "display": "none",
        "borderTop": f"1px solid {COLORS['border']}", "padding": "12px 16px",
        "alignItems": "center", "gap": "12px",
        "background": "rgba(76,201,240,0.02)",
    })

    return html.Div([collapsed, expanded], id={"type": "mp-row", "index": row_idx}, style={
        "background": COLORS["card_bg"], "borderRadius": "8px",
        "border": f"1px solid {COLORS['border']}", "marginBottom": "12px",
        "cursor": "pointer",
    })


def _ban_comparison(conn: sqlite3.Connection, your_id: int, opp_id: int,
                    your_abbr: str, opp_abbr: str) -> html.Div:
    """Build the ban comparison section."""
    your_bans = get_ban_summary(conn, your_id, opp_id)
    opp_bans = get_ban_summary(conn, opp_id, your_id)

    def _ban_card(label: str, bans: list[dict], color: str) -> dbc.Card:
        rows = []
        if not bans:
            rows.append(html.P("No ban data", style={"color": COLORS["muted"], "fontSize": "12px"}))
        for b in bans:
            rate = b["ban_count"] / b["total_series"] if b["total_series"] else 0
            bar_width = max(rate * 100, 2)
            rows.append(html.Div([
                html.Div(style={"width": "50%", "background": COLORS["border"],
                                "borderRadius": "4px", "height": "18px", "overflow": "hidden"},
                         children=[html.Div(style={
                             "width": f"{bar_width}%", "background": color,
                             "height": "100%", "borderRadius": "4px",
                         })]),
                html.Span(f"{b['map_name']} ({b['mode']}) — {b['ban_count']}/{b['total_series']}",
                          style={"fontSize": "12px", "marginLeft": "8px"}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}))

        return dbc.Card([
            dbc.CardBody([
                html.Div(label, style={"fontSize": "12px", "fontWeight": "600",
                                       "color": color, "marginBottom": "10px"}),
                *rows,
            ]),
        ], style={"background": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"})

    return html.Div([
        html.Div([
            html.Div("BAN COMPARISON", style={
                "background": COLORS["neutral"], "color": COLORS["page_bg"],
                "fontWeight": "700", "fontSize": "11px", "padding": "4px 10px",
                "borderRadius": "4px", "textTransform": "uppercase", "letterSpacing": "1px",
            }),
            html.Div(style={"flex": "1", "height": "1px", "background": COLORS["border"]}),
        ], style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "14px"}),
        dbc.Row([
            dbc.Col(_ban_card(f"{your_abbr} bans vs {opp_abbr}", your_bans, COLORS["your_team"]), md=6),
            dbc.Col(_ban_card(f"{opp_abbr} bans vs {your_abbr}", opp_bans, COLORS["opponent"]), md=6),
        ]),
    ], style={"padding": "16px 0"})


def layout():
    conn = get_db()
    options = team_dropdown_options(conn)
    conn.close()
    default_your = options[0]["value"] if options else None
    default_opp = options[1]["value"] if len(options) > 1 else None

    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.Select(
                    id="mp-your-team",
                    options=[{"label": o["label"], "value": o["value"]} for o in options],
                    value=default_your,
                    style={"background": COLORS["card_bg"], "border": "1px solid #0f3460",
                           "color": COLORS["your_team"]},
                ),
            ], width=3),
            dbc.Col(html.Div("vs", style={"fontSize": "20px", "color": "#555",
                                           "textAlign": "center", "paddingTop": "6px"}), width=1),
            dbc.Col([
                dbc.Select(
                    id="mp-opp-team",
                    options=[{"label": o["label"], "value": o["value"]} for o in options],
                    value=default_opp,
                    style={"background": COLORS["card_bg"], "border": "1px solid #0f3460",
                           "color": COLORS["opponent"]},
                ),
            ], width=3),
            dbc.Col(html.Div(id="mp-elo-badge"), width=5),
        ], className="mb-3", align="center"),
        html.Div(id="mp-content"),
    ], fluid=True)


def register_callbacks(app):
    from dash import ALL

    @app.callback(
        Output("mp-content", "children"),
        Output("mp-elo-badge", "children"),
        Input("mp-your-team", "value"),
        Input("mp-opp-team", "value"),
    )
    def update_matchup(your_id, opp_id):
        if not your_id or not opp_id or your_id == opp_id:
            return html.Div("Select two different teams"), ""
        your_id, opp_id = int(your_id), int(opp_id)
        conn = get_db()

        your_abbr = conn.execute(
            "SELECT abbreviation FROM teams WHERE team_id = ?", (your_id,)
        ).fetchone()[0]
        opp_abbr = conn.execute(
            "SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)
        ).fetchone()[0]

        matchup = _build_matchup_data(conn, your_id, opp_id)

        # Build sections by mode
        sections = []
        row_idx = 0
        for mode in ["SnD", "HP", "Control"]:
            maps_in_mode = matchup[mode]
            if not maps_in_mode:
                continue
            sections.append(html.Div([
                html.Div([
                    html.Div(mode, style={
                        "background": MODE_COLORS[mode], "color": COLORS["page_bg"],
                        "fontWeight": "700", "fontSize": "11px", "padding": "4px 10px",
                        "borderRadius": "4px", "textTransform": "uppercase", "letterSpacing": "1px",
                    }),
                    html.Div(style={"flex": "1", "height": "1px", "background": COLORS["border"]}),
                ], style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "14px"}),
                *[_map_row(m, row_idx + i) for i, m in enumerate(maps_in_mode)],
            ], style={"marginBottom": "20px"}))
            row_idx += len(maps_in_mode)

        # Ban comparison
        sections.append(_ban_comparison(conn, your_id, opp_id, your_abbr, opp_abbr))

        # Elo badge
        your_elo = get_current_elo(conn, your_id)
        opp_elo = get_current_elo(conn, opp_id)
        your_lc = " ⚠" if is_low_confidence(conn, your_id) else ""
        opp_lc = " ⚠" if is_low_confidence(conn, opp_id) else ""
        conn.close()

        elo_badge = html.Div(
            f"Elo: {your_elo:.0f}{your_lc} vs {opp_elo:.0f}{opp_lc}",
            style={
                "background": COLORS["card_bg"], "border": "1px solid #0f3460",
                "borderRadius": "6px", "padding": "6px 14px",
                "color": COLORS["muted"], "fontSize": "12px", "textAlign": "right",
            },
        )

        return html.Div(sections), elo_badge

    @app.callback(
        Output({"type": "mp-expand", "index": ALL}, "style"),
        Input({"type": "mp-row", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def toggle_expand(n_clicks_list):
        from dash import ctx
        triggered = ctx.triggered_id
        if not triggered:
            from dash.exceptions import PreventUpdate
            raise PreventUpdate

        # Build style list — toggle the clicked row, keep others as-is
        styles = []
        for i, clicks in enumerate(n_clicks_list):
            base_style = {
                "borderTop": f"1px solid {COLORS['border']}", "padding": "12px 16px",
                "alignItems": "center", "gap": "12px",
                "background": "rgba(76,201,240,0.02)",
            }
            if triggered.get("index") == i and clicks and clicks % 2 == 1:
                base_style["display"] = "flex"
            else:
                base_style["display"] = "none"
            styles.append(base_style)
        return styles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_matchup_prep.py -v`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/matchup_prep.py tests/test_dashboard_matchup_prep.py
git commit -m "feat: implement Match-Up Prep dashboard tab with expand/collapse"
```

---

### Task 6: Elo Tracker tab

**Files:**
- Modify: `src/cdm_stats/dashboard/tabs/elo_tracker.py`
- Create: `tests/test_dashboard_elo_tracker.py`

- [ ] **Step 1: Write test for elo tracker data builder**

Create `tests/test_dashboard_elo_tracker.py`:

```python
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, SEED_ELO


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


def test_build_elo_traces(db):
    from cdm_stats.dashboard.tabs.elo_tracker import _build_elo_traces
    traces = _build_elo_traces(db)
    # Should have entries for at least DVS and OUG (who have elo history)
    teams_with_data = {t["abbr"] for t in traces if len(t["elos"]) > 1}
    assert "DVS" in teams_with_data
    assert "OUG" in teams_with_data
    # Each trace should start at SEED_ELO
    dvs_trace = next(t for t in traces if t["abbr"] == "DVS")
    assert dvs_trace["elos"][0] == SEED_ELO
    # DVS won, so second elo should be above seed
    assert dvs_trace["elos"][1] > SEED_ELO
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_elo_tracker.py -v`
Expected: FAIL — `_build_elo_traces` does not exist.

- [ ] **Step 3: Implement elo_tracker.py**

Replace `src/cdm_stats/dashboard/tabs/elo_tracker.py` with:

```python
import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, get_all_teams
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO


def _week_number(date_str: str, earliest: str) -> int:
    from datetime import datetime
    d = datetime.strptime(date_str, "%Y-%m-%d")
    e = datetime.strptime(earliest, "%Y-%m-%d")
    return (d - e).days // 7 + 1


def _build_elo_traces(conn: sqlite3.Connection) -> list[dict]:
    """Build Elo trajectory data for all teams.

    Returns list of dicts with keys: team_id, abbr, weeks, elos, hover_texts.
    Week 0 = seed. Each subsequent point is the last Elo of that week.
    """
    teams = get_all_teams(conn)

    # Find earliest match date
    row = conn.execute("SELECT MIN(match_date) FROM matches").fetchone()
    if not row or not row[0]:
        return []
    earliest_date = row[0]

    traces = []
    for team_id, abbr in teams:
        history = get_elo_history(conn, team_id)

        # Group by week, take last Elo per week
        week_elo = {}
        week_hover = {}
        for h in history:
            wk = _week_number(h["match_date"], earliest_date)
            week_elo[wk] = h["elo_after"]
            # Build hover text: find the opponent and result for this match
            match = conn.execute(
                """SELECT team1_id, team2_id, series_winner_id
                   FROM matches WHERE match_id = ?""",
                (h["match_id"],),
            ).fetchone()
            if match:
                opp_id = match[1] if match[0] == team_id else match[0]
                opp_abbr = conn.execute(
                    "SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)
                ).fetchone()[0]
                result = "W" if match[2] == team_id else "L"
                week_hover[wk] = f"{abbr}: {h['elo_after']:.0f}<br>vs {opp_abbr} ({result})"

        weeks = sorted(week_elo.keys())
        traces.append({
            "team_id": team_id,
            "abbr": abbr,
            "weeks": [0] + weeks,
            "elos": [SEED_ELO] + [week_elo[w] for w in weeks],
            "hover_texts": ["Seed: 1000"] + [week_hover.get(w, "") for w in weeks],
        })

    return traces


def _build_figure(traces: list[dict]) -> go.Figure:
    """Build the Plotly Elo trajectory figure."""
    fig = go.Figure()

    for trace in traces:
        if len(trace["elos"]) <= 1:
            continue
        fig.add_trace(go.Scatter(
            x=trace["weeks"],
            y=trace["elos"],
            mode="lines+markers",
            name=trace["abbr"],
            text=trace["hover_texts"],
            hovertemplate="%{text}<extra></extra>",
            marker={"size": 5},
            line={"width": 2},
        ))

    # Seed line
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="gray", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")

    # Low confidence shading (weeks 1-6)
    fig.add_vrect(x0=0, x1=6, fillcolor="gray", opacity=0.05, line_width=0,
                  annotation_text="Low Confidence Zone", annotation_position="top left",
                  annotation_font_color="#666")

    max_week = max((t["weeks"][-1] for t in traces if t["weeks"]), default=1)
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 60, "r": 20, "t": 40, "b": 60},
        height=500,
        xaxis={
            "title": "Week",
            "tickmode": "array",
            "tickvals": list(range(0, max_week + 1)),
            "ticktext": ["Start"] + [f"W{w}" for w in range(1, max_week + 1)],
            "gridcolor": COLORS["border"],
        },
        yaxis={"title": "Elo Rating", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )

    return fig


def layout():
    return dbc.Container([
        dcc.Graph(id="elo-chart"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("elo-chart", "figure"),
        Input("elo-chart", "id"),  # triggers on load
    )
    def update_chart(_):
        conn = get_db()
        traces = _build_elo_traces(conn)
        conn.close()
        return _build_figure(traces)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_elo_tracker.py -v`
Expected: 1 test PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cdm_stats/dashboard/tabs/elo_tracker.py tests/test_dashboard_elo_tracker.py
git commit -m "feat: implement Elo Tracker dashboard tab"
```

---

### Task 7: Integration test and final verification

**Files:**
- Create: `tests/test_dashboard_app.py`

- [ ] **Step 1: Write integration test that the app initializes without errors**

Create `tests/test_dashboard_app.py`:

```python
def test_app_imports_and_initializes():
    """Verify the Dash app can be imported and all callbacks registered."""
    from cdm_stats.dashboard.app import app, register_all_callbacks
    register_all_callbacks()
    # App should have a layout
    assert app.layout is not None


def test_render_tab_returns_content():
    """Verify each tab renders without error."""
    from cdm_stats.dashboard.app import render_tab
    for tab in ["team-profile", "map-matrix", "matchup-prep", "elo-tracker"]:
        result = render_tab(tab)
        assert result is not None
```

- [ ] **Step 2: Run integration tests**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest tests/test_dashboard_app.py -v`
Expected: 2 tests PASS.

- [ ] **Step 3: Run full test suite to check for regressions**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run pytest -v`
Expected: All tests PASS, including all pre-existing tests.

- [ ] **Step 4: Manual smoke test**

Run: `cd /Users/AnshulSrivastava/Desktop/cdm_stats && uv run python -m cdm_stats.dashboard`
Open `http://localhost:8050` in browser. Verify:
- All four tabs render
- Team Profile shows data when a team is selected
- Map Matrix shows a colored heatmap with hover tooltips
- Match-Up Prep shows side-by-side stats, rows expand on click
- Elo Tracker shows line chart with all teams

- [ ] **Step 5: Commit integration tests**

```bash
git add tests/test_dashboard_app.py
git commit -m "test: add dashboard integration tests"
```

- [ ] **Step 6: Final commit — remove placeholder text from any tab modules**

Verify no tab still returns "coming soon" text. If all tabs are implemented, this step is a no-op.

```bash
git log --oneline -7
```

Expected: 7 new commits from this plan.
