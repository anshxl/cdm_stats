import os
import sqlite3
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).resolve().parents[3] / "data" / "cdl.db"))

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
        dbc.Tab(label="Scrim Performance", tab_id="scrim-performance"),
        dbc.Tab(label="Player Stats", tab_id="player-stats"),
    ]),
    html.Div(id="tab-content", className="mt-3"),
], fluid=True, className="px-0")


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@app.callback(Output("tab-content", "children"), Input("main-tabs", "active_tab"))
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


def register_all_callbacks():
    from cdm_stats.dashboard.tabs import team_profile, map_matrix, matchup_prep, elo_tracker
    from cdm_stats.dashboard.tabs import scrim_performance, player_stats
    team_profile.register_callbacks(app)
    map_matrix.register_callbacks(app)
    matchup_prep.register_callbacks(app)
    elo_tracker.register_callbacks(app)
    scrim_performance.register_callbacks(app)
    player_stats.register_callbacks(app)


def main():
    register_all_callbacks()
    port = int(os.environ.get("PORT", 8050))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None
    app.run(debug=debug, host="0.0.0.0", port=port)
