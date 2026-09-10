import os
import sqlite3
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.metrics.filters import FAMILIES

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).resolve().parents[3] / "data" / "cdl.db"))

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
)

from cdm_stats.dashboard.auth import init_auth

init_auth(app.server)

app.layout = dbc.Container([
    dbc.Navbar(
        dbc.Container(
            dbc.NavbarBrand(
                [
                    html.Img(
                        src="/assets/logos/cdm.png",
                        alt="CDM",
                        style={
                            "height": "36px",
                            "width": "36px",
                            "objectFit": "contain",
                            "marginRight": "12px",
                            "filter": "drop-shadow(0 2px 8px rgba(0, 0, 0, 0.5))",
                        },
                    ),
                    html.Span(
                        "CDM Stats",
                        style={"fontSize": "1.3rem", "fontWeight": "600"},
                    ),
                ],
                className="d-flex align-items-center",
            ),
            fluid=True,
        ),
        color="#131a2a",
        dark=True,
        className="mb-0",
    ),
    # Shared filter bar: competition families + date range, applied to every tab.
    dcc.Store(id="filter-store", data=None),
    dbc.Row([
        dbc.Col(html.Span("Events", className="filter-label"), width="auto"),
        dbc.Col(
            dbc.Checklist(
                id="filter-families",
                options=[{"label": f, "value": f} for f in FAMILIES],
                value=list(FAMILIES),
                inline=True,
                inputClassName="btn-check",
                labelClassName="btn btn-outline-light btn-sm me-1",
                labelCheckedClassName="active",
            ),
            width="auto",
        ),
        dbc.Col(html.Span("Dates", className="filter-label ms-3"), width="auto"),
        dbc.Col(
            dcc.DatePickerRange(
                id="filter-dates",
                start_date=None,
                end_date=None,
                clearable=True,
                display_format="YYYY-MM-DD",
                start_date_placeholder_text="From",
                end_date_placeholder_text="To",
            ),
            width="auto",
        ),
    ], className="align-items-center py-2 px-3 filter-bar"),
    dbc.Tabs(id="main-tabs", active_tab="team-profile", className="mt-0", children=[
        dbc.Tab(label="Team Profile", tab_id="team-profile"),
        dbc.Tab(label="Head to Head", tab_id="head-to-head"),
        dbc.Tab(label="Scrim Performance", tab_id="scrim-performance"),
        dbc.Tab(label="Elo", tab_id="elo"),
    ]),
    html.Div(id="tab-content", className="mt-3"),
], fluid=True, className="px-0")


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@app.callback(
    Output("filter-store", "data"),
    Input("filter-families", "value"),
    Input("filter-dates", "start_date"),
    Input("filter-dates", "end_date"),
)
def sync_filter(families, start, end):
    # DatePickerRange may hand back a full timestamp; keep the ISO date only.
    return {
        "families": list(families or []),
        "start": start[:10] if start else None,
        "end": end[:10] if end else None,
    }


@app.callback(Output("tab-content", "children"), Input("main-tabs", "active_tab"))
def render_tab(active_tab: str):
    from cdm_stats.dashboard.tabs import team_profile, head_to_head, elo_tracker, scrim_performance
    if active_tab == "team-profile":
        return team_profile.layout()
    elif active_tab == "head-to-head":
        return head_to_head.layout()
    elif active_tab == "scrim-performance":
        return scrim_performance.layout()
    elif active_tab == "elo":
        return elo_tracker.layout()
    return html.Div("Select a tab")


def register_all_callbacks():
    from cdm_stats.dashboard.tabs import team_profile, head_to_head, elo_tracker, scrim_performance
    team_profile.register_callbacks(app)
    head_to_head.register_callbacks(app)
    elo_tracker.register_callbacks(app)
    scrim_performance.register_callbacks(app)


def main():
    register_all_callbacks()
    port = int(os.environ.get("PORT", 8050))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None
    app.run(debug=debug, host="0.0.0.0", port=port)
