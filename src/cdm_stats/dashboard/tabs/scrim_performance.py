import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.components.week_pills import week_pills, pill_value_to_range
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
    return scrim_map_breakdown(conn, mode=mode, week_range=week_range)


def _build_trend_data(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
) -> list[dict]:
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
    conn = get_db()
    weeks = _get_available_weeks(conn)
    conn.close()
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
                html.Div(
                    week_pills("scrim-week-pills", weeks),
                    id="scrim-week-pills-container",
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
        Output("scrim-week-pills-container", "children"),
        Input("scrim-mode-filter", "value"),
    )
    def render_scrim_week_pills(_mode):
        conn = get_db()
        weeks = _get_available_weeks(conn)
        conn.close()
        return week_pills("scrim-week-pills", weeks)

    @app.callback(
        Output("scrim-summary-cards", "children"),
        Output("scrim-map-table", "children"),
        Output("scrim-trend-chart", "figure"),
        Input("scrim-mode-filter", "value"),
        Input("scrim-map-filter", "value"),
        Input("scrim-week-pills", "value"),
    )
    def update_scrim_tab(mode, map_name, week_value):
        conn = get_db()
        mode_val = mode if mode != "All" else None
        map_val = map_name if map_name != "All" else None
        wr = pill_value_to_range(week_value)

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

        map_data = _build_map_table_data(conn, mode=mode_val, week_range=wr)
        if map_data:
            header = html.Thead(html.Tr([
                html.Th("Map"), html.Th("Mode"), html.Th("Played"),
                html.Th("W"), html.Th("L"), html.Th("Win%"),
                html.Th("Avg Margin"),
            ]))
            body_rows = []
            for d in map_data:
                win_color = COLORS["win"] if d["win_pct"] >= 60 else (COLORS["loss"] if d["win_pct"] <= 40 else COLORS["neutral"])
                margin = d["avg_margin"]
                margin_color = COLORS["win"] if margin > 0 else (COLORS["loss"] if margin < 0 else COLORS["text"])
                margin_str = f"+{margin:.0f}" if margin > 0 else f"{margin:.0f}"
                body_rows.append(html.Tr([
                    html.Td(d["map_name"]),
                    html.Td(d["mode"], style={"color": MODE_COLORS.get(d["mode"], COLORS["text"])}),
                    html.Td(str(d["played"])),
                    html.Td(str(d["wins"])),
                    html.Td(str(d["losses"])),
                    html.Td(f"{d['win_pct']:.0f}%", style={"color": win_color, "fontWeight": "600"}),
                    html.Td(margin_str, style={"color": margin_color, "fontWeight": "600"}),
                ]))
            table = dbc.Table(
                [header, html.Tbody(body_rows)],
                bordered=True, hover=True, size="sm",
                style={"backgroundColor": COLORS["card_bg"]},
            )
        else:
            table = html.P("No scrim data found.", style={"color": COLORS["muted"]})

        trend_data = _build_trend_data(conn, mode=mode_val, map_name=map_val)
        fig = _trend_figure(trend_data)

        conn.close()
        return card_row, table, fig
