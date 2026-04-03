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
    return player_summary(conn, player=player, mode=mode, week_range=week_range)


def _build_kd_trend_data(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
) -> list[dict]:
    return player_weekly_trend(conn, player=player, mode=mode)


def _build_player_map_data(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
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

        card_data = _build_player_cards_data(conn, player=player_val, mode=mode_val, week_range=wr)
        cards = []
        for i, d in enumerate(card_data):
            color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
            cards.append(dbc.Col(_player_card(d, color), width=True))
        card_row = dbc.Row(cards) if cards else html.P("No player data found.", style={"color": COLORS["muted"]})

        trend_data = _build_kd_trend_data(conn, player=player_val, mode=mode_val)
        fig = _kd_trend_figure(trend_data)

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
