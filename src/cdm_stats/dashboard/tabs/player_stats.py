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
    conn = get_db()
    weeks = _get_available_weeks(conn, "tournament")
    conn.close()
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
                html.Div(
                    week_pills("player-week-pills", weeks),
                    id="player-week-pills-container",
                ),
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
