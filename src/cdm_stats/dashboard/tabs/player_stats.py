import sqlite3
from datetime import date as _date

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.components.week_pills import week_pills, pill_value_to_range
from cdm_stats.dashboard.helpers import COLORS, MODE_COLORS, YOUR_TEAM
from cdm_stats.db import queries_ops, queries_scrim, queries_tournament_player
from cdm_stats.db.queries import MODES, MODE_ORDER

PLAYER_COLORS = [
    "#7dd3fc",  # sky
    "#fb923c",  # orange
    "#5eead4",  # mint
    "#f472b6",  # rose
    "#facc15",  # gold
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
    season: int = 1,
) -> list[dict]:
    return _queries_for(source).player_summary(
        conn, player=player, mode=mode, week_range=week_range, season=season,
    )


def _build_kd_trend_data(
    conn: sqlite3.Connection,
    source: str = "tournament",
    player: str | None = None,
    mode: str | None = None,
    season: int = 1,
) -> list[dict]:
    return _queries_for(source).player_weekly_trend(
        conn, player=player, mode=mode, season=season,
    )


def _build_player_map_data(
    conn: sqlite3.Connection,
    source: str = "tournament",
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
) -> list[dict]:
    return _queries_for(source).player_map_breakdown(
        conn, player=player, mode=mode, week_range=week_range, season=season,
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


def _ops_trend_figure(trend_data: list[dict]) -> go.Figure:
    """Per-week operator kills per pull, one line per player.

    A week in which a player never pulled has no rate, so it's left out of that
    player's series entirely and the line breaks — better than plotting a zero
    that reads as "pulled and whiffed".
    """
    fig = go.Figure()
    players = sorted(set(d["player_name"] for d in trend_data))
    for i, p in enumerate(players):
        pdata = [d for d in trend_data if d["player_name"] == p]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        fig.add_trace(go.Scatter(
            x=[f"W{d['week']}" for d in pdata],
            y=[d["kills_per_pull"] for d in pdata],
            mode="lines+markers",
            name=p,
            connectgaps=False,
            marker={"size": 6, "color": color},
            line={"width": 2, "color": color},
            customdata=[(d["op_kills"], d["op_pulls"], d["maps"]) for d in pdata],
            hovertemplate=(
                f"{p}: %{{y:.2f}} K/pull<br>"
                "%{customdata[0]} kills / %{customdata[1]} pulls"
                " over %{customdata[2]} maps<extra></extra>"
            ),
        ))
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.4,
                  annotation_text="1.00 K/pull", annotation_position="bottom right")
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 50, "r": 20, "t": 30, "b": 50},
        height=400,
        yaxis={"title": "Op Kills per Pull", "gridcolor": COLORS["border"]},
        xaxis={"title": "Week", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def _ops_section(
    conn: sqlite3.Connection,
    source: str,
    player: str | None,
    mode: str | None,
    season: int,
):
    """Operator efficiency block. Absent entirely when there's nothing to show."""
    if source != "tournament":
        return None
    trend_data = queries_ops.ops_player_weekly_trend(
        conn, player=player, mode=mode, season=season,
    )
    if not trend_data:
        return None

    children = [
        html.H5("Operator Efficiency", style={"color": COLORS["text"]},
                className="mt-4 mb-2"),
        dcc.Graph(figure=_ops_trend_figure(trend_data)),
    ]
    maps_by_player: dict[str, int] = {}
    for d in trend_data:
        maps_by_player[d["player_name"]] = maps_by_player.get(d["player_name"], 0) + d["maps"]
    thin = [p for p, n in maps_by_player.items() if n < 4]
    if thin:
        children.append(html.Small(
            f"Under 4 maps of footage — directional only: {', '.join(sorted(thin))}",
            style={"color": COLORS["muted"]},
        ))
    return html.Div(children)


def _recent_map_title(m: dict) -> str:
    """'Standoff: vs. Wolves (July 23)'"""
    d = _date.fromisoformat(m["match_date"])
    return f"{m['map_name']}: vs. {m['opponent']} ({d.strftime('%B')} {d.day})"


def _recent_map_table(players: list[dict]) -> dbc.Table:
    header = html.Thead(html.Tr([
        html.Th("Player"), html.Th("Kills"), html.Th("Deaths"), html.Th("Assists"),
        html.Th("Op Kills"), html.Th("Op Pulls"),
    ]))
    body_rows = []
    for p in players:
        # Scoreboard and footage are ingested independently, so either side can
        # be missing. An em dash means "not ingested yet"; a 0 would misreport
        # it as "played and did nothing".
        stats = [p["kills"], p["deaths"], p["assists"], p["op_kills"], p["op_pulls"]]
        body_rows.append(html.Tr([
            html.Td(p["player_name"], style={"fontWeight": "600"}),
            *[
                html.Td(str(v) if v is not None else "—",
                        style={} if v is not None else {"color": COLORS["muted"]})
                for v in stats
            ],
        ]))
    return dbc.Table(
        [header, html.Tbody(body_rows)],
        bordered=True, hover=True, size="sm",
        style={"backgroundColor": COLORS["card_bg"], "marginBottom": "0"},
    )


def _recent_maps_block(
    conn: sqlite3.Connection,
    player: str | None,
    mode: str | None,
    week_range: tuple[int, int] | None,
    season: int,
):
    """Last 5 maps as expandable panels, newest first and open by default.

    Replaces the per-map aggregate for tournament play, where a map recurs only
    1-6 times a season and an average over that is mostly noise. Scrims keep the
    aggregate — they run 8-20 games per map, so the mean there means something.
    """
    maps = queries_tournament_player.recent_map_stats(
        conn, YOUR_TEAM, player=player, mode=mode, week_range=week_range, season=season,
    )
    title = html.H5("Last 5 Maps", style={"color": COLORS["text"]}, className="mt-4 mb-2")
    if not maps:
        return html.Div([
            title,
            html.P("No player data found.", style={"color": COLORS["muted"]}),
        ])

    items = [
        dbc.AccordionItem(
            _recent_map_table(m["players"]),
            title=_recent_map_title(m),
            item_id=f"map-{m['result_id']}",
        )
        for m in maps
    ]
    return html.Div([
        title,
        dbc.Accordion(items, active_item=f"map-{maps[0]['result_id']}"),
    ])


def _get_available_players(conn: sqlite3.Connection, source: str, season: int = 1) -> list[str]:
    if source == "tournament":
        rows = conn.execute(
            """SELECT DISTINCT tp.player_name
               FROM tournament_player_stats tp
               JOIN map_results mr ON tp.result_id = mr.result_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE m.season = ?
               ORDER BY tp.player_name""",
            (season,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT DISTINCT sp.player_name
               FROM scrim_player_stats sp
               JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
               WHERE sm.season = ?
               ORDER BY sp.player_name""",
            (season,),
        ).fetchall()
    return [r[0] for r in rows]


def _get_available_weeks(conn: sqlite3.Connection, source: str, season: int = 1) -> list[int]:
    if source == "tournament":
        rows = conn.execute(
            """SELECT DISTINCT tp.week
               FROM tournament_player_stats tp
               JOIN map_results mr ON tp.result_id = mr.result_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE m.season = ?
               ORDER BY tp.week""",
            (season,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT week FROM scrim_maps WHERE season = ? ORDER BY week",
            (season,),
        ).fetchall()
    return [r[0] for r in rows]


def layout(season: int = 1):
    conn = get_db()
    weeks = _get_available_weeks(conn, "tournament", season)
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
                        + [{"label": m, "value": m} for m in MODES],
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
        html.Div(id="player-ops-section"),
        html.Div(id="player-map-table"),
    ], fluid=True)



def _mode_legend() -> html.Div:
    return html.Div(
        [
            html.Span(
                mode,
                style={
                    "color": MODE_COLORS[mode],
                    "fontWeight": "600",
                    "marginRight": "14px",
                },
            )
            for mode in MODES
        ],
        style={"fontSize": "0.8rem", "marginBottom": "6px", "color": COLORS["muted"]},
    )


def register_callbacks(app):
    @app.callback(
        Output("player-filter", "options"),
        Input("player-source-filter", "value"),
        Input("season-store", "data"),
    )
    def populate_players(source, season):
        conn = get_db()
        players = _get_available_players(conn, source, season)
        conn.close()
        return [{"label": "All", "value": "All"}] + [{"label": p, "value": p} for p in players]

    @app.callback(
        Output("player-week-pills-container", "children"),
        Input("player-source-filter", "value"),
        Input("season-store", "data"),
    )
    def render_player_week_pills(source, season):
        conn = get_db()
        weeks = _get_available_weeks(conn, source, season)
        conn.close()
        return week_pills("player-week-pills", weeks)

    @app.callback(
        Output("player-summary-cards", "children"),
        Output("player-kd-chart", "figure"),
        Output("player-ops-section", "children"),
        Output("player-map-table", "children"),
        Input("player-source-filter", "value"),
        Input("player-filter", "value"),
        Input("player-mode-filter", "value"),
        Input("player-week-pills", "value"),
        Input("season-store", "data"),
    )
    def update_player_tab(source, player, mode, week_value, season):
        conn = get_db()
        player_val = player if player != "All" else None
        mode_val = mode if mode != "All" else None
        wr = pill_value_to_range(week_value)

        card_data = _build_player_cards_data(
            conn, source=source, player=player_val, mode=mode_val, week_range=wr, season=season,
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
            conn, source=source, player=player_val, mode=mode_val, season=season,
        )
        fig = _kd_trend_figure(trend_data)

        ops_section = _ops_section(conn, source, player_val, mode_val, season)

        if source == "tournament":
            recent = _recent_maps_block(conn, player_val, mode_val, wr, season)
            conn.close()
            return card_row, fig, ops_section, recent

        map_data = _build_player_map_data(
            conn, source=source, player=player_val, mode=mode_val, week_range=wr, season=season,
        )
        title_suffix = player_val if player_val else "Team Aggregate"
        title = html.H5(
            f"Per-Map Breakdown — {title_suffix}",
            style={"color": COLORS["text"]},
            className="mt-4 mb-2",
        )
        if map_data:
            sorted_map_data = sorted(
                map_data,
                key=lambda d: (MODE_ORDER.get(d["mode"], 99), d["map_name"]),
            )
            header = html.Thead(html.Tr([
                html.Th("Map"), html.Th("Games"),
                html.Th("Avg K/D"), html.Th("Avg Kills"), html.Th("Avg Deaths"),
                html.Th("Avg Assists"), html.Th("Pos Eng %"),
            ]))
            body_rows = []
            for d in sorted_map_data:
                kd_color = COLORS["win"] if d["avg_kd"] >= 1.0 else COLORS["loss"]
                map_color = MODE_COLORS.get(d["mode"], COLORS["text"])
                body_rows.append(html.Tr([
                    html.Td(d["map_name"], style={"color": map_color, "fontWeight": "600"}),
                    html.Td(str(d["games"])),
                    html.Td(f"{d['avg_kd']:.2f}", style={"color": kd_color, "fontWeight": "600"}),
                    html.Td(f"{d['avg_kills']:.1f}"),
                    html.Td(f"{d['avg_deaths']:.1f}"),
                    html.Td(f"{d['avg_assists']:.1f}"),
                    html.Td(f"{d['avg_pos_eng_pct']:.1f}%"),
                ]))
            table_body = dbc.Table(
                [header, html.Tbody(body_rows)],
                bordered=True, hover=True, size="sm",
                style={"backgroundColor": COLORS["card_bg"]},
            )
            table = html.Div([title, _mode_legend(), table_body])
        else:
            table = html.Div([
                title,
                html.P("No player data found.", style={"color": COLORS["muted"]}),
            ])

        conn.close()
        return card_row, fig, ops_section, table
