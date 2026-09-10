"""GL-only player block rendered inside the Team Profile tab.

Summary cards, K/D trend by match date, operator efficiency (S2+ only; blank
for S1), and recent series with per-map player tables.
"""
import sqlite3
from datetime import date as _date

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.helpers import COLORS, YOUR_TEAM
from cdm_stats.db import queries_ops, queries_tournament_player
from cdm_stats.db.queries import MODES
from cdm_stats.metrics.filters import MatchFilter

PLAYER_COLORS = [
    "#7dd3fc",  # sky
    "#fb923c",  # orange
    "#5eead4",  # mint
    "#f472b6",  # rose
    "#facc15",  # gold
]


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
                f"{data['op_kills'] / data['op_pulls']:.2f} Op K/Pull"
                if data.get("op_pulls") else "— Op K/Pull",
                style={"color": COLORS["text"], "fontSize": "0.95rem", "marginTop": "4px"},
            ),
            html.Small(
                f"{data['kills']}K / {data['deaths']}D / {data['assists']}A  ·  {data['games']} maps",
                style={"color": COLORS["muted"], "fontSize": "0.75rem"},
            ),
        ]),
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
    )


def _line_layout(fig: go.Figure, y_title: str) -> go.Figure:
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 50, "r": 20, "t": 30, "b": 50},
        height=400,
        yaxis={"title": y_title, "gridcolor": COLORS["border"]},
        xaxis={"title": "Date", "type": "date", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def _kd_trend_figure(trend_data: list[dict]) -> go.Figure:
    fig = go.Figure()
    players = sorted(set(d["player_name"] for d in trend_data))
    for i, p in enumerate(players):
        pdata = [d for d in trend_data if d["player_name"] == p]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        fig.add_trace(go.Scatter(
            x=[d["match_date"] for d in pdata],
            y=[d["kd"] for d in pdata],
            mode="lines+markers",
            name=p,
            marker={"size": 6, "color": color},
            line={"width": 2, "color": color},
            hovertemplate=f"{p}: %{{y:.2f}} K/D<br>%{{x}}<extra></extra>",
        ))
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.4,
                  annotation_text="1.0 K/D", annotation_position="bottom right")
    return _line_layout(fig, "K/D")


def _ops_trend_figure(trend_data: list[dict]) -> go.Figure:
    """Per-day operator kills per pull, one line per player. A day with no
    pulls has no rate, so the line breaks rather than plotting a zero."""
    fig = go.Figure()
    players = sorted(set(d["player_name"] for d in trend_data))
    for i, p in enumerate(players):
        pdata = [d for d in trend_data if d["player_name"] == p]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        fig.add_trace(go.Scatter(
            x=[d["match_date"] for d in pdata],
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
    return _line_layout(fig, "Op Kills per Pull")


def _ops_section(conn: sqlite3.Connection, player: str | None, mode: str | None, f: MatchFilter):
    """Operator efficiency block. Absent entirely when there's nothing to show
    (Season 1 has no footage)."""
    trend_data = queries_ops.ops_player_weekly_trend(conn, player=player, mode=mode, f=f)
    if not trend_data:
        return None

    children = [
        html.H5("Operator Efficiency", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
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


def _ordinal(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"


def _series_title(s: dict) -> str:
    """'vs. Q9 (W 3-1, Aug 8th)'"""
    d = _date.fromisoformat(s["match_date"])
    wl = "W" if s["our_maps"] > s["their_maps"] else "L"
    return (f"vs. {s['opponent']} ({wl} {s['our_maps']}-{s['their_maps']}, "
            f"{d.strftime('%b')} {_ordinal(d.day)})")


def _stat_cell(value_text: str, color: str, raw_text: str | None = None) -> html.Td:
    children = [html.Div(value_text, style={"color": color, "fontWeight": "600"})]
    if raw_text:
        children.append(html.Small(raw_text, style={"color": COLORS["muted"]}))
    return html.Td(children)


def _missing_cell() -> html.Td:
    # Scoreboard and footage are ingested independently, so either side can
    # be missing. An em dash means "not ingested"; a 0 would misreport it.
    return html.Td("—", style={"color": COLORS["muted"]})


def _map_table(players: list[dict]) -> dbc.Table:
    header = html.Thead(html.Tr([
        html.Th("Player"), html.Th("K/D"), html.Th("Pos Eng %"), html.Th("Op K/Pull"),
    ]))
    body_rows = []
    for p in players:
        if p["kills"] is None:
            kd_cell, eng_cell = _missing_cell(), _missing_cell()
        else:
            k, d, a = p["kills"], p["deaths"], p["assists"]
            if d == 0:
                kd_cell = _stat_cell("∞", COLORS["win"], f"{k}/{d}/{a}")
            else:
                kd = k / d
                kd_cell = _stat_cell(
                    f"{kd:.2f}",
                    COLORS["win"] if kd >= 1 else COLORS["loss"],
                    f"{k}/{d}/{a}",
                )
            denom = k + d + a
            if denom == 0:
                eng_cell = _missing_cell()
            else:
                pct = (k + a) / denom * 100
                eng_color = (COLORS["loss"] if pct < 50
                             else COLORS["neutral"] if pct < 60
                             else COLORS["win"])
                eng_cell = _stat_cell(f"{pct:.1f}%", eng_color)
        if p["op_kills"] is None or not p["op_pulls"]:
            op_cell = _missing_cell()
        else:
            rate = p["op_kills"] / p["op_pulls"]
            op_color = (COLORS["loss"] if rate < 1
                        else COLORS["neutral"] if rate < 2
                        else COLORS["win"])
            op_cell = _stat_cell(f"{rate:.2f}", op_color, f"{p['op_kills']}/{p['op_pulls']}")
        body_rows.append(html.Tr([
            html.Td(p["player_name"], style={"fontWeight": "600"}),
            kd_cell, eng_cell, op_cell,
        ]))
    return dbc.Table(
        [header, html.Tbody(body_rows)],
        bordered=True, hover=True, size="sm",
        style={"backgroundColor": COLORS["card_bg"], "marginBottom": "0"},
    )


def _series_body(s: dict) -> list:
    children = []
    for m in s["maps"]:
        wl = "W" if m["won"] else "L"
        children.append(html.H6(
            f"{m['map_name']} ({wl} {m['our_score']}-{m['their_score']})",
            style={"color": COLORS["text"]}, className="mt-2 mb-1",
        ))
        children.append(_map_table(m["players"]))
    return children


def _recent_series_block(conn, player, mode, f: MatchFilter, opponent_value: str = "last10"):
    """Recent series as expandable panels, newest first and open by default.

    `opponent_value`: "last10" (newest 10 series), "all" (every series), or a
    team abbreviation (every series vs. that team).
    """
    if opponent_value == "last10":
        limit, opponent, title_text = 10, None, "Last 10 Series"
    elif opponent_value == "all":
        limit, opponent, title_text = None, None, "All Series"
    else:
        limit, opponent, title_text = None, opponent_value, f"vs. {opponent_value}"
    series = queries_tournament_player.recent_series_stats(
        conn, YOUR_TEAM, player=player, mode=mode, f=f, limit=limit, opponent=opponent,
    )
    title = html.H5(title_text, style={"color": COLORS["text"]}, className="mt-4 mb-2")
    if not series:
        return html.Div([title, html.P("No player data found.", style={"color": COLORS["muted"]})])

    items = [
        dbc.AccordionItem(_series_body(s), title=_series_title(s), item_id=f"series-{s['match_id']}")
        for s in series
    ]
    return html.Div([title, dbc.Accordion(items, active_item=f"series-{series[0]['match_id']}")])


def _get_available_players(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT player_name FROM tournament_player_stats ORDER BY player_name"
    ).fetchall()
    return [r[0] for r in rows]


def _get_opponents(conn: sqlite3.Connection) -> list[str]:
    """Opponent abbreviations our team has ever played."""
    rows = conn.execute(
        """SELECT DISTINCT CASE WHEN t1.abbreviation = ? THEN t2.abbreviation
                                ELSE t1.abbreviation END AS opp
           FROM matches m
           JOIN teams t1 ON m.team1_id = t1.team_id
           JOIN teams t2 ON m.team2_id = t2.team_id
           WHERE ? IN (t1.abbreviation, t2.abbreviation)
           ORDER BY opp""",
        (YOUR_TEAM, YOUR_TEAM),
    ).fetchall()
    return [r[0] for r in rows]


def _dropdown(dd_id: str, options: list[dict], value):
    return dcc.Dropdown(
        id=dd_id, options=options, value=value, clearable=False,
        style={"backgroundColor": COLORS["card_bg"]},
    )


def layout(conn: sqlite3.Connection) -> html.Div:
    """Player block with its own player/mode/opponent controls. The date and
    family scope come from the shared filter bar."""
    all_opt = [{"label": "All", "value": "All"}]
    players = _get_available_players(conn)
    opponents = _get_opponents(conn)
    return html.Div([
        html.H4("Players", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        dbc.Row([
            dbc.Col([html.Label("Player", style={"color": COLORS["text"]}),
                     _dropdown("player-filter", all_opt + [{"label": p, "value": p} for p in players], "All")],
                    width=3),
            dbc.Col([html.Label("Mode", style={"color": COLORS["text"]}),
                     _dropdown("player-mode-filter", all_opt + [{"label": m, "value": m} for m in MODES], "All")],
                    width=3),
            dbc.Col([html.Label("Series", style={"color": COLORS["text"]}),
                     _dropdown("player-opponent-filter",
                               [{"label": "Last 10", "value": "last10"}, {"label": "All", "value": "all"}]
                               + [{"label": o, "value": o} for o in opponents], "last10")],
                    width=3),
        ], className="mb-3"),
        html.Div(id="player-summary-cards"),
        html.H5("K/D Trend", style={"color": COLORS["text"]}, className="mt-4 mb-2"),
        dcc.Graph(id="player-kd-chart"),
        html.Div(id="player-ops-section"),
        html.Div(id="player-recent-maps"),
    ])


def register_callbacks(app):
    @app.callback(
        Output("player-summary-cards", "children"),
        Output("player-kd-chart", "figure"),
        Output("player-ops-section", "children"),
        Output("player-recent-maps", "children"),
        Input("player-filter", "value"),
        Input("player-mode-filter", "value"),
        Input("player-opponent-filter", "value"),
        Input("filter-store", "data"),
    )
    def update_player_section(player, mode, opponent_value, filter_data):
        from cdm_stats.dashboard.app import get_db
        f = MatchFilter.from_dict(filter_data)
        conn = get_db()
        try:
            player_val = player if player != "All" else None
            mode_val = mode if mode != "All" else None

            card_data = queries_tournament_player.player_summary(
                conn, player=player_val, mode=mode_val, f=f,
            )
            if not card_data:
                card_row = dbc.Alert("No player data in this window.", color="info")
            else:
                cards = [
                    dbc.Col(_player_card(d, PLAYER_COLORS[i % len(PLAYER_COLORS)]), width=True)
                    for i, d in enumerate(card_data)
                ]
                card_row = dbc.Row(cards)

            trend_data = queries_tournament_player.player_weekly_trend(
                conn, player=player_val, mode=mode_val, f=f,
            )
            fig = _kd_trend_figure(trend_data)
            ops_section = _ops_section(conn, player_val, mode_val, f)
            recent = _recent_series_block(conn, player_val, mode_val, f, opponent_value or "last10")
            return card_row, fig, ops_section, recent
        finally:
            conn.close()
