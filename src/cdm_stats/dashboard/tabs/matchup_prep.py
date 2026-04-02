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
