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
