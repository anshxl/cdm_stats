import sqlite3

import dash_bootstrap_components as dbc
from dash import html, dcc, callback_context, ALL
from dash.dependencies import Input, Output, State

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.components.team_badge import (
    team_badge, team_dropdown_options_rich,
)
from cdm_stats.dashboard.helpers import (
    COLORS, MODE_COLORS, LOW_SAMPLE_THRESHOLD,
    wl_color, get_all_maps,
)
from cdm_stats.metrics.avoidance import (
    pick_win_loss, defend_win_loss,
)
from cdm_stats.metrics.map_strength import map_strength
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import (
    get_team_map_wl, team_ban_rates, opponent_ban_rates, team_pick_rates,
)


# ---------------------------------------------------------------------------
# Data builders (tested directly)
# ---------------------------------------------------------------------------

def _build_map_record_data(conn: sqlite3.Connection, team_id: int, season: int = 1) -> list[dict]:
    """Build per-map W/L records enriched with pick/defend splits and Map Strength."""
    base = get_team_map_wl(conn, team_id, season=season)
    maps = get_all_maps(conn)
    map_lookup = {(m[1], m[2]): m[0] for m in maps}

    for entry in base:
        map_id = map_lookup.get((entry["map_name"], entry["mode"]))
        if map_id:
            pwl = pick_win_loss(conn, team_id, map_id, season=season)
            dwl = defend_win_loss(conn, team_id, map_id, season=season)
            ms = map_strength(conn, team_id, map_id, season=season)
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
    conn: sqlite3.Connection, team_id: int, map_id: int, season: int = 1
) -> list[dict]:
    """Build individual match results for a team on a specific map.

    Returns list of dicts with: opponent, score, pick_context, picked_by, result, match_date.
    Sorted by date descending.
    """
    team_abbr = conn.execute(
        "SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)
    ).fetchone()[0]

    rows = conn.execute(
        """SELECT m.match_date, m.team1_id, m.team2_id,
                  mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score,
                  mr.pick_context, mr.picked_by_team_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND m.season = ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date DESC""",
        (map_id, season, team_id, team_id),
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
            score = f"{pick_score}-{non_pick_score}"

        # Determine who picked — always use team abbreviation
        if picker_id == team_id:
            picked_by = team_abbr
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


def _ban_rate_span(label: str, count: int, total: int, hi_color: str) -> html.Span:
    """'Banned 6/10' — hi_color at a >=50% rate, muted otherwise."""
    if total == 0:
        return html.Span(
            f"{label} —",
            style={"color": COLORS["muted"], "fontSize": "0.8rem", "width": "120px", "display": "inline-block"},
        )
    hot = count / total >= 0.5
    return html.Span(
        f"{label} {count}/{total}",
        style={"color": hi_color if hot else COLORS["muted"],
               "fontWeight": "600" if hot else "400",
               "fontSize": "0.8rem", "width": "120px", "display": "inline-block"},
    )


def _map_strength_card(conn: sqlite3.Connection, team_id: int, records: list[dict], season: int = 1) -> dbc.Card:
    """Render the MAP STRENGTH card with expandable rows showing pick/defend splits and match history."""
    header = dbc.CardHeader(
        html.H5("Map Strength", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    own_bans = team_ban_rates(conn, team_id, season=season)
    picks = team_pick_rates(conn, team_id, season=season)
    opp_bans = opponent_ban_rates(conn, team_id, season=season)

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
                _ban_rate_span("Banned", own_bans["by_map"].get(rec["map_id"], 0),
                               own_bans["total_series"], COLORS["ban"]),
                _ban_rate_span("Picked", picks["by_map"].get(rec["map_id"], 0),
                               picks["total_series"], COLORS["your_team"]),
                _ban_rate_span("Opp banned", opp_bans["by_map"].get(rec["map_id"], 0),
                               opp_bans["total_series"], COLORS["win"]),
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

        # Expandable detail: pick/defend splits + match history
        pick_color = wl_color(rec.get("pick_wins", 0), rec.get("pick_losses", 0))
        defend_color = wl_color(rec.get("defend_wins", 0), rec.get("defend_losses", 0))

        detail_children = [
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
                style={"paddingLeft": "24px", "marginBottom": "8px"},
            ),
        ]

        # Match history rows
        if rec.get("map_id"):
            match_history = _build_map_results_detail(conn, team_id, rec["map_id"], season=season)
            if match_history:
                # Header row
                detail_children.append(
                    html.Div(
                        [
                            html.Span("Date", style={"width": "90px", "display": "inline-block", "fontWeight": "600"}),
                            html.Span("Opp", style={"width": "50px", "display": "inline-block", "fontWeight": "600"}),
                            html.Span("Result", style={"width": "40px", "display": "inline-block", "fontWeight": "600"}),
                            html.Span("Score", style={"width": "60px", "display": "inline-block", "fontWeight": "600"}),
                            html.Span("Context", style={"width": "80px", "display": "inline-block", "fontWeight": "600"}),
                            html.Span("Picked By", style={"display": "inline-block", "fontWeight": "600"}),
                        ],
                        style={
                            "paddingLeft": "24px",
                            "fontSize": "0.7rem",
                            "color": COLORS["muted"],
                            "display": "flex",
                            "borderBottom": f"1px solid {COLORS['border']}",
                            "paddingBottom": "2px",
                            "marginBottom": "2px",
                        },
                    )
                )
                for mh in match_history:
                    result_color = COLORS["win"] if mh["result"] == "W" else COLORS["loss"]
                    detail_children.append(
                        html.Div(
                            [
                                html.Span(mh["match_date"], style={"width": "90px", "display": "inline-block"}),
                                html.Span(mh["opponent"], style={"width": "50px", "display": "inline-block"}),
                                html.Span(mh["result"], style={"width": "40px", "display": "inline-block", "color": result_color, "fontWeight": "700"}),
                                html.Span(mh["score"], style={"width": "60px", "display": "inline-block"}),
                                html.Span(mh["pick_context"], style={"width": "80px", "display": "inline-block", "color": COLORS["muted"]}),
                                html.Span(mh["picked_by"], style={"display": "inline-block", "color": COLORS["muted"]}),
                            ],
                            style={
                                "paddingLeft": "24px",
                                "fontSize": "0.75rem",
                                "color": COLORS["text"],
                                "display": "flex",
                                "padding": "2px 12px 2px 24px",
                            },
                        )
                    )

        detail = html.Div(
            detail_children,
            id={"type": "tp-expand", "index": f"{rec['map_name']}-{rec['mode']}"},
            style={"display": "none", "padding": "4px 12px 8px", "backgroundColor": "#0d1322"},
        )

        rows.append(html.Div([main_row, detail]))

    # Maps with ban activity but no games played never show up in the W/L
    # records — yet "always banned" is the loudest signal here. Give them a
    # slim, non-expandable row at the bottom.
    seen_ids = {rec.get("map_id") for rec in records}
    for map_id, map_name, mode in get_all_maps(conn):
        ob = own_bans["by_map"].get(map_id, 0)
        pb = opp_bans["by_map"].get(map_id, 0)
        if map_id in seen_ids or (ob == 0 and pb == 0):
            continue
        rows.append(html.Div(
            [
                html.Span(map_name, style={"fontWeight": "600", "width": "140px", "display": "inline-block"}),
                html.Span(mode, style={"color": MODE_COLORS.get(mode, COLORS["text"]), "width": "80px", "display": "inline-block", "fontSize": "0.85rem"}),
                html.Span("N/A", style={"color": COLORS["muted"], "fontWeight": "700", "width": "80px", "display": "inline-block", "fontSize": "1.1rem"}),
                html.Span("0-0", style={"color": COLORS["muted"], "fontWeight": "600", "width": "60px", "display": "inline-block"}),
                _ban_rate_span("Banned", ob, own_bans["total_series"], COLORS["ban"]),
                _ban_rate_span("Picked", picks["by_map"].get(map_id, 0),
                               picks["total_series"], COLORS["your_team"]),
                _ban_rate_span("Opp banned", pb, opp_bans["total_series"], COLORS["win"]),
            ],
            style={
                "padding": "8px 12px",
                "borderBottom": f"1px solid {COLORS['border']}",
                "display": "flex",
                "alignItems": "center",
            },
        ))

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


# ---------------------------------------------------------------------------
# Layout and callbacks
# ---------------------------------------------------------------------------

def layout(season: int = 1):
    """Return the team profile tab layout with team selector and content area."""
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.Label("Select Team", style={"color": COLORS["text"], "fontWeight": "600"}),
                dcc.Dropdown(
                    id="tp-team-select",
                    options=[],
                    placeholder="Choose a team...",
                    clearable=False,
                    optionHeight=36,
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
        Output("tp-team-select", "value"),
        Input("tp-team-select", "id"),  # fires once on load
    )
    def populate_teams(_):
        conn = get_db()
        try:
            options = team_dropdown_options_rich(conn)
            gl_id = next(
                (opt["value"] for opt in options if opt.get("search") == "GL"),
                None,
            )
            return options, gl_id
        finally:
            conn.close()

    # Update content when team is selected
    @app.callback(
        Output("tp-content", "children"),
        Input("tp-team-select", "value"),
        Input("season-store", "data"),
        prevent_initial_call=True,
    )
    def update_content(team_id, season):
        if not team_id:
            return html.Div("Select a team to view profile", style={"color": COLORS["muted"], "padding": "20px"})

        team_id = int(team_id)
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT abbreviation, team_name FROM teams WHERE team_id = ?",
                (team_id,),
            ).fetchone()
            abbr, full_name = row[0], row[1]

            records = _build_map_record_data(conn, team_id, season=season)

            elo = get_current_elo(conn, team_id, season=season)
            low_conf = is_low_confidence(conn, team_id, season=season)
            header = html.Div(
                [
                    team_badge(abbr, COLORS["your_team"], size=56, font_size="1.6rem"),
                    html.Span(
                        full_name,
                        style={
                            "color": COLORS["muted"],
                            "fontSize": "0.95rem",
                            "marginLeft": "14px",
                            "letterSpacing": "0.04em",
                            "textTransform": "uppercase",
                        },
                    ),
                    html.Span(
                        f"Elo - {elo:.0f}",
                        style={
                            "color": COLORS["your_team"],
                            "fontWeight": "700",
                            "fontSize": "1.1rem",
                            "marginLeft": "14px",
                        },
                    ),
                ] + ([html.Span(
                    "LOW CONFIDENCE",
                    style={"color": COLORS["neutral"], "fontSize": "0.7rem", "marginLeft": "8px"},
                )] if low_conf else []),
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "padding": "8px 12px 16px",
                },
            )

            return html.Div([
                header,
                dbc.Row([
                    dbc.Col(_map_strength_card(conn, team_id, records, season=season), md=8),
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
