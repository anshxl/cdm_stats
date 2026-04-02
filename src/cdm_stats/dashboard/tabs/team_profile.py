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
    pick_win_loss, defend_win_loss, avoidance_index, target_index,
)
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.db.queries import get_team_map_wl, get_team_ban_summary


# ---------------------------------------------------------------------------
# Data builders (tested directly)
# ---------------------------------------------------------------------------

def _build_map_record_data(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    """Build per-map W/L records enriched with pick/defend splits."""
    base = get_team_map_wl(conn, team_id)
    maps = get_all_maps(conn)
    map_lookup = {(m[1], m[2]): m[0] for m in maps}

    for entry in base:
        map_id = map_lookup.get((entry["map_name"], entry["mode"]))
        if map_id:
            pwl = pick_win_loss(conn, team_id, map_id)
            dwl = defend_win_loss(conn, team_id, map_id)
            entry["pick_wins"] = pwl["wins"]
            entry["pick_losses"] = pwl["losses"]
            entry["defend_wins"] = dwl["wins"]
            entry["defend_losses"] = dwl["losses"]
        else:
            entry["pick_wins"] = entry["pick_losses"] = 0
            entry["defend_wins"] = entry["defend_losses"] = 0
    return base


def _build_avoidance_target_data(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    """Build avoidance and target index data for every map."""
    maps = get_all_maps(conn)
    results = []
    for map_id, map_name, mode in maps:
        avoid = avoidance_index(conn, team_id, map_id)
        tgt = target_index(conn, team_id, map_id)
        results.append({
            "map_id": map_id,
            "map_name": map_name,
            "mode": mode,
            "avoid_ratio": avoid["ratio"],
            "avoid_n": avoid["opportunities"],
            "target_ratio": tgt["ratio"],
            "target_n": tgt["opportunities"],
        })
    return results


# ---------------------------------------------------------------------------
# UI card builders
# ---------------------------------------------------------------------------

def _map_record_card(records: list[dict]) -> dbc.Card:
    """Render the MAP RECORD card with expandable rows for pick/defend splits."""
    header = dbc.CardHeader(
        html.H5("Map Record", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    rows = []
    for rec in records:
        total = rec["wins"] + rec["losses"]
        if total == 0:
            continue
        win_rate = rec["wins"] / total if total else 0
        color = wl_color(rec["wins"], rec["losses"])
        mode_color = MODE_COLORS.get(rec["mode"], COLORS["text"])

        # Main row (clickable)
        main_row = html.Div(
            [
                html.Span(rec["map_name"], style={"fontWeight": "600", "width": "140px", "display": "inline-block"}),
                html.Span(rec["mode"], style={"color": mode_color, "width": "80px", "display": "inline-block", "fontSize": "0.85rem"}),
                html.Span(
                    f"{rec['wins']}-{rec['losses']}",
                    style={"color": color, "fontWeight": "600", "width": "60px", "display": "inline-block"},
                ),
                html.Span(
                    f"({win_rate:.0%})",
                    style={"color": color, "fontSize": "0.85rem"},
                ),
            ],
            id={"type": "tp-map-row", "index": f"{rec['map_name']}-{rec['mode']}"},
            style={
                "cursor": "pointer",
                "padding": "8px 12px",
                "borderBottom": f"1px solid {COLORS['border']}",
                "display": "flex",
                "alignItems": "center",
            },
        )

        # Expandable detail
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

    body = dbc.CardBody(rows, style={"padding": "0"})
    return dbc.Card(
        [header, body],
        style={"backgroundColor": COLORS["card_bg"], "border": f"1px solid {COLORS['border']}"},
        className="mb-3",
    )


def _avoidance_target_card(data: list[dict]) -> dbc.Card:
    """Render AVOIDANCE & TARGET INDEX card with horizontal bar indicators."""
    header = dbc.CardHeader(
        html.H5("Avoidance & Target Index", className="mb-0", style={"color": COLORS["text"]}),
        style={"backgroundColor": COLORS["card_bg"], "borderBottom": f"1px solid {COLORS['border']}"},
    )

    rows = []
    for d in data:
        # Skip maps with zero opportunities on both sides
        if d["avoid_n"] == 0 and d["target_n"] == 0:
            continue

        mode_color = MODE_COLORS.get(d["mode"], COLORS["text"])
        low_avoid = d["avoid_n"] < LOW_SAMPLE_THRESHOLD
        low_target = d["target_n"] < LOW_SAMPLE_THRESHOLD

        avoid_pct = d["avoid_ratio"] * 100
        target_pct = d["target_ratio"] * 100

        avoid_label = f"{avoid_pct:.0f}% (n={d['avoid_n']})"
        target_label = f"{target_pct:.0f}% (n={d['target_n']})"

        if low_avoid:
            avoid_label += " *"
        if low_target:
            target_label += " *"

        row = html.Div(
            [
                html.Div(
                    [
                        html.Span(d["map_name"], style={"fontWeight": "600", "width": "120px", "display": "inline-block"}),
                        html.Span(d["mode"], style={"color": mode_color, "fontSize": "0.8rem", "width": "70px", "display": "inline-block"}),
                    ],
                    style={"display": "flex", "alignItems": "center", "minWidth": "200px"},
                ),
                html.Div(
                    [
                        html.Div("Avoid", style={"fontSize": "0.7rem", "color": COLORS["muted"], "marginBottom": "2px"}),
                        html.Div(
                            html.Div(
                                style={
                                    "width": f"{min(avoid_pct, 100)}%",
                                    "height": "8px",
                                    "backgroundColor": COLORS["ban"] if avoid_pct > 50 else COLORS["neutral"],
                                    "borderRadius": "4px",
                                },
                            ),
                            style={"width": "100px", "height": "8px", "backgroundColor": "#2a2a4a", "borderRadius": "4px"},
                        ),
                        html.Span(
                            avoid_label,
                            style={"fontSize": "0.75rem", "color": COLORS["muted"] if low_avoid else COLORS["text"], "marginLeft": "6px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center", "gap": "4px", "flex": "1"},
                ),
                html.Div(
                    [
                        html.Div("Target", style={"fontSize": "0.7rem", "color": COLORS["muted"], "marginBottom": "2px"}),
                        html.Div(
                            html.Div(
                                style={
                                    "width": f"{min(target_pct, 100)}%",
                                    "height": "8px",
                                    "backgroundColor": COLORS["loss"] if target_pct > 50 else COLORS["neutral"],
                                    "borderRadius": "4px",
                                },
                            ),
                            style={"width": "100px", "height": "8px", "backgroundColor": "#2a2a4a", "borderRadius": "4px"},
                        ),
                        html.Span(
                            target_label,
                            style={"fontSize": "0.75rem", "color": COLORS["muted"] if low_target else COLORS["text"], "marginLeft": "6px"},
                        ),
                    ],
                    style={"display": "flex", "alignItems": "center", "gap": "4px", "flex": "1"},
                ),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "padding": "8px 12px",
                "borderBottom": f"1px solid {COLORS['border']}",
                "gap": "12px",
            },
        )
        rows.append(row)

    if not rows:
        rows = [html.Div("No avoidance/target data", style={"color": COLORS["muted"], "padding": "12px"})]

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
            avoid_target = _build_avoidance_target_data(conn, team_id)
            ban_data = get_team_ban_summary(conn, team_id)

            return html.Div([
                dbc.Row([
                    dbc.Col(_map_record_card(records), md=6),
                    dbc.Col([
                        _avoidance_target_card(avoid_target),
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

        # Find which row was clicked
        triggered_id = ctx.triggered[0]["prop_id"]
        new_styles = []
        for i, style in enumerate(styles):
            # Check if this is the one that was clicked
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
