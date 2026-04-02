import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, dcc, html

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import (
    COLORS,
    get_all_maps,
    get_all_teams,
)
from cdm_stats.metrics.avoidance import pick_win_loss, defend_win_loss
from cdm_stats.metrics.map_strength import map_strength


def _build_matrix_data(
    conn: sqlite3.Connection, mode_filter: str | None = None
) -> tuple[list[tuple[int, str]], list[tuple[int, str, str]], dict]:
    """Build the data structures for the map matrix heatmap.

    Returns:
        teams: list of (team_id, abbreviation)
        maps: list of (map_id, map_name, mode)
        matrix: dict of (team_id, map_id) -> cell data dict
    """
    teams = get_all_teams(conn)
    maps = get_all_maps(conn)

    if mode_filter and mode_filter != "All":
        maps = [(mid, name, mode) for mid, name, mode in maps if mode == mode_filter]

    matrix: dict[tuple[int, int], dict] = {}

    for team_id, abbr in teams:
        for map_id, map_name, mode in maps:
            pw = pick_win_loss(conn, team_id, map_id)
            dw = defend_win_loss(conn, team_id, map_id)
            ms = map_strength(conn, team_id, map_id)

            wins = pw["wins"] + dw["wins"]
            losses = pw["losses"] + dw["losses"]

            matrix[(team_id, map_id)] = {
                "wins": wins,
                "losses": losses,
                "pick_wl": pw,
                "defend_wl": dw,
                "strength": ms,
            }

    return teams, maps, matrix


def _build_heatmap_figure(
    teams: list[tuple[int, str]],
    maps: list[tuple[int, str, str]],
    matrix: dict,
) -> go.Figure:
    """Build a Plotly heatmap figure from the matrix data."""
    x_labels = [f"{name} ({mode})" for _, name, mode in maps]
    y_labels = [abbr for _, abbr in teams]

    z_values: list[list[float | None]] = []
    text_values: list[list[str]] = []
    hover_texts: list[list[str]] = []

    for team_id, abbr in teams:
        z_row: list[float | None] = []
        text_row: list[str] = []
        hover_row: list[str] = []

        for map_id, map_name, mode in maps:
            cell = matrix.get((team_id, map_id))
            if cell is None:
                z_row.append(None)
                text_row.append("")
                hover_row.append("")
                continue

            wins = cell["wins"]
            losses = cell["losses"]
            ms = cell["strength"]

            # Use Map Strength for heatmap color
            z_row.append(ms["rating"])
            text_row.append(f"{wins}-{losses}")

            # Build hover text
            pw = cell["pick_wl"]
            dw = cell["defend_wl"]

            strength_pct = f"{ms['rating']:.0%}" if ms["rating"] is not None else "N/A"
            low_sample = "\u26a0\ufe0f Low sample" if ms["low_confidence"] else ""

            hover = (
                f"<b>{abbr}</b> on <b>{map_name}</b> ({mode})<br>"
                f"Map Strength: {strength_pct} (w={ms['weighted_sample']:.1f})<br>"
                f"Overall: {wins}-{losses}<br>"
                f"Pick: {pw['wins']}-{pw['losses']}<br>"
                f"Defend: {dw['wins']}-{dw['losses']}<br>"
                f"{low_sample}"
            )
            hover_row.append(hover)

        z_values.append(z_row)
        text_values.append(text_row)
        hover_texts.append(hover_row)

    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=x_labels,
            y=y_labels,
            text=text_values,
            texttemplate="%{text}",
            hovertext=hover_texts,
            hoverinfo="text",
            colorscale=[
                [0, "#f87171"],    # red (loss)
                [0.5, "#6b7280"],  # gray (neutral)
                [1, "#4ade80"],    # green (win)
            ],
            zmin=0,
            zmax=1,
            colorbar=dict(
                title="Map Strength",
                tickvals=[0, 0.25, 0.5, 0.75, 1],
                ticktext=["0%", "25%", "50%", "75%", "100%"],
            ),
        )
    )

    fig.update_layout(
        xaxis=dict(
            side="top",
            tickangle=-45,
            color=COLORS["text"],
        ),
        yaxis=dict(
            autorange="reversed",
            color=COLORS["text"],
        ),
        paper_bgcolor=COLORS["page_bg"],
        plot_bgcolor=COLORS["card_bg"],
        font=dict(color=COLORS["text"]),
        margin=dict(t=120, l=80, r=40, b=40),
        height=max(400, len(teams) * 35 + 200),
    )

    return fig


def layout():
    """Return the layout for the Map Matrix tab."""
    return html.Div(
        [
            dbc.Row(
                dbc.Col(
                    dcc.Dropdown(
                        id="map-matrix-mode-filter",
                        options=[
                            {"label": "All Modes", "value": "All"},
                            {"label": "SnD", "value": "SnD"},
                            {"label": "HP", "value": "HP"},
                            {"label": "Control", "value": "Control"},
                        ],
                        value="All",
                        clearable=False,
                        style={
                            "backgroundColor": COLORS["card_bg"],
                            "color": "#000",
                            "width": "200px",
                        },
                    ),
                    width=3,
                ),
                className="mb-3",
            ),
            dcc.Graph(id="map-matrix-heatmap"),
        ]
    )


def register_callbacks(app):
    """Register callbacks for the Map Matrix tab."""

    @app.callback(
        Output("map-matrix-heatmap", "figure"),
        Input("map-matrix-mode-filter", "value"),
    )
    def update_heatmap(mode_filter):
        conn = get_db()
        try:
            teams, maps, matrix = _build_matrix_data(conn, mode_filter)
            return _build_heatmap_figure(teams, maps, matrix)
        finally:
            conn.close()
