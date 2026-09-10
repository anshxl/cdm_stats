import sqlite3
from datetime import date, timedelta

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, get_all_teams, team_logo_src
from cdm_stats.dashboard.team_colors import team_colors
from cdm_stats.metrics.elo import get_elo_history, get_current_elo, SEED_ELO
from cdm_stats.metrics.filters import MatchFilter, family_of, match_label

# Distinct colors for the team lines — tuned for the Twilight Ops dark canvas.
TEAM_COLORS = [
    "#7dd3fc",  # sky
    "#fb923c",  # orange
    "#5eead4",  # mint
    "#f472b6",  # rose
    "#facc15",  # gold
    "#c084fc",  # lavender
    "#a3e635",  # lime
    "#fb7185",  # coral
    "#60a5fa",  # azure
    "#fbbf24",  # amber
    "#34d399",  # emerald
    "#e879f9",  # fuchsia
    "#22d3ee",  # cyan
    "#fda4af",  # blush
]


def _build_elo_traces(conn: sqlite3.Connection, f: MatchFilter = MatchFilter()) -> list[dict]:
    """Elo trajectory per (non-hidden) team on a date axis across all seasons.

    The rating itself is one chronological chain and ignores the filter; the
    filter only decides which points are drawn. Returns dicts with keys:
    team_id, abbr, dates, elos, hover_texts.
    """
    traces = []
    for team_id, abbr in get_all_teams(conn):
        dates, elos, hovers = [], [], []
        for h in get_elo_history(conn, team_id):
            match = conn.execute(
                """SELECT m.team1_id, m.team2_id, m.series_winner_id,
                          m.competition, m.round, m.season, m.match_date
                   FROM matches m WHERE m.match_id = ?""",
                (h["match_id"],),
            ).fetchone()
            t1, t2, winner, competition, round_, season, match_date = match
            if family_of(competition, season) not in f.families:
                continue
            if (f.start and match_date < f.start) or (f.end and match_date > f.end):
                continue
            opp_id = t2 if t1 == team_id else t1
            opp_abbr = conn.execute(
                "SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)
            ).fetchone()[0]
            result = "W" if winner == team_id else "L"
            dates.append(match_date)
            elos.append(h["elo_after"])
            hovers.append(
                f"{abbr}: {h['elo_after']:.0f}<br>{match_date} · {match_label(competition, round_, season)}"
                f"<br>vs {opp_abbr} ({result})"
            )
        traces.append({"team_id": team_id, "abbr": abbr, "dates": dates,
                       "elos": elos, "hover_texts": hovers})
    return traces


def _build_figure(traces: list[dict]) -> go.Figure:
    fig = go.Figure()
    color_idx = 0
    plotted: list[tuple[dict, str]] = []
    for trace in traces:
        if not trace["elos"]:
            continue
        color = TEAM_COLORS[color_idx % len(TEAM_COLORS)]
        fig.add_trace(go.Scatter(
            x=trace["dates"],
            y=trace["elos"],
            mode="lines+markers",
            name=trace["abbr"],
            text=trace["hover_texts"],
            hovertemplate="%{text}<extra></extra>",
            marker={"size": 5, "color": color},
            line={"width": 2, "color": color},
        ))
        plotted.append((trace, color))
        color_idx += 1
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="#7d8aa3", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")

    x_range = None
    if plotted:
        all_dates = sorted(d for t, _ in plotted for d in t["dates"])
        first, last = date.fromisoformat(all_dates[0]), date.fromisoformat(all_dates[-1])
        span_days = max((last - first).days, 14)
        x_range = [(first - timedelta(days=3)).isoformat(),
                   (last + timedelta(days=max(span_days * 0.06, 3))).isoformat()]
        all_elos = [e for t, _ in plotted for e in t["elos"]]
        y_range = max(all_elos) - min(all_elos)
        logo_h = max(y_range * 0.05, 8)
        logo_w = span_days * 0.04 * 86_400_000  # date-axis image sizes are in ms
        for trace, _ in plotted:
            src = team_logo_src(trace["abbr"])
            if not src:
                continue
            fig.add_layout_image(dict(
                source=src, xref="x", yref="y",
                x=trace["dates"][-1], y=trace["elos"][-1],
                sizex=logo_w, sizey=logo_h,
                xanchor="left", yanchor="middle", sizing="contain", layer="above",
            ))

    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 60, "r": 60, "t": 40, "b": 60},
        height=500,
        xaxis={"title": "Date", "type": "date", "gridcolor": COLORS["border"], "range": x_range},
        yaxis={"title": "Elo Rating", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def _current_entries(conn: sqlite3.Connection) -> list[dict]:
    """Latest Elo per non-hidden team with any rated match (filter-independent)."""
    entries = []
    for i, (team_id, abbr) in enumerate(get_all_teams(conn)):
        if not get_elo_history(conn, team_id):
            continue
        primary, secondary = team_colors(abbr, TEAM_COLORS[i % len(TEAM_COLORS)])
        entries.append({"abbr": abbr, "elo": get_current_elo(conn, team_id),
                        "primary": primary, "secondary": secondary})
    entries.sort(key=lambda e: e["elo"], reverse=True)
    return entries


def _build_current_figure(entries: list[dict]) -> go.Figure:
    """Bar chart of each team's latest Elo, sorted descending."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[e["abbr"] for e in entries],
        y=[e["elo"] for e in entries],
        marker={
            "color": [e["primary"] for e in entries],
            "line": {"color": [e["secondary"] for e in entries], "width": 2},
        },
        text=[f"{e['elo']:.0f}" for e in entries],
        textposition="outside",
        hovertemplate="%{x}: %{y:.0f}<extra></extra>",
    ))
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="#7d8aa3", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")

    if entries:
        y_min = min(e["elo"] for e in entries) - 20
        y_max = max(e["elo"] for e in entries) + 60  # headroom for logos
        logo_w = 0.7
        logo_h = (y_max - y_min) * 0.07
        for i, e in enumerate(entries):
            src = team_logo_src(e["abbr"])
            if not src:
                continue
            fig.add_layout_image(dict(
                source=src, xref="x", yref="y",
                x=i, y=e["elo"] + (y_max - y_min) * 0.045,
                sizex=logo_w, sizey=logo_h,
                xanchor="center", yanchor="bottom", sizing="contain", layer="above",
            ))
    else:
        y_min, y_max = None, None

    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 60, "r": 20, "t": 40, "b": 60},
        height=500,
        xaxis={"title": "Team", "gridcolor": COLORS["border"]},
        yaxis={"title": "Current Elo Rating", "gridcolor": COLORS["border"],
               "range": [y_min, y_max] if entries else None},
        showlegend=False,
    )
    return fig


def layout():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                dbc.RadioItems(
                    id="elo-view-toggle",
                    options=[
                        {"label": "Trajectory", "value": "trajectory"},
                        {"label": "Current Standings", "value": "current"},
                    ],
                    value="trajectory",
                    inline=True,
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-info btn-sm me-1",
                    labelCheckedClassName="active",
                ),
            ], width="auto"),
            dbc.Col(html.Small(
                "Elo is one chronological chain across seasons and is not affected by the "
                "filter bar; the filter only chooses which points are drawn.",
                style={"color": COLORS["muted"]},
            ), width="auto", className="align-self-center"),
        ], className="mb-3 mt-2"),
        dcc.Graph(id="elo-chart"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("elo-chart", "figure"),
        Input("elo-view-toggle", "value"),
        Input("filter-store", "data"),
    )
    def update_chart(view, filter_data):
        conn = get_db()
        try:
            if view == "current":
                return _build_current_figure(_current_entries(conn))
            return _build_figure(_build_elo_traces(conn, MatchFilter.from_dict(filter_data)))
        finally:
            conn.close()
