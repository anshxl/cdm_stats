import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, get_all_teams
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO

# 14 distinct colors for 14 CDL teams — avoids Plotly's default 10-color wrap
TEAM_COLORS = [
    "#636EFA",  # blue
    "#EF553B",  # red
    "#00CC96",  # green
    "#AB63FA",  # purple
    "#FFA15A",  # orange
    "#19D3F3",  # cyan
    "#FF6692",  # pink
    "#B6E880",  # lime
    "#FF97FF",  # magenta
    "#FECB52",  # yellow
    "#1F77B4",  # steel blue
    "#2CA02C",  # forest green
    "#D62728",  # crimson
    "#8C564B",  # brown
]


def _week_number(date_str: str, earliest: str) -> int:
    from datetime import datetime
    d = datetime.strptime(date_str, "%Y-%m-%d")
    e = datetime.strptime(earliest, "%Y-%m-%d")
    return (d - e).days // 7 + 1


def _build_elo_traces(conn: sqlite3.Connection) -> list[dict]:
    """Build Elo trajectory data for all teams.
    Returns list of dicts with keys: team_id, abbr, weeks, elos, hover_texts.
    Week 0 = seed. Each subsequent point is the last Elo of that week.
    """
    teams = get_all_teams(conn)
    row = conn.execute("SELECT MIN(match_date) FROM matches").fetchone()
    if not row or not row[0]:
        return []
    earliest_date = row[0]

    traces = []
    for team_id, abbr in teams:
        history = get_elo_history(conn, team_id)
        week_elo = {}
        week_hover = {}
        for h in history:
            wk = _week_number(h["match_date"], earliest_date)
            week_elo[wk] = h["elo_after"]
            match = conn.execute(
                "SELECT team1_id, team2_id, series_winner_id FROM matches WHERE match_id = ?",
                (h["match_id"],),
            ).fetchone()
            if match:
                opp_id = match[1] if match[0] == team_id else match[0]
                opp_abbr = conn.execute(
                    "SELECT abbreviation FROM teams WHERE team_id = ?", (opp_id,)
                ).fetchone()[0]
                result = "W" if match[2] == team_id else "L"
                week_hover[wk] = f"{abbr}: {h['elo_after']:.0f}<br>vs {opp_abbr} ({result})"

        weeks = sorted(week_elo.keys())
        traces.append({
            "team_id": team_id,
            "abbr": abbr,
            "weeks": [0] + weeks,
            "elos": [SEED_ELO] + [week_elo[w] for w in weeks],
            "hover_texts": ["Seed: 1000"] + [week_hover.get(w, "") for w in weeks],
        })
    return traces


def _build_figure(traces: list[dict]) -> go.Figure:
    fig = go.Figure()
    color_idx = 0
    for trace in traces:
        if len(trace["elos"]) <= 1:
            continue
        color = TEAM_COLORS[color_idx % len(TEAM_COLORS)]
        fig.add_trace(go.Scatter(
            x=trace["weeks"],
            y=trace["elos"],
            mode="lines+markers",
            name=trace["abbr"],
            text=trace["hover_texts"],
            hovertemplate="%{text}<extra></extra>",
            marker={"size": 5, "color": color},
            line={"width": 2, "color": color},
        ))
        color_idx += 1
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="gray", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")
    fig.add_vrect(x0=0, x1=6, fillcolor="gray", opacity=0.05, line_width=0,
                  annotation_text="Low Confidence Zone", annotation_position="top left",
                  annotation_font_color="#666")
    max_week = max((t["weeks"][-1] for t in traces if t["weeks"]), default=1)
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 60, "r": 20, "t": 40, "b": 60},
        height=500,
        xaxis={
            "title": "Week",
            "tickmode": "array",
            "tickvals": list(range(0, max_week + 1)),
            "ticktext": ["Start"] + [f"W{w}" for w in range(1, max_week + 1)],
            "gridcolor": COLORS["border"],
        },
        yaxis={"title": "Elo Rating", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def layout():
    return dbc.Container([
        dcc.Graph(id="elo-chart"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("elo-chart", "figure"),
        Input("elo-chart", "id"),
    )
    def update_chart(_):
        conn = get_db()
        traces = _build_elo_traces(conn)
        conn.close()
        return _build_figure(traces)
