import sqlite3

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc
from dash.dependencies import Input, Output

from cdm_stats.dashboard.app import get_db
from cdm_stats.dashboard.helpers import COLORS, get_all_teams, team_logo_src
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO

# 14 distinct colors for 14 CDL teams — tuned for the Twilight Ops dark canvas.
# Each hue is bright enough to read on #0a0e18 but desaturated enough to avoid
# the neon-default-Plotly look.
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
    plotted: list[tuple[dict, str]] = []
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
        plotted.append((trace, color))
        color_idx += 1
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="#7d8aa3", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")
    fig.add_vrect(x0=0, x1=6, fillcolor="#7d8aa3", opacity=0.06, line_width=0,
                  annotation_text="Low Confidence Zone", annotation_position="top left",
                  annotation_font_color="#7d8aa3")

    # Drop a logo at the right end of every team line that has one. Sized in
    # data units relative to the visible range so it stays roughly stable.
    if plotted:
        all_elos = [e for trace, _ in plotted for e in trace["elos"]]
        y_range = max(all_elos) - min(all_elos)
        x_max = max(t["weeks"][-1] for t, _ in plotted)
        logo_h = max(y_range * 0.05, 8)
        logo_w = max(x_max * 0.04, 0.3)
        for trace, _ in plotted:
            src = team_logo_src(trace["abbr"])
            if not src:
                continue
            fig.add_layout_image(dict(
                source=src,
                xref="x", yref="y",
                x=trace["weeks"][-1], y=trace["elos"][-1],
                sizex=logo_w, sizey=logo_h,
                xanchor="left", yanchor="middle",
                sizing="contain",
                layer="above",
            ))

    max_week = max((t["weeks"][-1] for t in traces if t["weeks"]), default=1)
    fig.update_layout(
        plot_bgcolor=COLORS["page_bg"],
        paper_bgcolor=COLORS["page_bg"],
        font={"color": COLORS["text"]},
        margin={"l": 60, "r": 60, "t": 40, "b": 60},
        height=500,
        xaxis={
            "title": "Week",
            "tickmode": "array",
            "tickvals": list(range(0, max_week + 1)),
            "ticktext": ["Start"] + [f"W{w}" for w in range(1, max_week + 1)],
            "gridcolor": COLORS["border"],
            # Pad the right edge so end-of-line team logos aren't clipped.
            "range": [-0.3, max_week + max(max_week * 0.06, 0.6)],
        },
        yaxis={"title": "Elo Rating", "gridcolor": COLORS["border"]},
        legend={"font": {"size": 10}},
        hovermode="closest",
    )
    return fig


def _build_current_figure(traces: list[dict]) -> go.Figure:
    """Bar chart of each team's latest Elo, sorted descending."""
    entries = []
    color_idx = 0
    for trace in traces:
        if len(trace["elos"]) <= 1:
            continue
        entries.append({
            "abbr": trace["abbr"],
            "elo": trace["elos"][-1],
            "color": TEAM_COLORS[color_idx % len(TEAM_COLORS)],
        })
        color_idx += 1
    entries.sort(key=lambda e: e["elo"], reverse=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[e["abbr"] for e in entries],
        y=[e["elo"] for e in entries],
        marker={"color": [e["color"] for e in entries]},
        text=[f"{e['elo']:.0f}" for e in entries],
        textposition="outside",
        hovertemplate="%{x}: %{y:.0f}<extra></extra>",
    ))
    fig.add_hline(y=SEED_ELO, line_dash="dash", line_color="#7d8aa3", opacity=0.4,
                  annotation_text="Seed (1000)", annotation_position="bottom right")

    if entries:
        y_min = min(e["elo"] for e in entries) - 20
        y_max = max(e["elo"] for e in entries) + 60  # extra headroom for logos
        # Bars sit at integer x positions; size logos in those units.
        logo_w = 0.7
        logo_h = (y_max - y_min) * 0.07
        for i, e in enumerate(entries):
            src = team_logo_src(e["abbr"])
            if not src:
                continue
            fig.add_layout_image(dict(
                source=src,
                xref="x", yref="y",
                x=i, y=e["elo"] + (y_max - y_min) * 0.045,
                sizex=logo_w, sizey=logo_h,
                xanchor="center", yanchor="bottom",
                sizing="contain",
                layer="above",
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
        yaxis={
            "title": "Current Elo Rating",
            "gridcolor": COLORS["border"],
            "range": [y_min, y_max] if entries else None,
        },
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
        ], className="mb-3 mt-2"),
        dcc.Graph(id="elo-chart"),
    ], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output("elo-chart", "figure"),
        Input("elo-view-toggle", "value"),
    )
    def update_chart(view):
        conn = get_db()
        traces = _build_elo_traces(conn)
        conn.close()
        if view == "current":
            return _build_current_figure(traces)
        return _build_figure(traces)
