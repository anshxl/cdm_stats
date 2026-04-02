import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from cdm_stats.metrics.map_strength import map_strength
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO


def chart_map_strength(conn: sqlite3.Connection, team_id: int, output_path: str) -> None:
    maps = conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()
    abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]

    labels = [f"{m[1]} ({m[2]})" for m in maps]
    strength_vals = []
    for map_id, _, _ in maps:
        ms = map_strength(conn, team_id, map_id)
        strength_vals.append(ms["rating"] if ms["rating"] is not None else 0.0)

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(labels))

    colors = ["#4ade80" if v >= 0.6 else "#f87171" if v <= 0.4 else "#6b7280" for v in strength_vals]
    ax.bar(x, strength_vals, color=colors)

    ax.set_ylabel("Map Strength")
    ax.set_title(f"{abbr} — Map Strength Rating by Map")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylim(0, 1.0)
    ax.axhline(y=0.5, color="gray", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def chart_elo_trajectory(conn: sqlite3.Connection, team_id: int, output_path: str) -> None:
    abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]
    history = get_elo_history(conn, team_id)

    dates = [h["match_date"] for h in history]
    elos = [h["elo_after"] for h in history]

    # Prepend seed
    if dates:
        dates = ["Start"] + dates
        elos = [SEED_ELO] + elos

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(len(dates)), elos, marker="o", linewidth=2, color="#4472C4")
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha="right")
    ax.set_ylabel("Elo Rating")
    ax.set_title(f"{abbr} — Elo Trajectory")
    ax.axhline(y=SEED_ELO, color="gray", linestyle="--", alpha=0.5, label="Seed (1000)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def _week_number(date_str: str, earliest: str) -> int:
    from datetime import datetime
    d = datetime.strptime(date_str, "%Y-%m-%d")
    e = datetime.strptime(earliest, "%Y-%m-%d")
    return (d - e).days // 7 + 1


def chart_elo_all_teams(conn: sqlite3.Connection, output_path: str) -> None:
    teams = conn.execute("SELECT team_id, abbreviation FROM teams ORDER BY abbreviation").fetchall()

    # Find earliest match date across all teams for week numbering
    row = conn.execute("SELECT MIN(match_date) FROM matches").fetchone()
    if not row or not row[0]:
        return
    earliest_date = row[0]

    cmap = plt.cm.get_cmap("tab20", 20)
    colors = [cmap(i) for i in range(14)]

    fig, ax = plt.subplots(figsize=(14, 7))
    max_week = 0
    for idx, (team_id, abbr) in enumerate(teams):
        history = get_elo_history(conn, team_id)
        if not history:
            continue

        # Group by week, take last Elo per week
        week_elo = {}
        for h in history:
            wk = _week_number(h["match_date"], earliest_date)
            week_elo[wk] = h["elo_after"]

        weeks = sorted(week_elo.keys())
        if not weeks:
            continue
        max_week = max(max_week, weeks[-1])

        x = [0] + weeks
        y = [SEED_ELO] + [week_elo[w] for w in weeks]
        ax.plot(x, y, marker="o", markersize=4, linewidth=1.5, label=abbr, color=colors[idx % 14])

    ax.axhline(y=SEED_ELO, color="gray", linestyle="--", alpha=0.4)
    ax.set_xticks(range(0, max_week + 1))
    ax.set_xticklabels(["Start"] + [f"W{w}" for w in range(1, max_week + 1)])
    ax.set_xlabel("Week")
    ax.set_ylabel("Elo Rating")
    ax.set_title("All Teams — Elo Trajectory")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
