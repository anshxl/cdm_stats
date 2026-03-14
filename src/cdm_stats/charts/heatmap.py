import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from cdm_stats.metrics.avoidance import avoidance_index, target_index
from cdm_stats.metrics.elo import get_elo_history, SEED_ELO


def chart_avoidance_target(conn: sqlite3.Connection, team_id: int, output_path: str) -> None:
    maps = conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()
    abbr = conn.execute("SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)).fetchone()[0]

    labels = [f"{m[1]} ({m[2]})" for m in maps]
    avoid_vals = []
    target_vals = []
    for map_id, _, _ in maps:
        av = avoidance_index(conn, team_id, map_id)
        tg = target_index(conn, team_id, map_id)
        avoid_vals.append(av["ratio"])
        target_vals.append(tg["ratio"])

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(labels))
    width = 0.35

    ax.bar(x - width / 2, avoid_vals, width, label="Avoidance Index", color="#FF6B6B")
    ax.bar(x + width / 2, target_vals, width, label="Target Index", color="#4ECDC4")

    ax.set_ylabel("Index")
    ax.set_title(f"{abbr} — Avoidance vs Target Index by Map")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.legend()
    ax.set_ylim(0, 1.0)
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
