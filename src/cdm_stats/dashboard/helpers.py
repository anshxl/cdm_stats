import sqlite3
from cdm_stats.dashboard.app import get_db

LOW_SAMPLE_THRESHOLD = 4

# Color constants matching the design spec
COLORS = {
    "win": "#4ade80",
    "neutral": "#fbbf24",
    "loss": "#f87171",
    "your_team": "#4cc9f0",
    "opponent": "#f87171",
    "ban": "#e879f9",
    "card_bg": "#16213e",
    "page_bg": "#1a1a2e",
    "border": "#2a2a4a",
    "muted": "#666",
    "text": "#e0e0e0",
}

MODE_COLORS = {
    "SnD": "#4cc9f0",
    "HP": "#e879f9",
    "Control": "#fbbf24",
}


def wl_color(wins: int, losses: int) -> str:
    """Return color string based on win rate."""
    total = wins + losses
    if total == 0:
        return COLORS["muted"]
    rate = wins / total
    if rate >= 0.6:
        return COLORS["win"]
    if rate <= 0.4:
        return COLORS["loss"]
    return COLORS["neutral"]


def get_all_teams(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """Return list of (team_id, abbreviation) sorted alphabetically."""
    return conn.execute(
        "SELECT team_id, abbreviation FROM teams ORDER BY abbreviation"
    ).fetchall()


def get_all_maps(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    """Return list of (map_id, map_name, mode) sorted by mode then name."""
    return conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()


def team_dropdown_options(conn: sqlite3.Connection) -> list[dict]:
    """Return dropdown options for team selector."""
    teams = get_all_teams(conn)
    return [{"label": abbr, "value": tid} for tid, abbr in teams]
