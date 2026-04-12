import sqlite3
from pathlib import Path

LOW_SAMPLE_THRESHOLD = 4

# Team logos live in src/cdm_stats/dashboard/assets/logos/<abbr>.png (lowercase).
# Dash auto-serves the assets/ folder at /assets/, so the public URL is
# /assets/logos/<abbr>.png. Missing logos fall back to text — safe to roll out
# logos one team at a time.
_LOGO_DIR = Path(__file__).parent / "assets" / "logos"

# Twilight Ops palette — deep midnight canvas, mint/amber signal accents.
# Semantics are kept identical; only hex values changed so that
# your_team ≠ SnD and opponent ≠ loss (the previous palette collided).
COLORS = {
    "win":       "#5eead4",  # mint teal
    "neutral":   "#f5b544",  # warm amber
    "loss":      "#ef6f6c",  # softened coral
    "your_team": "#7dd3fc",  # sky cyan
    "opponent":  "#fb923c",  # warm orange
    "ban":       "#c084fc",  # muted lavender
    "card_bg":   "#131a2a",  # deep slate-navy
    "page_bg":   "#0a0e18",  # midnight ink
    "border":    "#222d46",  # hairline slate
    "muted":     "#7d8aa3",  # cool slate-grey
    "text":      "#e8ecf4",  # soft warm white
}

MODE_COLORS = {
    "SnD":     "#60a5fa",  # azure
    "HP":      "#f472b6",  # rose
    "Control": "#facc15",  # gold
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


_LOGO_EXTENSIONS = ("png", "svg", "webp", "jpg", "jpeg")


def team_logo_src(abbr: str | None) -> str | None:
    """Return the Dash asset URL for a team logo, or None if no file exists.

    Looks up <abbr>.<ext> (lowercase) in assets/logos/ for the supported
    extensions. Filesystem-checked per call (a few cheap stats — negligible
    vs the SQL queries on the same render path) so newly added logos appear
    on the next page render, no restart required.
    """
    if not abbr:
        return None
    key = abbr.lower()
    for ext in _LOGO_EXTENSIONS:
        if (_LOGO_DIR / f"{key}.{ext}").exists():
            return f"/assets/logos/{key}.{ext}"
    return None
