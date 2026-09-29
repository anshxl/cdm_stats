"""Shared request dependencies: DB connection, filter params, team lookup."""
import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import HTTPException

from cdm_stats.api.schemas import TeamInfo
from cdm_stats.metrics.filters import MatchFilter
from cdm_stats.metrics.insights import MIN_N
from cdm_stats.team_colors import TEAM_COLORS

ROOT = Path(__file__).resolve().parents[3]
# DB_PATH overrides the repo's data/cdl.db.
DB_PATH = Path(os.environ.get("DB_PATH", ROOT / "data" / "cdl.db"))
# public/ in dev; dist/ holds the same files after a Vite build (Docker).
_LOGO_DIRS = (ROOT / "frontend" / "public" / "logos", ROOT / "frontend" / "dist" / "logos")

Event = Literal["all", "spring", "summer", "regionals"]
Mode = Literal["SnD", "HP", "Control"]


def get_conn():
    """One connection per request; FastAPI may open, use and close it on
    different worker threads, hence check_same_thread=False."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


def match_filter(event: Event = "all", start: date | None = None,
                 end: date | None = None) -> MatchFilter:
    return MatchFilter.from_event(
        event, start.isoformat() if start else None, end.isoformat() if end else None,
    )


def logo_url(abbr: str) -> str | None:
    key = abbr.lower()
    for d in _LOGO_DIRS:
        if d.is_dir():
            for p in sorted(d.iterdir()):
                if p.stem.lower() == key:
                    return f"/logos/{p.name}"
    return None


def team_info(abbr: str, team_name: str) -> TeamInfo:
    primary, secondary = TEAM_COLORS.get(abbr, (None, None))
    return TeamInfo(abbreviation=abbr, team_name=team_name, primary_color=primary,
                    secondary_color=secondary, logo=logo_url(abbr))


def team_or_404(conn: sqlite3.Connection, abbr: str) -> dict:
    row = conn.execute(
        "SELECT team_id, abbreviation, team_name FROM teams WHERE abbreviation = ?", (abbr,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"Unknown team: {abbr}")
    return {"team_id": row[0], "abbreviation": row[1], "team_name": row[2]}


def low_sample_flag(n: int) -> dict | None:
    """Shared rule: n < MIN_N is flagged low_sample."""
    return {"kind": "low_sample", "label": f"n={n}"} if n < MIN_N else None
