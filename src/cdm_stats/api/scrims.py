"""/api/scrims and /api/scrims/options. Scrims have no event family: dates only."""
import sqlite3
from datetime import date

from fastapi import APIRouter, Depends, Query

from cdm_stats.api.deps import Mode, get_conn, low_sample_flag, match_filter, team_info, team_or_404
from cdm_stats.api.schemas import ScrimOptions, Scrims
from cdm_stats.db.queries import MODE_ORDER, MODES
from cdm_stats.db.queries_scrim import (
    player_map_stats, scrim_map_breakdown, scrim_map_results_detail, scrim_opponents,
    scrim_weekly_trend, scrim_win_loss,
)
from cdm_stats.db.queries_views import scrim_maps_for_mode
from cdm_stats.metrics.filters import MatchFilter

router = APIRouter()


def date_filter(start: date | None = None, end: date | None = None) -> MatchFilter:
    return match_filter("all", start, end)


def _frac(d: dict) -> dict:
    """The db layer returns win_pct as 0-100; the API speaks 0-1 like every other rate."""
    return {**d, "win_pct": round(d["win_pct"] / 100, 4)}


@router.get("/scrims", response_model=Scrims)
def scrims(mode: Mode | None = None, map_name: str | None = Query(None, alias="map"),
           opponent: str | None = None, f: MatchFilter = Depends(date_filter),
           conn: sqlite3.Connection = Depends(get_conn)):
    if opponent is not None:
        team_or_404(conn, opponent)
    by_mode = []
    for m in MODES:
        wl = scrim_win_loss(conn, mode=m, map_name=map_name, f=f, opponent=opponent)
        if wl["total"] > 0:
            by_mode.append({"mode": m, **_frac(wl)})
    # As in Dash: the map table follows mode + opponent but not the map filter.
    maps = sorted(scrim_map_breakdown(conn, mode=mode, f=f, opponent=opponent),
                  key=lambda d: (MODE_ORDER.get(d["mode"], 99), d["map_name"]))
    return {
        "overall": _frac(scrim_win_loss(conn, mode=mode, map_name=map_name, f=f,
                                        opponent=opponent)),
        "by_mode": by_mode,
        "maps": [{**_frac(d), "flag": low_sample_flag(d["played"]),
                  "recent": scrim_map_results_detail(conn, d["map_name"], limit=5, f=f,
                                                     opponent=opponent)}
                 for d in maps],
        "trend": [_frac(p) for p in scrim_weekly_trend(conn, mode=mode, map_name=map_name,
                                                        f=f, opponent=opponent)],
        "trend_by_mode": [{"mode": m, **_frac(p)} for m in MODES if mode in (None, m)
                          for p in scrim_weekly_trend(conn, mode=m, map_name=map_name, f=f,
                                                      opponent=opponent)],
        # Player stats follow every filter, map included (unlike the map table).
        "player_maps": player_map_stats(conn, mode=mode, map_name=map_name, opponent=opponent, f=f),
    }


@router.get("/scrims/options", response_model=ScrimOptions)
def scrim_options(mode: Mode | None = None, f: MatchFilter = Depends(date_filter),
                  conn: sqlite3.Connection = Depends(get_conn)):
    return {
        "maps": scrim_maps_for_mode(conn, mode),
        "opponents": [team_info(a, team_or_404(conn, a)["team_name"])
                      for a in scrim_opponents(conn, f)],
    }
