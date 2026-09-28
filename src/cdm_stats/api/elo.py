"""/api/elo — ratings use every competition; the filter chooses points and teams."""
import sqlite3
from typing import Literal

from fastapi import APIRouter, Depends

from cdm_stats.api.deps import get_conn, match_filter, team_info
from cdm_stats.api.schemas import Elo
from cdm_stats.db.queries_scope import teams_in_scope
from cdm_stats.db.queries_views import elo_points
from cdm_stats.metrics.elo import SEED_ELO, get_current_elo, is_low_confidence
from cdm_stats.metrics.filters import MatchFilter

router = APIRouter()


@router.get("/elo", response_model=Elo)
def elo(view: Literal["trajectory", "current"] = "trajectory",
        f: MatchFilter = Depends(match_filter), conn: sqlite3.Connection = Depends(get_conn)):
    teams = teams_in_scope(conn, f)
    trajectory, current = [], []
    for t in teams:
        info = team_info(t["abbreviation"], t["team_name"])
        low = is_low_confidence(conn, t["team_id"])
        if view == "trajectory":
            trajectory.append({"team": info, "low_confidence": low,
                               "points": elo_points(conn, t["team_id"], f)})
        else:
            current.append({"team": info, "elo": get_current_elo(conn, t["team_id"]),
                            "low_confidence": low})
    current.sort(key=lambda c: c["elo"], reverse=True)
    return {"view": view, "seed": SEED_ELO, "trajectory": trajectory, "current": current}
