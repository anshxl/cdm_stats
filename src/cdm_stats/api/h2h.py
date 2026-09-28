"""/api/h2h — per-mode map comparison with PICK / BAN / THEY BAN tags."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from cdm_stats.api.deps import get_conn, low_sample_flag, match_filter, team_info, team_or_404
from cdm_stats.api.schemas import H2H
from cdm_stats.db.queries import MODES
from cdm_stats.db.queries_views import matchup_data, recent_series
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.metrics.filters import MatchFilter
from cdm_stats.metrics.insights import h2h_tags

router = APIRouter()


def _rate(wl: dict) -> dict:
    n = wl["wins"] + wl["losses"]
    return {"value": wl["wins"] / n if n else None, "n": n, "flag": low_sample_flag(n)}


def _count(pair: tuple[int, int]) -> dict:
    return {"count": pair[0], "total": pair[1]}


def _elo(conn: sqlite3.Connection, team_id: int) -> dict:
    return {"elo": get_current_elo(conn, team_id), "low_confidence": is_low_confidence(conn, team_id)}


@router.get("/h2h", response_model=H2H)
def head_to_head(team: str, opp: str, f: MatchFilter = Depends(match_filter),
                 conn: sqlite3.Connection = Depends(get_conn)):
    you, them = team_or_404(conn, team), team_or_404(conn, opp)
    if you["team_id"] == them["team_id"]:
        raise HTTPException(422, "team and opp must differ")

    data = matchup_data(conn, you["team_id"], them["team_id"], f)
    modes = []
    for mode in MODES:
        entries = data.get(mode, [])
        rows = [{**e, "your_rate": _rate(e["your_wl"]), "opp_rate": _rate(e["opp_wl"])}
                for e in entries]
        # Tags use the same win rates the rows display; ties go to table order.
        tags = h2h_tags(
            [{"map": r["map_name"], "our_rate": r["your_rate"]["value"] or 0.0,
              "our_n": r["your_rate"]["n"], "their_rate": r["opp_rate"]["value"] or 0.0,
              "their_n": r["opp_rate"]["n"]} for r in rows],
            {r["map_name"]: r["opp_bans"][0] for r in rows},
            rows[0]["opp_bans"][1] if rows else 0,
        )
        for r in rows:
            r["tags"] = tags.get(r["map_name"], [])
            for key in ("opp_bans", "opp_bans_h2h", "opp_picks"):
                r[key] = _count(r[key])
        modes.append({"mode": mode, "maps": rows, "not_enough_data": not tags})

    return {
        "team": team_info(you["abbreviation"], you["team_name"]),
        "opp": team_info(them["abbreviation"], them["team_name"]),
        "elo": {"team": _elo(conn, you["team_id"]), "opp": _elo(conn, them["team_id"])},
        "opp_recent_series": recent_series(conn, them["team_id"], f),
        "modes": modes,
    }
