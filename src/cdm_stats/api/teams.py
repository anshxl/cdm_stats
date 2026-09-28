"""/api/scope and /api/teams/..."""
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from cdm_stats.api.deps import (
    Mode, get_conn, low_sample_flag, match_filter, team_info, team_or_404,
)
from cdm_stats.api.schemas import Flagged, Players, Profile, TeamInfo
from cdm_stats.db.queries import (
    MODES, get_team_id_by_abbr, opponent_ban_rates, team_ban_rates, team_pick_rates,
)
from cdm_stats.db.queries_ops import ops_player_weekly_trend
from cdm_stats.db.queries_scope import opponents_in_scope, series_match_ids, teams_in_scope
from cdm_stats.db.queries_tournament_player import (
    player_series_kd, player_summary, player_weekly_trend, recent_series_stats,
)
from cdm_stats.db.queries_views import (
    YOUR_TEAM, all_maps, available_players, map_record_data, map_results_detail, team_record,
)
from cdm_stats.metrics.elo import get_current_elo, is_low_confidence
from cdm_stats.metrics.filters import MatchFilter
from cdm_stats.metrics.insights import ban_highlight, map_flag, player_form_flag

router = APIRouter()


@router.get("/scope", response_model=list[TeamInfo])
def scope(f: MatchFilter = Depends(match_filter), conn: sqlite3.Connection = Depends(get_conn)):
    return [team_info(t["abbreviation"], t["team_name"]) for t in teams_in_scope(conn, f)]


@router.get("/teams/{abbr}/opponents", response_model=list[TeamInfo])
def opponents(abbr: str, f: MatchFilter = Depends(match_filter),
              conn: sqlite3.Connection = Depends(get_conn)):
    team = team_or_404(conn, abbr)
    return [team_info(t["abbreviation"], t["team_name"])
            for t in opponents_in_scope(conn, team["team_id"], f)]


def _rate(rates: dict, map_id: int | None) -> dict:
    return {"count": rates["by_map"].get(map_id, 0), "total": rates["total_series"]}


@router.get("/teams/{abbr}/profile", response_model=Profile)
def profile(abbr: str, f: MatchFilter = Depends(match_filter),
            conn: sqlite3.Connection = Depends(get_conn)):
    team = team_or_404(conn, abbr)
    tid = team["team_id"]
    record = team_record(conn, tid, f)
    played = record["map_wins"] + record["map_losses"]
    overall = record["map_wins"] / played if played else None

    own_bans = team_ban_rates(conn, tid, f=f)
    picks = team_pick_rates(conn, tid, f=f)
    opp_bans = opponent_ban_rates(conn, tid, f=f)

    rows = []
    for r in map_record_data(conn, tid, f):
        n = r["wins"] + r["losses"]
        if n == 0:
            continue
        rate = r["wins"] / n
        rows.append({
            **r,
            "win_rate": Flagged(value=rate, n=n, flag=map_flag(rate, n, overall or 0.0)),
            "banned": _rate(own_bans, r["map_id"]),
            "picked": _rate(picks, r["map_id"]),
            "opp_banned": _rate(opp_bans, r["map_id"]),
            "history": map_results_detail(conn, tid, r["map_id"], f) if r["map_id"] else [],
        })

    # Maps banned but never played still matter ("always banned"); same slim rows as Dash.
    maps = all_maps(conn)
    seen = {r["map_id"] for r in rows}
    for map_id, map_name, mode in maps:
        if map_id in seen or not (own_bans["by_map"].get(map_id) or opp_bans["by_map"].get(map_id)):
            continue
        rows.append({
            "map_id": map_id, "map_name": map_name, "mode": mode, "wins": 0, "losses": 0,
            "win_rate": Flagged(value=None, n=0, flag=low_sample_flag(0)),
            "strength": {"rating": None, "weighted_sample": 0, "total_played": 0,
                         "low_confidence": True},
            "pick_wins": 0, "pick_losses": 0, "defend_wins": 0, "defend_losses": 0,
            "banned": _rate(own_bans, map_id), "picked": _rate(picks, map_id),
            "opp_banned": _rate(opp_bans, map_id), "history": [],
        })

    ban_summary = []
    for mode in MODES:
        counts = {name: own_bans["by_map"].get(mid, 0) for mid, name, m in maps if m == mode}
        hit = ban_highlight(counts, own_bans["total_series"])
        if hit:
            ban_summary.append({"mode": mode, "map_name": hit[0], "flag": hit[1]})

    return {
        "team": team_info(team["abbreviation"], team["team_name"]),
        "elo": get_current_elo(conn, tid),  # one chain over every competition
        "low_confidence": is_low_confidence(conn, tid),
        "record": record,
        "overall_map_win_rate": overall,
        "maps": rows,
        "ban_summary": ban_summary,
    }


@router.get("/teams/{abbr}/players", response_model=Players)
def players(abbr: str, player: str | None = None, mode: Mode | None = None,
            series: str = "last10", f: MatchFilter = Depends(match_filter),
            conn: sqlite3.Connection = Depends(get_conn)):
    """`series`: last10 | all | an opponent abbreviation. It selects the series
    behind the tiles, both trends and the recent-series list."""
    team = team_or_404(conn, abbr)
    if team["abbreviation"] != YOUR_TEAM:
        raise HTTPException(404, f"Player stats exist only for {YOUR_TEAM}")
    if series not in ("last10", "all") and get_team_id_by_abbr(conn, series) is None:
        raise HTTPException(422, f"Invalid series: {series}")

    ids = series_match_ids(conn, team["team_id"], f, series)
    kds = player_series_kd(conn, ids, player=player, mode=mode)
    ops_trend = ops_player_weekly_trend(conn, player=player, mode=mode, f=f, match_ids=ids)
    op_maps: dict[str, int] = {}
    for d in ops_trend:
        op_maps[d["player_name"]] = op_maps.get(d["player_name"], 0) + d["maps"]

    tiles = []
    for t in player_summary(conn, player=player, mode=mode, f=f, match_ids=ids):
        series_kds = kds.get(t["player_name"], [])
        n_ops = op_maps.get(t["player_name"], 0)
        tiles.append({
            **t,
            "kd": Flagged(value=t["kd"], n=len(series_kds),
                          flag=low_sample_flag(len(series_kds)) or player_form_flag(series_kds)),
            "op_kills_per_pull": Flagged(
                value=round(t["op_kills"] / t["op_pulls"], 2) if t["op_pulls"] else None,
                n=n_ops, flag=low_sample_flag(n_ops)),
        })

    return {
        "players": available_players(conn),
        "tiles": tiles,
        "kd_trend": player_weekly_trend(conn, player=player, mode=mode, f=f, match_ids=ids),
        "ops_trend": ops_trend,
        "recent_series": recent_series_stats(conn, YOUR_TEAM, player=player, mode=mode, f=f,
                                             limit=None, match_ids=ids),
    }
