"""View-level queries moved out of the Dash UI modules.

Each function returns plain data (dicts/lists) so an API layer can serve it.
"""
import sqlite3

from cdm_stats.db.queries import get_team_map_wl, team_ban_rates, team_pick_rates
from cdm_stats.metrics.avoidance import pick_win_loss, defend_win_loss
from cdm_stats.metrics.filters import MatchFilter, family_of, match_label
from cdm_stats.metrics.map_strength import map_strength

YOUR_TEAM = "GL"


def all_maps(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    """(map_id, map_name, mode) sorted by mode then name."""
    return conn.execute(
        "SELECT map_id, map_name, mode FROM maps ORDER BY mode, map_name"
    ).fetchall()


def _abbr(conn: sqlite3.Connection, team_id: int) -> str:
    return conn.execute(
        "SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# Team profile
# ---------------------------------------------------------------------------

def team_record(conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()) -> dict:
    """Series and map W/L over the filtered window (DQ'd maps excluded)."""
    fw, fp = f.sql()
    sw, sl = conn.execute(
        f"""SELECT SUM(m.series_winner_id = ?), SUM(m.series_winner_id != ?)
            FROM matches m WHERE (m.team1_id = ? OR m.team2_id = ?){fw}""",
        [team_id, team_id, team_id, team_id] + fp,
    ).fetchone()
    mw, ml = conn.execute(
        f"""SELECT SUM(mr.winner_team_id = ?), SUM(mr.winner_team_id != ?)
            FROM map_results mr JOIN matches m ON mr.match_id = m.match_id
            WHERE (m.team1_id = ? OR m.team2_id = ?) AND mr.dq = 0{fw}""",
        [team_id, team_id, team_id, team_id] + fp,
    ).fetchone()
    return {"series_wins": sw or 0, "series_losses": sl or 0,
            "map_wins": mw or 0, "map_losses": ml or 0}


def map_record_data(conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()) -> list[dict]:
    """Per-map W/L records enriched with pick/defend splits and Map Strength."""
    base = get_team_map_wl(conn, team_id, f=f)
    map_lookup = {(name, mode): map_id for map_id, name, mode in all_maps(conn)}

    for entry in base:
        map_id = map_lookup.get((entry["map_name"], entry["mode"]))
        if map_id:
            pwl = pick_win_loss(conn, team_id, map_id, f=f)
            dwl = defend_win_loss(conn, team_id, map_id, f=f)
            entry["map_id"] = map_id
            entry["pick_wins"] = pwl["wins"]
            entry["pick_losses"] = pwl["losses"]
            entry["defend_wins"] = dwl["wins"]
            entry["defend_losses"] = dwl["losses"]
            entry["strength"] = map_strength(conn, team_id, map_id, f=f)
        else:
            entry["map_id"] = None
            entry["pick_wins"] = entry["pick_losses"] = 0
            entry["defend_wins"] = entry["defend_losses"] = 0
            entry["strength"] = {"rating": None, "weighted_sample": 0, "total_played": 0, "low_confidence": True}
    return base


def map_results_detail(
    conn: sqlite3.Connection, team_id: int, map_id: int, f: MatchFilter = MatchFilter()
) -> list[dict]:
    """Individual results for a team on one map, newest first, oriented to that team.

    Each dict: match_date, opponent, score, pick_context, picked_by, result, family, label.
    """
    team_abbr = _abbr(conn, team_id)
    fw, fp = f.sql()
    rows = conn.execute(
        f"""SELECT m.match_date, m.team1_id, m.team2_id,
                  mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score,
                  mr.pick_context, mr.picked_by_team_id,
                  m.competition, m.round, m.season
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?){fw}
           ORDER BY m.match_date DESC""",
        [map_id, team_id, team_id] + fp,
    ).fetchall()

    results = []
    for (match_date, t1_id, t2_id, winner_id, pick_score, non_pick_score, pick_ctx, picker_id,
         competition, round_, season) in rows:
        opp_id = t2_id if team_id == t1_id else t1_id
        opp_abbr = _abbr(conn, opp_id)
        if picker_id == opp_id:
            score = f"{non_pick_score}-{pick_score}"
        else:
            score = f"{pick_score}-{non_pick_score}"
        if picker_id == team_id:
            picked_by = team_abbr
        elif picker_id == opp_id:
            picked_by = opp_abbr
        else:
            picked_by = "N/A"
        results.append({
            "match_date": match_date,
            "opponent": opp_abbr,
            "score": score,
            "pick_context": pick_ctx,
            "picked_by": picked_by,
            "result": "W" if winner_id == team_id else "L",
            "family": family_of(competition, season),
            "label": match_label(competition, round_, season),
        })
    return results


# ---------------------------------------------------------------------------
# Head to head
# ---------------------------------------------------------------------------

def head_to_head(
    conn: sqlite3.Connection, team_id: int, opp_id: int, map_id: int, f: MatchFilter = MatchFilter()
) -> dict:
    """W-L between two specific teams on a specific map."""
    fw, fp = f.sql()
    row = conn.execute(
        f"""SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?)
               OR (m.team1_id = ? AND m.team2_id = ?)){fw}""",
        [team_id, opp_id, map_id, team_id, opp_id, opp_id, team_id] + fp,
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def team_map_wl(
    conn: sqlite3.Connection, team_id: int, map_id: int, f: MatchFilter = MatchFilter()
) -> dict:
    """Overall W-L for a team on a map (all opponents)."""
    fw, fp = f.sql()
    row = conn.execute(
        f"""SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?){fw}""",
        [team_id, team_id, map_id, team_id, team_id] + fp,
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def recent_series(
    conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter(), limit: int = 10
) -> list[dict]:
    """A team's most recent series, newest first, oriented to that team."""
    fw, fp = f.sql()
    rows = conn.execute(
        f"""SELECT m.match_date, m.team1_id, m.team2_id, m.series_winner_id,
                   m.competition, m.round, m.season,
                   SUM(mr.winner_team_id = ?), SUM(mr.winner_team_id != ?)
            FROM matches m JOIN map_results mr ON mr.match_id = m.match_id
            WHERE (m.team1_id = ? OR m.team2_id = ?) AND mr.dq = 0{fw}
            GROUP BY m.match_id
            ORDER BY m.match_date DESC, m.match_id DESC
            LIMIT ?""",
        [team_id, team_id, team_id, team_id] + fp + [limit],
    ).fetchall()
    out = []
    for match_date, t1, t2, winner, competition, round_, season, won, lost in rows:
        opp_id = t2 if t1 == team_id else t1
        out.append({
            "match_date": match_date, "opponent": _abbr(conn, opp_id),
            "result": "W" if winner == team_id else "L",
            "score": f"{won}-{lost}",
            "family": family_of(competition, season),
            "label": match_label(competition, round_, season),
        })
    return out


def matchup_data(
    conn: sqlite3.Connection, your_id: int, opp_id: int, f: MatchFilter = MatchFilter()
) -> dict[str, list[dict]]:
    """Per-mode map comparison between two teams.

    Returns {"SnD": [...], "HP": [...], "Control": [...]}; each mode's maps are
    sorted by Map Strength delta (your advantage first).
    """
    result: dict[str, list[dict]] = {"SnD": [], "HP": [], "Control": []}

    opp_bans = team_ban_rates(conn, opp_id, f=f)
    opp_bans_h2h = team_ban_rates(conn, opp_id, f=f, opponent_id=your_id)
    opp_picks = team_pick_rates(conn, opp_id, f=f)

    for map_id, map_name, mode in all_maps(conn):
        your_ms = map_strength(conn, your_id, map_id, f=f)
        opp_ms = map_strength(conn, opp_id, map_id, f=f)
        if your_ms["rating"] is not None and opp_ms["rating"] is not None:
            delta = your_ms["rating"] - opp_ms["rating"]
        else:
            delta = None

        entry = {
            "map_id": map_id,
            "map_name": map_name,
            "mode": mode,
            "h2h": head_to_head(conn, your_id, opp_id, map_id, f=f),
            "your_wl": team_map_wl(conn, your_id, map_id, f=f),
            "opp_wl": team_map_wl(conn, opp_id, map_id, f=f),
            "your_strength": your_ms,
            "opp_strength": opp_ms,
            "delta": delta,
            "your_pick_wl": pick_win_loss(conn, your_id, map_id, f=f),
            "your_defend_wl": defend_win_loss(conn, your_id, map_id, f=f),
            "opp_pick_wl": pick_win_loss(conn, opp_id, map_id, f=f),
            "opp_defend_wl": defend_win_loss(conn, opp_id, map_id, f=f),
            "opp_bans": (opp_bans["by_map"].get(map_id, 0), opp_bans["total_series"]),
            "opp_bans_h2h": (opp_bans_h2h["by_map"].get(map_id, 0), opp_bans_h2h["total_series"]),
            "opp_picks": (opp_picks["by_map"].get(map_id, 0), opp_picks["total_series"]),
        }
        if mode in result:
            result[mode].append(entry)

    for mode in result:
        result[mode].sort(key=lambda m: m["delta"] if m["delta"] is not None else -999, reverse=True)
    return result


# ---------------------------------------------------------------------------
# Scrims
# ---------------------------------------------------------------------------

def scrim_maps_for_mode(conn: sqlite3.Connection, mode: str | None = None) -> list[str]:
    """Distinct scrim map names for a mode; None or "All" means every mode."""
    if mode and mode != "All":
        rows = conn.execute(
            "SELECT DISTINCT map_name FROM scrim_maps WHERE mode = ? ORDER BY map_name", (mode,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT map_name FROM scrim_maps ORDER BY map_name").fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Player section
# ---------------------------------------------------------------------------

def available_players(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT player_name FROM tournament_player_stats ORDER BY player_name"
    ).fetchall()
    return [r[0] for r in rows]


def player_opponents(conn: sqlite3.Connection, team_abbr: str = YOUR_TEAM) -> list[str]:
    """Opponent abbreviations the team has ever played."""
    rows = conn.execute(
        """SELECT DISTINCT CASE WHEN t1.abbreviation = ? THEN t2.abbreviation
                                ELSE t1.abbreviation END AS opp
           FROM matches m
           JOIN teams t1 ON m.team1_id = t1.team_id
           JOIN teams t2 ON m.team2_id = t2.team_id
           WHERE ? IN (t1.abbreviation, t2.abbreviation)
           ORDER BY opp""",
        (team_abbr, team_abbr),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Elo
# ---------------------------------------------------------------------------

def elo_points(conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()) -> list[dict]:
    """A team's Elo after each match inside `f`, oldest first.

    The rating is one chain over every competition; `f` only chooses which
    points are returned. Each dict: match_date, elo, opponent, result, label.
    """
    fw, fp = f.sql()
    rows = conn.execute(
        f"""SELECT te.match_date, te.elo_after, m.team1_id, m.team2_id, m.series_winner_id,
                   m.competition, m.round, m.season
            FROM team_elo te JOIN matches m ON te.match_id = m.match_id
            WHERE te.team_id = ?{fw}
            ORDER BY te.match_date, te.elo_id""",
        [team_id] + fp,
    ).fetchall()
    return [
        {"match_date": match_date, "elo": elo,
         "opponent": _abbr(conn, t2 if t1 == team_id else t1),
         "result": "W" if winner == team_id else "L",
         "label": match_label(competition, round_, season)}
        for match_date, elo, t1, t2, winner, competition, round_, season in rows
    ]
