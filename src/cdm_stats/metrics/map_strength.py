import sqlite3

from cdm_stats.metrics.elo import get_current_elo, SEED_ELO

CONTEXT_WEIGHTS = {
    "Opener": 0.5,
    "Neutral": 1.0,
    "Must-Win": 3.0,
    "Close-Out": 2.0,
    "Coin-Toss": 1.0,
    "Unknown": 0.5,
}

LOW_SAMPLE_THRESHOLD = 3


def _get_league_avg_elo(conn: sqlite3.Connection, match_id: int) -> float:
    """Get the average Elo of all teams at the time of a match.

    Uses each team's most recent Elo before or at this match.
    Falls back to SEED_ELO for teams with no history yet.
    """
    match_date = conn.execute(
        "SELECT match_date FROM matches WHERE match_id = ?", (match_id,)
    ).fetchone()[0]

    team_ids = [r[0] for r in conn.execute("SELECT team_id FROM teams").fetchall()]
    total = 0.0
    for tid in team_ids:
        row = conn.execute(
            """SELECT elo_after FROM team_elo
               WHERE team_id = ? AND match_date <= ?
               ORDER BY match_date DESC, elo_id DESC LIMIT 1""",
            (tid, match_date),
        ).fetchone()
        total += row[0] if row else SEED_ELO
    return total / len(team_ids) if team_ids else SEED_ELO


def _get_opponent_elo_at_match(
    conn: sqlite3.Connection, team_id: int, match_id: int
) -> float:
    """Get the opponent's Elo just before a specific match."""
    match = conn.execute(
        "SELECT team1_id, team2_id, match_date FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    team1_id, team2_id, match_date = match
    opp_id = team2_id if team_id == team1_id else team1_id

    row = conn.execute(
        """SELECT elo_after FROM team_elo
           WHERE team_id = ? AND match_date < ?
           ORDER BY match_date DESC, elo_id DESC LIMIT 1""",
        (opp_id, match_date),
    ).fetchone()
    return row[0] if row else SEED_ELO


def map_strength(
    conn: sqlite3.Connection, team_id: int, map_id: int
) -> dict:
    """Compute the Map Strength Rating for a team on a specific map.

    Returns:
        {
            "rating": float | None (0.0-1.0, None if no data),
            "weighted_sample": float,
            "total_played": int,
            "low_confidence": bool,
        }
    """
    rows = conn.execute(
        """SELECT mr.match_id, mr.winner_team_id, mr.pick_context, mr.slot
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND mr.dq = 0
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date""",
        (map_id, team_id, team_id),
    ).fetchall()

    if not rows:
        return {
            "rating": None,
            "weighted_sample": 0.0,
            "total_played": 0,
            "low_confidence": True,
        }

    weighted_sum = 0.0
    weight_total = 0.0

    for match_id, winner_id, pick_context, slot in rows:
        result = 1.0 if winner_id == team_id else 0.0
        context_weight = CONTEXT_WEIGHTS.get(pick_context, 1.0)

        opp_elo = _get_opponent_elo_at_match(conn, team_id, match_id)
        league_avg = _get_league_avg_elo(conn, match_id)
        opponent_quality = opp_elo / league_avg if league_avg > 0 else 1.0

        weight = context_weight * opponent_quality
        weighted_sum += result * weight
        weight_total += weight

    rating = weighted_sum / weight_total if weight_total > 0 else None
    total_played = len(rows)

    return {
        "rating": rating,
        "weighted_sample": weight_total,
        "total_played": total_played,
        "low_confidence": total_played < LOW_SAMPLE_THRESHOLD,
    }


def all_team_map_strengths(
    conn: sqlite3.Connection,
) -> dict[tuple[int, int], dict]:
    """Compute Map Strength for every (team, map) pair.

    Returns dict keyed by (team_id, map_id) -> map_strength result dict.
    """
    teams = conn.execute("SELECT team_id FROM teams").fetchall()
    maps = conn.execute("SELECT map_id FROM maps").fetchall()

    result = {}
    for (team_id,) in teams:
        for (map_id,) in maps:
            result[(team_id, map_id)] = map_strength(conn, team_id, map_id)

    return result
