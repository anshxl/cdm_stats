import sqlite3


def pick_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND dq = 0""",
        (team_id, team_id, team_id, map_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def defend_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE picked_by_team_id IS NOT NULL
             AND picked_by_team_id != ?
             AND map_id = ?
             AND mr.dq = 0
             AND (m.team1_id = ? OR m.team2_id = ?)""",
        (team_id, team_id, team_id, map_id, team_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}



def pick_context_distribution(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict[str, int]:
    """Breakdown of how often a team picks this map in each context."""
    rows = conn.execute(
        """SELECT pick_context, COUNT(*)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND dq = 0
           GROUP BY pick_context""",
        (team_id, map_id),
    ).fetchall()
    result = {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
    for ctx, count in rows:
        if ctx in result:
            result[ctx] = count
    return result
