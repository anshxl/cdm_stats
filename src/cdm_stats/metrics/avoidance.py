import sqlite3

from cdm_stats.metrics.filters import MatchFilter


def pick_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int, f: MatchFilter = MatchFilter()) -> dict:
    fw, fp = f.sql()
    row = conn.execute(
        f"""SELECT
               SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.picked_by_team_id = ? AND mr.map_id = ? AND mr.dq = 0{fw}""",
        [team_id, team_id, team_id, map_id] + fp,
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def defend_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int, f: MatchFilter = MatchFilter()) -> dict:
    fw, fp = f.sql()
    row = conn.execute(
        f"""SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE picked_by_team_id IS NOT NULL
             AND picked_by_team_id != ?
             AND map_id = ?
             AND mr.dq = 0
             AND (m.team1_id = ? OR m.team2_id = ?){fw}""",
        [team_id, team_id, team_id, map_id, team_id, team_id] + fp,
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}



