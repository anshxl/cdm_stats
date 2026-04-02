import sqlite3


def dominance_flag(mode: str, winner_score: int, loser_score: int) -> str | None:
    margin = winner_score - loser_score
    if mode == "SnD":
        if margin >= 5:
            return "Dominant"
        if margin == 1:
            return "Contested"
    elif mode == "HP":
        if margin >= 70:
            return "Dominant"
        if margin < 25:
            return "Contested"
    elif mode == "Control":
        if margin >= 3:
            return "Dominant"
        if margin == 1:
            return "Contested"
    return None


def score_margins(conn: sqlite3.Connection, team_id: int, map_id: int) -> list[dict]:
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]

    rows = conn.execute(
        """SELECT mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score,
                  mr.match_id, mr.slot
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.map_id = ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           ORDER BY m.match_date""",
        (map_id, team_id, team_id),
    ).fetchall()

    results = []
    for winner_id, pick_score, non_pick_score, match_id, slot in rows:
        winner_score = max(pick_score, non_pick_score)
        loser_score = min(pick_score, non_pick_score)
        margin = winner_score - loser_score
        if winner_id != team_id:
            margin = -margin
        results.append({
            "match_id": match_id,
            "slot": slot,
            "margin": margin,
            "won": winner_id == team_id,
            "dominance": dominance_flag(mode, winner_score, loser_score) if winner_id == team_id else None,
        })
    return results
