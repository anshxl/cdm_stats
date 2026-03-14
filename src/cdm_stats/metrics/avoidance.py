import sqlite3
from cdm_stats.db.queries import SLOT_MODES


def pick_win_loss(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    row = conn.execute(
        """SELECT
               SUM(CASE WHEN winner_team_id = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN winner_team_id != ? THEN 1 ELSE 0 END)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND slot != 5""",
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
             AND slot != 5
             AND (m.team1_id = ? OR m.team2_id = ?)""",
        (team_id, team_id, team_id, map_id, team_id, team_id),
    ).fetchone()
    return {"wins": row[0] or 0, "losses": row[1] or 0}


def _get_pick_opportunities(conn: sqlite3.Connection, team_id: int, mode: str) -> list[dict]:
    """Get all slots where team_id had pick priority for the given mode."""
    valid_slots = [s for s, m in SLOT_MODES.items() if m == mode and s != 5]

    rows = conn.execute(
        """SELECT mr.match_id, mr.slot, mr.picked_by_team_id, mr.map_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE (m.team1_id = ? OR m.team2_id = ?)
             AND mr.slot IN ({})
             AND mr.slot != 5
           ORDER BY mr.match_id, mr.slot""".format(",".join("?" * len(valid_slots))),
        (team_id, team_id, *valid_slots),
    ).fetchall()

    opportunities = []
    for row in rows:
        match_id, slot, picker_id, map_id = row
        if picker_id == team_id:
            opportunities.append({"match_id": match_id, "slot": slot, "map_id": map_id})
    return opportunities


def avoidance_index(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    """How often team avoids this map when they have pick priority on its mode."""
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]
    opportunities = _get_pick_opportunities(conn, team_id, mode)
    if not opportunities:
        return {"ratio": 0.0, "opportunities": 0}

    avoided = sum(1 for opp in opportunities if opp["map_id"] != map_id)
    return {"ratio": avoided / len(opportunities), "opportunities": len(opportunities)}


def target_index(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict:
    """How often opponents avoid this map when picking against this team."""
    mode = conn.execute("SELECT mode FROM maps WHERE map_id = ?", (map_id,)).fetchone()[0]
    valid_slots = [s for s, m in SLOT_MODES.items() if m == mode and s != 5]

    rows = conn.execute(
        """SELECT mr.match_id, mr.slot, mr.picked_by_team_id, mr.map_id
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE (m.team1_id = ? OR m.team2_id = ?)
             AND mr.picked_by_team_id IS NOT NULL
             AND mr.picked_by_team_id != ?
             AND mr.slot IN ({})
             AND mr.slot != 5
           ORDER BY mr.match_id, mr.slot""".format(",".join("?" * len(valid_slots))),
        (team_id, team_id, team_id, *valid_slots),
    ).fetchall()

    if not rows:
        return {"ratio": 0.0, "opportunities": 0}

    avoided = sum(1 for r in rows if r[3] != map_id)
    return {"ratio": avoided / len(rows), "opportunities": len(rows)}


def pick_context_distribution(conn: sqlite3.Connection, team_id: int, map_id: int) -> dict[str, int]:
    """Breakdown of how often a team picks this map in each context."""
    rows = conn.execute(
        """SELECT pick_context, COUNT(*)
           FROM map_results
           WHERE picked_by_team_id = ? AND map_id = ? AND slot != 5
           GROUP BY pick_context""",
        (team_id, map_id),
    ).fetchall()
    result = {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
    for ctx, count in rows:
        if ctx in result:
            result[ctx] = count
    return result
