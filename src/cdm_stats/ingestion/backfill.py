import sqlite3
from cdm_stats.metrics.elo import update_elo


def backfill_elo(conn: sqlite3.Connection) -> int:
    """Wipe all Elo history and recalculate from matches in chronological order.
    Returns the number of matches processed."""
    conn.execute("DELETE FROM team_elo")
    conn.commit()

    matches = conn.execute(
        "SELECT match_id FROM matches ORDER BY match_date, match_id"
    ).fetchall()

    for (match_id,) in matches:
        update_elo(conn, match_id)

    return len(matches)
