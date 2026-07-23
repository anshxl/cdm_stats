import sqlite3


def ops_player_summary(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
) -> list[dict]:
    """Return per-player operator totals, ranked by kills per pull.

    kills_per_pull is SUM(kills) / SUM(pulls), not the mean of per-map rates —
    a map with one pull shouldn't weigh the same as a map with five. It is None
    when a player has no pulls at all in the filtered set.
    """
    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("op.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("op.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = " WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT op.player_name,
                   SUM(op.op_kills) as op_kills,
                   SUM(op.op_pulls) as op_pulls,
                   COUNT(DISTINCT op.result_id) as maps
            FROM ops_player_stats op
            JOIN map_results mr ON op.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
            {where}
            GROUP BY op.player_name""",
        params,
    ).fetchall()

    summary = [
        {
            "player_name": r[0], "op_kills": r[1], "op_pulls": r[2], "maps": r[3],
            "kills_per_pull": round(r[1] / r[2], 2) if r[2] else None,
        }
        for r in rows
    ]
    # Players with no pulls sort last — they have no conversion rate to rank.
    summary.sort(key=lambda d: (d["kills_per_pull"] is None, -(d["kills_per_pull"] or 0)))
    return summary
