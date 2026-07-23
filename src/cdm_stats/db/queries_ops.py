import sqlite3


def ops_player_weekly_trend(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    season: int = 1,
) -> list[dict]:
    """Return per-week operator kills per pull per player, for the trend chart.

    Within a week, kills_per_pull is SUM(kills) / SUM(pulls) across that week's
    maps — not the mean of per-map rates, so a map with one pull doesn't weigh
    the same as a map with five. It is None for a week in which the player
    never pulled, which the chart renders as a gap rather than a zero.

    Mirrors queries_tournament_player.player_weekly_trend: no week_range filter,
    because a trend shows the whole season.
    """
    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("op.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)

    where = " WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT op.player_name, op.week,
                   SUM(op.op_kills) as op_kills,
                   SUM(op.op_pulls) as op_pulls,
                   COUNT(DISTINCT op.result_id) as maps
            FROM ops_player_stats op
            JOIN map_results mr ON op.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
            {where}
            GROUP BY op.player_name, op.week
            ORDER BY op.player_name, op.week""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "week": r[1],
            "op_kills": r[2], "op_pulls": r[3], "maps": r[4],
            "kills_per_pull": round(r[2] / r[3], 2) if r[3] else None,
        }
        for r in rows
    ]
