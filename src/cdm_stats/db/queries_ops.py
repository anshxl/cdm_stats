import sqlite3

from cdm_stats.metrics.filters import MatchFilter


def ops_player_weekly_trend(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    f: MatchFilter = MatchFilter(),
    match_ids: list[int] | None = None,
) -> list[dict]:
    """Return per-match-day operator kills per pull per player, for the trend chart.

    Within a day, kills_per_pull is SUM(kills) / SUM(pulls) across that day's
    maps — not the mean of per-map rates, so a map with one pull doesn't weigh
    the same as a map with five. It is None for a day on which the player
    never pulled, which the chart renders as a gap rather than a zero.

    Keyed by match_date (not week) so seasons do not collide on the x-axis.
    """
    fw, fp = f.sql("mt")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if player:
        conditions.append("op.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)

    if match_ids is not None:
        conditions.append(f"mt.match_id IN ({','.join('?' * len(match_ids))})")
        params.extend(match_ids)
    where = " WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT op.player_name, mt.match_date,
                   SUM(op.op_kills) as op_kills,
                   SUM(op.op_pulls) as op_pulls,
                   COUNT(DISTINCT op.result_id) as maps
            FROM ops_player_stats op
            JOIN map_results mr ON op.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
            {where}
            GROUP BY op.player_name, mt.match_date
            ORDER BY op.player_name, mt.match_date""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "match_date": r[1],
            "op_kills": r[2], "op_pulls": r[3], "maps": r[4],
            "kills_per_pull": round(r[2] / r[3], 2) if r[3] else None,
        }
        for r in rows
    ]
