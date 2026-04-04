import sqlite3


def player_summary(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return per-player totals: kills, deaths, assists, K/D."""
    conditions = []
    params: list = []

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT tp.player_name,
                   SUM(tp.kills) as kills,
                   SUM(tp.deaths) as deaths,
                   SUM(tp.assists) as assists,
                   COUNT(*) as games,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.kills + tp.deaths + tp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY tp.player_name
            ORDER BY tp.player_name""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "kills": r[1], "deaths": r[2], "assists": r[3],
            "games": r[4],
            "kd": round(r[1] / r[2], 2) if r[2] > 0 else 0.0,
            "avg_pos_eng_pct": r[5] or 0.0,
        }
        for r in rows
    ]


def player_weekly_trend(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
) -> list[dict]:
    """Return per-week K/D per player for trend chart."""
    conditions = []
    params: list = []

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT tp.player_name, tp.week,
                   SUM(tp.kills) as kills,
                   SUM(tp.deaths) as deaths
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY tp.player_name, tp.week
            ORDER BY tp.player_name, tp.week""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "week": r[1],
            "kills": r[2], "deaths": r[3],
            "kd": round(r[2] / r[3], 2) if r[3] > 0 else 0.0,
        }
        for r in rows
    ]


def player_map_breakdown(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
) -> list[dict]:
    """Return per-map player averages."""
    conditions = []
    params: list = []

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT m.map_name, m.mode,
                   COUNT(DISTINCT tp.result_id) as games,
                   ROUND(AVG(tp.kills), 1) as avg_kills,
                   ROUND(AVG(tp.deaths), 1) as avg_deaths,
                   ROUND(AVG(tp.assists), 1) as avg_assists,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.deaths, 0)), 2) as avg_kd,
                   ROUND(AVG(CAST(tp.kills AS REAL) / NULLIF(tp.kills + tp.deaths + tp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            {where}
            GROUP BY m.map_name, m.mode
            ORDER BY m.mode, m.map_name""",
        params,
    ).fetchall()

    return [
        {
            "map_name": r[0], "mode": r[1], "games": r[2],
            "avg_kills": r[3], "avg_deaths": r[4], "avg_assists": r[5],
            "avg_kd": r[6] or 0.0, "avg_pos_eng_pct": r[7] or 0.0,
        }
        for r in rows
    ]
