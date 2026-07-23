import sqlite3


def player_summary(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
) -> list[dict]:
    """Return per-player totals: kills, deaths, assists, K/D."""
    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = " WHERE " + " AND ".join(conditions)

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
            JOIN matches mt ON mr.match_id = mt.match_id
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
    season: int = 1,
) -> list[dict]:
    """Return per-week K/D per player for trend chart."""
    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)

    where = " WHERE " + " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT tp.player_name, tp.week,
                   SUM(tp.kills) as kills,
                   SUM(tp.deaths) as deaths
            FROM tournament_player_stats tp
            JOIN map_results mr ON tp.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
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


def recent_map_stats(
    conn: sqlite3.Connection,
    your_team: str,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
    limit: int = 5,
) -> list[dict]:
    """Return the most recent maps, newest first, with raw per-player stats.

    A map qualifies if it has scoreboard stats OR operator stats — the two are
    ingested independently and footage lags the scoreboard, so restricting to
    either one alone would hide maps that have real data. Any individual stat
    is None when its source hasn't been ingested for that player and map.

    `your_team` is the abbreviation to exclude when naming the opponent; we sit
    on either side of a match, so it can't be inferred from team1/team2.
    """
    # Player/map identity from both stat sources, so neither can hide a map.
    stat_results = """
        SELECT result_id, week, player_name FROM tournament_player_stats
        UNION
        SELECT result_id, week, player_name FROM ops_player_stats
    """

    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("sr.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("sr.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = " WHERE " + " AND ".join(conditions)

    maps = conn.execute(
        f"""SELECT mr.result_id, mt.match_date, m.map_name, m.mode,
                   CASE WHEN t1.abbreviation = ? THEN t2.abbreviation
                        ELSE t1.abbreviation END as opponent
            FROM ({stat_results}) sr
            JOIN map_results mr ON sr.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
            JOIN teams t1 ON mt.team1_id = t1.team_id
            JOIN teams t2 ON mt.team2_id = t2.team_id
            {where}
            GROUP BY mr.result_id
            ORDER BY mt.match_date DESC, mt.match_id DESC, mr.slot DESC
            LIMIT ?""",
        [your_team] + params + [limit],
    ).fetchall()

    if not maps:
        return []

    result_ids = [r[0] for r in maps]
    placeholders = ",".join("?" * len(result_ids))
    player_clause = " AND sr.player_name = ?" if player else ""

    rows = conn.execute(
        f"""SELECT sr.result_id, sr.player_name,
                   tp.kills, tp.deaths, tp.assists, op.op_kills, op.op_pulls
            FROM ({stat_results}) sr
            LEFT JOIN tournament_player_stats tp
                   ON tp.result_id = sr.result_id
                  AND tp.player_name = sr.player_name
            LEFT JOIN ops_player_stats op
                   ON op.result_id = sr.result_id
                  AND op.player_name = sr.player_name
            WHERE sr.result_id IN ({placeholders}){player_clause}
            GROUP BY sr.result_id, sr.player_name
            ORDER BY sr.player_name""",
        result_ids + ([player] if player else []),
    ).fetchall()

    by_result: dict[int, list[dict]] = {}
    for r in rows:
        by_result.setdefault(r[0], []).append({
            "player_name": r[1], "kills": r[2], "deaths": r[3], "assists": r[4],
            "op_kills": r[5], "op_pulls": r[6],
        })

    return [
        {
            "result_id": m[0], "match_date": m[1], "map_name": m[2],
            "mode": m[3], "opponent": m[4], "players": by_result.get(m[0], []),
        }
        for m in maps
    ]


def player_map_breakdown(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
) -> list[dict]:
    """Return per-map player averages."""
    conditions = ["mt.season = ?"]
    params: list = [season]

    if player:
        conditions.append("tp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("m.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("tp.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = " WHERE " + " AND ".join(conditions)

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
            JOIN matches mt ON mr.match_id = mt.match_id
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
