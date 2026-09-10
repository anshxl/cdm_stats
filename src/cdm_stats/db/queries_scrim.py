import sqlite3

from cdm_stats.metrics.filters import MatchFilter


def _opponent_condition(conditions: list, params: list, opponent: str | None) -> None:
    """Append an opponent-abbreviation filter against scrim_maps.opponent_id."""
    if opponent:
        conditions.append(
            "opponent_id = (SELECT team_id FROM teams WHERE abbreviation = ?)"
        )
        params.append(opponent)


def scrim_win_loss(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
    week_range: tuple[int, int] | None = None,
    f: MatchFilter = MatchFilter(),
    opponent: str | None = None,
) -> dict:
    """Return W, L, Win% for scrims with optional filters."""
    fw, fp = f.date_sql("scrim_date")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("map_name = ?")
        params.append(map_name)
    if week_range:
        conditions.append("week BETWEEN ? AND ?")
        params.extend(week_range)
    _opponent_condition(conditions, params, opponent)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    row = conn.execute(
        f"""SELECT
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                COUNT(*) as total
            FROM scrim_maps{where}""",
        params,
    ).fetchone()

    wins, losses, total = row[0] or 0, row[1] or 0, row[2] or 0
    win_pct = round(wins / total * 100, 2) if total > 0 else 0.0
    return {"wins": wins, "losses": losses, "total": total, "win_pct": win_pct}


def scrim_map_breakdown(
    conn: sqlite3.Connection,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    f: MatchFilter = MatchFilter(),
    opponent: str | None = None,
) -> list[dict]:
    """Return per-map: played, W, L, Win%, avg scores."""
    fw, fp = f.date_sql("scrim_date")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("week BETWEEN ? AND ?")
        params.extend(week_range)
    _opponent_condition(conditions, params, opponent)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT map_name, mode,
                   COUNT(*) as played,
                   SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                   ROUND(AVG(our_score - opponent_score), 1) as avg_margin
            FROM scrim_maps{where}
            GROUP BY map_name, mode
            ORDER BY mode, map_name""",
        params,
    ).fetchall()

    return [
        {
            "map_name": r[0], "mode": r[1], "played": r[2],
            "wins": r[3], "losses": r[4],
            "win_pct": round(r[3] / r[2] * 100, 2) if r[2] > 0 else 0.0,
            "avg_margin": r[5],
        }
        for r in rows
    ]


def scrim_weekly_trend(
    conn: sqlite3.Connection,
    mode: str | None = None,
    map_name: str | None = None,
    f: MatchFilter = MatchFilter(),
    opponent: str | None = None,
) -> list[dict]:
    """Return per-scrim-day win rate for the trend chart (keyed by match_date)."""
    fw, fp = f.date_sql("scrim_date")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if map_name:
        conditions.append("map_name = ?")
        params.append(map_name)
    _opponent_condition(conditions, params, opponent)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT scrim_date,
                   COUNT(*) as played,
                   SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins
            FROM scrim_maps{where}
            GROUP BY scrim_date
            ORDER BY scrim_date""",
        params,
    ).fetchall()

    return [
        {
            "match_date": r[0], "played": r[1], "wins": r[2],
            "win_pct": round(r[2] / r[1] * 100, 2) if r[1] > 0 else 0.0,
        }
        for r in rows
    ]


def scrim_map_results_detail(
    conn: sqlite3.Connection,
    map_name: str,
    week_range: tuple[int, int] | None = None,
    limit: int = 5,
    f: MatchFilter = MatchFilter(),
    opponent: str | None = None,
) -> list[dict]:
    """Return individual scrim results on a specific map, newest first.

    If week_range is given, returns all matches within that range (no limit).
    Otherwise returns up to `limit` most recent matches.
    """
    fw, fp = f.date_sql("sm.scrim_date")
    conditions = ["sm.map_name = ?" + fw]
    params: list = [map_name] + fp
    if week_range:
        conditions.append("sm.week BETWEEN ? AND ?")
        params.extend(week_range)
    if opponent:
        conditions.append("t.abbreviation = ?")
        params.append(opponent)

    # scrim_date is stored as text like '7-Jul', which sorts alphabetically —
    # week plus insertion id is the reliable chronological key.
    sql = f"""SELECT sm.scrim_date, sm.week, t.abbreviation,
                     sm.our_score, sm.opponent_score, sm.result
              FROM scrim_maps sm
              JOIN teams t ON sm.opponent_id = t.team_id
              WHERE {' AND '.join(conditions)}
              ORDER BY sm.week DESC, sm.scrim_map_id DESC"""
    if week_range is None:
        sql += f" LIMIT {int(limit)}"

    rows = conn.execute(sql, params).fetchall()
    return [
        {
            "date": r[0], "week": r[1], "opponent": r[2],
            "our_score": r[3], "opp_score": r[4], "result": r[5],
        }
        for r in rows
    ]


def player_summary(
    conn: sqlite3.Connection,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    f: MatchFilter = MatchFilter(),
) -> list[dict]:
    """Return per-player totals: kills, deaths, assists, K/D."""
    fw, fp = f.date_sql("sm.scrim_date")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if player:
        conditions.append("sp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("sm.mode = ?")
        params.append(mode)
    if week_range:
        conditions.append("sm.week BETWEEN ? AND ?")
        params.extend(week_range)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT sp.player_name,
                   SUM(sp.kills) as kills,
                   SUM(sp.deaths) as deaths,
                   SUM(sp.assists) as assists,
                   COUNT(*) as games,
                   ROUND(AVG(CAST(sp.kills + sp.assists AS REAL) / NULLIF(sp.kills + sp.deaths + sp.assists, 0) * 100), 1) as avg_pos_eng_pct
            FROM scrim_player_stats sp
            JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
            {where}
            GROUP BY sp.player_name
            ORDER BY sp.player_name""",
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
    f: MatchFilter = MatchFilter(),
) -> list[dict]:
    """Return per-scrim-day K/D per player for the trend chart (keyed by match_date)."""
    fw, fp = f.date_sql("sm.scrim_date")
    conditions = ["1=1" + fw]
    params: list = list(fp)

    if player:
        conditions.append("sp.player_name = ?")
        params.append(player)
    if mode:
        conditions.append("sm.mode = ?")
        params.append(mode)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT sp.player_name, sm.scrim_date,
                   SUM(sp.kills) as kills,
                   SUM(sp.deaths) as deaths
            FROM scrim_player_stats sp
            JOIN scrim_maps sm ON sp.scrim_map_id = sm.scrim_map_id
            {where}
            GROUP BY sp.player_name, sm.scrim_date
            ORDER BY sp.player_name, sm.scrim_date""",
        params,
    ).fetchall()

    return [
        {
            "player_name": r[0], "match_date": r[1],
            "kills": r[2], "deaths": r[3],
            "kd": round(r[2] / r[3], 2) if r[3] > 0 else 0.0,
        }
        for r in rows
    ]


