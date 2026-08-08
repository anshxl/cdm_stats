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
                   ROUND(AVG(CAST(tp.kills + tp.assists AS REAL) / NULLIF(tp.kills + tp.deaths + tp.assists, 0) * 100), 1) as avg_pos_eng_pct
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


def recent_series_stats(
    conn: sqlite3.Connection,
    your_team: str,
    player: str | None = None,
    mode: str | None = None,
    week_range: tuple[int, int] | None = None,
    season: int = 1,
    limit: int | None = 10,
    opponent: str | None = None,
) -> list[dict]:
    """Return the most recent series, newest first, each with its maps and
    raw per-player stats.

    `limit=None` returns every qualifying series. `opponent` (an abbreviation)
    restricts to series against that team.

    A series qualifies if any of its maps has scoreboard stats OR operator
    stats — the two are ingested independently and footage lags the
    scoreboard, so restricting to either one alone would hide real data. Any
    individual stat is None when its source hasn't been ingested. The player
    and mode filters restrict which maps appear in a series, but the series
    score (`our_maps`/`their_maps`) always counts every map played.

    `your_team` is the abbreviation to exclude when naming the opponent; we sit
    on either side of a match, so it can't be inferred from team1/team2.
    """
    our_id = conn.execute(
        "SELECT team_id FROM teams WHERE abbreviation = ?", (your_team,)
    ).fetchone()[0]

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
    if opponent:
        conditions.append("? IN (t1.abbreviation, t2.abbreviation)")
        params.append(opponent)

    where = " WHERE " + " AND ".join(conditions)
    limit_clause = "LIMIT ?" if limit is not None else ""
    limit_params = [limit] if limit is not None else []

    series = conn.execute(
        f"""SELECT mt.match_id, mt.match_date,
                   CASE WHEN t1.abbreviation = ? THEN t2.abbreviation
                        ELSE t1.abbreviation END as opponent
            FROM ({stat_results}) sr
            JOIN map_results mr ON sr.result_id = mr.result_id
            JOIN maps m ON mr.map_id = m.map_id
            JOIN matches mt ON mr.match_id = mt.match_id
            JOIN teams t1 ON mt.team1_id = t1.team_id
            JOIN teams t2 ON mt.team2_id = t2.team_id
            {where}
            GROUP BY mt.match_id
            ORDER BY mt.match_date DESC, mt.match_id DESC
            {limit_clause}""",
        [your_team] + params + limit_params,
    ).fetchall()

    if not series:
        return []

    match_ids = [r[0] for r in series]
    m_placeholders = ",".join("?" * len(match_ids))

    # Every map of the selected series — the series score must count maps the
    # mode/player filters hide from display.
    map_rows = conn.execute(
        f"""SELECT mr.match_id, mr.result_id, mr.slot, m.map_name, m.mode,
                   mr.winner_team_id, mr.picked_by_team_id,
                   mr.picking_team_score, mr.non_picking_team_score
            FROM map_results mr
            JOIN maps m ON mr.map_id = m.map_id
            WHERE mr.match_id IN ({m_placeholders})
            ORDER BY mr.match_id, mr.slot""",
        match_ids,
    ).fetchall()

    maps_by_match: dict[int, list[dict]] = {}
    for match_id, result_id, slot, map_name, map_mode, winner_id, picker_id, pick_score, non_pick_score in map_rows:
        won = winner_id == our_id
        if picker_id is not None:
            our_score = pick_score if picker_id == our_id else non_pick_score
        else:
            # Slot-5 coin toss has no picker; the winner always holds the
            # higher score in every mode, so orient by the result.
            our_score = max(pick_score, non_pick_score) if won else min(pick_score, non_pick_score)
        their_score = pick_score + non_pick_score - our_score
        maps_by_match.setdefault(match_id, []).append({
            "result_id": result_id, "slot": slot, "map_name": map_name,
            "mode": map_mode, "won": won,
            "our_score": our_score, "their_score": their_score,
        })

    all_result_ids = [m["result_id"] for maps in maps_by_match.values() for m in maps]
    r_placeholders = ",".join("?" * len(all_result_ids))
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
            WHERE sr.result_id IN ({r_placeholders}){player_clause}
            GROUP BY sr.result_id, sr.player_name
            ORDER BY sr.player_name""",
        all_result_ids + ([player] if player else []),
    ).fetchall()

    by_result: dict[int, list[dict]] = {}
    for r in rows:
        by_result.setdefault(r[0], []).append({
            "player_name": r[1], "kills": r[2], "deaths": r[3], "assists": r[4],
            "op_kills": r[5], "op_pulls": r[6],
        })

    out = []
    for match_id, match_date, opp in series:
        all_maps = maps_by_match.get(match_id, [])
        shown = [
            {**m, "players": by_result[m["result_id"]]}
            for m in all_maps
            if m["result_id"] in by_result and (not mode or m["mode"] == mode)
        ]
        out.append({
            "match_id": match_id, "match_date": match_date, "opponent": opp,
            "our_maps": sum(1 for m in all_maps if m["won"]),
            "their_maps": sum(1 for m in all_maps if not m["won"]),
            "maps": shown,
        })
    return out


