import csv
import sqlite3
from datetime import date as _date
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr, get_map_id


def ingest_tournament_players(
    conn: sqlite3.Connection,
    file: IO,
) -> list[dict]:
    """Ingest tournament player-level CSV.

    CSV columns: Date, Opponent, Map, Player, Kills, Deaths, Assists.
    Week and Mode are optional (s1 format includes them; s2 dropped them):
    Mode is derived from the map (maps are mode-exclusive), and Week defaults
    to the ISO week of the date when absent.
    Matches are resolved by (match_date, opponent_id appears on either side,
    map_id) — unique in practice because a team can't play the same map in two
    different series on the same day. Matches must already be ingested.
    """
    reader = csv.DictReader(file)
    results: list[dict] = []

    for row in reader:
        date = row["Date"].strip()
        week_raw = (row.get("Week") or "").strip()
        week = int(week_raw) if week_raw else _date.fromisoformat(date).isocalendar()[1]
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = (row.get("Mode") or "").strip() or None
        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())

        desc = f"{date} {map_name} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if opponent_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        map_id = get_map_id(conn, map_name, mode)
        if map_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown map: {map_name}"})
            continue

        # Find the map_result: match on date where opponent is on either side,
        # then the specific map in that match. A map can recur across same-day
        # series (e.g. UB then Finals), so resolve sequentially: take the first
        # matching map_result (in match/slot order) not yet filled for this
        # player. CSV rows are authored in that same order, so the Nth CSV
        # occurrence lands in the Nth series. This also subsumes dup-skip.
        result_rows = conn.execute(
            """SELECT mr.result_id
               FROM map_results mr
               JOIN matches mt ON mr.match_id = mt.match_id
               WHERE mt.match_date = ?
                 AND (mt.team1_id = ? OR mt.team2_id = ?)
                 AND mr.map_id = ?
               ORDER BY mt.match_id, mr.slot""",
            (date, opponent_id, opponent_id, map_id),
        ).fetchall()

        if not result_rows:
            results.append({"status": "error", "row": desc,
                            "errors": "No matching map_result found"})
            continue

        result_id = None
        for (rid,) in result_rows:
            already = conn.execute(
                "SELECT 1 FROM tournament_player_stats WHERE result_id = ? AND player_name = ?",
                (rid, player_name),
            ).fetchone()
            if not already:
                result_id = rid
                break

        if result_id is None:
            # every matching map_result already has this player → duplicate row
            results.append({"status": "skipped", "row": desc})
            continue

        conn.execute(
            """INSERT INTO tournament_player_stats
               (result_id, week, player_name, kills, deaths, assists)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (result_id, week, player_name, kills, deaths, assists),
        )
        results.append({"status": "ok", "row": desc})

    conn.commit()
    return results
