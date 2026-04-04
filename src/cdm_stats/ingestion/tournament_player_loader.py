import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr, get_map_id


def ingest_tournament_players(
    conn: sqlite3.Connection,
    file: IO,
) -> list[dict]:
    """Ingest tournament player-level CSV.

    CSV columns: Date, Week, Opponent, Map, Mode, Player, Kills, Deaths, Assists.
    Matches are resolved by (match_date, opponent_id appears on either side,
    map_id) — unique in practice because a team can't play the same map in two
    different series on the same day. Matches must already be ingested.
    """
    reader = csv.DictReader(file)
    results: list[dict] = []

    for row in reader:
        date = row["Date"].strip()
        week = int(row["Week"].strip())
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())

        desc = f"{date} {map_name} {mode} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if opponent_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        map_id = get_map_id(conn, map_name, mode)
        if map_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown map: {map_name} ({mode})"})
            continue

        # Find the map_result: match on date where opponent is on either side,
        # then the specific map in that match. Unique in practice.
        result_rows = conn.execute(
            """SELECT mr.result_id
               FROM map_results mr
               JOIN matches mt ON mr.match_id = mt.match_id
               WHERE mt.match_date = ?
                 AND (mt.team1_id = ? OR mt.team2_id = ?)
                 AND mr.map_id = ?""",
            (date, opponent_id, opponent_id, map_id),
        ).fetchall()

        if not result_rows:
            results.append({"status": "error", "row": desc,
                            "errors": "No matching map_result found"})
            continue
        if len(result_rows) > 1:
            results.append({"status": "error", "row": desc,
                            "errors": "Multiple matching map_results — ambiguous"})
            continue

        result_id = result_rows[0][0]

        existing = conn.execute(
            "SELECT stat_id FROM tournament_player_stats WHERE result_id = ? AND player_name = ?",
            (result_id, player_name),
        ).fetchone()

        if existing:
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
