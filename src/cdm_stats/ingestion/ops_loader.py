import csv
import sqlite3
from datetime import date as _date
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr, get_map_id
from cdm_stats.ingestion._helpers import resolve_result_id


def ingest_ops_kills(
    conn: sqlite3.Connection,
    file: IO,
) -> list[dict]:
    """Ingest operator kills/pulls CSV (footage-derived, our roster only).

    CSV columns: Date, Opponent, Map, Player, OpKills, OpPulls, FootageMin.
    Week is optional and defaults to the ISO week of the date. The derived
    OpKillsPerMin / OpKillsPerPull columns are ignored — they're recomputed at
    query time. Matches must already be ingested.
    """
    reader = csv.DictReader(file)
    results: list[dict] = []

    for row in reader:
        date = row["Date"].strip()
        week_raw = (row.get("Week") or "").strip()
        week = int(week_raw) if week_raw else _date.fromisoformat(date).isocalendar()[1]
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        player_name = row["Player"].strip()
        op_kills = int(row["OpKills"].strip())
        op_pulls = int(row["OpPulls"].strip())
        footage_min = float(row["FootageMin"].strip())

        desc = f"{date} {map_name} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if opponent_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        map_id = get_map_id(conn, map_name, None)
        if map_id is None:
            results.append({"status": "error", "row": desc,
                            "errors": f"Unknown map: {map_name}"})
            continue

        result_id, reason = resolve_result_id(
            conn, date, opponent_id, map_id, "ops_player_stats", player_name,
        )
        if result_id is None:
            if reason == "duplicate":
                results.append({"status": "skipped", "row": desc})
            else:
                results.append({"status": "error", "row": desc, "errors": reason})
            continue

        conn.execute(
            """INSERT INTO ops_player_stats
               (result_id, week, player_name, op_kills, op_pulls, footage_min)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (result_id, week, player_name, op_kills, op_pulls, footage_min),
        )
        results.append({"status": "ok", "row": desc})

    conn.commit()
    return results
