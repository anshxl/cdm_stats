import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr
from cdm_stats.ingestion._helpers import insert_bans_for_match


def _group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["team1"], row["team2"])
        matches.setdefault(key, []).append(row)
    return matches


def _find_match(
    conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int
) -> tuple[int, str] | None:
    row = conn.execute(
        """SELECT match_id, match_format FROM matches
           WHERE match_date = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return (row[0], row[1]) if row else None


def ingest_playoff_bans(
    conn: sqlite3.Connection, file: IO[str]
) -> list[dict]:
    reader = csv.DictReader(file)
    grouped = _group_rows_by_match(reader)
    results = []

    for key, bans in grouped.items():
        date, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id is None or team2_id is None:
            missing = [a for a, i in ((team1_abbr, team1_id), (team2_abbr, team2_id)) if i is None]
            results.append({
                "match": key,
                "status": "error",
                "errors": [f"Unknown team abbreviation: {', '.join(missing)}"],
            })
            continue

        match = _find_match(conn, date, team1_id, team2_id)
        if match is None:
            results.append({
                "match": key,
                "status": "error",
                "errors": ["Match not found — ingest match first"],
            })
            continue

        match_id, match_format = match
        result = insert_bans_for_match(conn, match_id, match_format, bans, team1_abbr, team2_abbr)
        result["match"] = key
        results.append(result)

    return results
