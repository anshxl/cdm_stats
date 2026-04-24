import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr, insert_map_ban
from cdm_stats.ingestion.tournament_loader import (
    FORMAT_BAN_MODES,
    FORMAT_EXPECTED_BANS,
)


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


def _validate_bans(
    conn: sqlite3.Connection,
    bans: list[dict],
    match_format: str,
    team1_abbr: str,
    team2_abbr: str,
) -> list[str]:
    errors = []
    allowed_modes = FORMAT_BAN_MODES[match_format]
    expected_count = FORMAT_EXPECTED_BANS[match_format]

    if len(bans) != expected_count:
        errors.append(
            f"Expected {expected_count} bans for {match_format}, got {len(bans)}"
        )

    for ban in bans:
        if ban["banned_by"] not in (team1_abbr, team2_abbr):
            errors.append(f"Ban by '{ban['banned_by']}' is not one of the teams")
        map_row = conn.execute(
            "SELECT mode FROM maps WHERE map_name = ?", (ban["map"],)
        ).fetchone()
        if map_row is None:
            errors.append(f"Unknown map: {ban['map']}")
        elif map_row[0] not in allowed_modes:
            errors.append(
                f"Map '{ban['map']}' is mode '{map_row[0]}', not allowed for "
                f"{match_format} bans (allowed: {sorted(allowed_modes)})"
            )

    return errors


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

        existing = conn.execute(
            "SELECT COUNT(*) FROM map_bans WHERE match_id = ?", (match_id,)
        ).fetchone()[0]
        if existing > 0:
            results.append({
                "match": key,
                "status": "skipped",
                "reason": "bans already exist",
            })
            continue

        errors = _validate_bans(conn, bans, match_format, team1_abbr, team2_abbr)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        try:
            for ban in bans:
                ban_team_id = get_team_id_by_abbr(conn, ban["banned_by"])
                map_row = conn.execute(
                    "SELECT map_id FROM maps WHERE map_name = ?", (ban["map"],)
                ).fetchone()
                insert_map_ban(conn, match_id, ban_team_id, map_row[0])
            conn.commit()
            results.append({"match": key, "status": "ok", "bans": len(bans)})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results
