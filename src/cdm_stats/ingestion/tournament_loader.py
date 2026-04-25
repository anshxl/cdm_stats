import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    insert_match,
    insert_map_result,
    FORMAT_SLOT_MODES,
)
from cdm_stats.ingestion._helpers import insert_bans_for_match


FORMAT_WIN_THRESHOLD = {
    "TOURNAMENT_BO5": 3,
    "TOURNAMENT_BO7": 4,
}


def _group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        series = row.get("series", "1")
        key = (row["date"], row["team1"], row["team2"], series)
        matches.setdefault(key, []).append(row)
    return matches


def _is_duplicate_match(
    conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int, series_number: int
) -> bool:
    row = conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND series_number = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, series_number, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return row is not None


def _validate_tournament_match(
    conn: sqlite3.Connection, key: tuple, rows: list[dict], match_format: str
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr, *_ = key
    slot_modes = FORMAT_SLOT_MODES[match_format]
    win_threshold = FORMAT_WIN_THRESHOLD[match_format]

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    for i, row in enumerate(rows):
        slot = i + 1
        if slot not in slot_modes:
            errors.append(f"Slot {slot} not valid for format {match_format}")
            continue
        mode = slot_modes[slot]
        if get_map_id(conn, row["map"], mode) is None:
            errors.append(f"Unknown map '{row['map']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
    t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
    if max(t1_wins, t2_wins) != win_threshold:
        errors.append(
            f"No team reached {win_threshold} wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}"
        )

    return errors


def _ingest_maps(
    conn: sqlite3.Connection, grouped: dict[tuple, list[dict]]
) -> list[dict]:
    results = []
    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr, series_str = key
        series_number = int(series_str)
        match_format = rows[0]["format"]
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate_match(conn, date, team1_id, team2_id, series_number):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_tournament_match(conn, key, rows, match_format)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        slot_modes = FORMAT_SLOT_MODES[match_format]
        win_threshold = FORMAT_WIN_THRESHOLD[match_format]

        t1_series = 0
        t2_series = 0
        map_result_data = []

        for i, row in enumerate(rows):
            slot = i + 1
            mode = slot_modes[slot]
            map_id = get_map_id(conn, row["map"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            team1_score = int(row["team1_score"])
            team2_score = int(row["team2_score"])

            map_result_data.append((
                slot, map_id, None, winner_id,
                team1_score, team2_score,
                t1_series, t2_series, "Unknown",
            ))

            if winner_id == team1_id:
                t1_series += 1
            else:
                t2_series += 1

        series_winner_id = team1_id if t1_series == win_threshold else team2_id

        try:
            match_id = insert_match(
                conn, date, team1_id, team2_id,
                None,
                series_winner_id,
                match_format,
                series_number=series_number,
            )
            for data in map_result_data:
                insert_map_result(conn, match_id, *data)
            conn.commit()
            from cdm_stats.metrics.elo import update_elo
            update_elo(conn, match_id)
            results.append({"match": key, "status": "ok", "match_id": match_id})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results


def _ingest_bans(
    conn: sqlite3.Connection, grouped: dict[tuple, list[dict]]
) -> list[dict]:
    results = []
    for key, bans in grouped.items():
        date, team1_abbr, team2_abbr, series_str = key
        series_number = int(series_str)
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if not team1_id or not team2_id:
            results.append({"match": key, "status": "error", "errors": ["Unknown team"]})
            continue

        match = conn.execute(
            """SELECT match_id, match_format FROM matches
               WHERE match_date = ? AND series_number = ? AND (
                   (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
               )""",
            (date, series_number, team1_id, team2_id, team2_id, team1_id),
        ).fetchone()

        if not match:
            results.append({"match": key, "status": "error", "errors": ["Match not found — ingest maps first"]})
            continue

        match_id, match_format = match
        result = insert_bans_for_match(conn, match_id, match_format, bans, team1_abbr, team2_abbr)
        result["match"] = key
        results.append(result)

    return results


def ingest_tournament(
    conn: sqlite3.Connection, maps_file: IO[str], bans_file: IO[str]
) -> list[dict]:
    maps_reader = csv.DictReader(maps_file)
    maps_grouped = _group_rows_by_match(maps_reader)
    map_results = _ingest_maps(conn, maps_grouped)

    bans_reader = csv.DictReader(bans_file)
    bans_grouped = _group_rows_by_match(bans_reader)
    ban_results = _ingest_bans(conn, bans_grouped)

    return map_results + ban_results
