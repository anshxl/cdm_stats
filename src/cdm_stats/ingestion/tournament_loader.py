import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
    insert_map_ban,
    FORMAT_SLOT_MODES,
)
from cdm_stats.ingestion.csv_loader import derive_pick_context


FORMAT_WIN_THRESHOLD = {
    "TOURNAMENT_BO5": 3,
    "TOURNAMENT_BO7": 4,
    "CDL_PLAYOFF_BO5": 3,
    "CDL_PLAYOFF_BO7": 4,
}

FORMAT_BAN_MODES = {
    "TOURNAMENT_BO5": {"HP", "SnD", "Control"},
    "TOURNAMENT_BO7": {"HP", "SnD"},
    "CDL_PLAYOFF_BO5": {"HP", "SnD", "Control"},
    "CDL_PLAYOFF_BO7": {"HP", "SnD"},
}

FORMAT_EXPECTED_BANS = {
    "TOURNAMENT_BO5": 6,
    "TOURNAMENT_BO7": 4,
    "CDL_PLAYOFF_BO5": 6,
    "CDL_PLAYOFF_BO7": 4,
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


def _validate_bans(
    conn: sqlite3.Connection, bans: list[dict], match_format: str,
    team1_abbr: str, team2_abbr: str
) -> list[str]:
    errors = []
    allowed_modes = FORMAT_BAN_MODES[match_format]
    expected_count = FORMAT_EXPECTED_BANS[match_format]

    if len(bans) != expected_count:
        errors.append(f"Expected {expected_count} bans for {match_format}, got {len(bans)}")

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
                f"Map '{ban['map']}' is mode '{map_row[0]}', not allowed for {match_format} bans "
                f"(allowed: {allowed_modes})"
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
        is_playoff = match_format.startswith("CDL_PLAYOFF")
        higher_seed_id = None
        if is_playoff:
            higher_seed_abbr = rows[0].get("higher_seed")
            higher_seed_id = get_team_id_by_abbr(conn, higher_seed_abbr) if higher_seed_abbr else None

        t1_series = 0
        t2_series = 0
        map_result_data = []
        max_slot = max(slot_modes.keys())

        for i, row in enumerate(rows):
            slot = i + 1
            mode = slot_modes[slot]
            map_id = get_map_id(conn, row["map"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            team1_score = int(row["team1_score"])
            team2_score = int(row["team2_score"])

            if is_playoff and higher_seed_id:
                # Playoff: alternating picks by seed, last slot is coin toss
                if slot == max_slot:
                    picker_id = None
                elif slot % 2 == 1:
                    picker_id = higher_seed_id
                else:
                    other_id = team2_id if higher_seed_id == team1_id else team1_id
                    picker_id = other_id

                # Scores oriented by picker
                if picker_id is None:
                    picking_team_score = team1_score
                    non_picking_team_score = team2_score
                    pick_context = "Coin-Toss"
                else:
                    if picker_id == team1_id:
                        picking_team_score = team1_score
                        non_picking_team_score = team2_score
                        pick_context = derive_pick_context(
                            slot, t1_series, t2_series,
                            win_threshold=win_threshold
                        )
                    else:
                        picking_team_score = team2_score
                        non_picking_team_score = team1_score
                        pick_context = derive_pick_context(
                            slot, t2_series, t1_series,
                            win_threshold=win_threshold
                        )
            else:
                # Tournament: no picks known
                picker_id = None
                picking_team_score = team1_score
                non_picking_team_score = team2_score
                pick_context = "Unknown"

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context,
            ))

            if winner_id == team1_id:
                t1_series += 1
            else:
                t2_series += 1

        series_winner_id = team1_id if t1_series == win_threshold else team2_id

        try:
            match_id = insert_match(
                conn, date, team1_id, team2_id,
                higher_seed_id,  # None for tournament, higher seed for playoff
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
        match_format = bans[0]["format"]
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if not team1_id or not team2_id:
            results.append({"match": key, "status": "error", "errors": ["Unknown team"]})
            continue

        match = conn.execute(
            """SELECT match_id FROM matches
               WHERE match_date = ? AND series_number = ? AND (
                   (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
               )""",
            (date, series_number, team1_id, team2_id, team2_id, team1_id),
        ).fetchone()

        if not match:
            results.append({"match": key, "status": "error", "errors": ["Match not found — ingest maps first"]})
            continue

        match_id = match[0]

        existing = conn.execute(
            "SELECT COUNT(*) FROM map_bans WHERE match_id = ?", (match_id,)
        ).fetchone()[0]
        if existing > 0:
            results.append({"match": key, "status": "skipped", "reason": "bans already exist"})
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
