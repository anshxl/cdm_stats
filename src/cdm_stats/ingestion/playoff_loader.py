import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
)
from cdm_stats.ingestion._helpers import (
    derive_pick_context,
    group_rows_by_match,
    is_duplicate_match,
)

FORMAT_TO_MATCH_FORMAT = {
    "Bo5": "CDL_PLAYOFF_BO5",
    "Bo7": "CDL_PLAYOFF_BO7",
}

WIN_THRESHOLD = {"Bo5": 3, "Bo7": 4}


def _expected_picker(slot: int, fmt: str, a: str, b: str) -> str | None:
    """Return the abbreviation expected to pick at this slot, or None for forced (Bo5 slot 3)."""
    if fmt == "Bo5":
        # Pattern: A, B, NULL, B, A
        return [a, b, None, b, a][slot - 1]
    # Bo7 pattern: A, B, A, B, A, B, A
    return [a, b, a, b, a, b, a][slot - 1]


def _validate_series(
    conn: sqlite3.Connection, key: tuple, rows: list[dict]
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr = key

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    # Series-level fields must be identical across rows
    series_fields = ("date", "round", "format", "team1", "team2", "die_roll_winner")
    for field in series_fields:
        values = {r[field] for r in rows}
        if len(values) > 1:
            errors.append(f"Inconsistent series-level column '{field}': {values}")

    fmt = rows[0]["format"]
    if fmt not in FORMAT_TO_MATCH_FORMAT:
        errors.append(f"Invalid format '{fmt}' (must be Bo5 or Bo7)")
        return errors  # downstream checks depend on format

    die_roll = rows[0]["die_roll_winner"]
    if die_roll not in (team1_abbr, team2_abbr):
        errors.append(f"die_roll_winner '{die_roll}' is not one of the teams")

    # Slots must be sequential 1..N
    slots = sorted(int(r["slot"]) for r in rows)
    expected = list(range(1, len(rows) + 1))
    if slots != expected:
        errors.append(f"Slots are not sequential 1..{len(rows)}: {slots}")

    max_slots = 5 if fmt == "Bo5" else 7
    if len(rows) > max_slots:
        errors.append(f"{fmt} series has {len(rows)} rows, max is {max_slots}")

    # Map / winner sanity
    match_format = FORMAT_TO_MATCH_FORMAT[fmt]
    for row in rows:
        slot = int(row["slot"])
        if slot < 1 or slot > max_slots:
            continue
        mode = get_mode_for_slot(slot, match_format)
        if get_map_id(conn, row["map_name"], mode) is None:
            errors.append(f"Unknown map '{row['map_name']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    # Pick pattern: A=die_roll_winner, B=other
    a = die_roll
    b = team2_abbr if a == team1_abbr else team1_abbr
    for row in rows:
        slot = int(row["slot"])
        if slot < 1 or slot > max_slots:
            continue
        actual = (row.get("picked_by") or "").strip() or None
        expected_picker = _expected_picker(slot, fmt, a, b)
        if expected_picker is None:
            if actual is not None:
                errors.append(
                    f"Slot {slot} ({fmt}) is a forced pick (no picker) but picked_by='{actual}'"
                )
        else:
            if actual != expected_picker:
                errors.append(
                    f"Slot {slot} pick: expected '{expected_picker}', got '{actual or '<blank>'}'"
                )

    # Series winner: explicit override or derived from win threshold
    has_override = any((r.get("series_winner") or "").strip() for r in rows)
    threshold = WIN_THRESHOLD[fmt]
    if not has_override:
        t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
        t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
        if max(t1_wins, t2_wins) != threshold:
            errors.append(
                f"No team reached {threshold} wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}"
            )
    else:
        override = next(r["series_winner"].strip() for r in rows if (r.get("series_winner") or "").strip())
        if override not in (team1_abbr, team2_abbr):
            errors.append(f"series_winner '{override}' is not one of the teams")

    return errors


def ingest_playoffs(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    grouped = group_rows_by_match(reader)
    results = []

    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and is_duplicate_match(conn, date, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_series(conn, key, rows)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        fmt = rows[0]["format"]
        match_format = FORMAT_TO_MATCH_FORMAT[fmt]
        threshold = WIN_THRESHOLD[fmt]
        round_label = rows[0]["round"]
        die_roll_id = get_team_id_by_abbr(conn, rows[0]["die_roll_winner"])

        # Walk slots, derive map result fields
        t1_series = 0
        t2_series = 0
        map_result_data = []
        for row in sorted(rows, key=lambda r: int(r["slot"])):
            slot = int(row["slot"])
            mode = get_mode_for_slot(slot, match_format)
            map_id = get_map_id(conn, row["map_name"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            winner_score = int(row["winner_score"])
            loser_score = int(row["loser_score"])
            dq_val = (row.get("dq") or "").strip()
            dq = 1 if dq_val == "1" else 0

            picked_by_abbr = (row.get("picked_by") or "").strip()
            picker_id = get_team_id_by_abbr(conn, picked_by_abbr) if picked_by_abbr else None

            if picker_id is None or picker_id == winner_id:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            else:
                picking_team_score = loser_score
                non_picking_team_score = winner_score

            if picker_id is None:
                pick_context = "Coin-Toss"
            else:
                if picker_id == team1_id:
                    pick_context = derive_pick_context(
                        slot, t1_series, t2_series, win_threshold=threshold,
                    )
                else:
                    pick_context = derive_pick_context(
                        slot, t2_series, t1_series, win_threshold=threshold,
                    )

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context, dq,
            ))

            if winner_id == team1_id:
                t1_series += 1
            else:
                t2_series += 1

        override = next(
            ((r.get("series_winner") or "").strip() for r in rows if (r.get("series_winner") or "").strip()),
            None,
        )
        if override:
            series_winner_id = get_team_id_by_abbr(conn, override)
        else:
            series_winner_id = team1_id if t1_series == threshold else team2_id

        try:
            match_id = insert_match(
                conn, date, team1_id, team2_id, die_roll_id, series_winner_id,
                match_format=match_format, round_=round_label,
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
