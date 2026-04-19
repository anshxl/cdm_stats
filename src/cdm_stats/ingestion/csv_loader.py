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
    group_rows_by_match as _group_rows_by_match,
    is_duplicate_match as _is_duplicate_match,
)


def _validate_match(
    conn: sqlite3.Connection, key: tuple, rows: list[dict]
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr = key

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    two_v_two = rows[0]["two_v_two_winner"]
    if two_v_two not in (team1_abbr, team2_abbr):
        errors.append(f"2v2 winner '{two_v_two}' is not one of the teams")

    slots = [int(r["slot"]) for r in rows]
    expected_slots = list(range(1, len(rows) + 1))
    if slots != expected_slots:
        errors.append(f"Slots are not sequential: {slots}")

    for row in rows:
        slot = int(row["slot"])
        mode = get_mode_for_slot(slot)
        if get_map_id(conn, row["map_name"], mode) is None:
            errors.append(f"Unknown map '{row['map_name']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    # Check exactly one team reaches 3 wins (unless series_winner is explicitly set)
    has_override = any((r.get("series_winner") or "").strip() for r in rows)
    if not has_override:
        t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
        t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
        if max(t1_wins, t2_wins) != 3:
            errors.append(f"No team reached 3 wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}")
    else:
        override = next(r["series_winner"].strip() for r in rows if (r.get("series_winner") or "").strip())
        if override not in (team1_abbr, team2_abbr):
            errors.append(f"series_winner '{override}' is not one of the teams")

    return errors


def ingest_csv(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    grouped = _group_rows_by_match(reader)
    results = []

    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate_match(conn, date, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_match(conn, key, rows)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        two_v_two_id = get_team_id_by_abbr(conn, rows[0]["two_v_two_winner"])

        # Walk slots to derive fields
        t1_series = 0
        t2_series = 0
        prev_loser_id = None
        map_result_data = []

        for row in sorted(rows, key=lambda r: int(r["slot"])):
            slot = int(row["slot"])
            mode = get_mode_for_slot(slot)
            map_id = get_map_id(conn, row["map_name"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            winner_score = int(row["winner_score"])
            loser_score = int(row["loser_score"])
            dq_val = (row.get("dq") or "").strip()
            dq = 1 if dq_val == "1" else 0

            # Derive picker
            if slot == 1:
                picker_id = two_v_two_id
            elif slot == 5:
                picked_by_abbr = (row.get("picked_by") or "").strip()
                picker_id = get_team_id_by_abbr(conn, picked_by_abbr) if picked_by_abbr else None
            else:
                picker_id = prev_loser_id

            # Derive scores relative to picker
            if picker_id is None:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            elif picker_id == winner_id:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            else:
                picking_team_score = loser_score
                non_picking_team_score = winner_score

            # Derive pick context
            if picker_id is None:
                pick_context = "Coin-Toss"
            else:
                if picker_id == team1_id:
                    pick_context = derive_pick_context(slot, t1_series, t2_series)
                else:
                    pick_context = derive_pick_context(slot, t2_series, t1_series)

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context, dq,
            ))

            # Update running scores and prev loser
            if winner_id == team1_id:
                t1_series += 1
                prev_loser_id = team2_id
            else:
                t2_series += 1
                prev_loser_id = team1_id

        # Derive series winner (or use explicit override)
        override = next(
            ((r.get("series_winner") or "").strip() for r in rows if (r.get("series_winner") or "").strip()),
            None,
        )
        if override:
            series_winner_id = get_team_id_by_abbr(conn, override)
        else:
            series_winner_id = team1_id if t1_series == 3 else team2_id

        # Atomic insert
        try:
            match_id = insert_match(conn, date, team1_id, team2_id, two_v_two_id, series_winner_id)
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
