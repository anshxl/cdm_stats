"""Season 2 ingestion: one map-centric file covers every S2 format.

Each CSV row is one map. Mode is derived from the map (not a slot->mode
series), so the same loader handles Ro3 / Bo5 / Bo7 across every S2 stage and
the parallel regional competition. Rows land in the shared matches/map_results
tables (season=2 + a competition tag), so all existing metrics work unchanged.
"""

import csv
import sqlite3
from typing import IO

from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_by_name,
    insert_match,
    insert_map_result,
    insert_map_ban,
)
from cdm_stats.ingestion._helpers import derive_pick_context
from cdm_stats.ingestion.formats import FORMATS

SEASON = 2


def _group_rows(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    series: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["competition"], row["team1"], row["team2"])
        series.setdefault(key, []).append(row)
    return series


def _group_matches(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    # Include stage so two series between the same teams on the same day
    # (e.g. a bracket's UB match and Finals) are kept distinct.
    series: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["competition"], row["stage"], row["team1"], row["team2"])
        series.setdefault(key, []).append(row)
    return series


def _is_duplicate(conn, date, competition, stage, team1_id, team2_id) -> bool:
    return conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND competition = ? AND round = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, competition, stage, team1_id, team2_id, team2_id, team1_id),
    ).fetchone() is not None


def _validate(conn, key, rows) -> list[str]:
    date, competition, stage, team1_abbr, team2_abbr = key
    errors = []

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    fmt = rows[0]["format"]
    if fmt not in FORMATS:
        errors.append(f"Unknown format '{fmt}'")
        return errors  # win threshold is needed for the checks below
    if any(r["format"] != fmt for r in rows):
        errors.append("Inconsistent format across the series' rows")

    t1_wins = t2_wins = 0
    for r in rows:
        if get_map_by_name(conn, r["map"]) is None:
            errors.append(f"Unknown map: {r['map']}")
        try:
            s1, s2 = int(r["team1_score"]), int(r["team2_score"])
        except ValueError:
            errors.append(f"Non-integer score for map '{r['map']}'")
            continue
        if s1 == s2:
            errors.append(f"Tied score {s1}-{s2} for map '{r['map']}'")
            continue
        if s1 > s2:
            t1_wins += 1
        else:
            t2_wins += 1
        picker = (r.get("picked_by") or "").strip()
        if picker and picker not in (team1_abbr, team2_abbr):
            errors.append(f"picked_by '{picker}' is not one of the teams")

    override = _override(rows)
    if override and override not in (team1_abbr, team2_abbr):
        errors.append(f"series_winner '{override}' is not one of the teams")
    elif not override and not errors:
        threshold = FORMATS[fmt].win_threshold
        # Formats like Ro3 play every map, so the winner may exceed the threshold
        # (e.g. 3-0 in a first-to-2). Incomplete = nobody reached it yet.
        if max(t1_wins, t2_wins) < threshold:
            errors.append(
                f"No team reached {threshold} wins ({fmt}): "
                f"{team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}"
            )

    return errors


def _override(rows) -> str:
    return next(
        ((r.get("series_winner") or "").strip() for r in rows if (r.get("series_winner") or "").strip()),
        "",
    )


def ingest_s2_matches(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    results = []

    for key, rows in _group_matches(reader).items():
        date, competition, stage, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate(conn, date, competition, stage, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate(conn, key, rows)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        fmt = rows[0]["format"]
        threshold = FORMATS[fmt].win_threshold
        stage = rows[0]["stage"]

        t1_wins = t2_wins = 0
        map_data = []
        for slot, r in enumerate(rows, start=1):
            map_id, _mode = get_map_by_name(conn, r["map"])
            s1, s2 = int(r["team1_score"]), int(r["team2_score"])
            winner_id = team1_id if s1 > s2 else team2_id

            picker = (r.get("picked_by") or "").strip()
            picker_id = get_team_id_by_abbr(conn, picker) if picker else None
            if picker_id == team2_id:
                picking_score, non_picking_score = s2, s1
            else:  # picker is team1, or unknown (convention: orient on team1)
                picking_score, non_picking_score = s1, s2

            if slot == 1:
                pick_context = "Opener"
            elif picker_id is None:
                pick_context = "Unknown"
            else:
                picker_wins = t1_wins if picker_id == team1_id else t2_wins
                opp_wins = t2_wins if picker_id == team1_id else t1_wins
                pick_context = derive_pick_context(
                    slot, picker_wins, opp_wins, win_threshold=threshold
                )

            dq = 1 if (r.get("dq") or "").strip() == "1" else 0
            map_data.append((
                slot, map_id, picker_id, winner_id,
                picking_score, non_picking_score, t1_wins, t2_wins, pick_context, dq,
            ))

            if winner_id == team1_id:
                t1_wins += 1
            else:
                t2_wins += 1

        override = _override(rows)
        if override:
            series_winner_id = get_team_id_by_abbr(conn, override)
        else:
            series_winner_id = team1_id if t1_wins > t2_wins else team2_id

        try:
            match_id = insert_match(
                conn, date, team1_id, team2_id, None, series_winner_id,
                match_format=fmt, round_=stage, season=SEASON, competition=competition,
            )
            for data in map_data:
                insert_map_result(conn, match_id, *data)
            conn.commit()
            from cdm_stats.metrics.elo import update_elo
            update_elo(conn, match_id)
            results.append({"match": key, "status": "ok", "match_id": match_id})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results


def _find_match(conn, date, competition, team1_id, team2_id):
    """Return (match_id, match_format) for the series, or None."""
    return conn.execute(
        """SELECT match_id, match_format FROM matches
           WHERE match_date = ? AND competition = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, competition, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()


def ingest_s2_bans(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    results = []

    for key, bans in _group_rows(reader).items():
        date, competition, team1_abbr, team2_abbr = key

        # A short row (missing a value, e.g. competition) shifts columns left and
        # leaves the trailing 'map' empty. Catch it here so the error is clear
        # instead of a misleading "no matching series".
        if any(not (b.get("map") or "").strip() for b in bans):
            results.append({"match": key, "status": "error", "errors": [
                "Malformed row(s): missing fields. Expected columns "
                "date,competition,team1,team2,banned_by,map (is the competition value present?)"
            ]})
            continue

        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        match = _find_match(conn, date, competition, team1_id, team2_id) if (team1_id and team2_id) else None
        if match is None:
            results.append({"match": key, "status": "error",
                            "errors": ["No matching series found for these bans"]})
            continue
        match_id, match_format = match

        errors = []
        for b in bans:
            if b["banned_by"] not in (team1_abbr, team2_abbr):
                errors.append(f"banned_by '{b['banned_by']}' is not one of the teams")
            if get_map_by_name(conn, b["map"]) is None:
                errors.append(f"Unknown map: {b['map']}")
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        # Insert only bans not already recorded, so a partial file can be topped up.
        existing = {
            row for row in conn.execute(
                "SELECT team_id, map_id FROM map_bans WHERE match_id = ?", (match_id,)
            ).fetchall()
        }
        inserted = 0
        try:
            for b in bans:
                team_id = get_team_id_by_abbr(conn, b["banned_by"])
                map_id, _mode = get_map_by_name(conn, b["map"])
                if (team_id, map_id) in existing:
                    continue
                insert_map_ban(conn, match_id, team_id, map_id)
                existing.add((team_id, map_id))
                inserted += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})
            continue

        if inserted == 0:
            results.append({"match": key, "status": "skipped", "reason": "all bans already recorded"})
            continue

        result = {"match": key, "status": "ok", "bans": inserted}
        expected = FORMATS[match_format].expected_bans
        total = len(existing)
        if expected and total != expected:
            result["warning"] = f"match now has {total} bans, expected {expected} for {match_format}"
        results.append(result)

    return results
