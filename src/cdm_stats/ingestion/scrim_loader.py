import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import get_team_id_by_abbr


def _parse_score(score_str: str) -> tuple[int, int]:
    """Parse 'X-Y' score string into (our_score, opponent_score)."""
    parts = score_str.strip().split("-")
    return int(parts[0]), int(parts[1])


def _validate_result(our_score: int, opp_score: int, result: str) -> bool:
    """Check that result matches scores."""
    if result == "W":
        return our_score > opp_score
    return our_score < opp_score


def ingest_scrims_team(conn: sqlite3.Connection, file: IO) -> list[dict]:
    """Ingest scrim team-level CSV. Returns list of result dicts per row."""
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) group
    game_counts: dict[tuple, int] = {}

    rows_to_insert = []
    for row in reader:
        date = row["Date"].strip()
        week = int(row["Week"].strip())
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        score_str = row["Score"].strip()
        result_raw = row["Result"].strip()
        result = {"1": "W", "0": "L"}.get(result_raw, result_raw)

        desc = f"{date} vs {opponent_abbr} {map_name} {mode}"

        # Validate opponent
        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if not opponent_id:
            results.append({"status": "error", "row": desc, "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        # Parse and validate score
        try:
            our_score, opp_score = _parse_score(score_str)
        except (ValueError, IndexError):
            results.append({"status": "error", "row": desc, "errors": f"Invalid score format: {score_str}"})
            continue

        if not _validate_result(our_score, opp_score, result):
            results.append({"status": "error", "row": desc, "errors": f"Score {score_str} does not match result {result}"})
            continue

        if mode not in ("SnD", "HP", "Control"):
            results.append({"status": "error", "row": desc, "errors": f"Invalid mode: {mode}"})
            continue

        # Assign game_number
        key = (date, opponent_id, map_name, mode)
        game_counts[key] = game_counts.get(key, 0) + 1
        game_number = game_counts[key]

        rows_to_insert.append({
            "date": date, "week": week, "opponent_id": opponent_id,
            "map_name": map_name, "mode": mode, "game_number": game_number,
            "our_score": our_score, "opponent_score": opp_score,
            "result": result, "desc": desc,
        })

    for r in rows_to_insert:
        # Duplicate check
        existing = conn.execute(
            """SELECT scrim_map_id FROM scrim_maps
               WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?
                 AND mode = ? AND game_number = ?""",
            (r["date"], r["opponent_id"], r["map_name"], r["mode"], r["game_number"]),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": r["desc"]})
            continue

        conn.execute(
            """INSERT INTO scrim_maps
               (scrim_date, week, opponent_id, map_name, mode, game_number,
                our_score, opponent_score, result)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["date"], r["week"], r["opponent_id"], r["map_name"], r["mode"],
             r["game_number"], r["our_score"], r["opponent_score"], r["result"]),
        )
        results.append({"status": "ok", "row": r["desc"]})

    conn.commit()
    return results


def ingest_scrims_players(conn: sqlite3.Connection, file: IO) -> list[dict]:
    """Ingest scrim player-level CSV. Team CSV must be ingested first."""
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) to match team CSV ordering
    game_counts: dict[tuple, int] = {}
    # Track which (scrim_map_id, player) combos we've seen in this batch
    seen_in_batch: dict[tuple, int] = {}

    for row in reader:
        date = row["Date"].strip()
        opponent_abbr = row["Opponent"].strip()
        map_name = row["Map"].strip()
        mode = row["Mode"].strip()
        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())

        desc = f"{date} {map_name} {mode} {player_name}"

        opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
        if not opponent_id:
            results.append({"status": "error", "row": desc, "errors": f"Unknown opponent: {opponent_abbr}"})
            continue

        # Determine game_number — same logic as team CSV: sequential per group
        key = (date, opponent_id, map_name, mode)

        # If we've already seen this player for this key at the current game_number,
        # that means we've moved to the next game
        current_game = game_counts.get(key, 1)
        batch_key = (key, current_game, player_name)
        if batch_key in seen_in_batch:
            game_counts[key] = current_game + 1
            current_game = game_counts[key]

        game_number = current_game
        seen_in_batch[(key, game_number, player_name)] = True

        # Find matching scrim_maps row
        scrim_map = conn.execute(
            """SELECT scrim_map_id FROM scrim_maps
               WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?
                 AND mode = ? AND game_number = ?""",
            (date, opponent_id, map_name, mode, game_number),
        ).fetchone()

        if not scrim_map:
            results.append({"status": "error", "row": desc, "errors": "No matching scrim map found"})
            continue

        scrim_map_id = scrim_map[0]

        # Duplicate check
        existing = conn.execute(
            "SELECT stat_id FROM scrim_player_stats WHERE scrim_map_id = ? AND player_name = ?",
            (scrim_map_id, player_name),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": desc})
            continue

        conn.execute(
            """INSERT INTO scrim_player_stats
               (scrim_map_id, player_name, kills, deaths, assists)
               VALUES (?, ?, ?, ?, ?)""",
            (scrim_map_id, player_name, kills, deaths, assists),
        )
        results.append({"status": "ok", "row": desc})

    conn.commit()
    return results
