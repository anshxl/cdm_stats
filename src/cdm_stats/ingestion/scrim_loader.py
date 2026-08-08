import csv
import sqlite3
from datetime import date, datetime, timedelta
from typing import IO

from cdm_stats.db.queries import get_team_id_by_abbr

# Monday of each season's week 1 — the anchor for inferring Week from Date.
# New season → add its first scrim week's Monday here.
SEASON_WEEK1_MONDAY = {
    1: date(2026, 2, 23),
    2: date(2026, 6, 8),
}


def parse_scrim_date(raw: str, season: int) -> date:
    """Parse an ISO date ('2026-06-11') or legacy '11-Jun' (year from the
    season anchor)."""
    raw = raw.strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        d = datetime.strptime(raw, "%d-%b").date()
        return d.replace(year=SEASON_WEEK1_MONDAY[season].year)


def infer_week(d: date, season: int) -> int:
    """Week = Mon-Sun calendar week counted from the season's anchor Monday."""
    monday = d - timedelta(days=d.weekday())
    return (monday - SEASON_WEEK1_MONDAY[season]).days // 7 + 1


def _mode_for_map(conn: sqlite3.Connection, map_name: str) -> str | None:
    row = conn.execute(
        "SELECT mode FROM maps WHERE map_name = ?", (map_name,)
    ).fetchone()
    return row[0] if row else None


def _parse_score(score_str: str) -> tuple[int, int]:
    """Parse 'X-Y' score string into (our_score, opponent_score)."""
    parts = score_str.strip().split("-")
    return int(parts[0]), int(parts[1])


def _parse_common(conn: sqlite3.Connection, row: dict, season: int) -> tuple[dict | None, str, str]:
    """Parse the Date/Opponent/Map fields shared by both scrim CSVs.

    Returns (parsed, desc, error). Week comes from Date, mode from the map
    pool — the legacy Week/Mode/Result columns are ignored if present.
    """
    date_raw = row["Date"].strip()
    opponent_abbr = row["Opponent"].strip()
    map_name = row["Map"].strip()
    desc = f"{date_raw} vs {opponent_abbr} {map_name}"

    if season not in SEASON_WEEK1_MONDAY:
        return None, desc, f"No week anchor for season {season} — add it to SEASON_WEEK1_MONDAY"
    try:
        d = parse_scrim_date(date_raw, season)
    except ValueError:
        return None, desc, f"Invalid date: {date_raw}"

    opponent_id = get_team_id_by_abbr(conn, opponent_abbr)
    if not opponent_id:
        return None, desc, f"Unknown opponent: {opponent_abbr}"

    mode = _mode_for_map(conn, map_name)
    if not mode:
        return None, desc, f"Unknown map: {map_name}"

    return {
        "date": d.isoformat(),
        "week": infer_week(d, season),
        "opponent_id": opponent_id,
        "map_name": map_name,
        "mode": mode,
    }, desc, ""


def ingest_scrims_team(conn: sqlite3.Connection, file: IO, season: int = 1) -> list[dict]:
    """Ingest scrim team-level CSV (Date,Opponent,Map,Score).

    Week, mode, and result are derived — Score is always Us-Them.
    Returns list of result dicts per row.
    """
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) group
    game_counts: dict[tuple, int] = {}

    rows_to_insert = []
    for row in reader:
        parsed, desc, error = _parse_common(conn, row, season)
        if error:
            results.append({"status": "error", "row": desc, "errors": error})
            continue

        score_str = row["Score"].strip()
        try:
            our_score, opp_score = _parse_score(score_str)
        except (ValueError, IndexError):
            results.append({"status": "error", "row": desc, "errors": f"Invalid score format: {score_str}"})
            continue
        if our_score == opp_score:
            results.append({"status": "error", "row": desc, "errors": f"Tied score: {score_str}"})
            continue

        # Assign game_number
        key = (parsed["date"], parsed["opponent_id"], parsed["map_name"], parsed["mode"])
        game_counts[key] = game_counts.get(key, 0) + 1

        rows_to_insert.append({
            **parsed,
            "game_number": game_counts[key],
            "our_score": our_score, "opponent_score": opp_score,
            "result": "W" if our_score > opp_score else "L",
            "desc": desc,
        })

    for r in rows_to_insert:
        # Duplicate check
        existing = conn.execute(
            """SELECT scrim_map_id FROM scrim_maps
               WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?
                 AND mode = ? AND game_number = ? AND season = ?""",
            (r["date"], r["opponent_id"], r["map_name"], r["mode"], r["game_number"], season),
        ).fetchone()

        if existing:
            results.append({"status": "skipped", "row": r["desc"]})
            continue

        conn.execute(
            """INSERT INTO scrim_maps
               (scrim_date, week, opponent_id, map_name, mode, game_number,
                our_score, opponent_score, result, season)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["date"], r["week"], r["opponent_id"], r["map_name"], r["mode"],
             r["game_number"], r["our_score"], r["opponent_score"], r["result"], season),
        )
        results.append({"status": "ok", "row": r["desc"]})

    conn.commit()
    return results


def ingest_scrims_players(conn: sqlite3.Connection, file: IO, season: int = 1) -> list[dict]:
    """Ingest scrim player-level CSV (Date,Opponent,Map,Player,Kills,Deaths,Assists).

    Team CSV must be ingested first.
    """
    reader = csv.DictReader(file)
    results = []

    # Track game_number per (date, opponent, map, mode) to match team CSV ordering
    game_counts: dict[tuple, int] = {}
    # Track which (scrim_map_id, player) combos we've seen in this batch
    seen_in_batch: dict[tuple, int] = {}

    for row in reader:
        parsed, desc, error = _parse_common(conn, row, season)
        if error:
            results.append({"status": "error", "row": desc, "errors": error})
            continue

        player_name = row["Player"].strip()
        kills = int(row["Kills"].strip())
        deaths = int(row["Deaths"].strip())
        assists = int(row["Assists"].strip())
        desc = f"{desc} {player_name}"

        # Determine game_number — same logic as team CSV: sequential per group
        key = (parsed["date"], parsed["opponent_id"], parsed["map_name"], parsed["mode"])

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
                 AND mode = ? AND game_number = ? AND season = ?""",
            (parsed["date"], parsed["opponent_id"], parsed["map_name"],
             parsed["mode"], game_number, season),
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
