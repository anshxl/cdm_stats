import sqlite3

SLOT_MODES = {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"}


def get_mode_for_slot(slot: int) -> str:
    return SLOT_MODES[slot]


def get_team_id_by_abbr(conn: sqlite3.Connection, abbr: str) -> int | None:
    row = conn.execute(
        "SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)
    ).fetchone()
    return row[0] if row else None


def get_map_id(conn: sqlite3.Connection, map_name: str, mode: str) -> int | None:
    row = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = ? AND mode = ?", (map_name, mode)
    ).fetchone()
    return row[0] if row else None


def insert_match(
    conn: sqlite3.Connection,
    match_date: str,
    team1_id: int,
    team2_id: int,
    two_v_two_winner_id: int,
    series_winner_id: int,
) -> int:
    cursor = conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id)
           VALUES (?, ?, ?, ?, ?)""",
        (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id),
    )
    return cursor.lastrowid


def insert_map_result(
    conn: sqlite3.Connection,
    match_id: int,
    slot: int,
    map_id: int,
    picked_by_team_id: int | None,
    winner_team_id: int,
    picking_team_score: int,
    non_picking_team_score: int,
    team1_score_before: int,
    team2_score_before: int,
    pick_context: str,
) -> int:
    cursor = conn.execute(
        """INSERT INTO map_results
           (match_id, slot, map_id, picked_by_team_id, winner_team_id,
            picking_team_score, non_picking_team_score,
            team1_score_before, team2_score_before, pick_context)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, slot, map_id, picked_by_team_id, winner_team_id,
         picking_team_score, non_picking_team_score,
         team1_score_before, team2_score_before, pick_context),
    )
    return cursor.lastrowid
