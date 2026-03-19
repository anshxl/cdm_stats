import sqlite3

FORMAT_SLOT_MODES = {
    "CDL_BO5":         {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO5": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO7": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP", 6: "Control", 7: "SnD"},
    "TOURNAMENT_BO5":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD"},
    "TOURNAMENT_BO7":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD", 6: "Control", 7: "SnD"},
}

# Keep SLOT_MODES as alias for CDL_BO5 for backward compatibility
SLOT_MODES = FORMAT_SLOT_MODES["CDL_BO5"]


def get_mode_for_slot(slot: int, match_format: str = "CDL_BO5") -> str:
    return FORMAT_SLOT_MODES[match_format][slot]


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
    two_v_two_winner_id: int | None,
    series_winner_id: int,
    match_format: str = "CDL_BO5",
    series_number: int = 1,
) -> int:
    cursor = conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, match_format, series_number),
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


def insert_map_ban(
    conn: sqlite3.Connection,
    match_id: int,
    team_id: int,
    map_id: int,
) -> int:
    cursor = conn.execute(
        "INSERT INTO map_bans (match_id, team_id, map_id) VALUES (?, ?, ?)",
        (match_id, team_id, map_id),
    )
    return cursor.lastrowid


def get_ban_summary(
    conn: sqlite3.Connection, team_id: int, opponent_id: int
) -> list[dict]:
    """Get ban frequency for team_id in matches against opponent_id."""
    rows = conn.execute(
        """SELECT mb.team_id, m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?) OR (m.team1_id = ? AND m.team2_id = ?))
           GROUP BY mb.team_id, m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        (team_id, team_id, opponent_id, opponent_id, team_id),
    ).fetchall()

    total_series = conn.execute(
        """SELECT COUNT(*) FROM matches
           WHERE match_format != 'CDL_BO5'
             AND ((team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?))""",
        (team_id, opponent_id, opponent_id, team_id),
    ).fetchone()[0]

    return [
        {"map_name": r[1], "mode": r[2], "ban_count": r[3], "total_series": total_series}
        for r in rows
    ]


def get_team_map_wl(
    conn: sqlite3.Connection, team_id: int, format_filter: str | None = None
) -> list[dict]:
    """Get W-L per map for a team, optionally filtered by format prefix (e.g. 'TOURNAMENT')."""
    if format_filter:
        rows = conn.execute(
            """SELECT m2.map_name, m2.mode,
                      SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END) as losses
               FROM map_results mr
               JOIN maps m2 ON mr.map_id = m2.map_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE (m.team1_id = ? OR m.team2_id = ?)
                 AND m.match_format LIKE ? || '%'
               GROUP BY m2.map_name, m2.mode
               ORDER BY m2.mode, wins DESC""",
            (team_id, team_id, team_id, team_id, format_filter),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT m2.map_name, m2.mode,
                      SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END) as wins,
                      SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END) as losses
               FROM map_results mr
               JOIN maps m2 ON mr.map_id = m2.map_id
               JOIN matches m ON mr.match_id = m.match_id
               WHERE (m.team1_id = ? OR m.team2_id = ?)
               GROUP BY m2.map_name, m2.mode
               ORDER BY m2.mode, wins DESC""",
            (team_id, team_id, team_id, team_id),
        ).fetchall()

    return [{"map_name": r[0], "mode": r[1], "wins": r[2], "losses": r[3]} for r in rows]


def get_team_ban_summary(
    conn: sqlite3.Connection, team_id: int
) -> dict:
    """Get ban tendencies for a team: what they ban and what opponents ban against them."""
    # What this team bans
    team_bans = conn.execute(
        """SELECT m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           WHERE mb.team_id = ?
           GROUP BY m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        (team_id,),
    ).fetchall()

    # What opponents ban against this team
    opp_bans = conn.execute(
        """SELECT m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id != ?
             AND (m.team1_id = ? OR m.team2_id = ?)
           GROUP BY m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        (team_id, team_id, team_id),
    ).fetchall()

    total_series = conn.execute(
        """SELECT COUNT(*) FROM matches
           WHERE match_format != 'CDL_BO5'
             AND (team1_id = ? OR team2_id = ?)""",
        (team_id, team_id),
    ).fetchone()[0]

    return {
        "team_bans": [{"map_name": r[0], "mode": r[1], "ban_count": r[2], "total_series": total_series} for r in team_bans],
        "opponent_bans": [{"map_name": r[0], "mode": r[1], "ban_count": r[2], "total_series": total_series} for r in opp_bans],
        "total_series": total_series,
    }
