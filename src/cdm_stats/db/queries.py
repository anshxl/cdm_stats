import sqlite3

from cdm_stats.metrics.filters import MatchFilter

from cdm_stats.ingestion.formats import FORMATS

# The three game modes, in canonical display order.
MODES = ("SnD", "HP", "Control")
MODE_ORDER = {mode: i for i, mode in enumerate(MODES)}


def get_mode_for_slot(slot: int, match_format: str = "CDL_BO5") -> str:
    return FORMATS[match_format].slot_modes[slot]


def get_team_id_by_abbr(conn: sqlite3.Connection, abbr: str) -> int | None:
    row = conn.execute(
        "SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)
    ).fetchone()
    return row[0] if row else None


def get_map_id(
    conn: sqlite3.Connection, map_name: str, mode: str | None = None
) -> int | None:
    # mode=None: resolve by name alone (maps are mode-exclusive — see CLAUDE.md).
    if mode is None:
        row = conn.execute(
            "SELECT map_id FROM maps WHERE map_name = ?", (map_name,)
        ).fetchone()
    else:
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
    round_: str | None = None,
    season: int = 1,
    competition: str | None = None,
) -> int:
    cursor = conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number, round,
                                season, competition)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id,
         match_format, series_number, round_, season, competition),
    )
    return cursor.lastrowid


def get_map_by_name(conn: sqlite3.Connection, map_name: str) -> tuple[int, str] | None:
    """Return (map_id, mode) for a map. S2 derives mode from the map name."""
    row = conn.execute(
        "SELECT map_id, mode FROM maps WHERE map_name = ?", (map_name,)
    ).fetchone()
    return (row[0], row[1]) if row else None


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
    dq: int = 0,
) -> int:
    cursor = conn.execute(
        """INSERT INTO map_results
           (match_id, slot, map_id, picked_by_team_id, winner_team_id,
            picking_team_score, non_picking_team_score,
            team1_score_before, team2_score_before, pick_context, dq)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, slot, map_id, picked_by_team_id, winner_team_id,
         picking_team_score, non_picking_team_score,
         team1_score_before, team2_score_before, pick_context, dq),
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
    conn: sqlite3.Connection, team_id: int, opponent_id: int, f: MatchFilter = MatchFilter()
) -> list[dict]:
    """Get ban frequency for team_id in matches against opponent_id."""
    fw, fp = f.sql()
    rows = conn.execute(
        f"""SELECT mb.team_id, m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id = ?
             AND ((m.team1_id = ? AND m.team2_id = ?) OR (m.team1_id = ? AND m.team2_id = ?)){fw}
           GROUP BY mb.team_id, m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        [team_id, team_id, opponent_id, opponent_id, team_id] + fp,
    ).fetchall()

    total_series = conn.execute(
        f"""SELECT COUNT(*) FROM matches m
           WHERE m.match_format != 'CDL_BO5'
             AND ((m.team1_id = ? AND m.team2_id = ?) OR (m.team1_id = ? AND m.team2_id = ?)){fw}""",
        [team_id, opponent_id, opponent_id, team_id] + fp,
    ).fetchone()[0]

    return [
        {"map_name": r[1], "mode": r[2], "ban_count": r[3], "total_series": total_series}
        for r in rows
    ]


def team_ban_rates(
    conn: sqlite3.Connection,
    team_id: int,
    f: MatchFilter = MatchFilter(),
    opponent_id: int | None = None,
) -> dict:
    """Per-map ban counts for a team, optionally only in series vs opponent_id.

    Returns {"total_series": N, "by_map": {map_id: ban_count}}. `total_series`
    counts only series where this team's bans were recorded — ban ingestion is
    partial, so dividing by all series played would understate every rate.
    """
    conditions = ["mb.team_id = ?"]
    params: list = [team_id]
    if opponent_id is not None:
        conditions.append("? IN (m.team1_id, m.team2_id)")
        params.append(opponent_id)
    fw, fp = f.sql()
    where = " AND ".join(conditions) + fw
    params += fp

    by_map = dict(conn.execute(
        f"""SELECT mb.map_id, COUNT(*)
            FROM map_bans mb
            JOIN matches m ON mb.match_id = m.match_id
            WHERE {where}
            GROUP BY mb.map_id""",
        params,
    ).fetchall())

    total_series = conn.execute(
        f"""SELECT COUNT(DISTINCT mb.match_id)
            FROM map_bans mb
            JOIN matches m ON mb.match_id = m.match_id
            WHERE {where}""",
        params,
    ).fetchone()[0]

    return {"total_series": total_series, "by_map": by_map}


def team_pick_rates(
    conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()
) -> dict:
    """Per-map counts of maps this team picked, across all series.

    Same shape as team_ban_rates: `total_series` counts only series where
    pick data was recorded (picked_by is missing for some events).
    """
    fw, fp = f.sql()
    by_map = dict(conn.execute(
        f"""SELECT mr.map_id, COUNT(*)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE mr.picked_by_team_id = ? AND mr.dq = 0{fw}
           GROUP BY mr.map_id""",
        [team_id] + fp,
    ).fetchall())

    total_series = conn.execute(
        f"""SELECT COUNT(DISTINCT mr.match_id)
           FROM map_results mr
           JOIN matches m ON mr.match_id = m.match_id
           WHERE ? IN (m.team1_id, m.team2_id)
             AND mr.picked_by_team_id IS NOT NULL{fw}""",
        [team_id] + fp,
    ).fetchone()[0]

    return {"total_series": total_series, "by_map": by_map}


def opponent_ban_rates(
    conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()
) -> dict:
    """Per-map counts of what opponents ban against a team, across all series.

    Same shape and denominator rule as team_ban_rates: `total_series` counts
    only series where an opponent's bans were recorded.
    """
    fw, fp = f.sql()
    where = """mb.team_id != ?
               AND ? IN (m.team1_id, m.team2_id)""" + fw
    params = [team_id, team_id] + fp

    by_map = dict(conn.execute(
        f"""SELECT mb.map_id, COUNT(*)
            FROM map_bans mb
            JOIN matches m ON mb.match_id = m.match_id
            WHERE {where}
            GROUP BY mb.map_id""",
        params,
    ).fetchall())

    total_series = conn.execute(
        f"""SELECT COUNT(DISTINCT mb.match_id)
            FROM map_bans mb
            JOIN matches m ON mb.match_id = m.match_id
            WHERE {where}""",
        params,
    ).fetchone()[0]

    return {"total_series": total_series, "by_map": by_map}


def get_team_map_wl(
    conn: sqlite3.Connection, team_id: int, format_filter: str | None = None,
    f: MatchFilter = MatchFilter(),
) -> list[dict]:
    """Get W-L per map for a team, optionally filtered by format prefix (e.g. 'TOURNAMENT')."""
    fw, fp = f.sql()
    fmt = " AND m.match_format LIKE ? || '%'" if format_filter else ""
    rows = conn.execute(
        f"""SELECT m2.map_name, m2.mode,
                  SUM(CASE WHEN mr.winner_team_id = ? THEN 1 ELSE 0 END) as wins,
                  SUM(CASE WHEN mr.winner_team_id != ? THEN 1 ELSE 0 END) as losses
           FROM map_results mr
           JOIN maps m2 ON mr.map_id = m2.map_id
           JOIN matches m ON mr.match_id = m.match_id
           WHERE (m.team1_id = ? OR m.team2_id = ?)
             AND mr.dq = 0{fmt}{fw}
           GROUP BY m2.map_name, m2.mode
           ORDER BY m2.mode, wins DESC""",
        [team_id, team_id, team_id, team_id] + ([format_filter] if format_filter else []) + fp,
    ).fetchall()

    return [{"map_name": r[0], "mode": r[1], "wins": r[2], "losses": r[3]} for r in rows]


def get_team_ban_summary(
    conn: sqlite3.Connection, team_id: int, f: MatchFilter = MatchFilter()
) -> dict:
    """Get ban tendencies for a team: what they ban and what opponents ban against them."""
    fw, fp = f.sql()
    # What this team bans
    team_bans = conn.execute(
        f"""SELECT m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id = ?{fw}
           GROUP BY m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        [team_id] + fp,
    ).fetchall()

    # What opponents ban against this team
    opp_bans = conn.execute(
        f"""SELECT m2.map_name, m2.mode, COUNT(*) as ban_count
           FROM map_bans mb
           JOIN maps m2 ON mb.map_id = m2.map_id
           JOIN matches m ON mb.match_id = m.match_id
           WHERE mb.team_id != ?
             AND (m.team1_id = ? OR m.team2_id = ?){fw}
           GROUP BY m2.map_name, m2.mode
           ORDER BY ban_count DESC""",
        [team_id, team_id, team_id] + fp,
    ).fetchall()

    total_series = conn.execute(
        f"""SELECT COUNT(*) FROM matches m
           WHERE m.match_format != 'CDL_BO5'
             AND (m.team1_id = ? OR m.team2_id = ?){fw}""",
        [team_id, team_id] + fp,
    ).fetchone()[0]

    return {
        "team_bans": [{"map_name": r[0], "mode": r[1], "ban_count": r[2], "total_series": total_series} for r in team_bans],
        "opponent_bans": [{"map_name": r[0], "mode": r[1], "ban_count": r[2], "total_series": total_series} for r in opp_bans],
        "total_series": total_series,
    }
