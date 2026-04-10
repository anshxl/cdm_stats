import sqlite3

K_FACTOR = 32
SEED_ELO = 1000.0
LOW_CONFIDENCE_THRESHOLD = 7

MODE_MAX_MARGINS = {"SnD": 9, "HP": 250, "Control": 4}


def normalize_margin(winner_score: int, loser_score: int, mode: str) -> float:
    margin = abs(winner_score - loser_score)
    return margin / MODE_MAX_MARGINS[mode]


def get_current_elo(conn: sqlite3.Connection, team_id: int) -> float:
    row = conn.execute(
        "SELECT elo_after FROM team_elo WHERE team_id = ? ORDER BY match_date DESC, elo_id DESC LIMIT 1",
        (team_id,),
    ).fetchone()
    return row[0] if row else SEED_ELO


def get_elo_history(conn: sqlite3.Connection, team_id: int) -> list[dict]:
    rows = conn.execute(
        """SELECT elo_after, match_date, match_id
           FROM team_elo WHERE team_id = ?
           ORDER BY match_date, elo_id""",
        (team_id,),
    ).fetchall()
    return [{"elo_after": r[0], "match_date": r[1], "match_id": r[2]} for r in rows]


def is_low_confidence(conn: sqlite3.Connection, team_id: int) -> bool:
    count = conn.execute(
        "SELECT COUNT(*) FROM team_elo WHERE team_id = ?", (team_id,)
    ).fetchone()[0]
    return count < LOW_CONFIDENCE_THRESHOLD


def update_elo(conn: sqlite3.Connection, match_id: int) -> None:
    # Idempotent: skip if Elo rows already exist for this match
    existing = conn.execute(
        "SELECT COUNT(*) FROM team_elo WHERE match_id = ?", (match_id,)
    ).fetchone()[0]
    if existing > 0:
        return

    match = conn.execute(
        "SELECT team1_id, team2_id, series_winner_id, match_date FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    team1_id, team2_id, winner_id, match_date = match

    elo1 = get_current_elo(conn, team1_id)
    elo2 = get_current_elo(conn, team2_id)

    expected1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))
    expected2 = 1 - expected1

    # Compute continuous margin-weighted dominance score
    map_rows = conn.execute(
        """SELECT mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score, m.mode
           FROM map_results mr
           JOIN maps m ON mr.map_id = m.map_id
           WHERE mr.match_id = ? AND mr.dq = 0
           ORDER BY mr.slot""",
        (match_id,),
    ).fetchall()

    signed_margins = []
    for map_winner_id, pick_score, non_pick_score, mode in map_rows:
        winner_score = max(pick_score, non_pick_score)
        loser_score = min(pick_score, non_pick_score)
        norm = normalize_margin(winner_score, loser_score, mode)
        # Positive if series winner won this map, negative if series loser won
        if map_winner_id == winner_id:
            signed_margins.append(norm)
        else:
            signed_margins.append(-norm)

    avg_dominance = sum(signed_margins) / len(signed_margins) if signed_margins else 0.0
    winner_actual = max(0.5, 0.5 + avg_dominance * 0.5)
    loser_actual = 1.0 - winner_actual

    result1 = winner_actual if winner_id == team1_id else loser_actual
    result2 = 1.0 - result1

    new_elo1 = elo1 + K_FACTOR * (result1 - expected1)
    new_elo2 = elo2 + K_FACTOR * (result2 - expected2)

    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team1_id, match_id, new_elo1, match_date),
    )
    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team2_id, match_id, new_elo2, match_date),
    )
    conn.commit()


def recalculate_all_elo(conn: sqlite3.Connection) -> int:
    """Delete all Elo history and recompute from scratch in chronological order."""
    conn.execute("DELETE FROM team_elo")
    conn.commit()

    matches = conn.execute(
        "SELECT match_id FROM matches ORDER BY match_date, match_id"
    ).fetchall()

    for (match_id,) in matches:
        update_elo(conn, match_id)

    return len(matches)
