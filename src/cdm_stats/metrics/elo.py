import sqlite3

K_FACTOR = 32
SEED_ELO = 1000.0
LOW_CONFIDENCE_THRESHOLD = 7


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

    # Margin-weighted result: 3-0 → 1.0/0.0, 3-1 → 0.85/0.15, 3-2 → 0.7/0.3
    maps_won_by_winner = conn.execute(
        "SELECT COUNT(*) FROM map_results WHERE match_id = ? AND winner_team_id = ?",
        (match_id, winner_id),
    ).fetchone()[0]
    maps_won_by_loser = conn.execute(
        "SELECT COUNT(*) FROM map_results WHERE match_id = ? AND winner_team_id != ?",
        (match_id, winner_id),
    ).fetchone()[0]
    margin = maps_won_by_winner - maps_won_by_loser  # 3, 2, or 1
    winner_score = {3: 1.0, 2: 0.85, 1: 0.7}.get(margin, 1.0)
    loser_score = 1.0 - winner_score

    result1 = winner_score if winner_id == team1_id else loser_score
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
