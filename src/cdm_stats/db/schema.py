import sqlite3

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS teams (
        team_id      INTEGER PRIMARY KEY,
        team_name    TEXT NOT NULL,
        abbreviation TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS maps (
        map_id   INTEGER PRIMARY KEY,
        map_name TEXT NOT NULL,
        mode     TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
        UNIQUE(map_name, mode)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS matches (
        match_id            INTEGER PRIMARY KEY,
        match_date          DATE NOT NULL,
        team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
        team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
        two_v_two_winner_id INTEGER NOT NULL REFERENCES teams(team_id),
        series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
        CHECK(team1_id != team2_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS map_results (
        result_id              INTEGER PRIMARY KEY,
        match_id               INTEGER NOT NULL REFERENCES matches(match_id),
        slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 5),
        map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
        picked_by_team_id      INTEGER REFERENCES teams(team_id),
        winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
        picking_team_score     INTEGER NOT NULL,
        non_picking_team_score INTEGER NOT NULL,
        team1_score_before     INTEGER NOT NULL,
        team2_score_before     INTEGER NOT NULL,
        pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                   'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss'
                               )),
        UNIQUE(match_id, slot)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_elo (
        elo_id     INTEGER PRIMARY KEY,
        team_id    INTEGER NOT NULL REFERENCES teams(team_id),
        match_id   INTEGER NOT NULL REFERENCES matches(match_id),
        elo_after  REAL NOT NULL,
        match_date DATE NOT NULL,
        UNIQUE(team_id, match_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS team_map_notes (
        note_id    INTEGER PRIMARY KEY,
        team_id    INTEGER NOT NULL REFERENCES teams(team_id),
        map_id     INTEGER NOT NULL REFERENCES maps(map_id),
        note       TEXT NOT NULL,
        created_at DATE NOT NULL
    )
    """,
]


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    for ddl in TABLES:
        conn.execute(ddl)
    conn.commit()
