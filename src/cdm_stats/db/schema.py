import sqlite3

SCHEMA_VERSION = 10

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
        two_v_two_winner_id INTEGER REFERENCES teams(team_id),
        series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
        -- match_format values are governed by the FORMATS registry, not a CHECK,
        -- so adding a format needs no migration. See ingestion/formats.py.
        match_format        TEXT NOT NULL DEFAULT 'CDL_BO5',
        series_number       INTEGER NOT NULL DEFAULT 1,
        round               TEXT,
        season              INTEGER NOT NULL DEFAULT 1,
        competition         TEXT,
        CHECK(team1_id != team2_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS map_results (
        result_id              INTEGER PRIMARY KEY,
        match_id               INTEGER NOT NULL REFERENCES matches(match_id),
        slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 7),
        map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
        picked_by_team_id      INTEGER REFERENCES teams(team_id),
        winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
        picking_team_score     INTEGER NOT NULL,
        non_picking_team_score INTEGER NOT NULL,
        team1_score_before     INTEGER NOT NULL,
        team2_score_before     INTEGER NOT NULL,
        pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                   'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss', 'Unknown'
                               )),
        dq                     INTEGER NOT NULL DEFAULT 0 CHECK(dq IN (0, 1)),
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
    """
    CREATE TABLE IF NOT EXISTS map_bans (
        ban_id    INTEGER PRIMARY KEY,
        match_id  INTEGER NOT NULL REFERENCES matches(match_id),
        team_id   INTEGER NOT NULL REFERENCES teams(team_id),
        map_id    INTEGER NOT NULL REFERENCES maps(map_id),
        UNIQUE(match_id, team_id, map_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scrim_maps (
        scrim_map_id   INTEGER PRIMARY KEY,
        scrim_date     DATE NOT NULL,
        week           INTEGER NOT NULL,
        opponent_id    INTEGER NOT NULL REFERENCES teams(team_id),
        map_name       TEXT NOT NULL,
        mode           TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
        game_number    INTEGER NOT NULL DEFAULT 1,
        our_score      INTEGER NOT NULL,
        opponent_score INTEGER NOT NULL,
        result         TEXT NOT NULL CHECK(result IN ('W', 'L')),
        season         INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scrim_player_stats (
        stat_id      INTEGER PRIMARY KEY,
        scrim_map_id INTEGER NOT NULL REFERENCES scrim_maps(scrim_map_id),
        player_name  TEXT NOT NULL,
        kills        INTEGER NOT NULL,
        deaths       INTEGER NOT NULL,
        assists      INTEGER NOT NULL,
        UNIQUE(scrim_map_id, player_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tournament_player_stats (
        stat_id      INTEGER PRIMARY KEY,
        result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
        week         INTEGER NOT NULL,
        player_name  TEXT NOT NULL,
        kills        INTEGER NOT NULL,
        deaths       INTEGER NOT NULL,
        assists      INTEGER NOT NULL,
        UNIQUE(result_id, player_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops_player_stats (
        stat_id      INTEGER PRIMARY KEY,
        result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
        week         INTEGER NOT NULL,
        player_name  TEXT NOT NULL,
        op_kills     INTEGER NOT NULL,
        op_pulls     INTEGER NOT NULL,
        footage_min  REAL NOT NULL,
        UNIQUE(result_id, player_name)
    )
    """,
]


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    for ddl in TABLES:
        conn.execute(ddl)
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()


def migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= SCHEMA_VERSION:
        return

    if version < 1:
        conn.execute("PRAGMA foreign_keys = OFF")

        # Rebuild matches table
        conn.execute("""CREATE TABLE matches_new (
            match_id            INTEGER PRIMARY KEY,
            match_date          DATE NOT NULL,
            team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
            team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
            two_v_two_winner_id INTEGER REFERENCES teams(team_id),
            series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
            match_format        TEXT NOT NULL DEFAULT 'CDL_BO5',
            series_number       INTEGER NOT NULL DEFAULT 1,
            CHECK(team1_id != team2_id)
        )""")
        conn.execute("""INSERT INTO matches_new
            (match_id, match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, match_format, series_number)
            SELECT match_id, match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id, 'CDL_BO5', 1
            FROM matches""")
        conn.execute("DROP TABLE matches")
        conn.execute("ALTER TABLE matches_new RENAME TO matches")

        # Rebuild map_results table
        conn.execute("""CREATE TABLE map_results_new (
            result_id              INTEGER PRIMARY KEY,
            match_id               INTEGER NOT NULL REFERENCES matches(match_id),
            slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 7),
            map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
            picked_by_team_id      INTEGER REFERENCES teams(team_id),
            winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
            picking_team_score     INTEGER NOT NULL,
            non_picking_team_score INTEGER NOT NULL,
            team1_score_before     INTEGER NOT NULL,
            team2_score_before     INTEGER NOT NULL,
            pick_context           TEXT NOT NULL CHECK(pick_context IN (
                                       'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss', 'Unknown'
                                   )),
            UNIQUE(match_id, slot)
        )""")
        conn.execute("""INSERT INTO map_results_new
            SELECT * FROM map_results""")
        conn.execute("DROP TABLE map_results")
        conn.execute("ALTER TABLE map_results_new RENAME TO map_results")

        # Create map_bans table
        conn.execute("""CREATE TABLE IF NOT EXISTS map_bans (
            ban_id    INTEGER PRIMARY KEY,
            match_id  INTEGER NOT NULL REFERENCES matches(match_id),
            team_id   INTEGER NOT NULL REFERENCES teams(team_id),
            map_id    INTEGER NOT NULL REFERENCES maps(map_id),
            UNIQUE(match_id, team_id, map_id)
        )""")

        conn.execute("PRAGMA foreign_keys = ON")

    if version < 2:
        # Add series_number column if missing (v1 -> v2)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(matches)").fetchall()]
        if "series_number" not in cols:
            conn.execute("ALTER TABLE matches ADD COLUMN series_number INTEGER NOT NULL DEFAULT 1")

    if version < 3:
        conn.execute("""CREATE TABLE IF NOT EXISTS scrim_maps (
            scrim_map_id   INTEGER PRIMARY KEY,
            scrim_date     DATE NOT NULL,
            week           INTEGER NOT NULL,
            opponent_id    INTEGER NOT NULL REFERENCES teams(team_id),
            map_name       TEXT NOT NULL,
            mode           TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
            game_number    INTEGER NOT NULL DEFAULT 1,
            our_score      INTEGER NOT NULL,
            opponent_score INTEGER NOT NULL,
            result         TEXT NOT NULL CHECK(result IN ('W', 'L'))
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scrim_player_stats (
            stat_id      INTEGER PRIMARY KEY,
            scrim_map_id INTEGER NOT NULL REFERENCES scrim_maps(scrim_map_id),
            player_name  TEXT NOT NULL,
            kills        INTEGER NOT NULL,
            deaths       INTEGER NOT NULL,
            assists      INTEGER NOT NULL,
            UNIQUE(scrim_map_id, player_name)
        )""")

    if version < 4:
        conn.execute("""CREATE TABLE IF NOT EXISTS tournament_player_stats (
            stat_id      INTEGER PRIMARY KEY,
            result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
            week         INTEGER NOT NULL,
            player_name  TEXT NOT NULL,
            kills        INTEGER NOT NULL,
            deaths       INTEGER NOT NULL,
            assists      INTEGER NOT NULL,
            UNIQUE(result_id, player_name)
        )""")

    if version < 5:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
        if "dq" not in cols:
            conn.execute(
                "ALTER TABLE map_results ADD COLUMN dq INTEGER NOT NULL DEFAULT 0 CHECK(dq IN (0, 1))"
            )

    if version < 6:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(matches)").fetchall()]
        if "round" not in cols:
            conn.execute("ALTER TABLE matches ADD COLUMN round TEXT")

    if version < 7:
        match_cols = [r[1] for r in conn.execute("PRAGMA table_info(matches)").fetchall()]
        if "season" not in match_cols:
            conn.execute("ALTER TABLE matches ADD COLUMN season INTEGER NOT NULL DEFAULT 1")
        scrim_cols = [r[1] for r in conn.execute("PRAGMA table_info(scrim_maps)").fetchall()]
        if "season" not in scrim_cols:
            conn.execute("ALTER TABLE scrim_maps ADD COLUMN season INTEGER NOT NULL DEFAULT 1")

    if version < 8:
        # Drop the match_format CHECK by rebuilding matches. Formats are governed
        # by the FORMATS registry now, so adding one needs no further migration.
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("""CREATE TABLE matches_new (
            match_id            INTEGER PRIMARY KEY,
            match_date          DATE NOT NULL,
            team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
            team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
            two_v_two_winner_id INTEGER REFERENCES teams(team_id),
            series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
            match_format        TEXT NOT NULL DEFAULT 'CDL_BO5',
            series_number       INTEGER NOT NULL DEFAULT 1,
            round               TEXT,
            season              INTEGER NOT NULL DEFAULT 1,
            CHECK(team1_id != team2_id)
        )""")
        conn.execute("""INSERT INTO matches_new
            (match_id, match_date, team1_id, team2_id, two_v_two_winner_id,
             series_winner_id, match_format, series_number, round, season)
            SELECT match_id, match_date, team1_id, team2_id, two_v_two_winner_id,
                   series_winner_id, match_format, series_number, round, season
            FROM matches""")
        conn.execute("DROP TABLE matches")
        conn.execute("ALTER TABLE matches_new RENAME TO matches")
        conn.execute("PRAGMA foreign_keys = ON")

    if version < 9:
        # Add competition column to separate parallel S2 competitions (CDM vs
        # regional World Cup). Existing rows stay NULL.
        cols = [r[1] for r in conn.execute("PRAGMA table_info(matches)").fetchall()]
        if "competition" not in cols:
            conn.execute("ALTER TABLE matches ADD COLUMN competition TEXT")

    if version < 10:
        # Operator kills/pulls per player per map, from footage review. Kept
        # separate from tournament_player_stats because footage coverage lags
        # scoreboard coverage — the two fill in independently.
        conn.execute("""CREATE TABLE IF NOT EXISTS ops_player_stats (
            stat_id      INTEGER PRIMARY KEY,
            result_id    INTEGER NOT NULL REFERENCES map_results(result_id),
            week         INTEGER NOT NULL,
            player_name  TEXT NOT NULL,
            op_kills     INTEGER NOT NULL,
            op_pulls     INTEGER NOT NULL,
            footage_min  REAL NOT NULL,
            UNIQUE(result_id, player_name)
        )""")

    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()
