import sqlite3

# 14 CDL teams for the current season
TEAMS = [
    ("Atlanta FaZe", "ATL"),
    ("Boston Breach", "BOS"),
    ("Carolina Royal Ravens", "CAR"),
    ("Las Vegas Legion", "LV"),
    ("Los Angeles Guerrillas", "LAG"),
    ("Los Angeles Thieves", "LAT"),
    ("Miami Heretics", "MIA"),
    ("Minnesota ROKKR", "MIN"),
    ("New York Subliners", "NYSL"),
    ("OpTic Texas", "OPT"),
    ("Seattle Surge", "SEA"),
    ("Toronto Ultra", "TOR"),
    ("Cloud9 New York", "C9"),
    ("Tampa Bay Mutineers", "TB"),
]

# 13 maps: 5 SnD, 5 HP, 3 Control
MAPS = [
    ("Invasion", "SnD"),
    ("Karachi", "SnD"),
    ("Rio", "SnD"),
    ("Skidrow", "SnD"),
    ("Terminal", "SnD"),
    ("Highrise", "HP"),
    ("Invasion", "HP"),
    ("Karachi", "HP"),
    ("Rio", "HP"),
    ("Sub Base", "HP"),
    ("Highrise", "Control"),
    ("Invasion", "Control"),
    ("Karachi", "Control"),
]


def seed_teams(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO teams (team_name, abbreviation) VALUES (?, ?)",
        TEAMS,
    )
    conn.commit()


def seed_maps(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO maps (map_name, mode) VALUES (?, ?)",
        MAPS,
    )
    conn.commit()
