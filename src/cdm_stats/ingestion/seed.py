import sqlite3

# 14 CDL teams for the current season
TEAMS = [
    ("Diavolos", "DVS"),
    ("OU Gaming", "OUG"),
    ("Qing Jiu Club", "Q9"),
    ("Stand Point Game", "SPG"),
    ("Wolves", "Wolves"),
    ("Elevate", "ELV"),
    ("XROCK", "XROCK"),
    ("Al-Ula Club", "ALU"),
    ("Galorys", "GAL"),
    ("GodLike", "GL"),
    ("HF", "HF"),
    ("Team StarMagic", "TSM"),
    ("Elite Titan Esports", "ETs"),
    ("Team Felines", "Felines"),
]

# 13 maps: 5 SnD, 5 HP, 3 Control
MAPS = [
    ("Tunisia", "SnD"),
    ("Firing Range", "SnD"),
    ("Slums", "SnD"),
    ("Meltdown", "SnD"),
    ("Coastal", "SnD"),
    ("Summit", "HP"),
    ("Hacienda", "HP"),
    ("Takeoff", "HP"),
    ("Arsenal", "HP"),
    ("Combine", "HP"),
    ("Raid", "Control"),
    ("Standoff", "Control"),
    ("Crossroads Strike", "Control"),
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
