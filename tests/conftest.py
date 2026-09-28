# tests/conftest.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    """Fresh in-memory DB with schema and seed data."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    """DB with one ingested match (DVS 3-1 OUG)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id


@pytest.fixture
def db_with_tournament_match(db):
    """DB with schema migrated and one tournament match (ELV vs ALU) with bans."""
    from cdm_stats.db.schema import migrate
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    migrate(db)

    maps_csv = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

    bans_csv = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""

    ingest_tournament(db, io.StringIO(maps_csv), io.StringIO(bans_csv))
    return db


# ---------------------------------------------------------------------------
# API fixtures: a file-backed copy of the seeded test DB (the API opens one
# connection per request on a worker thread, so :memory: cannot be shared).
# ---------------------------------------------------------------------------

API_SCRIM_CSV = """Date,Opponent,Map,Score
2026-02-25,DVS,Tunisia,6-3
2026-02-25,DVS,Summit,250-200
2026-03-03,OUG,Raid,3-1
2026-03-03,OUG,Tunisia,2-6"""


def _build_api_data(conn):
    """GL plays 12 Bo5s (8 spring, 4 summer) alternating DVS/OUG, winning
    Tunisia/Raid/Slums and losing Summit every time. Alpha heats up over the
    last 5 series. DVS bans Hacienda vs GL; GL bans Standoff every series.
    Plus a hidden-team match (Felines) and one regionals match (ELV-ALU)."""
    from cdm_stats.db.queries import (
        get_team_id_by_abbr, insert_match, insert_map_result, insert_map_ban, get_map_id,
    )
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    from cdm_stats.metrics.elo import recalculate_all_elo

    t = {a: get_team_id_by_abbr(conn, a) for a in
         ("GL", "DVS", "OUG", "Felines", "ELV", "ALU")}
    mp = {n: get_map_id(conn, n) for n in
          ("Tunisia", "Summit", "Raid", "Slums", "Hacienda", "Standoff")}

    for i in range(12):
        opp = t["DVS"] if i % 2 == 0 else t["OUG"]
        if i < 8:
            date, kw = f"2026-01-{i + 1:02d}", {"season": 1}
        else:
            date, kw = f"2026-07-{i - 7:02d}", {"season": 2, "competition": "CDM",
                                                 "match_format": "Bo5"}
        mid = insert_match(conn, date, t["GL"], opp, t["GL"], t["GL"], **kw)
        # (slot, map, picker, winner, picker score, other score)
        maps = [(1, "Tunisia", t["GL"], t["GL"], 6, 2), (2, "Summit", opp, opp, 250, 200),
                (3, "Raid", t["GL"], t["GL"], 3, 1), (4, "Slums", opp, t["GL"], 4, 6)]
        for slot, name, picker, winner, ps, ns in maps:
            rid = insert_map_result(conn, mid, slot, mp[name], picker, winner, ps, ns,
                                    0, 0, "Neutral")
            alpha_kills = 20 if i >= 7 else 10
            conn.executemany(
                """INSERT INTO tournament_player_stats
                   (result_id, week, player_name, kills, deaths, assists) VALUES (?, 1, ?, ?, 10, 2)""",
                [(rid, "Alpha", alpha_kills), (rid, "Bravo", 10)],
            )
            if slot == 1:
                conn.execute(
                    """INSERT INTO ops_player_stats
                       (result_id, week, player_name, op_kills, op_pulls, footage_min)
                       VALUES (?, 1, 'Alpha', 2, 1, 10.0)""", (rid,))
        insert_map_ban(conn, mid, t["GL"], mp["Standoff"])
        if opp == t["DVS"]:
            insert_map_ban(conn, mid, t["DVS"], mp["Hacienda"])

    mid = insert_match(conn, "2026-01-20", t["Felines"], t["DVS"], t["DVS"], t["DVS"], season=1)
    insert_map_result(conn, mid, 1, mp["Tunisia"], t["DVS"], t["DVS"], 6, 0, 0, 0, "Opener")
    mid = insert_match(conn, "2026-08-01", t["ELV"], t["ALU"], t["ELV"], t["ELV"], season=2,
                       competition="Regional Major", match_format="Bo5")
    insert_map_result(conn, mid, 1, mp["Summit"], t["ELV"], t["ELV"], 250, 100, 0, 0, "Opener")

    ingest_scrims_team(conn, io.StringIO(API_SCRIM_CSV))
    conn.commit()
    recalculate_all_elo(conn)
    conn.commit()


@pytest.fixture
def api_db_path(db, tmp_path):
    _build_api_data(db)
    path = tmp_path / "api.db"
    dest = sqlite3.connect(path)
    db.backup(dest)
    dest.close()
    return path


@pytest.fixture
def client(api_db_path, monkeypatch):
    from fastapi.testclient import TestClient
    from cdm_stats.api.app import create_app, get_conn

    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    app = create_app(dist_dir=None)

    def _conn():
        conn = sqlite3.connect(api_db_path, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()

    app.dependency_overrides[get_conn] = _conn
    return TestClient(app)
