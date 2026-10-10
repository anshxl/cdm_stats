import argparse
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cdl.db")


def get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def cmd_init(_args: argparse.Namespace) -> None:
    from cdm_stats.db.schema import create_tables, migrate
    from cdm_stats.ingestion.seed import seed_teams, seed_maps

    conn = get_db()
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    conn.close()
    print("Database initialized and seeded.")


def run_ingest(loader, *paths: str, key: str = "match", **load_kw) -> None:
    """Open paths, run loader(conn, *files, **load_kw), print per-row status.

    `key` is the result field naming each row ("match" or "row"). OK rows with
    a "bans" count print that extra. Skipped rows are only counted — on a
    re-ingest they're the bulk of the output and drown out OK/ERROR lines.
    """
    from cdm_stats.db.schema import migrate

    conn = get_db()
    migrate(conn)
    files = [open(p) for p in paths]
    try:
        results = loader(conn, *files, **load_kw)
    finally:
        for f in files:
            f.close()

    skipped = 0
    for r in results:
        ident = r[key]
        if r["status"] == "ok":
            extra = f" ({r['bans']} bans)" if "bans" in r else ""
            print(f"  OK: {ident}{extra}")
            if r.get("warning"):
                print(f"     warning: {r['warning']}")
        elif r["status"] == "skipped":
            skipped += 1
        else:
            print(f"  ERROR: {ident}: {r['errors']}")

    if skipped:
        print(f"  SKIPPED: {skipped}")

    conn.close()


def cmd_ingest(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.csv_loader import ingest_csv

    run_ingest(ingest_csv, args.csv_file)










def cmd_ingest_playoffs(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.playoff_loader import ingest_playoffs

    run_ingest(ingest_playoffs, args.csv_file)


def cmd_ingest_playoff_bans(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans

    run_ingest(ingest_playoff_bans, args.csv_file)


def cmd_ingest_tournament(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.tournament_loader import ingest_tournament

    run_ingest(ingest_tournament, args.maps_csv, args.bans_csv)






def cmd_ingest_scrims_team(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team

    run_ingest(ingest_scrims_team, args.csv_file, key="row", season=args.season)


def cmd_ingest_scrims_players(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_players

    run_ingest(ingest_scrims_players, args.csv_file, key="row", season=args.season)


def cmd_ingest_tournament_players(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players

    run_ingest(ingest_tournament_players, args.csv_file, key="row")


def cmd_ingest_ops(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.ops_loader import ingest_ops_kills

    run_ingest(ingest_ops_kills, args.csv_file, key="row")


def cmd_ingest_scrim_ops(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_ops_loader import ingest_scrim_ops

    run_ingest(ingest_scrim_ops, args.json_file, key="row", season=args.season)


def cmd_ingest_s2_matches(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.s2_loader import ingest_s2_matches

    run_ingest(ingest_s2_matches, args.csv_file)


def cmd_ingest_s2_bans(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.s2_loader import ingest_s2_bans

    run_ingest(ingest_s2_bans, args.csv_file)


def cmd_backfill(_args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.backfill import backfill_elo

    conn = get_db()
    count = backfill_elo(conn)
    conn.close()
    print(f"Backfill complete: {count} matches reprocessed.")


def main() -> None:
    parser = argparse.ArgumentParser(description="CDM Stats — CDL Analytics Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Create DB and seed teams/maps")

    p_ingest = sub.add_parser("ingest", help="Ingest match data from CSV")
    p_ingest.add_argument("csv_file", help="Path to CSV file")

    p_ingest_t = sub.add_parser("ingest-tournament", help="Ingest tournament/playoff data from CSVs")
    p_ingest_t.add_argument("maps_csv", help="Path to maps CSV file")
    p_ingest_t.add_argument("bans_csv", help="Path to bans CSV file")

    p_ingest_pf = sub.add_parser("ingest-playoffs", help="Ingest CDL playoff series from CSV")
    p_ingest_pf.add_argument("csv_file", help="Path to playoffs CSV file")

    p_ingest_pfb = sub.add_parser("ingest-playoff-bans", help="Ingest CDL playoff bans from CSV")
    p_ingest_pfb.add_argument("csv_file", help="Path to playoff bans CSV file")

    p_scrim_team = sub.add_parser("ingest-scrims-team", help="Ingest scrim team-level CSV")
    p_scrim_team.add_argument("csv_file", help="Path to scrim team CSV file")
    p_scrim_team.add_argument("--season", type=int, default=1, help="Season number (default 1)")

    p_scrim_players = sub.add_parser("ingest-scrims-players", help="Ingest scrim player-level CSV")
    p_scrim_players.add_argument("csv_file", help="Path to scrim player CSV file")
    p_scrim_players.add_argument("--season", type=int, default=1, help="Season number (default 1)")

    p_tp = sub.add_parser("ingest-tournament-players", help="Ingest tournament player-level CSV")
    p_tp.add_argument("csv_file", help="Path to tournament player CSV file")

    p_ops = sub.add_parser("ingest-ops", help="Ingest operator kills/pulls CSV (footage-derived)")
    p_ops.add_argument("csv_file", help="Path to ops kills CSV file")

    p_scrim_ops = sub.add_parser("ingest-scrim-ops", help="Ingest scrim operator JSON from the VOD-review tool")
    p_scrim_ops.add_argument("json_file", help="Path to operator_data JSON file")
    p_scrim_ops.add_argument("--season", type=int, default=1, help="Season number (default 1)")

    p_s2m = sub.add_parser("ingest-s2-matches", help="Ingest Season 2 match data (one row per map)")
    p_s2m.add_argument("csv_file", help="Path to S2 matches CSV file")

    p_s2b = sub.add_parser("ingest-s2-bans", help="Ingest Season 2 bans (attributed)")
    p_s2b.add_argument("csv_file", help="Path to S2 bans CSV file")

    sub.add_parser("backfill", help="Wipe and recalculate Elo")

    args = parser.parse_args()

    commands = {
        "init": cmd_init,
        "ingest": cmd_ingest,
        "ingest-tournament": cmd_ingest_tournament,
        "ingest-playoffs": cmd_ingest_playoffs,
        "ingest-playoff-bans": cmd_ingest_playoff_bans,
        "ingest-scrims-team": cmd_ingest_scrims_team,
        "ingest-scrims-players": cmd_ingest_scrims_players,
        "ingest-tournament-players": cmd_ingest_tournament_players,
        "ingest-ops": cmd_ingest_ops,
        "ingest-scrim-ops": cmd_ingest_scrim_ops,
        "ingest-s2-matches": cmd_ingest_s2_matches,
        "ingest-s2-bans": cmd_ingest_s2_bans,
        "backfill": cmd_backfill,
    }

    if args.command in commands:
        commands[args.command](args)


if __name__ == "__main__":
    main()
