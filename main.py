import argparse
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cdl.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


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


def cmd_ingest(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.csv_loader import ingest_csv

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_csv(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['match']}")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()


def cmd_export_matrix(_args: argparse.Namespace) -> None:
    from cdm_stats.export.excel import export_map_matrix

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    path = os.path.join(OUTPUT_DIR, "map_matrix.xlsx")
    export_map_matrix(conn, path)
    conn.close()
    print(f"Map Matrix exported to {path}")


def cmd_export_matchup(args: argparse.Namespace) -> None:
    from cdm_stats.export.excel import export_matchup_prep
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    your_id = get_team_id_by_abbr(conn, args.your_team)
    opp_id = get_team_id_by_abbr(conn, args.opponent)
    if not your_id or not opp_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"matchup_{args.your_team}_vs_{args.opponent}.xlsx")
    export_matchup_prep(conn, your_id, opp_id, path)
    conn.close()
    print(f"Match-Up Prep exported to {path}")


def cmd_chart_heatmap(args: argparse.Namespace) -> None:
    from cdm_stats.charts.heatmap import chart_avoidance_target
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    team_id = get_team_id_by_abbr(conn, args.team)
    if not team_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"heatmap_{args.team}.png")
    chart_avoidance_target(conn, team_id, path)
    conn.close()
    print(f"Heatmap exported to {path}")


def cmd_chart_elo(args: argparse.Namespace) -> None:
    from cdm_stats.charts.heatmap import chart_elo_trajectory
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    team_id = get_team_id_by_abbr(conn, args.team)
    if not team_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    path = os.path.join(OUTPUT_DIR, f"elo_{args.team}.png")
    chart_elo_trajectory(conn, team_id, path)
    conn.close()
    print(f"Elo trajectory exported to {path}")


def cmd_ingest_playoffs(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.playoff_loader import ingest_playoffs

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_playoffs(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['match']}")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()


def cmd_ingest_playoff_bans(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_playoff_bans(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']} ({r['bans']} bans)")
        elif r["status"] == "skipped":
            print(f"  SKIPPED: {r['match']} ({r.get('reason', '')})")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()


def cmd_ingest_tournament(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    from cdm_stats.db.schema import migrate

    conn = get_db()
    migrate(conn)
    with open(args.maps_csv) as mf, open(args.bans_csv) as bf:
        results = ingest_tournament(conn, mf, bf)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['match']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED: {r['match']} ({r.get('reason', '')})")
        else:
            print(f"  ERROR: {r['match']}: {r['errors']}")

    conn.close()


def cmd_export_profile(args: argparse.Namespace) -> None:
    from cdm_stats.export.excel import export_team_profile
    from cdm_stats.db.queries import get_team_id_by_abbr

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    team_id = get_team_id_by_abbr(conn, args.team)
    if not team_id:
        print("Error: unknown team abbreviation")
        sys.exit(1)
    fmt = args.format if args.format else None
    suffix = f"_{args.format}" if args.format else ""
    path = os.path.join(OUTPUT_DIR, f"profile_{args.team}{suffix}.xlsx")
    export_team_profile(conn, team_id, path, format_filter=fmt)
    conn.close()
    print(f"Team Profile exported to {path}")


def cmd_chart_elo_all(_args: argparse.Namespace) -> None:
    from cdm_stats.charts.heatmap import chart_elo_all_teams

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = get_db()
    path = os.path.join(OUTPUT_DIR, "elo_all_teams.png")
    chart_elo_all_teams(conn, path)
    conn.close()
    print(f"All-teams Elo trajectory exported to {path}")


def cmd_ingest_scrims_team(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_scrims_team(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['row']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['row']}")
        else:
            print(f"  ERROR: {r['row']}: {r['errors']}")

    conn.close()


def cmd_ingest_scrims_players(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_players

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_scrims_players(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['row']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['row']}")
        else:
            print(f"  ERROR: {r['row']}: {r['errors']}")

    conn.close()


def cmd_ingest_tournament_players(args: argparse.Namespace) -> None:
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players

    conn = get_db()
    with open(args.csv_file) as f:
        results = ingest_tournament_players(conn, f)

    for r in results:
        if r["status"] == "ok":
            print(f"  OK: {r['row']}")
        elif r["status"] == "skipped":
            print(f"  SKIPPED (duplicate): {r['row']}")
        else:
            print(f"  ERROR: {r['row']}: {r['errors']}")

    conn.close()


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

    sub_export = sub.add_parser("export", help="Export data to Excel")
    export_sub = sub_export.add_subparsers(dest="export_type", required=True)
    export_sub.add_parser("matrix", help="Export Map Matrix")
    p_matchup = export_sub.add_parser("matchup", help="Export Match-Up Prep")
    p_matchup.add_argument("your_team", help="Your team abbreviation")
    p_matchup.add_argument("opponent", help="Opponent team abbreviation")
    p_profile = export_sub.add_parser("profile", help="Export Team Profile (W-L + bans)")
    p_profile.add_argument("team", help="Team abbreviation")
    p_profile.add_argument("--format", help="Filter by format prefix (e.g. TOURNAMENT, CDL_PLAYOFF)", default=None)

    sub_chart = sub.add_parser("chart", help="Generate charts")
    chart_sub = sub_chart.add_subparsers(dest="chart_type", required=True)
    p_heatmap = chart_sub.add_parser("heatmap", help="Avoidance vs Target heatmap")
    p_heatmap.add_argument("team", help="Team abbreviation")
    p_elo = chart_sub.add_parser("elo", help="Elo trajectory")
    p_elo.add_argument("team", help="Team abbreviation")
    chart_sub.add_parser("elo-all", help="Elo trajectory for all teams")

    p_ingest_t = sub.add_parser("ingest-tournament", help="Ingest tournament/playoff data from CSVs")
    p_ingest_t.add_argument("maps_csv", help="Path to maps CSV file")
    p_ingest_t.add_argument("bans_csv", help="Path to bans CSV file")

    p_ingest_pf = sub.add_parser("ingest-playoffs", help="Ingest CDL playoff series from CSV")
    p_ingest_pf.add_argument("csv_file", help="Path to playoffs CSV file")

    p_ingest_pfb = sub.add_parser("ingest-playoff-bans", help="Ingest CDL playoff bans from CSV")
    p_ingest_pfb.add_argument("csv_file", help="Path to playoff bans CSV file")

    p_scrim_team = sub.add_parser("ingest-scrims-team", help="Ingest scrim team-level CSV")
    p_scrim_team.add_argument("csv_file", help="Path to scrim team CSV file")

    p_scrim_players = sub.add_parser("ingest-scrims-players", help="Ingest scrim player-level CSV")
    p_scrim_players.add_argument("csv_file", help="Path to scrim player CSV file")

    p_tp = sub.add_parser("ingest-tournament-players", help="Ingest tournament player-level CSV")
    p_tp.add_argument("csv_file", help="Path to tournament player CSV file")

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
        "backfill": cmd_backfill,
    }

    if args.command in commands:
        commands[args.command](args)
    elif args.command == "export":
        if args.export_type == "matrix":
            cmd_export_matrix(args)
        elif args.export_type == "matchup":
            cmd_export_matchup(args)
        elif args.export_type == "profile":
            cmd_export_profile(args)
    elif args.command == "chart":
        if args.chart_type == "heatmap":
            cmd_chart_heatmap(args)
        elif args.chart_type == "elo":
            cmd_chart_elo(args)
        elif args.chart_type == "elo-all":
            cmd_chart_elo_all(args)


if __name__ == "__main__":
    main()
