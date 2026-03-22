# CLI Reference

All commands are run with `uv run python main.py <command>`.

## Setup

| Command | Description |
|---------|-------------|
| `init` | Creates the SQLite database, runs migrations, and seeds the teams + maps tables. Run once on first setup, safe to re-run (idempotent). |

## Data Ingestion

| Command | Description |
|---------|-------------|
| `ingest <csv_file>` | Ingests match data from a CSV file. Derives pick order, pick context, series scores, and picker-relative scores automatically. Updates Elo after each match. Skips duplicates. |
| `ingest-tournament <maps_csv> <bans_csv>` | Ingests tournament/playoff data from two CSV files (maps and bans). Runs migrations before ingesting. |
| `backfill` | Wipes all Elo data and recalculates from scratch in chronological order. Use after changing K-factor or fixing historical data. |

## Export (Excel)

| Command | Description |
|---------|-------------|
| `export matrix` | Generates `map_matrix.xlsx` — a 14-team × 13-map grid. Each cell shows Pick W-L, Defend W-L, Avoidance %, and Target %, color-coded by strength. |
| `export matchup <your_team> <opponent>` | Generates `matchup_<your>_vs_<opp>.xlsx` — per-map comparison of both teams' pick/defend records, avoidance/target indices, and Elo ratings. |
| `export profile <team> [--format FMT]` | Generates `profile_<team>.xlsx` — team W-L summary and ban data. Optional `--format` filters by series format (e.g. `TOURNAMENT`, `CDL_PLAYOFF`). |

## Charts (PNG)

| Command | Description |
|---------|-------------|
| `chart heatmap <team>` | Generates `heatmap_<team>.png` — grouped bar chart of avoidance vs target index per map. |
| `chart elo <team>` | Generates `elo_<team>.png` — single team's Elo rating over time. |
| `chart elo-all` | Generates `elo_all_teams.png` — all teams' Elo trajectories on one chart. |

## Key Stats

- **Avoidance % (Av):** How often a team *passes* on a map when they have a pick opportunity for that mode. High avoidance = they avoid it, likely a weakness.
- **Target % (Tg):** How often *opponents* pass on a map when picking against this team. High target = opponents don't want to give them this map, likely a strength.
- Read them together: high avoid + low target = team weakness (they dodge it, opponents force it). Low avoid + high target = team strength (they pick it, opponents avoid it).
- All avoidance/target numbers exclude slot 5 (coin toss) and are shown with sample size `(n=N)`. Treat `n < 4` as unreliable.

## Notes

- All outputs go to the `output/` directory.
- Team arguments use abbreviations (e.g. `OUG`, `GL`, `DVS`).
- Database lives at `data/cdl.db`.
