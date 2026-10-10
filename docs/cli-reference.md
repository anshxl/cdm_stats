# CLI Reference

All commands are run with `uv run python main.py <command>`. Every ingest command skips rows that are already in the database, so re-running a whole file is safe.

## Setup

| Command | Description |
|---------|-------------|
| `init` | Creates the SQLite database (`data/cdl.db`), runs migrations, and seeds the teams and maps tables. Safe to re-run. |
| `backfill` | Wipes all Elo data and recalculates it in date order. Use after changing the K-factor or fixing historical data. |

## Season 2 league data (current)

| Command | Description |
|---------|-------------|
| `ingest-s2-matches <csv_file>` | One row per map: `date,competition,stage,format,team1,team2,map,team1_score,team2_score,picked_by,series_winner,dq,advantaged_team`. Updates Elo. |
| `ingest-s2-bans <csv_file>` | Attributed bans for already-ingested series: `date,competition,stage,team1,team2,banned_by,map`. |
| `ingest-tournament-players <csv_file>` | Player K/D/A per league map: `Date,Opponent,Map,Player,Kills,Deaths,Assists` (`Week`, `Mode` optional). Matches must be ingested first. |
| `ingest-ops <csv_file>` | Operator kills/pulls per player per league map, from footage review: `Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin` (`Week` optional). Matches must be ingested first. |

## Scrims (pass `--season 2` for Season 2 files)

| Command | Description |
|---------|-------------|
| `ingest-scrims-team <csv_file> --season N` | One row per scrim map: `Date,Opponent,Map,Score` (our score first, e.g. `250-200`). Week, mode and result are derived. |
| `ingest-scrims-players <csv_file> --season N` | Player K/D/A per scrim map: `Date,Opponent,Map,Player,Kills,Deaths,Assists`. Run after the team file. |
| `ingest-scrim-ops <json_file> --season N` | The VOD-review tool's operator JSON (`godlike-operator-data`). Matches each map to a scrim on date + opponent + map, so run after the team file. The date comes from `date`, or from the scrim name (`AlUla 7 October`). The opponent can be an abbreviation or the start of a team name. Kills and pulls are counted from `pullDetails`, with a warning when the totals disagree. Re-runs update changed rows. |

A new season needs its first Monday in `SEASON_WEEK1_MONDAY` (`src/cdm_stats/ingestion/scrim_loader.py`).

## Season 1 data (only needed to rebuild the database)

| Command | Description |
|---------|-------------|
| `ingest <csv_file>` | Season 1 league matches. Derives pick order and pick context. Updates Elo. |
| `ingest-tournament <maps_csv> <bans_csv>` | Tournament series from a maps CSV and a bans CSV. |
| `ingest-playoffs <csv_file>` | CDL playoff series (LCQ through Finals), one row per map. Updates Elo. |
| `ingest-playoff-bans <csv_file>` | Bans for already-ingested playoff series: `date,team1,team2,banned_by,map`. |

## Key stats

- **Avoidance %:** how often a team passes on a map when it has a pick for that mode. High avoidance suggests a weakness.
- **Target %:** how often opponents pass on a map when picking against this team. High target suggests a strength.
- Avoidance and target exclude slot 5 (coin toss) and show their sample size. Treat `n < 4` as unreliable.

Team arguments use abbreviations (e.g. `OUG`, `GL`, `DVS`).
