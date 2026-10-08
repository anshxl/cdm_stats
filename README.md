# CDM Stats

Match, scrim and player analytics for GodLike.

Data comes in as CSV and JSON files, goes into a SQLite database (`data/cdl.db`) through a CLI, and is shown in a web dashboard.

## What it tracks

- **League matches:** map results, picks and bans, Elo ratings, and map strength for every team.
- **Scrims:** map results, player K/D/A, and operator stats (op kills per pull) from VOD review.
- **Players:** K/D and operator efficiency over time, in league matches and in scrims.

## Dashboard

A FastAPI backend with a React frontend. It has five pages:

- **Elo:** team ratings and how they changed over the season.
- **Team:** one team's map strength, recent series and player stats.
- **H2H:** two teams compared, with an optional pre-match brief written by Claude.
- **Scrims:** our scrim win rates, map breakdown and player stats.
- **Training:** player participation, read live from the Discord bot.

## Run it

```
uv sync --extra dev
uv run uvicorn cdm_stats.api.app:app --reload   # API on :8000
cd frontend && npm ci && npm run dev            # dashboard on :5173
```

## Add data

```
uv run python main.py ingest-scrims-team data/s2/scrims_team.csv --season 2
uv run python main.py ingest-scrims-players data/s2/scrims_players.csv --season 2
uv run python main.py ingest-scrim-ops data/op_data/<export>.json --season 2
```

Run `uv run python main.py --help` for every command. Run the tests with `uv run pytest`.
