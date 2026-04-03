# Scrim Data Integration — Design Spec

## Overview

Integrate scrim match results and player performance stats into the CDL analytics platform. Scrims are tracked independently from tournament data — separate tables, separate ingestion, separate dashboard tabs. No changes to existing tournament logic.

## Approach

**Separate tables (Approach A).** Scrim data lives in its own `scrim_maps` and `scrim_player_stats` tables with no structural dependency on the tournament schema. The only shared reference is `teams.team_id` for scrim opponents (all scrim opponents are CDL teams). Future cross-referencing between scrim and tournament data can be done via joins on team/map names if needed.

## Scrim Data Characteristics

- No series structure — scrims are freeform, any number of maps in any order
- No pick/ban mechanic — both teams agree on what to play
- No Elo or Map Strength calculations for scrims
- Player stats tracked for GL only (5 fixed players)
- Scrim opponents are always CDL teams (FK to `teams`)

---

## Schema

### `scrim_maps`

One row per map played in a scrim session.

```sql
CREATE TABLE scrim_maps (
    scrim_map_id    INTEGER PRIMARY KEY,
    scrim_date      DATE NOT NULL,
    week            INTEGER NOT NULL,
    opponent_id     INTEGER NOT NULL REFERENCES teams(team_id),
    map_name        TEXT NOT NULL,
    mode            TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')),
    game_number     INTEGER NOT NULL DEFAULT 1,
    our_score       INTEGER NOT NULL,
    opponent_score  INTEGER NOT NULL,
    result          TEXT NOT NULL CHECK(result IN ('W', 'L'))
);
```

`game_number` disambiguates when the same map+mode is played multiple times against the same opponent on the same day. Defaults to 1. The natural key for duplicate detection and player stat linking becomes (scrim_date, opponent_id, map_name, mode, game_number).

### `scrim_player_stats`

One row per player per map. Linked to `scrim_maps` via FK.

```sql
CREATE TABLE scrim_player_stats (
    stat_id         INTEGER PRIMARY KEY,
    scrim_map_id    INTEGER NOT NULL REFERENCES scrim_maps(scrim_map_id),
    player_name     TEXT NOT NULL,
    kills           INTEGER NOT NULL,
    deaths          INTEGER NOT NULL,
    assists         INTEGER NOT NULL,
    UNIQUE(scrim_map_id, player_name)
);
```

**Derived at query time (not stored):**
- K/D = kills / deaths
- Total Engagements = kills + deaths + assists
- Pos Eng % = kills / (kills + deaths + assists)

---

## Ingestion

New module: `src/cdm_stats/ingestion/scrim_loader.py`

### Team CSV

**Columns:** `Date, Week, Opponent, Map, Mode, Score, Result`

- `Opponent` matched against `teams.abbreviation`
- `Score` parsed as `"X-Y"` to extract `our_score` and `opponent_score`
- `Result` validated as W/L and cross-checked against scores
- Duplicate detection on natural key: (scrim_date, opponent_id, map_name, mode, game_number)
- `game_number` auto-assigned during ingestion: if the same (date, opponent, map, mode) appears multiple times in the CSV, they get sequential game_numbers (1, 2, 3...)

### Player CSV

**Columns:** `Date, Week, Opponent, Map, Mode, Player, Kills, Deaths, Assists`

- K/D, Total Eng, Pos Eng % columns in the source sheet are ignored (derived at query time)
- Each row linked to its `scrim_maps` row by matching on (Date, Opponent, Map, Mode, game_number) — game_number is auto-assigned in the same order as the team CSV (sequential appearance of duplicate map+mode combos)
- Duplicate detection on UNIQUE(scrim_map_id, player_name)

### Ingestion Flow

1. Team CSV ingested first — creates `scrim_maps` rows
2. Player CSV ingested second — links to existing `scrim_maps` rows via natural key lookup
3. Both idempotent — re-running does not create duplicates

### CLI Commands

Added to `main.py`:

- `python main.py ingest-scrims-team <csv_path>`
- `python main.py ingest-scrims-players <csv_path>`

---

## Queries

New module: `src/cdm_stats/db/queries_scrim.py`

All queries take optional filters and default to returning everything.

### Scrim Map Queries

- `scrim_win_loss(mode=None, map_name=None, week_range=None)` — W, L, Win%
- `scrim_map_breakdown(mode=None, week_range=None)` — per-map: Played, W, L, Win%, Avg Our Score, Avg Opp Score
- `scrim_weekly_trend(mode=None, map_name=None)` — per-week win rate for trend chart

### Player Stat Queries

- `player_summary(player=None, mode=None, week_range=None)` — per-player: total Kills, Deaths, Assists, K/D
- `player_weekly_trend(player=None, mode=None)` — per-week K/D per player for trend chart
- `player_map_breakdown(player=None, mode=None, week_range=None)` — per-map: Games, Avg K/D, Avg Kills, Avg Deaths, Avg Assists, Avg Pos Eng%

---

## Dashboard

Two new tabs added to the existing Dash app.

### Tab: Scrim Performance

**Filters:** Mode (SnD / HP / Control / All), Week range, Map (specific or All)

**Content:**
- **W/L summary cards** — overall scrim win rate, plus per-mode breakdown
- **Map breakdown table** — rows are maps, columns: Played, W, L, Win%, Our Avg Score, Opp Avg Score. Sortable. Respects mode/week filters.
- **Weekly trend line chart** — win rate by week, filterable by map or mode. Answers "are we improving on this map?"

### Tab: Player Stats

**Filters:** Player (specific or All), Mode (SnD / HP / Control / All), Week range

**Content:**
- **Player summary cards** — one per player: overall K/D, total Kills, Deaths, Assists across filtered range
- **Weekly K/D trend chart** — line chart, one line per player (or single player if filtered), x-axis is week
- **Per-map breakdown table** — rows are maps, columns: Games Played, Avg K/D, Avg Kills, Avg Deaths, Avg Assists, Avg Pos Eng%

---

## Scope Boundary

**In scope:**
- 2 new tables (`scrim_maps`, `scrim_player_stats`)
- 1 new ingestion module (`scrim_loader.py`) with two CSV loaders
- 2 new CLI commands
- 1 new query module (`queries_scrim.py`)
- 2 new dashboard tabs (Scrim Performance, Player Stats)
- Tests for ingestion, queries, and dashboard tabs

**Out of scope:**
- No changes to existing tournament tables, metrics, or dashboard tabs
- No scrim data in Match-Up Prep tab (future work)
- No Elo or Map Strength calculations for scrims
- No Google Sheets export for scrim data
