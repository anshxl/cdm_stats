# CDM Stats — Design Specification

## Overview

Data pipeline for tracking and analyzing CDL match results at the map level. Ingests match data from CSV, stores in SQLite, computes derived metrics, and exports formatted Excel workbooks for coaching staff.

All outputs are designed to inform human judgment — every metric is presented with sample size, and low-confidence flags are surfaced prominently.

---

## Tech Stack

- **Python >=3.12** (system default)
- **uv** for dependency and environment management
- **SQLite** via `sqlite3` — raw SQL with parameterized queries, no ORM
- **openpyxl** for Excel workbook generation
- **matplotlib** for local chart generation

---

## League Structure (Constants)

- 14 teams, single group stage
- Each team plays every other team exactly once: 13 matches per team, 91 total matches
- All series are Best-of-5
- Mode order is fixed: **SnD → HP → Control → SnD → HP** (slots 1–5)
- Slot 5 is always a coin toss — excluded from all pick/avoidance calculations
- Map pools are mutually exclusive:
  - 5 SnD maps → valid in slots 1, 4
  - 5 HP maps → valid in slots 2, 5
  - 3 Control maps → valid in slot 3

### Pick/Ban Mechanic

- 2v2 mini-game winner picks Map 1
- After each map, the **loser** of that map picks the next map
- This continues through Map 4; Map 5 is a coin toss (picked_by = NULL)
- A team that sweeps 3-0 had exactly **one** pick opportunity

---

## Database Schema

### `teams`
```sql
CREATE TABLE teams (
    team_id      INTEGER PRIMARY KEY,
    team_name    TEXT NOT NULL,
    abbreviation TEXT NOT NULL UNIQUE
);
```

### `maps`
```sql
CREATE TABLE maps (
    map_id   INTEGER PRIMARY KEY,
    map_name TEXT NOT NULL,
    mode     TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control'))
);
```

No `bo5_slot` column — valid slots are derived from mode:
- SnD → {1, 4}, HP → {2, 5}, Control → {3}

### `matches`
```sql
CREATE TABLE matches (
    match_id            INTEGER PRIMARY KEY,
    match_date          DATE NOT NULL,
    team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
    team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
    two_v_two_winner_id INTEGER NOT NULL REFERENCES teams(team_id),
    series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
    CHECK(team1_id != team2_id)
);
```

For same-date matches, `match_id` insertion order is the tiebreaker for Elo chronological ordering.

### `map_results`
```sql
CREATE TABLE map_results (
    result_id              INTEGER PRIMARY KEY,
    match_id               INTEGER NOT NULL REFERENCES matches(match_id),
    slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 5),
    map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
    picked_by_team_id      INTEGER REFERENCES teams(team_id),  -- NULL for slot 5
    winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
    picking_team_score     INTEGER NOT NULL,
    non_picking_team_score INTEGER NOT NULL,
    team1_score_before     INTEGER NOT NULL,
    team2_score_before     INTEGER NOT NULL,
    pick_context           TEXT NOT NULL CHECK(pick_context IN (
                               'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss'
                           )),
    UNIQUE(match_id, slot)
);
```

#### `pick_context` derivation rules

| Condition | Context |
|-----------|---------|
| Slot 5 | `Coin-Toss` |
| Slot 1 | `Opener` |
| Picker's series score == 0, opponent == 2 | `Must-Win` |
| Picker's series score == 1, opponent == 2 | `Must-Win` |
| Picker's series score == 2, opponent < 2 | `Close-Out` |
| All other cases | `Neutral` |

Control (slot 3) can be `Must-Win` when the picking team is down 0-2.

### `team_elo`
```sql
CREATE TABLE team_elo (
    elo_id     INTEGER PRIMARY KEY,
    team_id    INTEGER NOT NULL REFERENCES teams(team_id),
    match_id   INTEGER NOT NULL REFERENCES matches(match_id),
    elo_after  REAL NOT NULL,
    match_date DATE NOT NULL,
    UNIQUE(team_id, match_id)
);
```

Full Elo history retained — never overwrite, always insert.

### `team_map_notes`
```sql
CREATE TABLE team_map_notes (
    note_id    INTEGER PRIMARY KEY,
    team_id    INTEGER NOT NULL REFERENCES teams(team_id),
    map_id     INTEGER NOT NULL REFERENCES maps(map_id),
    note       TEXT NOT NULL,
    created_at DATE NOT NULL
);
```

For coaching staff to flag suspected strategic hiding or other qualitative observations.

---

## CSV Ingestion

### Input format

One row per map, grouped by match, ordered by slot:

```csv
date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Rewind,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Vault,ATL,3,1
```

Teams referenced by abbreviation. A 3-0 sweep has 3 rows, a 3-2 has 5 rows.

### Derivation pipeline (per match, slots processed in order)

1. **Group CSV rows by match** (date + team1 + team2)
2. **Derive `picked_by_team_id`:**
   - Slot 1 → 2v2 winner
   - Slot 5 → NULL (coin toss)
   - Slots 2–4 → loser of previous map
3. **Compute running series scores** (`team1_score_before`, `team2_score_before`) by accumulating wins
4. **Derive `pick_context`** from slot + series state using rules table
5. **Compute `picking_team_score` / `non_picking_team_score`** — remap winner/loser scores based on whether the picker won. For slot 5 (no picker), store `winner_score` as `picking_team_score` and `loser_score` as `non_picking_team_score` by convention.
6. **Derive `series_winner_id`** from accumulated map wins (the team that reaches 3)
7. **Insert match + map_results atomically** in a single transaction
8. **Compute Elo update** for both teams after match insert

### Validation rules (reject match on failure)

- Team abbreviations exist in `teams` table
- Map name exists in `maps` table and mode matches slot's expected mode
- Winner is one of team1/team2
- 2v2 winner is one of team1/team2
- Slots are sequential starting from 1, no gaps
- Exactly one team reaches 3 wins by the final slot
- No duplicate match (same date + teams)

---

## Metrics Layer

All computed at query time from raw DB data — no materialized tables. Each metric is a function that takes a DB connection and parameters, runs SQL via `queries.py`, returns structured results.

### Elo (`elo.py`)

- Series-level only, one update per match per team
- Seed: 1000 for all teams, K-factor: 32
- Formula: `Expected = 1 / (1 + 10^((opp_elo - team_elo) / 400))`, `New = Current + K * (result - expected)`
- `low_confidence` flag when team has < 7 matches
- Functions: `update_elo()`, `get_current_elo()`, `get_elo_history()`

### Avoidance & Target (`avoidance.py`)

- `pick_win_loss(team, map)` — W/L on maps the team chose (excl. slot 5)
- `defend_win_loss(team, map)` — W/L when opponent chose this map (excl. slot 5)
- `avoidance_index(team, map, mode)` — times team had pick priority on this mode's slot but chose differently / total pick opportunities on that slot
- `target_index(team, map, mode)` — same from opponent's perspective
- All return ratio + sample size
- Unreliable below 4 opportunities — flagged in output

### Pick opportunity counting

Walk `map_results` rows per match in slot order. A swept team (0-3) may have had 0 pick opportunities. Use actual opportunity count as denominator, never match count.

### Margin (`margin.py`)

- Raw margins derived from stored scores, never normalized across modes
- Dominance flag thresholds:

| Mode | Dominant | Contested |
|------|----------|-----------|
| SnD | Won by 3+ rounds | Won by 1 round |
| HP | Won by 70+ points | Won by < 25 points |
| Control | Won by 2+ rounds | Won by 1 round |

### Pick Context Distribution

Per team per map: breakdown of picks by context (Opener, Neutral, Must-Win, Close-Out). Computed via grouped query on `map_results`.

---

## Excel Export (`export/excel.py`)

Generates `.xlsx` workbooks using `openpyxl`. Output to `output/` directory (auto-created on first run, git-ignored). Re-running overwrites the target file — inherently idempotent.

### Sheet 1: Map Matrix

- Rows = 14 teams, Columns = 13 maps (grouped by mode)
- Cell content: `Pick: W-L | Defend: W-L | Avoid: X% (n=N) | Target: X% (n=N)`
- Color coding:
  - Green fill — strength (high pick + defend W/L)
  - Red fill — weakness (low defend W/L or high avoidance)
  - Yellow fill — contested or sample size < 4
- Sample size < 4 → yellow fill regardless of other metrics

### Sheet 2: Match-Up Prep

- Parameterized by your team + opponent
- Generates a standalone file per matchup: `output/matchup_ATL_vs_LAT.xlsx`
- Per map row: opponent's avoidance & target index, your pick & defend W/L, dominance flags (all with sample sizes)
- Footer: Elo ratings for both teams with low-confidence flag if applicable

---

## Charts (`charts/heatmap.py`)

Generated on demand via CLI, saved as `.png` to `output/`:

- **Avoidance vs Target heatmap** — per team, maps on axes, color = index value
- **Elo trajectory** — per team, line chart across the season

---

## CLI Interface (`main.py`)

```
python main.py ingest data/matches.csv           # load CSV into DB
python main.py export matrix                      # Map Matrix workbook
python main.py export matchup ATL LAT             # Match-Up Prep for ATL vs LAT
python main.py chart heatmap ATL                  # avoidance vs target heatmap
python main.py chart elo ATL                      # Elo trajectory
python main.py backfill                           # wipe and recalculate Elo from existing DB rows in chronological order
python main.py init                               # create DB + seed teams/maps
```

---

## Project Structure

```
cdm_stats/
├── CLAUDE.md
├── pyproject.toml
├── .python-version
├── data/
│   └── cdl.db
├── src/
│   └── cdm_stats/
│       ├── __init__.py
│       ├── db/
│       │   ├── schema.py
│       │   └── queries.py
│       ├── ingestion/
│       │   ├── csv_loader.py
│       │   ├── backfill.py
│       │   └── seed.py
│       ├── metrics/
│       │   ├── elo.py
│       │   ├── avoidance.py
│       │   └── margin.py
│       ├── export/
│       │   └── excel.py
│       └── charts/
│           └── heatmap.py
├── tests/
│   ├── test_csv_loader.py
│   ├── test_elo.py
│   └── test_pick_context.py
├── output/                    # generated .xlsx and .png files
└── main.py
```

---

## Constraints (surfaced in all coaching-facing output)

1. **Sample size is everything.** No metric presented without its sample size.
2. **Elo is a second-half-of-season tool.** Low-confidence flag before 7 matches.
3. **Map 5 excluded** from all pick/avoidance metrics. Tracked for W/L only.
4. **Strategic hiding is not measurable.** Use `team_map_notes` for manual annotation.
5. **Avoidance requires opportunity.** Use opportunity count as denominator, not match count.
6. **Margin is mode-specific.** Never compare across modes.
