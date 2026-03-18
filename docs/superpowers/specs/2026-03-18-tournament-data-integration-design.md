# Tournament Data Integration — Design Spec

## Problem

The current pipeline handles CDL regular season matches (BO5, fixed mode order, known picks via 2v2 mechanic). A different tournament format needs to be supported: series between two known teams (ELV vs ALU) where bans are known but picks are not, the mode order differs, and series can be BO7.

Specifically: 1 BO5 + 2 BO7 = 17 maps played across 3 series. Bans are 1 per team per mode (BO5) or 1 per team for HP and SnD only (BO7). Picks were never broadcast.

## Goals

1. Store tournament map results (scores, winners) alongside regular season data
2. Store ban data per series
3. Keep pick-dependent metrics (pick W/L, defend W/L, avoidance index, target index) uncontaminated
4. Surface ban summaries and head-to-head records as context for coaching staff
5. Support different mode orders per format

## Non-Goals

- Inferring picks from ban/map data
- Creating standalone ban-derived metrics (bans are ambiguous — see Caveats)
- Building a generic tournament bracket system

---

## Schema Changes

### SQLite migration note

SQLite does not support `ALTER TABLE ... ALTER COLUMN` or `ALTER TABLE ... DROP CONSTRAINT`. The changes to `matches` (making `two_v_two_winner_id` nullable) and `map_results` (extending `slot` range to 1-7, adding `'Unknown'` to `pick_context`) require **table rebuild migrations**: create new table with updated constraints, copy existing data, drop old table, rename new table. These migrations must run inside a transaction to protect existing data. The `schema.py` module should gain a `migrate()` function that detects the current schema version and applies needed rebuilds.

### `matches` — add `match_format`, make `two_v_two_winner_id` nullable

New table definition (via rebuild migration):

```sql
CREATE TABLE matches (
    match_id            INTEGER PRIMARY KEY,
    match_date          DATE NOT NULL,
    team1_id            INTEGER NOT NULL REFERENCES teams(team_id),
    team2_id            INTEGER NOT NULL REFERENCES teams(team_id),
    two_v_two_winner_id INTEGER REFERENCES teams(team_id),  -- nullable for tournament
    series_winner_id    INTEGER NOT NULL REFERENCES teams(team_id),
    match_format        TEXT NOT NULL DEFAULT 'CDL_BO5'
        CHECK(match_format IN ('CDL_BO5', 'TOURNAMENT_BO5', 'TOURNAMENT_BO7')),
    CHECK(team1_id != team2_id)
);
```

- Existing rows get `'CDL_BO5'` and retain their `two_v_two_winner_id` values
- Tournament rows set `two_v_two_winner_id = NULL`

### `map_results` — extend slot range, add `'Unknown'` pick_context

New table definition (via rebuild migration):

```sql
CREATE TABLE map_results (
    result_id              INTEGER PRIMARY KEY,
    match_id               INTEGER NOT NULL REFERENCES matches(match_id),
    slot                   INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 7),  -- was 1-5
    map_id                 INTEGER NOT NULL REFERENCES maps(map_id),
    picked_by_team_id      INTEGER REFERENCES teams(team_id),
    winner_team_id         INTEGER NOT NULL REFERENCES teams(team_id),
    picking_team_score     INTEGER NOT NULL,
    non_picking_team_score INTEGER NOT NULL,
    team1_score_before     INTEGER NOT NULL,
    team2_score_before     INTEGER NOT NULL,
    pick_context           TEXT NOT NULL CHECK(pick_context IN (
                               'Opener', 'Neutral', 'Must-Win', 'Close-Out', 'Coin-Toss', 'Unknown'
                           )),
    UNIQUE(match_id, slot)
);
```

Tournament maps get:
- `picked_by_team_id = NULL`
- `pick_context = 'Unknown'`
- `picking_team_score` = team1's score, `non_picking_team_score` = team2's score (repurposed when pick is unknown, oriented by team1/team2 from the match row)

### New table: `map_bans`

```sql
CREATE TABLE map_bans (
    ban_id    INTEGER PRIMARY KEY,
    match_id  INTEGER NOT NULL REFERENCES matches(match_id),
    team_id   INTEGER NOT NULL REFERENCES teams(team_id),
    map_id    INTEGER NOT NULL REFERENCES maps(map_id),
    UNIQUE(match_id, team_id, map_id)
);
```

One row per ban per series. Mode is derivable by joining `maps`.

### Format-dependent slot-mode mapping

Replace the hardcoded `SLOT_MODES` dict in `queries.py` with:

```python
FORMAT_SLOT_MODES = {
    "CDL_BO5":        {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "TOURNAMENT_BO5": {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD"},
    "TOURNAMENT_BO7": {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD", 6: "Control", 7: "SnD"},
}
```

The `get_mode_for_slot()` helper takes a `match_format` parameter instead of using a global constant.

---

## Ingestion

### Two separate CSVs

**Maps CSV** — one row per map played:
```
date,team1,team2,format,map,winner,team1_score,team2_score
```

**Bans CSV** — one row per ban:
```
date,team1,team2,format,banned_by,map
```

Linked by the `(date, team1, team2)` match key, consistent with existing ingestion.

### New `tournament_loader.py`

Separate module from `csv_loader.py` — the CDL loader stays untouched.

**Maps loader logic:**
1. Groups rows by match key `(date, team1, team2)`
2. Reads `format` to determine slot-mode mapping and max slots
3. Validates map modes against `FORMAT_SLOT_MODES[format]` per slot
4. Validates that a series winner is reached (first to 3 for BO5, first to 4 for BO7)
5. Sets `picked_by_team_id = NULL`, `pick_context = 'Unknown'` for all rows
6. Stores scores as team1/team2 (using `picking_team_score` / `non_picking_team_score` columns)
7. Sets `two_v_two_winner_id = NULL` on the match row
8. Fires Elo updates normally (series-level)
9. Atomic transaction per match, duplicate detection via existing match key

**Bans loader logic:**
1. Looks up existing match by `(date, team1, team2)` — match must already exist
2. Validates the banned map's mode is allowed for the format:
   - `TOURNAMENT_BO5`: HP, SnD, and Control bans allowed
   - `TOURNAMENT_BO7`: HP and SnD bans only
3. Inserts into `map_bans`
4. Validates expected ban count per series:
   - BO5: 2 teams × 3 modes (HP, SnD, Control) × 1 ban = 6 bans
   - BO7: 2 teams × 2 modes (HP, SnD) × 1 ban = 4 bans

### CLI

```
python main.py ingest-tournament <maps_csv> <bans_csv>
```

---

## Metrics Impact

### Unchanged (tournament data flows in)

- **Elo**: Series-level, format-agnostic. All 3 series update ELV and ALU ratings.
- **Score margins & dominance flags**: Mode-specific. The current `score_margins()` derives winner/loser scores via `max(picking_team_score, non_picking_team_score)` and `min(...)`, which produces correct results regardless of whether scores are picker-oriented or team1/team2-oriented — the winner always has the higher score. No code changes needed in the margin logic itself.
- **Overall W/L per map**: Tournament results contribute to a team's record on a map.

### Automatically excluded (tournament rows drop out, but refactor needed)

- **Pick W/L**: Filters on `picked_by_team_id IS NOT NULL` — tournament rows drop out. No logic changes.
- **Defend W/L**: Same filter. No logic changes.
- **Avoidance index**: `_get_pick_opportunities()` and `avoidance_index()` currently import the hardcoded `SLOT_MODES` dict to determine valid slots per mode. With the move to `FORMAT_SLOT_MODES`, these functions need updating. The simplest approach: **filter to CDL_BO5 matches only** for pick-dependent metrics (avoidance/target), since tournament matches have no pick data anyway. This is done by adding a `WHERE m.match_format = 'CDL_BO5'` clause to the underlying queries, which is both correct and explicit.
- **Target index**: Same approach — filter to CDL_BO5 matches. The `SLOT_MODES` lookup used in `target_index()` similarly needs the format-aware refactor.

### Minor filter addition

- **Pick context distribution**: Exclude `'Unknown'` from the Opener/Neutral/Must-Win/Close-Out breakdowns.

### New: Ban summary (display only)

Factual display for matchup prep, not a computed metric:

> **ELV bans vs ALU:** Hacienda HP (3/3), Tunisia SnD (2/3), Raid Control (1/3)
> **ALU bans vs ELV:** Summit HP (2/3), Coastal SnD (3/3), Crossroads Control (1/3)

Query: group `map_bans` by `(team_id, map_id)` filtered to matches between the two teams, count frequency out of total series played.

---

## Export Changes

### Map Matrix — no changes

Tournament data with unknown picks doesn't affect pick/defend/avoidance/target. The matrix remains regular-season-only for pick-dependent metrics.

### Match-Up Prep — new section when applicable

When the selected matchup has tournament series data, add a **"Head-to-Head Tournament Data"** section:

- Ban summary (frequency per map per team)
- Per-map W/L between the two teams from tournament series
- Score margins and dominance flags from those maps

This section only appears when tournament data exists for the specific matchup.

---

## Caveats

### Ban ambiguity

A ban does NOT straightforwardly mean "we're weak on this map." It could mean:
- Banning team's weakness on the map
- Opponent's strength on the map
- Relative disadvantage (both decent, but opponent is better)
- Strategic hiding for later stages

With only 3 series between the same two teams, you cannot decompose which interpretation applies from ban data alone. Bans are **context for coaching staff to interpret alongside other signals** — not a standalone metric. A ban that aligns with regular season avoidance is convergent evidence; a ban that contradicts regular season picks suggests opponent-specific fear.

### Score column convention

When `picked_by_team_id IS NULL`, `picking_team_score` holds team1's score and `non_picking_team_score` holds team2's score. Any query reading these columns must check the pick context. This convention is documented here and in code comments — it avoids adding new columns for a case that applies to a subset of data.

### Sample size

17 maps across 3 series is useful head-to-head context but not statistically robust. All tournament-derived displays should include series count / map count.
