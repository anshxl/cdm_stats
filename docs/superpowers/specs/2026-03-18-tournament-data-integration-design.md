# Tournament Data Integration — Design Spec

## Problem

The current pipeline handles CDL regular season matches (BO5, fixed mode order, known picks via 2v2 mechanic). Two additional format families need to be supported:

1. **External tournament** (ELV vs ALU): 1 BO5 + 2 BO7 = 17 maps across 3 series. Bans are known, picks are not. Mode order differs from CDL.
2. **CDL playoffs**: BO5 rounds and BO7 finals. Both bans and picks are known. Picks are seed-determined (alternating), not reactive to map results. Mode order matches regular season for BO5; BO7 finals extend to 7 slots.

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
        CHECK(match_format IN ('CDL_BO5', 'CDL_PLAYOFF_BO5', 'CDL_PLAYOFF_BO7',
                                'TOURNAMENT_BO5', 'TOURNAMENT_BO7')),
    CHECK(team1_id != team2_id)
);
```

- Existing rows get `'CDL_BO5'` and retain their `two_v_two_winner_id` values
- Tournament rows (unknown picks) set `two_v_two_winner_id = NULL`
- CDL playoff rows set `two_v_two_winner_id` to the higher-seeded team (first picker)
- The `two_v_two_winner_id` field semantically means "team with first pick priority" across all formats — determined by 2v2 in regular season, by seeding in playoffs, absent in external tournaments

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
    "CDL_BO5":         {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO5": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
    "CDL_PLAYOFF_BO7": {1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP", 6: "Control", 7: "SnD"},
    "TOURNAMENT_BO5":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD"},
    "TOURNAMENT_BO7":  {1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD", 6: "Control", 7: "SnD"},
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
4. Validates expected ban count per series per format:
   - `TOURNAMENT_BO5`: 2 teams × 3 modes (HP, SnD, Control) × 1 ban = 6 bans
   - `TOURNAMENT_BO7`: 2 teams × 2 modes (HP, SnD) × 1 ban = 4 bans
   - `CDL_PLAYOFF_BO5`: 2 teams × 2 modes (SnD, HP) × 1 ban = 4 bans
   - `CDL_PLAYOFF_BO7`: 2 teams × 2 modes (SnD, HP) × 1 ban = 4 bans

### CDL Playoff ingestion

CDL playoffs have both bans and picks known, but the pick mechanic differs from regular season.

**CDL Playoff ban/pick process:**
1. **Bans** (SnD and HP only, no Control bans): Higher seed (HS) bans 1 SnD → Lower seed (LS) bans 1 SnD → HS bans 1 HP → LS bans 1 HP. Total: 4 bans per series (both BO5 and BO7).
2. **Picks** (alternating by seed): HS picks slot 1, LS picks slot 2, HS picks slot 3, LS picks slot 4, and so on. Last slot (5 for BO5, 7 for BO7) is a coin toss.

**CSV format** — same two-CSV approach (maps + bans):

Maps CSV:
```
date,team1,team2,format,higher_seed,map,winner,team1_score,team2_score
```

The `higher_seed` column identifies which team (team1 or team2 abbreviation) has first pick priority. This replaces the `2v2_winner` from regular season.

Bans CSV:
```
date,team1,team2,format,banned_by,map
```

**Playoff loader logic** (extends `tournament_loader.py`):
1. Groups rows by match key `(date, team1, team2)`
2. Reads `format` and `higher_seed` to determine pick assignment
3. Assigns `picked_by_team_id` by alternating: HS for odd slots, LS for even slots, **except the last slot** (slot 5 for BO5, slot 7 for BO7) which is always a coin toss with `picked_by_team_id = NULL`. The coin-toss override takes precedence over the alternating rule.
4. Derives `pick_context` using `derive_pick_context()`, which must be updated to accept the **win threshold** (3 for BO5, 4 for BO7) and the **last slot number** (5 for BO5, 7 for BO7) instead of hardcoding `slot == 5` for coin toss and `opponent_score == 2` for Must-Win. The generalized rules:
   - Last slot → `Coin-Toss`
   - Slot 1 → `Opener`
   - Opponent is 1 win from series victory AND picker is further away → `Must-Win`
   - Picker is 1 win from series victory AND opponent is further away → `Close-Out`
   - All other cases → `Neutral`
5. Scores stored as picker-oriented (`picking_team_score` / `non_picking_team_score`) since the picker is known
6. Sets `two_v_two_winner_id` to the higher-seeded team
7. Validates bans: SnD and HP only, 4 bans total per series
8. Same atomic transaction and duplicate detection as tournament loader

### CLI

```
python main.py ingest-tournament <maps_csv> <bans_csv>
```

The loader reads the `format` column from the CSV to determine which ingestion path to use (tournament vs. playoff). A single CLI command handles all non-regular-season formats.

---

## Metrics Impact

### Unchanged (tournament data flows in)

- **Elo**: Series-level, format-agnostic. All series update team ratings regardless of format.
- **Score margins & dominance flags**: Mode-specific. The current `score_margins()` derives winner/loser scores via `max(picking_team_score, non_picking_team_score)` and `min(...)`, which produces correct results regardless of whether scores are picker-oriented or team1/team2-oriented — the winner always has the higher score. No code changes needed in the margin logic itself.
- **Overall W/L per map**: All formats contribute to a team's record on a map.
- **`derive_pick_context()`**: Must be generalized to accept `win_threshold` (3 for BO5, 4 for BO7) and `last_slot` (5 for BO5, 7 for BO7) parameters. The current hardcoded checks (`slot == 5` for coin toss, `opponent_score == 2` for Must-Win) break for BO7. This affects both the CDL regular-season loader and the new loaders — the CDL loader should pass `win_threshold=3, last_slot=5` explicitly.

### Pick-dependent metrics: format-aware refactor

The `_get_pick_opportunities()`, `avoidance_index()`, and `target_index()` functions currently import the hardcoded `SLOT_MODES` dict. With `FORMAT_SLOT_MODES`, these need updating.

**Which formats contribute to pick-dependent metrics:**
- `CDL_BO5` — yes (picks known via 2v2 mechanic)
- `CDL_PLAYOFF_BO5` / `CDL_PLAYOFF_BO7` — yes (picks known via seeding)
- `TOURNAMENT_BO5` / `TOURNAMENT_BO7` — no (picks unknown)

The underlying queries should join `matches` to get `match_format`, then:
- Exclude rows where `picked_by_team_id IS NULL` (this naturally drops tournament rows and coin-toss slots)
- Use `FORMAT_SLOT_MODES[match_format]` to determine valid slots per mode when computing pick opportunities

This means CDL playoff pick data feeds into avoidance/target index alongside regular season data, which is correct — a team's playoff picks are genuine strategic signals.

**Pick W/L and Defend W/L** filter on `picked_by_team_id IS NOT NULL` — tournament rows drop out automatically, playoff rows contribute. No logic changes beyond the `SLOT_MODES` refactor.

### Minor filter addition

- **Pick context distribution**: Exclude `'Unknown'` from the Opener/Neutral/Must-Win/Close-Out breakdowns.

### New: Ban summary (display only)

Factual display for matchup prep, not a computed metric. Aggregates bans from all formats (tournament and playoff) between the two teams:

> **ELV bans vs ALU:** Hacienda HP (3/3), Tunisia SnD (2/3), Raid Control (1/3)
> **ALU bans vs ELV:** Summit HP (2/3), Coastal SnD (3/3), Crossroads Control (1/3)

Query: group `map_bans` by `(team_id, map_id)` filtered to matches between the two teams, count frequency out of total series with bans.

---

## Export Changes

### Map Matrix

CDL playoff data (with known picks) contributes to pick/defend/avoidance/target metrics alongside regular season. External tournament data (unknown picks) does not. No structural changes to the matrix layout.

### Match-Up Prep — new section when applicable

When the selected matchup has ban data (tournament or playoff), add a **"Head-to-Head Ban & Tournament Data"** section:

- Ban summary (frequency per map per team, across all series with bans)
- Per-map W/L between the two teams from non-regular-season series
- Score margins and dominance flags from those maps

This section only appears when relevant data exists for the specific matchup.

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
