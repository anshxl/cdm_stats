# Dashboard Improvements — Design

**Date:** 2026-04-04
**Scope:** Tab reordering, week-selector UX change, Player Stats tournament/scrim source toggle.

---

## Goals

1. Reorder main tabs and remove the unused Map Matrix tab.
2. Replace the week `RangeSlider` on Scrim Performance and Player Stats with discrete pill buttons ("view one week at a time, optionally zoom out to All").
3. Add a Tournament/Scrim source toggle to Player Stats, backed by a new `tournament_player_stats` table mirroring the scrim player schema.

## Non-Goals

- No changes to Match-Up Prep, Team Profile, or Elo Tracker tabs.
- No tournament player data ingestion UI — loader will be invoked via CLI/backfill; empty-state handled in the tab.
- No cross-source comparison view (Tournament + Scrim side-by-side). Toggle switches view only.
- No week filter added to tabs that don't currently have one.

---

## A. Tab Reorder + Remove Map Matrix

**New tab order** in `src/cdm_stats/dashboard/app.py`:

1. Match-Up Prep (`matchup-prep`) — new default `active_tab`
2. Team Profile (`team-profile`)
3. Player Stats (`player-stats`)
4. Scrim Performance (`scrim-performance`)
5. Elo Tracker (`elo-tracker`)

**Changes:**
- Update `dbc.Tabs` `active_tab` default and child order in `app.py`.
- Remove the `map-matrix` `dbc.Tab`, the `map-matrix` branch in `render_tab`, and the `map_matrix` import + `register_callbacks` call.
- Delete `src/cdm_stats/dashboard/tabs/map_matrix.py`.

---

## B. Week Pills (shared component)

**Problem:** `dcc.RangeSlider` is the wrong primitive — weeks are discrete, and the primary use case is "show me one week." Range selection is rarely needed.

**Component:** new module `src/cdm_stats/dashboard/components/week_pills.py` exposing:

```python
def week_pills(component_id: str, weeks: list[int]) -> dbc.RadioItems: ...
```

- Renders a Bootstrap button group via `dbc.RadioItems` with `inputClassName="btn-check"` + `labelClassName="btn btn-outline-primary"` styling.
- Options: `[{"label": "All", "value": "all"}]` followed by one option per week (`{"label": f"W{w}", "value": w}`).
- Default selected value: `"all"`.
- Single-select only.

**Callback value contract:**
- Callbacks receive a single value: the string `"all"` or an integer week.
- A helper `pill_value_to_range(value) -> tuple[int, int] | None` converts:
  - `"all"` → `None`
  - `int w` → `(w, w)`
- Downstream query functions (`scrim_win_loss`, `scrim_map_breakdown`, `player_summary`, `player_map_breakdown`, etc.) keep their existing `week_range: tuple[int, int] | None` signature — unchanged.

**Application:**
- **Scrim Performance** (`tabs/scrim_performance.py`): replace `dcc.RangeSlider(id="scrim-week-slider", ...)` with `week_pills("scrim-week-pills", weeks)`. Update the two callbacks that currently read `scrim-week-slider.value` and its four dynamic outputs (`min`, `max`, `marks`, `value`) — the new component only needs `options` and `value` refreshed.
- **Player Stats** (`tabs/player_stats.py`): same substitution with `id="player-week-pills"`.

Weekly trend charts remain unfiltered (they show the full season for trend context).

---

## C. Player Stats: Tournament/Scrim Source Toggle

**UI:** add a `dbc.RadioItems` segmented control labeled "Source" with options `[Tournament, Scrim]` next to the existing mode toggle in `tabs/player_stats.py`. Default value: `Tournament`.

**Callback routing:** the tab's main update callback gains `source` as an Input. A dispatch layer selects the query module:

- `source == "scrim"` → existing `cdm_stats.db.queries_scrim.player_summary / player_weekly_trend / player_map_breakdown`
- `source == "tournament"` → new `cdm_stats.db.queries_tournament_player.player_summary / player_weekly_trend / player_map_breakdown` with identical signatures and identical return shapes

Player dropdown options are re-queried from the active source's underlying table so the list reflects players with data in that source.

**Empty-state:** if `source == "tournament"` and `tournament_player_stats` has no rows (or none for the selected player), render an info alert: "No tournament player data ingested yet."

### New Schema

Mirrors `scrim_player_stats` exactly, with `week` stored denormalized on the stats row:

```sql
CREATE TABLE tournament_player_stats (
    stat_id       INTEGER PRIMARY KEY,
    result_id     INTEGER NOT NULL REFERENCES map_results(result_id),
    week          INTEGER NOT NULL,
    player_name   TEXT NOT NULL,
    kills         INTEGER NOT NULL,
    deaths        INTEGER NOT NULL,
    assists       INTEGER NOT NULL,
    UNIQUE(result_id, player_name)
);
```

Added to `src/cdm_stats/db/schema.py` alongside existing `CREATE TABLE` statements.

### New Loader

`src/cdm_stats/ingestion/tournament_player_loader.py` with `ingest_tournament_players(conn, file)` function.

- Reads CSV with columns: `Date, Week, Opponent, Map, Mode, Player, Kills, Deaths, Assists` (exact mirror of `scrims_players.csv`).
- For each row, resolves `opponent_id` via `get_team_id_by_abbr` and `map_id` via `get_map_id`, then looks up the matching `map_results.result_id` by `(match_date, opponent_id on either side of the match, map_id)` — unique in practice because a team can't play the same map in two different series on the same day. Errors if >1 row matches.
- Duplicate detection on `(result_id, player_name)` — skips rows already present.
- Returns list of `{"status": ok|skipped|error, "row": desc, "errors": ...}` dicts matching the existing scrim loader return contract.
- Commits in a single transaction.

### New Queries Module

`src/cdm_stats/db/queries_tournament_player.py` with three functions matching the scrim-side signatures:

```python
def player_summary(conn, player: str, mode: str | None, week_range: tuple[int,int] | None) -> dict: ...
def player_weekly_trend(conn, player: str, mode: str | None) -> list[dict]: ...
def player_map_breakdown(conn, player: str, mode: str | None, week_range: tuple[int,int] | None) -> list[dict]: ...
```

These query `tournament_player_stats` joined to `map_results` and `maps`. Return shapes are identical to the scrim-side equivalents so the tab's rendering functions need no changes.

---

## Affected Files

**Modified:**
- `src/cdm_stats/dashboard/app.py` — tab order, default, Map Matrix removal
- `src/cdm_stats/dashboard/tabs/scrim_performance.py` — week pills
- `src/cdm_stats/dashboard/tabs/player_stats.py` — week pills + source toggle + dispatch
- `src/cdm_stats/db/schema.py` — add `tournament_player_stats` table

**New:**
- `src/cdm_stats/dashboard/components/__init__.py`
- `src/cdm_stats/dashboard/components/week_pills.py`
- `src/cdm_stats/db/queries_tournament_player.py`
- `src/cdm_stats/ingestion/tournament_player_loader.py`

**Deleted:**
- `src/cdm_stats/dashboard/tabs/map_matrix.py`

## Testing

- Manual: launch dashboard, confirm tab order, week pills behavior (All + single-week selection updates tables/charts), source toggle flips Player Stats between tournament and scrim data, empty-state shows when tournament table is empty.
- Schema change: run schema init on a fresh DB, confirm table created. Re-run on existing DB, confirm idempotent (existing `schema.py` pattern).
- Loader: create a minimal tournament player CSV, ingest, verify rows land with correct `result_id` FKs. Re-ingest, verify duplicates skipped.
