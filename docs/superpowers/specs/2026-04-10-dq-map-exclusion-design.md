# DQ Map Exclusion — Design

**Goal:** Exclude DQ'd maps from all map-level metrics (Elo margin, pick/defend W-L, pick context distribution, map strength, per-map W-L) while preserving the face-value result row for audit. Series outcomes continue to be handled by the existing `series_winner` CSV override.

**Context:** Margin-weighted Elo (shipped in the 2026-04-02 plan) averages signed per-map margins across every map in a series. A DQ'd map contributes a large negative margin for the series winner, drastically reducing their Elo gain. The same logic applies to pick/defend records — crediting a DQ'd pick-win misrepresents the data. Observed in the 2026-03-20 GL vs OUG series where OUG was DQ'd on Standoff.

## Schema

Add one column to `map_results`:

```sql
dq INTEGER NOT NULL DEFAULT 0 CHECK(dq IN (0, 1))
```

Bump `SCHEMA_VERSION` to 5. Migration is a single `ALTER TABLE map_results ADD COLUMN dq INTEGER NOT NULL DEFAULT 0`. SQLite supports this without rebuild.

`winner_team_id` on a DQ'd row stays at face value (whoever the server said won). The `dq` flag is the exclusion signal.

## Ingestion

Add an optional `dq` column to `matches.csv`. Blank or `0` → not DQ'd; `1` → DQ'd.

- `csv_loader.ingest_csv` reads `row.get("dq")`, parses to `0`/`1`, defaults to `0` when absent. CSVs without the column continue to load.
- `queries.insert_map_result` gains a `dq: int = 0` parameter and writes it.

No extra validation — a DQ row is just a normal row with the flag set.

## Metrics filter

Every query that aggregates `map_results` adds `AND mr.dq = 0` (or `AND dq = 0` when unqualified):

- `metrics/elo.py::update_elo` — map_rows query (the signed-margin loop). The existing `else 0.0` fallback on an empty list already handles the theoretical all-DQ'd edge case; no special handling needed.
- `metrics/avoidance.py::pick_win_loss`
- `metrics/avoidance.py::defend_win_loss`
- `metrics/avoidance.py::pick_context_distribution`
- `metrics/map_strength.py::map_strength` — rows query
- `db/queries.py::get_team_map_wl` — both the format-filtered and unfiltered branches

Unchanged: `margin.dominance_flag` (pure function, callers already filter upstream), ban-summary queries, match-level queries.

## Data correction

1. Add `dq` column to `matches.csv` header. Leave every existing row blank except the 2026-03-20 GL vs OUG Standoff row (slot 3), which gets `dq=1`.
2. Apply the same flag to the live DB with a targeted `UPDATE` keyed on `match_date`, team abbreviations, and `slot = 3`. Verify exactly one row updated.
3. Run `recalculate_all_elo(conn)`. Spot-check: GL's Elo should gain noticeably more than before; OUG should lose slightly more.
4. Commit `data/cdl.db` and `data/matches.csv`.

## Tests

One test per affected metric, plus loader coverage:

- `test_elo.py` — 3-map series with the middle map flagged DQ; assert Elo delta equals the 2-map-only computation (and differs from the face-value 3-map computation).
- `test_avoidance.py` — DQ'd pick-win doesn't increment `pick_win_loss.wins`; DQ'd defend-loss doesn't increment `defend_win_loss.losses`; DQ'd pick doesn't appear in any `pick_context_distribution` bucket.
- `test_map_strength.py` — DQ'd map is excluded from `total_played` and from the rating.
- `test_queries.py` — DQ'd map doesn't contribute to `get_team_map_wl`.
- `test_csv_loader.py` — row with `dq=1` persists as `1`; blank `dq` persists as `0`; CSV without the `dq` column still loads successfully (backwards compat).
