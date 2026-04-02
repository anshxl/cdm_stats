# Map Strength Rating & Dashboard Restructure — Design Spec

**Date:** 2026-04-02
**Status:** Approved

---

## Problem

The current Avoidance Index and Target Index metrics measure absence of action — how often a map is *not* picked. This defaults to misleading values (e.g., 100% target when no one picked a map against a team) and captures noise (experimentation, VOD reactivity, strategic hiding) rather than signal.

## Solution

Replace avoidance/target with a **Map Strength Rating** — a context-weighted, Elo-adjusted win rate that measures what teams actually DO on maps they play, especially under pressure.

---

## 1. Map Strength Rating

### Formula

```
Map Strength = sum(result × context_weight × opponent_quality) / sum(context_weight × opponent_quality)
```

- **result**: 1 for win, 0 for loss
- **Output**: 0.0–1.0 (displayed as percentage)

### Context Weights

| Context     | Weight | Rationale                                    |
|-------------|--------|----------------------------------------------|
| Must-Win    | 3.0    | Maximum pressure, strongest signal           |
| Close-Out   | 2.0    | High stakes but team has series lead         |
| Neutral     | 1.0    | Standard baseline                            |
| Opener      | 0.5    | Pre-match pick, less reactive                |
| Coin-Toss   | 1.0    | Legacy only (pre-picked_by data)             |
| Unknown     | 0.5    | Tournament format, no pick data available    |

### Opponent Quality

```
opponent_quality = opponent_elo / league_avg_elo
```

- Uses opponent's Elo at the time of the match (from `team_elo` table)
- Centered around 1.0 — beating a 1200-rated team with league avg 1000 gives 1.2x weight
- **Not circular**: Elo is derived from series-level results across ALL matches. Map Strength uses Elo as an independent measure of opponent quality for a specific map.

### Map 5 Special Handling

Both the picker AND defender get **Must-Win (3.0)** weight on Map 5 results. At 2-2, both teams are under maximum pressure — the picker's choice and the defender's response both carry strong signal.

### Low Sample Handling

- Maps with < 3 total plays: flag as low confidence, dim in UI
- Weighted sample size displayed alongside rating: `sum(context_weight × opponent_quality)` — a 3.0-weighted Must-Win against a strong opponent counts more than a 0.5-weighted Opener against a weak one

---

## 2. Map Matrix Tab

League-wide at-a-glance view. Rows = teams, columns = maps.

### Changes

| Element        | Current                          | New                                      |
|----------------|----------------------------------|------------------------------------------|
| Heatmap color  | Raw win rate                     | Map Strength Rating                      |
| Cell text      | W-L record                       | W-L record (unchanged)                   |
| Hover tooltip  | Avoidance %, Target %, sample    | Map Strength %, weighted sample, W-L     |
| Low sample     | No treatment                     | Cells with < 3 plays get dimmed styling  |
| Mode filter    | Dropdown                         | Unchanged                                |

### Removed from Map Matrix

- Avoidance Index
- Target Index

---

## 3. Team Profile Tab

Single-team drill-down view.

### Map Cards

Each map card shows:
- **Map Strength Rating** (large, color-coded: green > 0.6, red < 0.4, yellow between)
- **Overall W-L** record
- **Pick W-L** (wins/losses when this team picked the map)
- **Defend W-L** (wins/losses when opponent picked the map)

### Expandable Results Table

Clicking a map card reveals individual results:

| Column      | Description                                    |
|-------------|------------------------------------------------|
| Opponent    | Team abbreviation                              |
| Score       | Map score (e.g., 6-3)                          |
| Pick Context| Opener / Neutral / Must-Win / Close-Out        |
| Picked By   | This team or opponent                          |
| Result      | W or L                                         |

Sorted by date descending.

### Pick Context Distribution

Inline bar per map showing proportion of picks in each context. Reveals whether a team uses this map as a comfort pick (mostly Openers) or a pressure pick (mostly Must-Win).

### Removed from Team Profile

- Avoidance Index
- Target Index

---

## 4. Match-Up Prep Tab

Filtered by upcoming opponent. Helps coaching staff plan map strategy.

### Changes

| Element          | Current                              | New                                           |
|------------------|--------------------------------------|-----------------------------------------------|
| Per-map metrics  | Avoidance %, Target %, Pick/Defend   | Map Strength (both teams), Pick/Defend        |
| Comparison       | None                                 | Side-by-side strength + matchup delta         |
| Sorting          | By map                               | By matchup delta (largest advantage first)    |

### Head-to-Head Display

For each map:
- Your team's Map Strength vs. opponent's Map Strength, color-coded
- **Matchup delta**: `your_strength - opponent_strength` — positive = advantage, negative = avoid
- Pick W-L and Defend W-L as expandable secondary detail

### Kept

- Elo ratings with low-confidence flag (< 7 matches)
- Sample sizes on all numbers

### Removed

- Avoidance Index
- Target Index

---

## 5. Code Changes Summary

### New

- `src/cdm_stats/metrics/map_strength.py` — Map Strength Rating calculation
  - `map_strength(conn, team_id, map_id) -> dict` — returns rating, weighted_sample, confidence flag
  - `all_team_map_strengths(conn) -> dict` — bulk calculation for Map Matrix

### Modified

- `src/cdm_stats/dashboard/tabs/map_matrix.py` — use Map Strength for heatmap colors and tooltips
- `src/cdm_stats/dashboard/tabs/team_profile.py` — replace avoidance/target with Map Strength cards + expandable results
- `src/cdm_stats/dashboard/tabs/matchup_prep.py` — side-by-side Map Strength comparison with delta

### Removed

- `avoidance_index()` from `src/cdm_stats/metrics/avoidance.py`
- `target_index()` from `src/cdm_stats/metrics/avoidance.py`
- `_get_pick_opportunities()` from `src/cdm_stats/metrics/avoidance.py`
- All avoidance/target references from dashboard tabs and exports

### Kept (in avoidance.py, renamed file TBD)

- `pick_win_loss()` — used in Team Profile and Match-Up Prep
- `defend_win_loss()` — used in Team Profile and Match-Up Prep
- `pick_context_distribution()` — used in Team Profile

---

## 6. Context Weights Rationale

The BO5 loser-picks-next format creates natural pressure escalation:

- **Opener (0.5)**: Pre-match 2v2 winner picks. Strategic but low-pressure. Weakest signal.
- **Neutral (1.0)**: Standard pick, series not yet decided. Baseline.
- **Close-Out (2.0)**: Picker is up 2-1. They chose this map to finish the series — meaningful confidence signal.
- **Must-Win (3.0)**: Picker is facing elimination or both teams are at match point. Maximum pressure pick. Strongest signal of genuine map strength.
- **Map 5 at 2-2**: Both teams under maximum pressure. Weighted 3.0 for BOTH picker and defender results.

This weighting scheme amplifies the signal from high-stakes decisions while dampening noise from early-series experimentation.
