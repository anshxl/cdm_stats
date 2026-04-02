# Plotly Dash Dashboard — Design Spec

## Problem

The current Excel exports (`export/excel.py`) are the primary way coaching staff and players see CDM stats. They suffer from three issues:

1. **Information overload per cell** — metrics like `P:2-1 | D:1-0 | Av:60%(n=5) | Tg:40%(n=5)` are unreadable at a glance.
2. **No visual design** — raw openpyxl formatting with basic green/red/yellow fills. Doesn't feel professional.
3. **Friction to share** — Excel files must be exported, opened, then screenshotted or attached in Discord. No quick way to explore and grab what you need.

## Solution

A local Plotly Dash web dashboard served at `http://localhost:8050`. Analysts explore interactively, then screenshot specific views to share on Discord. The dashboard is additive — existing Excel and matplotlib exports remain untouched.

## Audience

- **Primary:** Coaching staff and analysts — need density, filtering, drill-down capability.
- **Secondary:** Players — need clean, scannable views. A glance should tell the story without parsing numbers.

## Stack

- `dash` — app framework
- `dash-bootstrap-components` — dark theme (`dbc.themes.DARKLY` or similar), layout primitives
- `plotly` — interactive charts (Elo trajectory, heatmap)
- Existing `cdm_stats.metrics.*` and `cdm_stats.db.queries` — called directly from callbacks. No data layer duplication.

## Dashboard Structure

Four tabs, accessible via a top navigation bar:

### 1. Team Profile

Single-team deep dive. The "one-pager" for any team.

**Filters:** Team dropdown.

**Layout:** Card-based grid with four sections:

- **Map Record** — list of all maps for the selected team, each row showing map name, W-L record, and win %. Rows colored green (60%+), yellow (40-60%), red (<40%). Includes a totals row.
- **Pick vs Defend W-L** — click any map row in the Map Record card to expand it inline, revealing two stat blocks: wins-losses on pick and wins-losses on defend. Same expand/collapse pattern as Match-Up Prep. Sample size displayed.
- **Avoidance & Target Index** — horizontal paired bars per map. Red bar = avoidance %, cyan bar = target %. Sample size shown per map. Maps with n < 4 visually muted.
- **Ban Tendencies** — horizontal bar chart showing the team's ban rates and opponent ban rates against them. Displayed as percentage bars with raw counts.

**Footer:** Elo rating with low-confidence flag if < 7 matches played.

### 2. Map Matrix

League-wide overview. All 14 teams × all maps.

**Filters:** Mode dropdown (All / SnD / HP / Control).

**Layout:** Heatmap-style grid.

- Rows = teams (sorted alphabetically by abbreviation).
- Columns = maps (grouped by mode).
- Each cell background colored on a green-to-red gradient based on overall win %.
- Cell text shows **W-L record only** — no other stats in the cell.
- **Hover tooltip** shows full detail: pick W-L, defend W-L, avoidance %, target %, sample sizes.
- **Click** a cell to navigate to that team's profile.
- Cells with total sample size < 4 get a dashed border or muted opacity to flag low confidence.

### 3. Match-Up Prep

Head-to-head pre-match view. The most important tab for game-day prep.

**Filters:** Your Team dropdown + Opponent dropdown.

**Layout:** Maps grouped by mode with colored mode badges (SnD = cyan, HP = purple, Control = amber). Each map is a row with a mirror layout:

- **Left side (blue tint):** Your team's stats.
- **Center-left:** Map name + head-to-head W-L record between these two teams.
- **Center:** "VS" divider.
- **Right side (red tint):** Opponent's stats.

**Default (collapsed) view per map row shows three stats per team:**
- Overall W-L (color-coded: green for winning, yellow for .500, red for losing)
- Avoidance % with sample size
- Target % with sample size

**Expanded view (click W-L to toggle):**
- Sub-row slides in below showing Pick W-L and Defend W-L for both teams.
- Subtle visual differentiation (lighter background, border highlight) so it reads as detail, not a new map.

**Ban Comparison section** at bottom:
- Side-by-side cards showing head-to-head ban tendencies between the two specific teams.
- Horizontal bars with raw counts (e.g., "Vault (SnD) — 2/3").

**Elo comparison** shown in the filter bar area: both team ratings with low-confidence flags.

### 4. Elo Tracker

Season-long Elo progression.

**Layout:** Plotly interactive line chart.

- All 14 teams plotted, x-axis = week number, y-axis = Elo rating.
- Click a team in the legend to isolate/highlight (native Plotly behavior).
- Hover shows: exact Elo, opponent abbreviation, W/L result for that data point.
- Horizontal dashed line at seed Elo (1000).
- Low-confidence zone (first ~6 matches) indicated with a shaded region or annotation.
- Dropdown to toggle between all-teams and single-team view.

## Color System

| Meaning | Color | Usage |
|---------|-------|-------|
| Strength / winning | `#4ade80` (green) | W-L records above 60% |
| Contested / neutral | `#fbbf24` (yellow) | W-L records at 40-60% |
| Weakness / losing | `#f87171` (red) | W-L records below 40% |
| Your team accent | `#4cc9f0` (cyan) | Left side of matchup prep, mode badges |
| Opponent accent | `#f87171` (red) | Right side of matchup prep |
| Ban data | `#e879f9` (purple) | Ban tendency bars |
| Card background | `#16213e` | All card surfaces |
| Page background | `#1a1a2e` | Dashboard background |
| Border / divider | `#2a2a4a` | Card borders, section dividers |
| Low confidence | Muted opacity or dashed border | Cells/stats with n < 4 |

## Project Structure

```
src/cdm_stats/dashboard/
├── __init__.py
├── app.py              # Dash app init, theme, tab routing, launch
├── tabs/
│   ├── __init__.py
│   ├── team_profile.py  # Layout + callbacks
│   ├── map_matrix.py    # Layout + callbacks
│   ├── matchup_prep.py  # Layout + callbacks
│   └── elo_tracker.py   # Layout + callbacks
```

**Launch:** `python -m cdm_stats.dashboard` — starts the Dash server at `http://localhost:8050`.

`app.py` handles:
- Dash app initialization with dark Bootstrap theme
- Top-level tab navigation layout
- Tab content rendering via callbacks that delegate to each tab module

Each tab module exports:
- A `layout()` function returning the Dash component tree
- Callback registrations via a `register_callbacks(app)` function

## Data Flow

All tabs query SQLite directly via existing functions in `cdm_stats.metrics.*` and `cdm_stats.db.queries`. No caching layer — at this data scale (14 teams, ~180 map results max) queries are instant.

Dash callbacks fire on dropdown/click interactions and re-query + re-render the affected components.

The DB connection is opened per-callback using the existing `data/cdl.db` path.

## What Stays Unchanged

- `export/excel.py` — remains available for anyone who still wants Excel output.
- `charts/heatmap.py` — matplotlib charts remain for any non-dashboard use.
- All `metrics/` and `db/` modules — consumed as-is, no modifications needed.

## Dependencies to Add

- `dash`
- `dash-bootstrap-components`
- `plotly` (installed as a dash dependency, but listed explicitly)

Added to `pyproject.toml` under `[project.dependencies]`.

## Sample Size Discipline

Every stat that depends on a count must display its sample size. The dashboard enforces this by:
- Showing `n=X` next to avoidance and target percentages everywhere they appear.
- Muting or dashing cells/stats where n < 4 (the `LOW_SAMPLE_THRESHOLD` from the existing codebase).
- Never presenting a percentage without its denominator being accessible (inline or via tooltip).
