# CLAUDE.md — cdm-stats

This file is the contract every Claude agent (main session and subagents) reads before acting in this repo. Subagents inherit it automatically; do not restate these rules in subagent prompts.

---

## 1. Tooling rules — stop asking permission

These are the canonical command forms for this project. The forms listed here are pre-approved in [.claude/settings.local.json](.claude/settings.local.json) (and in some cases enforced by a hook). **Do not invent alternates.**

### Tests

**Always use `uv run pytest`.** Never `pytest`, never `python -m pytest`, never `python3 -m pytest`. A PreToolUse hook ([.claude/hooks/enforce-uv-run-pytest.sh](.claude/hooks/enforce-uv-run-pytest.sh)) blocks non-conforming forms with a message — if you see that block message, rewrite the command, do not ask the user for a permission override.

Tests live flat under `tests/` (no `unit/`/`load/` split). Examples that are correct and pre-approved:

```
uv run pytest
uv run pytest tests/test_elo.py
uv run pytest tests/test_elo.py -v
uv run pytest tests/test_dashboard_app.py::test_build_layout -xvs
```

Pytest config lives in [pyproject.toml](pyproject.toml) under `[tool.pytest.ini_options]` (`pythonpath = ["src"]`). Don't duplicate it in a `pytest.ini` or top-level `conftest.py`.

### Python scripts, CLI, and REPL

Always `uv run python ...`. Never bare `python` or `python3` — the project virtualenv must be the one in use.

The main CLI entrypoint is [main.py](main.py) — see [docs/cli-reference.md](docs/cli-reference.md) for the full command list:

```
uv run python main.py init
uv run python main.py ingest data/matches.csv
uv run python main.py export matrix
uv run python main.py chart elo-all
uv run python main.py backfill
```

Dashboard (Dash + Bootstrap) runs locally on port 8050:

```
uv run python -m cdm_stats.dashboard
```

Production uses gunicorn via the [Procfile](Procfile) — don't invoke gunicorn manually during development.

### Linting and formatting

No linter is currently configured. Don't introduce `ruff`, `black`, or `flake8` configuration without asking — the user hasn't opted in.

### Git — read-only commands (pre-approved, use freely)

These never need a permission prompt. Reviewer subagents in particular: just run them.

```
git status
git diff
git log
git show
git branch
git blame
```

### Git — local mutations (pre-approved)

```
git add
git commit
git checkout
```

### Dependency management (pre-approved via `uv run:*`)

```
uv sync
uv add <pkg>
uv remove <pkg>
uv run <anything>
```

### What should not be attempted without user confirmation

These aren't blocked by settings but are high-blast-radius — surface the intent first, don't just run them:

- `git push`, `git push --force` — pushing is a user decision
- `git reset --hard`, `git rebase`, `git branch -D` — destructive history edits
- `rm -rf`, `rm -r` — bulk delete
- Anything that touches [data/cdl.db](data/cdl.db) destructively (drop tables, wipe rows) — the DB is the canonical record of ingested matches. Use `backfill` through the CLI if Elo recalc is needed; don't hand-edit SQLite.
- `cat .env` / printing secrets — keep them out of context

---

## 2. Project at a glance

- **Language / runtime:** Python 3.12, managed with `uv` (see [.python-version](.python-version)).
- **Framework:** Dash + dash-bootstrap-components for the dashboard; plain `argparse` CLI ([main.py](main.py)) for ingestion and exports.
- **Storage:** SQLite at [data/cdl.db](data/cdl.db), accessed via raw `sqlite3` + parameterized queries. No ORM.
- **Layout:**
  - [src/cdm_stats/db/](src/cdm_stats/db/) — schema, migrations, query functions
  - [src/cdm_stats/ingestion/](src/cdm_stats/ingestion/) — CSV loaders (match, scrim, tournament), backfill, seed
  - [src/cdm_stats/metrics/](src/cdm_stats/metrics/) — Elo, avoidance/target, margin, map strength
  - [src/cdm_stats/export/](src/cdm_stats/export/) — Excel exports (map matrix, matchup, profile)
  - [src/cdm_stats/charts/](src/cdm_stats/charts/) — matplotlib chart generation
  - [src/cdm_stats/dashboard/](src/cdm_stats/dashboard/) — Dash app, tabs, components
  - [tests/](tests/) — pytest, flat layout
  - [docs/superpowers/specs/](docs/superpowers/specs/) — design docs from brainstorming
  - [docs/superpowers/plans/](docs/superpowers/plans/) — implementation plans
  - [scripts/](scripts/) — currently unused; add one-shot scripts here if needed
- **Install / sync:** `uv sync` (reads [pyproject.toml](pyproject.toml) + [uv.lock](uv.lock))
- **Dashboard dev server:** `uv run python -m cdm_stats.dashboard` → http://localhost:8050

---

## 3. Domain rules (hard constraints)

Treat these as project invariants — they underpin every metric. Violating them silently corrupts downstream outputs.

- **League shape:** 14 teams, single group stage, each team plays every other team once (13 matches per team). All series are Bo5.
- **Map order is fixed by mode:** SnD → HP → Control → SnD → HP. Slot determines mode.
- **Slot 5 is a coin toss** — excluded from all pick/avoidance/target calculations. Tracked for W/L only.
- **Pick/ban mechanic:** winner of the pre-match 2v2 picks Map 1; after each map the *loser* picks the next. A team swept 3-0 had exactly one pick opportunity — do **not** assume two picks per series.
- **Pick opportunity count is the correct denominator** for avoidance/target indices, not match count. Walk `map_results` in slot order — do not shortcut with `COUNT`.
- **Margin is mode-specific.** Never compare HP point differentials to SnD round differentials.
- **Elo is series-level only** (one update per match per team), K=32, seed=1000. Unreliable in the first half of the season — flag `low_confidence` below 7 matches played.
- **Sample size travels with every metric.** Flag `n < 4` as unreliable in any coaching-facing output.

See [docs/cli-reference.md](docs/cli-reference.md) for derivation details (pick context, avoidance, target, dominance flags).

### Database invariants

- Elo history is append-only — never overwrite a `team_elo` row; insert a new one. Current Elo is the latest row per team.
- Match ingestion (one `matches` row + up to 5 `map_results` + 2 `team_elo`) must be atomic — wrap in a single transaction.
- Elo updates must be processed in chronological `match_date` order. Backfills must sort first.
- `pick_context` is stored (not derived at query time) for performance. If the derivation logic in [src/cdm_stats/db/schema.py](src/cdm_stats/db/schema.py) or the ingestion path changes, run `backfill` to regenerate.

---

## 4. Workflow contract

The standard flow for any non-trivial work in this repo is:

**brainstorm → spec → plan → execute (with subagents)**

### Where artifacts live

- Specs: `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`
- Plans: `docs/superpowers/plans/YYYY-MM-DD-<topic>-plan.md`

Use the current date in the filename. Topic should be a short kebab-case slug.

### Execution via subagents

Implementation is dispatched to subagents from the main session. Every subagent inherits this file automatically — there is **no need** to restate the rules in Section 1 inside subagent prompts. Briefing a subagent on tooling is wasted tokens.

### Reviewer-subagent rule

Every command listed in Section 1 is pre-approved. If you (as a reviewer or any subagent) catch yourself about to ask permission for one of those commands, **you are wrong** — just run it.

If a command isn't covered by Section 1 and isn't in [.claude/settings.local.json](.claude/settings.local.json), surface the request to the parent agent, **not directly to the user**. The main session owns the user relationship; subagents talk to the parent.

### What the main session owns

- The user relationship and all user-facing communication.
- Spec and plan authorship.
- Final decisions on dispatching subagents and integrating their outputs.
- Anything with high blast radius (Section 1 confirmation list, destructive DB edits) — even if the user explicitly asks, the main session pauses and confirms intent before doing it.
