# CDM Stats

Map-level analytics for Call of Duty League matches.

Ingests match and map result data via CLI, stores it in SQLite, and computes:

- **Elo ratings** — series-level, weighted by per-map score margins (normalized by mode) so blowouts move ratings more than nail-biters
- **Pick/Defend W/L** — win rates on maps a team picked vs. maps forced on them by opponents
- **Avoidance & Target indices** — how often a team dodges a map when picking, and how often opponents dodge it against them
- **Dominance flags** — per-mode margin thresholds categorizing map wins as dominant, contested, or standard
- **Pick context distribution** — breakdown of map picks by situational pressure (opener, neutral, must-win, close-out)

Outputs to Google Sheets for map matrix and match-up prep views.
