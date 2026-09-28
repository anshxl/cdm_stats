"""Which teams and series a filter covers — options for selects and the player Series."""
import sqlite3

from cdm_stats.metrics.filters import MatchFilter

# Teams present in only one season (Felines: S1 only; RAG, i7: S2 only). They
# break cross-season continuity, so they are hidden from pickers. Their matches
# still count in every metric.
HIDDEN_TEAMS = {"Felines", "RAG", "i7"}

_HIDDEN_SQL = f"t.abbreviation NOT IN ({','.join('?' * len(HIDDEN_TEAMS))})"


def _team_rows(conn: sqlite3.Connection, match_where: str, params: list) -> list[dict]:
    """Visible teams on either side of any match satisfying `match_where`."""
    rows = conn.execute(
        f"""SELECT t.team_id, t.abbreviation, t.team_name FROM teams t
            WHERE {_HIDDEN_SQL}
              AND EXISTS (SELECT 1 FROM matches m
                          WHERE t.team_id IN (m.team1_id, m.team2_id) {match_where})
            ORDER BY t.abbreviation""",
        sorted(HIDDEN_TEAMS) + params,
    ).fetchall()
    return [{"team_id": r[0], "abbreviation": r[1], "team_name": r[2]} for r in rows]


def teams_in_scope(conn: sqlite3.Connection, f: MatchFilter) -> list[dict]:
    """Teams with at least one match in `f`, hidden teams removed."""
    fw, fp = f.sql("m")
    return _team_rows(conn, fw, fp)


def opponents_in_scope(conn: sqlite3.Connection, team_id: int, f: MatchFilter) -> list[dict]:
    """Teams `team_id` played in `f`, hidden teams removed."""
    fw, fp = f.sql("m")
    return [
        r for r in _team_rows(conn, " AND ? IN (m.team1_id, m.team2_id)" + fw, [team_id] + fp)
        if r["team_id"] != team_id
    ]


def series_match_ids(conn: sqlite3.Connection, team_id: int, f: MatchFilter, series: str) -> list[int]:
    """Match ids of `team_id`'s matches in `f` for a Series choice: `last10`
    (newest ten), `all`, or an opponent abbreviation (unknown → [])."""
    fw, fp = f.sql("m")
    where = "? IN (m.team1_id, m.team2_id)" + fw
    params: list = [team_id] + fp
    if series not in ("last10", "all"):
        where += """ AND (SELECT team_id FROM teams WHERE abbreviation = ?)
                     IN (m.team1_id, m.team2_id)"""
        params.append(series)
    limit = " LIMIT 10" if series == "last10" else ""
    rows = conn.execute(
        f"""SELECT m.match_id FROM matches m WHERE {where}
            ORDER BY m.match_date DESC, m.match_id DESC{limit}""",
        params,
    ).fetchall()
    return [r[0] for r in rows]
