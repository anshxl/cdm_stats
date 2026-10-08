import json
import re
import sqlite3
from datetime import datetime
from typing import IO

from cdm_stats.ingestion.scrim_loader import SEASON_WEEK1_MONDAY

# The VOD-review tool's map names that differ from ours.
MAP_ALIASES = {"Crossroads": "Crossroads Strike"}

# "Elevate 5 October" -> ("Elevate", "5", "October"), used when `date` is empty.
NAME_RE = re.compile(r"^(.+?)\s+(\d{1,2})\s+([A-Za-z]+)$")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _secs(t: str) -> float:
    """'1:02:40' / '58:15' / '10:05.6' -> seconds."""
    total = 0.0
    for part in t.split(":"):
        total = total * 60 + float(part)
    return total


def _resolve_opponent(conn: sqlite3.Connection, text: str) -> tuple[int | None, str]:
    """Match an abbreviation or a team name, ignoring case and punctuation.
    A name may be a prefix of ours ('AlUla' -> 'Al-Ula Club')."""
    key = _norm(text)
    teams = conn.execute("SELECT team_id, abbreviation, team_name FROM teams").fetchall()
    for tid, abbr, _ in teams:
        if _norm(abbr) == key:
            return tid, abbr
    hits = [(tid, abbr) for tid, abbr, name in teams if key and _norm(name).startswith(key)]
    return hits[0] if len(hits) == 1 else (None, text)


def _scrim_date(scrim: dict, season: int) -> str:
    if scrim.get("date"):
        return scrim["date"]
    m = NAME_RE.match(scrim["name"].strip())
    if not m:
        raise ValueError(f"No date and no '<Opponent> <day> <Month>' name: {scrim['name']!r}")
    year = SEASON_WEEK1_MONDAY[season].year
    return datetime.strptime(f"{m[2]} {m[3]} {year}", "%d %B %Y").date().isoformat()


def ingest_scrim_ops(conn: sqlite3.Connection, file: IO, season: int = 1) -> list[dict]:
    """Ingest the VOD-review tool's operator JSON (`godlike-operator-data`).

    Each map is matched to `scrim_maps` on date + opponent + map, so the team
    CSV must be ingested first. Kills and pulls come from `pullDetails` (the raw
    records); a warning flags rows whose totals disagree. Re-running updates
    changed rows and skips identical ones, so a corrected export flows through.
    """
    data = json.load(file)
    roster = {p["operatorKey"]: p["name"] for p in data["team"]["players"]}
    # Our spelling of each player, from the K/D/A rows (e.g. SkullGuy -> Skullguy).
    known = {_norm(r[0]): r[0] for r in
             conn.execute("SELECT DISTINCT player_name FROM scrim_player_stats")}
    results: list[dict] = []

    for scrim in data["scrims"]:
        try:
            date = _scrim_date(scrim, season)
        except ValueError as e:
            results.append({"status": "error", "row": scrim["name"], "errors": str(e)})
            continue
        m = NAME_RE.match(scrim["name"].strip())
        opp_id, opp = _resolve_opponent(conn, m[1] if m else scrim["name"])
        if opp_id is None:
            results.append({"status": "error", "row": scrim["name"], "errors": f"Unknown opponent: {opp}"})
            continue

        for match in scrim["matches"]:
            map_name = MAP_ALIASES.get(match["map"], match["map"])
            desc = f"{date} vs {opp} {map_name}"
            ids = conn.execute(
                "SELECT scrim_map_id FROM scrim_maps WHERE scrim_date = ? AND opponent_id = ? AND map_name = ?",
                (date, opp_id, map_name),
            ).fetchall()
            if len(ids) != 1:
                why = "No matching scrim map (ingest the team CSV first)" if not ids else "Map played twice that day"
                results.append({"status": "error", "row": desc, "errors": why})
                continue
            scrim_map_id = ids[0][0]
            footage_min = round((_secs(match["vodEnd"]) - _secs(match["vodStart"])) / 60, 2)

            for key, p in match["players"].items():
                name = roster.get(key, key)
                name = known.get(_norm(name), name)
                pulls = p["pullDetails"]
                op_kills = sum(x["kills"] for x in pulls)
                op_pulls = len(pulls)
                op_time = round(sum(x["duration"] for x in pulls), 1)
                row = f"{desc} {name}"
                warning = None
                if (p["kills"], p["pulls"]) != (op_kills, op_pulls):
                    warning = (f"totals say {p['kills']} kills / {p['pulls']} pulls, "
                               f"pull records say {op_kills} / {op_pulls}; used the records")

                old = conn.execute(
                    "SELECT op_kills, op_pulls, op_time_sec, footage_min FROM scrim_ops_stats "
                    "WHERE scrim_map_id = ? AND player_name = ?", (scrim_map_id, name),
                ).fetchone()
                new = (op_kills, op_pulls, op_time, footage_min)
                if old == new:
                    results.append({"status": "skipped", "row": row})
                    continue
                conn.execute(
                    """INSERT INTO scrim_ops_stats
                       (scrim_map_id, player_name, op_kills, op_pulls, op_time_sec, footage_min)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(scrim_map_id, player_name) DO UPDATE SET
                         op_kills = excluded.op_kills, op_pulls = excluded.op_pulls,
                         op_time_sec = excluded.op_time_sec, footage_min = excluded.footage_min""",
                    (scrim_map_id, name, *new),
                )
                if old:
                    warning = "; ".join(filter(None, [f"updated from {old}", warning]))
                results.append({"status": "ok", "row": row, "warning": warning})

    conn.commit()
    return results
