"""/api/h2h/brief: a short pre-match brief in which Claude narrates the H2H tags.

Claude sees only the facts built here (tagged maps and the numbers behind them), never
the raw tables, and does no math. Needs ANTHROPIC_API_KEY.

Briefs are cached per (team, opp, event) until the next deploy (new data always comes
with a deploy). Only `event` is accepted, not start/end, so the cache cannot be
bypassed; cache misses are capped at DAILY_LIMIT per UTC day. Failures are never cached.
"""
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder

from cdm_stats.api.deps import Event, get_conn, match_filter
from cdm_stats.api.h2h import head_to_head
from cdm_stats.api.schemas import Brief
from cdm_stats.metrics.insights import MIN_N

router = APIRouter()

# uvicorn only shows INFO for its own loggers; this puts the usage lines in the Railway logs.
log = logging.getLogger("uvicorn.error")

MODEL = "claude-sonnet-5-5"
RECENT_SERIES = 5
DAILY_LIMIT = 100
UNAVAILABLE = "Brief unavailable. Try again later."
_cache: dict[tuple[str, str, str], str] = {}
_calls: dict[str, int] = {}  # UTC day -> model calls

SYSTEM = """You write a short pre-match brief for a Call of Duty League coaching staff. The facts
come from their stats dashboard. Use only these facts. Do not calculate, estimate, or
add any number that is not in the facts. Write 3 to 5 plain sentences, with no headings
and no lists.
- Start with the clearest map edge. Then give the ban risk. Then give the context (Elo,
  the head-to-head record, and the opponent's recent form).
- Give the sample size with each rate, for example "71% over 7 maps".
- Use careful words: "suggests", "leans", "has tended to". Never say "will",
  "should win", or "guaranteed".
- If Elo low_confidence is true, say that the Elo gap is not yet reliable.
- For a mode with not_enough_data, say that there is not enough data. For a mode with
  no_clear_edge, say that there is no clear edge. Do not guess.
- Never compare margins across modes."""


class BriefUnavailable(Exception):
    """No brief this time. The message is for the server log only, never the client."""


def _pct(value: float) -> str:
    # Half up, like the page's toFixed(0); round() would round halves to even.
    return f"{int(value * 100 + 0.5)}%"


def _map_facts(r: dict) -> dict:
    out = {"map": r["map_name"], "tags": r["tags"]}
    if {"pick", "ban"} & set(r["tags"]):
        out["our_win_rate"] = f"{_pct(r['your_rate']['value'])} (n={r['your_rate']['n']})"
        out["their_win_rate"] = f"{_pct(r['opp_rate']['value'])} (n={r['opp_rate']['n']})"
    if "they_ban" in r["tags"]:
        b = r["opp_bans"]
        out["their_ban_rate"] = f"{_pct(b['count'] / b['total'])} ({b['count']} of {b['total']} series)"
    return out


def _status(maps: list[dict]) -> str:
    if any(r["tags"] for r in maps):
        return "tagged"
    if any(r["your_rate"]["n"] >= MIN_N and r["opp_rate"]["n"] >= MIN_N for r in maps):
        return "no_clear_edge"
    return "not_enough_data"


def brief_facts(h2h: dict, event: str) -> dict:
    """The facts Claude may use, from a head_to_head() response. Numbers are preformatted
    text with their sample size, so the model copies them instead of computing."""
    elo = h2h["elo"]
    return {
        "team": f"{h2h['team']['team_name']} ({h2h['team']['abbreviation']})",
        "opp": f"{h2h['opp']['team_name']} ({h2h['opp']['abbreviation']})",
        "event": event,
        "elo": {"team": round(elo["team"]["elo"]), "opp": round(elo["opp"]["elo"]),
                "low_confidence": elo["team"]["low_confidence"] or elo["opp"]["low_confidence"]},
        "h2h_series": f"{h2h['h2h_series']['wins']}-{h2h['h2h_series']['losses']}",
        "opp_recent": [f"{s['result']} {s['score']} vs {s['opponent']}"
                       for s in h2h["opp_recent_series"][:RECENT_SERIES]],
        "modes": [{"mode": m["mode"], "status": _status(m["maps"]),
                   "maps": [_map_facts(r) for r in m["maps"] if r["tags"]]}
                  for m in h2h["modes"]],
    }


def brief_prompt(facts: dict) -> tuple[str, str]:
    # Sorted keys: the same view always sends the same prompt.
    return SYSTEM, json.dumps(facts, sort_keys=True)


def generate_brief(system: str, user: str) -> str:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise BriefUnavailable("ANTHROPIC_API_KEY unset")
    client = anthropic.Anthropic(timeout=30.0)
    try:
        # No temperature or prefill: Sonnet 5.5 rejects both. max_tokens covers thinking too.
        resp = client.messages.create(
            model=MODEL, max_tokens=4000, output_config={"effort": "low"},
            system=system, messages=[{"role": "user", "content": user}],
        )
    except anthropic.APIError as e:
        # Class and status only: never log or return the SDK's message text.
        log.warning("brief call failed: %s status=%s", type(e).__name__, getattr(e, "status_code", None))
        raise BriefUnavailable(type(e).__name__) from e
    log.info("brief usage: input=%s output=%s", resp.usage.input_tokens, resp.usage.output_tokens)
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    if resp.stop_reason in ("refusal", "max_tokens") or not text:
        raise BriefUnavailable(f"stop_reason={resp.stop_reason}")
    return text


@router.post("/h2h/brief", response_model=Brief)
def h2h_brief(team: str, opp: str, event: Event = "all",
              conn: sqlite3.Connection = Depends(get_conn)):
    key = (team, opp, event)
    if key in _cache:
        return {"text": _cache[key]}
    # Same 404 / 422 as /api/h2h. jsonable_encoder turns its TeamInfo models into dicts.
    h2h = jsonable_encoder(head_to_head(team, opp, match_filter(event), conn))
    # ponytail: no lock, so two simultaneous clicks on a new view can make two calls.
    day = datetime.now(timezone.utc).date().isoformat()
    if _calls.get(day, 0) >= DAILY_LIMIT:
        log.warning("brief daily limit reached (%s)", DAILY_LIMIT)
        raise HTTPException(503, UNAVAILABLE)
    _calls[day] = _calls.get(day, 0) + 1  # counted before the call, so failures count too
    try:
        text = generate_brief(*brief_prompt(brief_facts(h2h, event)))
    except BriefUnavailable as e:
        log.warning("brief unavailable: %s", e)
        raise HTTPException(503, UNAVAILABLE) from e
    _cache[key] = text
    return {"text": text}
