"""Highlight flags and H2H tags over plain data (spec section 5). No DB access."""

MIN_N = 4
MAP_DELTA_PTS = 15
BAN_HIGHLIGHT_RATE = 0.40
HOT_COLD_KD_DELTA = 0.15
HOT_COLD_LAST_N = 5
HOT_COLD_MIN_SERIES = 5
THEY_BAN_RATE = 0.30

# Rounding guard so exact boundaries (e.g. 0.65 - 0.5) are not lost to float error.
_EPS_DIGITS = 9


def _low_sample(n: int) -> dict:
    return {"kind": "low_sample", "label": f"n={n}"}


def map_flag(win_rate: float, n: int, team_win_rate: float) -> dict | None:
    if n < MIN_N:
        return _low_sample(n)
    delta_pts = round((win_rate - team_win_rate) * 100, _EPS_DIGITS)
    if delta_pts >= MAP_DELTA_PTS:
        return {"kind": "up", "label": f"+{round(delta_pts)} vs avg"}
    if delta_pts <= -MAP_DELTA_PTS:
        return {"kind": "down", "label": f"{round(delta_pts)} vs avg"}
    return None


def _most_banned(ban_counts: dict[str, int], series_with_ban_data: int,
                 min_rate: float) -> tuple[str, float] | None:
    """Most-banned map (first in input order on ties) if it clears min_rate."""
    if series_with_ban_data < MIN_N or not ban_counts:
        return None
    top = max(ban_counts, key=ban_counts.get)  # max() keeps the first on ties
    rate = ban_counts[top] / series_with_ban_data
    if ban_counts[top] <= 0 or round(rate, _EPS_DIGITS) < min_rate:
        return None
    return top, rate


def ban_highlight(ban_counts: dict[str, int],
                  series_with_ban_data: int) -> tuple[str, dict] | None:
    hit = _most_banned(ban_counts, series_with_ban_data, BAN_HIGHLIGHT_RATE)
    if hit is None:
        return None
    name, rate = hit
    return name, {"kind": "down", "label": f"Banned {round(rate * 100)}%"}


def _kd(pairs: list[tuple[int, int]]) -> float:
    kills = sum(k for k, _ in pairs)
    deaths = sum(d for _, d in pairs)
    return kills / deaths if deaths else float(kills)


def player_form_flag(series_kds: list[tuple[int, int]]) -> dict | None:
    """series_kds: (kills, deaths) per series, ordered oldest -> newest."""
    if len(series_kds) < HOT_COLD_MIN_SERIES:
        return None
    recent = _kd(series_kds[-HOT_COLD_LAST_N:])
    delta = round(recent - _kd(series_kds), _EPS_DIGITS)
    if delta >= HOT_COLD_KD_DELTA:
        return {"kind": "up", "label": f"Hot · {recent:.2f} L5"}
    if delta <= -HOT_COLD_KD_DELTA:
        return {"kind": "down", "label": f"Cold · {recent:.2f} L5"}
    return None


def h2h_tags(rows: list[dict], opp_ban_counts: dict[str, int],
             opp_series_with_ban_data: int) -> dict[str, list[str]]:
    """Tags for one mode. rows: {map, our_rate, our_n, their_rate, their_n}.

    Returns {map_name: [tags]}; at most one of each tag. Ties go to the first
    row in input order. An empty dict means "Not enough data".
    """
    tags: dict[str, list[str]] = {}
    eligible = [r for r in rows if r["our_n"] >= MIN_N and r["their_n"] >= MIN_N]
    if eligible:
        best = max(eligible, key=lambda r: r["our_rate"] - r["their_rate"])
        if best["our_rate"] - best["their_rate"] > 0:
            tags.setdefault(best["map"], []).append("pick")
        worst = max(eligible, key=lambda r: r["their_rate"] - r["our_rate"])
        if worst["their_rate"] - worst["our_rate"] > 0:
            tags.setdefault(worst["map"], []).append("ban")
    hit = _most_banned(opp_ban_counts, opp_series_with_ban_data, THEY_BAN_RATE)
    if hit is not None:
        tags.setdefault(hit[0], []).append("they_ban")
    return tags
