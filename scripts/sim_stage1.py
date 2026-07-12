"""One-off Monte Carlo: P(GL top-4 Masters finish) in S2 Stage 1.

Stage 1 Masters is a single round robin of 8 teams. Each series is Ro3 (all 3
maps always played). Standings: points = total map wins; tiebreak = map diff.
Top 4 Masters auto-qualify for Stage 2.

Model: hold current Elo fixed, treat each remaining series as 3 i.i.d. map
coin-flips. Per-map win prob is backed out from the *series* Elo expectation
(our Elo is series-calibrated) by inverting the Ro3 win function
f(p) = 3p^2 - 2p^3 = P(win >=2 of 3).

Run: uv run python scripts/sim_stage1.py
"""

import itertools
import random
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "cdl.db"
MASTERS = ["ALU", "DVS", "ELV", "GAL", "GL", "OUG", "Q9", "Wolves"]
QUALIFY = 4
TRIALS = 50_000
SEED = 42


def ro3_win(p: float) -> float:
    """P(win a best-of-3) given per-map win prob p."""
    return 3 * p * p - 2 * p * p * p


def per_map_prob(series_expectation: float) -> float:
    """Invert ro3_win: per-map p that reproduces a given series win prob."""
    lo, hi = 0.0, 1.0
    for _ in range(60):  # binary search; 60 iters => ~1e-18 precision
        mid = (lo + hi) / 2
        if ro3_win(mid) < series_expectation:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def load_state(conn):
    """Return (elo, cur_win, cur_loss, played_pairs) for Masters teams."""
    conn.row_factory = sqlite3.Row
    ab = {r["team_id"]: r["abbreviation"] for r in conn.execute("SELECT * FROM teams")}
    tid = {v: k for k, v in ab.items() if v in MASTERS}

    elo = {}
    for t, i in tid.items():
        row = conn.execute(
            "SELECT elo_after FROM team_elo WHERE team_id=? ORDER BY elo_id DESC LIMIT 1",
            (i,),
        ).fetchone()
        elo[t] = row["elo_after"]

    cur_win, cur_loss = defaultdict(int), defaultdict(int)
    played = set()
    series = conn.execute(
        "SELECT match_id, team1_id, team2_id FROM matches "
        "WHERE season=2 AND competition='CDM' AND round='Stage 1'"
    ).fetchall()
    for s in series:
        t1, t2 = ab[s["team1_id"]], ab[s["team2_id"]]
        if t1 not in MASTERS or t2 not in MASTERS:
            continue  # cross-group games don't count toward Masters seeding
        played.add(frozenset((t1, t2)))
        maps = conn.execute(
            "SELECT winner_team_id FROM map_results WHERE match_id=? AND dq=0", (s["match_id"],)
        ).fetchall()
        a = sum(1 for m in maps if m["winner_team_id"] == s["team1_id"])
        b = len(maps) - a
        cur_win[t1] += a; cur_loss[t1] += b
        cur_win[t2] += b; cur_loss[t2] += a
    return elo, cur_win, cur_loss, played


def simulate(cur_win, cur_loss, pmap, force=None):
    """Run TRIALS. `force` = (frozenset(pair), winner_abbr) to condition that
    series on a given winner (its map margin is still simulated, restricted)."""
    fpair, fwinner = force if force else (None, None)
    qualify, gl_seed = Counter(), Counter()
    pts_sum, diff_sum = defaultdict(int), defaultdict(int)
    for _ in range(TRIALS):
        pts = {t: cur_win[t] for t in MASTERS}
        diff = {t: cur_win[t] - cur_loss[t] for t in MASTERS}
        for (a, b), pa in pmap.items():
            aw = sum(1 for _ in range(3) if random.random() < pa)
            if fpair == frozenset((a, b)):  # resample until forced team wins
                want_a = fwinner == a
                while (aw >= 2) != want_a:
                    aw = sum(1 for _ in range(3) if random.random() < pa)
            bw = 3 - aw
            pts[a] += aw; pts[b] += bw
            diff[a] += aw - bw; diff[b] += bw - aw
        ranked = sorted(MASTERS, key=lambda t: (pts[t], diff[t], random.random()), reverse=True)
        for t in ranked[:QUALIFY]:
            qualify[t] += 1
        gl_seed[ranked.index("GL") + 1] += 1
        for t in MASTERS:
            pts_sum[t] += pts[t]; diff_sum[t] += diff[t]
    return qualify, gl_seed, pts_sum, diff_sum


def gl_top4_given(cur_win, cur_loss, pmap, fixed):
    """P(GL top-4) conditioning on exact scorelines. `fixed` maps
    frozenset(pair) -> map wins for the alphabetically-first team; those series
    are applied deterministically and excluded from the random draws. Reseeds so
    every grid cell simulates the other series on identical random numbers."""
    random.seed(SEED)
    cw, cl = dict(cur_win), dict(cur_loss)
    sub = {}
    for (a, b), pa in pmap.items():
        if frozenset((a, b)) in fixed:
            aw = fixed[frozenset((a, b))]; bw = 3 - aw
            cw[a] += aw; cl[a] += bw; cw[b] += bw; cl[b] += aw
        else:
            sub[(a, b)] = pa
    _, gl_seed, _, _ = simulate(cw, cl, sub)
    return sum(gl_seed[s] for s in range(1, QUALIFY + 1)) / TRIALS * 100


def main():
    random.seed(SEED)
    conn = sqlite3.connect(DB)
    elo, cur_win, cur_loss, played = load_state(conn)

    remaining = [
        tuple(sorted(p))
        for p in itertools.combinations(MASTERS, 2)
        if frozenset(p) not in played
    ]
    pmap = {}
    for a, b in remaining:
        exp_a = 1 / (1 + 10 ** ((elo[b] - elo[a]) / 400))
        pmap[(a, b)] = per_map_prob(exp_a)

    print(f"Masters Stage 1 top-{QUALIFY} simulation  |  {TRIALS:,} trials  |  "
          f"{len(remaining)} series remaining\n")
    print("Remaining series & per-map win prob (favorite):")
    for (a, b), pa in sorted(pmap.items(), key=lambda kv: -abs(kv[1] - 0.5)):
        fav, p = (a, pa) if pa >= 0.5 else (b, 1 - pa)
        print(f"  {a:6} vs {b:6}   {fav} {p*100:4.1f}%/map  (series {ro3_win(p)*100:4.1f}%)")

    scenarios = [
        ("BASELINE (ALU series simulated)", None),
        ("IF GL BEATS ALU", (frozenset(("ALU", "GL")), "GL")),
        ("IF GL LOSES TO ALU", (frozenset(("ALU", "GL")), "ALU")),
    ]
    gl_seeds_by_scn = {}
    for title, force in scenarios:
        qualify, gl_seed, pts_sum, diff_sum = simulate(cur_win, cur_loss, pmap, force)
        gl_seeds_by_scn[title] = gl_seed
        print(f"\n=== {title} ===")
        print(f"{'team':7}{'P(top4)':>9}{'exp pts':>9}{'exp diff':>9}{'now':>10}")
        for t in sorted(MASTERS, key=lambda x: -qualify[x]):
            now = f"{cur_win[t]}pt {cur_win[t]-cur_loss[t]:+d}"
            star = " <= GL" if t == "GL" else ""
            print(f"{t:7}{qualify[t]/TRIALS*100:>8.1f}%{pts_sum[t]/TRIALS:>9.1f}"
                  f"{diff_sum[t]/TRIALS:>+9.1f}{now:>10}{star}")

    print("\nGL P(top-4) summary:")
    for title in gl_seeds_by_scn:
        top4 = sum(gl_seeds_by_scn[title][s] for s in range(1, QUALIFY + 1)) / TRIALS * 100
        print(f"  {title:34} {top4:5.1f}%")

    # --- GL P(top-4) by ALU scoreline (ALU is GL's only remaining game) ---
    # GL-ALU pair sorts to (ALU, GL): ALU map wins => GL 3-0=0, 2-1=1, L1-2=2, L0-3=3
    alu_pair = frozenset(("ALU", "GL"))
    if any(frozenset(p) == alu_pair for p in pmap):
        print("\n=== GL P(top-4) by ALU scoreline (GL's last game) ===")
        for label, alu_w in [("GL 3-0", 0), ("GL 2-1", 1), ("GL 1-2 (L)", 2), ("GL 0-3 (L)", 3)]:
            p = gl_top4_given(cur_win, cur_loss, pmap, {alu_pair: alu_w})
            print(f"  {label:12} {p:5.1f}%")


if __name__ == "__main__":
    # self-check: inversion reproduces the series expectation, and per-map probs are complementary
    for e in (0.5, 0.6, 0.75, 0.9):
        assert abs(ro3_win(per_map_prob(e)) - e) < 1e-9
    assert abs(per_map_prob(0.6) + per_map_prob(0.4) - 1.0) < 1e-9
    main()
