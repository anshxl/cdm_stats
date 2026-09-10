"""One-off Monte Carlo: GL's most likely first Bracket Decider opponent (S2 Stage 2).

Stage 2: Masters and Challenger groups of 6, single round robin, all Bo5.
Standings: series wins, tiebreak map diff. Masters top-2 -> playoffs;
Masters 3-4 enter Bracket Decider R3, Masters 5-6 enter R2, Challenger 1-4 enter R1.
R1: Chal 1st picks opponent from {3rd,4th}, other pair plays; winners to R2.
R2: Masters 5th picks a R1 winner, 6th gets the other; winners to R3.
R3: Masters 3rd picks a R2 winner, 4th gets the other.

Model: hold current Elo fixed; per-map win prob backed out from series Elo
expectation by inverting the Bo5 win function. Pickers choose the weakest
available opponent by Stage 2 record (series wins, then map diff).

Run: uv run python scripts/sim_stage2.py
"""

import itertools
import random
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "cdl.db"
MASTERS = ["ELV", "GAL", "GL", "OUG", "Q9", "Wolves"]
CHALLENGER = ["ALU", "DVS", "ETs", "RAG", "RVL", "XROCK"]
TRIALS = 100_000
SEED = 42


def bo5_win(p: float) -> float:
    """P(win a best-of-5) given per-map win prob p."""
    q = 1 - p
    return 10 * p**3 * q**2 + 5 * p**4 * q + p**5


def per_map_prob(series_expectation: float) -> float:
    lo, hi = 0.0, 1.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if bo5_win(mid) < series_expectation:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def load_state(conn):
    conn.row_factory = sqlite3.Row
    ab = {r["team_id"]: r["abbreviation"] for r in conn.execute("SELECT * FROM teams")}
    teams = MASTERS + CHALLENGER
    tid = {v: k for k, v in ab.items() if v in teams}

    elo = {}
    for t, i in tid.items():
        row = conn.execute(
            "SELECT elo_after FROM team_elo WHERE team_id=? ORDER BY elo_id DESC LIMIT 1",
            (i,),
        ).fetchone()
        elo[t] = row["elo_after"]

    sw = {t: 0 for t in teams}
    mdiff = {t: 0 for t in teams}
    played = set()
    for s in conn.execute(
        "SELECT match_id, team1_id, team2_id, series_winner_id FROM matches "
        "WHERE season=2 AND competition='CDM' AND round='Stage 2'"
    ):
        t1, t2 = ab[s["team1_id"]], ab[s["team2_id"]]
        played.add(frozenset((t1, t2)))
        maps = conn.execute(
            "SELECT winner_team_id FROM map_results WHERE match_id=? AND dq=0", (s["match_id"],)
        ).fetchall()
        a = sum(1 for m in maps if m["winner_team_id"] == s["team1_id"])
        b = len(maps) - a
        mdiff[t1] += a - b
        mdiff[t2] += b - a
        sw[ab[s["series_winner_id"]]] += 1
    return elo, sw, mdiff, played


def sim_bo5(pmap_a: float):
    """Play a Bo5; return (a_maps, b_maps)."""
    a = b = 0
    while a < 3 and b < 3:
        if random.random() < pmap_a:
            a += 1
        else:
            b += 1
    return a, b


def main():
    random.seed(SEED)
    conn = sqlite3.connect(DB)
    elo, sw, mdiff, played = load_state(conn)

    def pmap(a, b):
        exp_a = 1 / (1 + 10 ** ((elo[b] - elo[a]) / 400))
        return per_map_prob(exp_a)

    remaining = [
        p
        for grp in (MASTERS, CHALLENGER)
        for p in itertools.combinations(grp, 2)
        if frozenset(p) not in played
    ]
    print(f"Stage 2 bracket-decider simulation | {TRIALS:,} trials")
    print("Remaining group series:")
    for a, b in remaining:
        pa = pmap(a, b)
        fav, p = (a, pa) if pa >= 0.5 else (b, 1 - pa)
        print(f"  {a:6} vs {b:6}   {fav} {p*100:4.1f}%/map (series {bo5_win(p)*100:4.1f}%)")

    gl_finish = Counter()
    first_opp = Counter()  # (round, opponent) -> count
    for _ in range(TRIALS):
        w, d = dict(sw), dict(mdiff)
        for a, b in remaining:
            am, bm = sim_bo5(pmap(a, b))
            d[a] += am - bm
            d[b] += bm - am
            w[a if am > bm else b] += 1
        rank = lambda grp: sorted(grp, key=lambda t: (w[t], d[t], random.random()), reverse=True)
        m, c = rank(MASTERS), rank(CHALLENGER)
        record = lambda t: (w[t], d[t])
        weakest = lambda opts: min(opts, key=record)

        gl_finish[m.index("GL") + 1] += 1

        # R1: Chal 1st picks from {3rd, 4th}
        pick = weakest([c[2], c[3]])
        other = c[3] if pick is c[2] else c[2]
        r1 = [(c[0], pick), (c[1], other)]
        r1w = [a if random.random() < bo5_win(pmap(a, b)) else b for a, b in r1]

        # R2: Masters 5th picks a R1 winner
        pick = weakest(r1w)
        other = r1w[1] if pick is r1w[0] else r1w[0]
        if m[4] == "GL":
            first_opp[("R2", pick)] += 1
            continue
        r2 = [(m[4], pick), (m[5], other)]
        r2w = [a if random.random() < bo5_win(pmap(a, b)) else b for a, b in r2]

        # R3: Masters 3rd picks a R2 winner
        pick = weakest(r2w)
        other = r2w[1] if pick is r2w[0] else r2w[0]
        if m[2] == "GL":
            first_opp[("R3", pick)] += 1
        elif m[3] == "GL":
            first_opp[("R3", other)] += 1

    print("\nGL Masters finish:")
    for pos in sorted(gl_finish):
        print(f"  {pos}th: {gl_finish[pos]/TRIALS*100:5.1f}%")

    print("\nGL first bracket-decider opponent:")
    for (rnd, opp), n in first_opp.most_common():
        print(f"  {rnd}  vs {opp:7} {n/TRIALS*100:5.1f}%")

    by_opp = Counter()
    for (rnd, opp), n in first_opp.items():
        by_opp[opp] += n
    print("\nBy opponent (any round):")
    for opp, n in by_opp.most_common():
        print(f"  {opp:7} {n/TRIALS*100:5.1f}%")


if __name__ == "__main__":
    for e in (0.5, 0.6, 0.75, 0.9):
        assert abs(bo5_win(per_map_prob(e)) - e) < 1e-9
    assert abs(per_map_prob(0.6) + per_map_prob(0.4) - 1.0) < 1e-9
    main()
