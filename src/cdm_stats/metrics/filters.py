"""Competition-family + date filter shared by every query and dashboard tab.

Requires `matches m` to be in scope of the query the fragment is spliced into.
"""
from dataclasses import dataclass

FAMILIES = ("CDM Spring", "CDM Summer", "Regionals")

# Season 1 rows predate the competition column (NULL). Everything that is not
# league play (splits, majors, regionals) is grouped as "Regionals".
def family_sql(alias: str = "m") -> str:
    return f"""CASE
    WHEN {alias}.season = 1 AND {alias}.competition IS NULL THEN 'CDM Spring'
    WHEN {alias}.season = 2 AND {alias}.competition = 'CDM' THEN 'CDM Summer'
    ELSE 'Regionals' END"""


FAMILY_SQL = family_sql("m")


@dataclass(frozen=True)
class MatchFilter:
    families: frozenset[str] = frozenset(FAMILIES)
    start: str | None = None  # ISO date, inclusive
    end: str | None = None    # ISO date, inclusive

    def sql(self, alias: str = "m") -> tuple[str, list]:
        """Return (" AND ...", params) to append after an existing WHERE that
        has `matches <alias>` in scope."""
        clauses, params = [], []
        if self.families != frozenset(FAMILIES):
            fams = sorted(self.families)
            clauses.append(f"{family_sql(alias)} IN ({','.join('?' * len(fams))})")
            params += fams
        dw, dp = self.date_sql(f"{alias}.match_date")
        return ("".join(f" AND {c}" for c in clauses) + dw, params + dp)

    @classmethod
    def from_dict(cls, data: dict | None) -> "MatchFilter":
        """Build from the dashboard filter-store payload (None/{} = default)."""
        data = data or {}
        fams = data.get("families")
        return cls(
            families=frozenset(fams) if fams is not None else frozenset(FAMILIES),
            start=(data.get("start") or None),
            end=(data.get("end") or None),
        )

    def date_sql(self, col: str) -> tuple[str, list]:
        """Date-only fragment for tables with no `matches` join (scrims)."""
        clauses, params = [], []
        if self.start:
            clauses.append(f"{col} >= ?")
            params.append(self.start)
        if self.end:
            clauses.append(f"{col} <= ?")
            params.append(self.end)
        return ("".join(f" AND {c}" for c in clauses), params)


def family_of(competition: str | None, season: int) -> str:
    if season == 1 and competition is None:
        return "CDM Spring"
    if season == 2 and competition == "CDM":
        return "CDM Summer"
    return "Regionals"


def match_label(competition: str | None, round_: str | None, season: int) -> str:
    head = competition if family_of(competition, season) == "Regionals" else family_of(competition, season)
    return f"{head} · {round_}" if round_ else head
