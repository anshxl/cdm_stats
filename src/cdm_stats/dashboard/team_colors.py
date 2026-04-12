"""Per-team brand colors for duotone chart rendering.

Each entry maps a team abbreviation (matching ``teams.abbreviation`` in the
DB exactly, case-sensitive) to a ``(primary, secondary)`` hex pair:

    primary   — fill color for bars / line stroke
    secondary — outline color for bars / accent stripe

Either slot can be ``None`` to fall back to the chart's default palette,
so this registry can be filled in incrementally without breaking the chart.
Drop in real hex values as you have time; teams without an entry (or with
``None`` slots) keep using the existing high-contrast Twilight Ops palette.

Reference: ``sqlite3 data/cdl.db "SELECT abbreviation, team_name FROM teams"``
"""
from __future__ import annotations

TeamColorPair = tuple[str | None, str | None]


TEAM_COLORS: dict[str, TeamColorPair] = {
    # abbreviation:  (primary,  secondary)        # full team name
    "ALU":           ("#004362",     "#A8A06E"),            # Al-Ula Club
    "DVS":           ("#ED3046",     "#FFFFFF"),            # Diavolos
    "ELV":           ("#E10202",     "#FFFFFF"),            # Elevate
    "ETs":           ("#4B0082",     "#FFFFFF"),            # Elite Titan Esports
    "Felines":       ("#B2223F",     "#3B0101"),            # Team Felines
    "GAL":           ("#4B0082",     "#245EF0"),            # Galorys
    "GL":            ("#F1C61B",     "#000000"),            # GodLike
    "HF":            ("#C0C0C0",     "#5F0202"),            # HF
    "OUG":           ("#2626CC",     "#F82E2E"),            # OU Gaming
    "Q9":            ("#FB923C",     "#FF2A00"),            # Qing Jiu Club
    "SPG":           ("#896E05",     "#FFD900"),            # Stand Point Game
    "TSM":           ("#EEEAEA",     "#1A1A1A"),            # Team StarMagic
    "Wolves":        ("#000000",     "#F1C61B"),            # Wolves
    "XROCK":         ("#BC0BBC",     "#FFFFFF"),            # XROCK
}


def team_colors(abbr: str, fallback: str) -> tuple[str, str]:
    """Resolve a team's (primary, secondary) hex pair, with fallback.

    Args:
        abbr: Team abbreviation, case-sensitive (must match the DB).
        fallback: Hex string used when the team has no entry, or when an
            entry slot is ``None``. Pass the chart's default palette color
            for this team so unfilled rows render monotone.

    Returns:
        ``(primary, secondary)`` — both guaranteed non-None.
    """
    primary, secondary = TEAM_COLORS.get(abbr, (None, None))
    return primary or fallback, secondary or fallback
