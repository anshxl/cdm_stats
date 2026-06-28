"""Single source of truth for match formats.

Adding a format = one entry in FORMATS. Slot order, win threshold, and ban
rules all live here; nothing else should hard-code format details.

ponytail: all formats are slot-based (fixed slot->mode series). A future S2
format that drops fixed slots needs a new field + ingestion path, not a
reshape of this dataclass — add it when that format is finalized.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Format:
    win_threshold: int                    # series wins needed to take the match
    slot_modes: dict[int, str] = field(default_factory=dict)  # slot (1-based) -> mode; empty = mode comes from the map (S2)
    ban_modes: frozenset[str] = field(default_factory=frozenset)  # modes eligible for bans
    expected_bans: int = 0                # 0 = bans not tracked for this format


FORMATS: dict[str, Format] = {
    "CDL_BO5": Format(
        slot_modes={1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
        win_threshold=3,
    ),
    "CDL_PLAYOFF_BO5": Format(
        slot_modes={1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP"},
        win_threshold=3,
        ban_modes=frozenset({"HP", "SnD", "Control"}),
        expected_bans=6,
    ),
    "CDL_PLAYOFF_BO7": Format(
        slot_modes={1: "SnD", 2: "HP", 3: "Control", 4: "SnD", 5: "HP", 6: "Control", 7: "SnD"},
        win_threshold=4,
        ban_modes=frozenset({"HP", "SnD"}),
        expected_bans=4,
    ),
    "TOURNAMENT_BO5": Format(
        slot_modes={1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD"},
        win_threshold=3,
        ban_modes=frozenset({"HP", "SnD", "Control"}),
        expected_bans=6,
    ),
    "TOURNAMENT_BO7": Format(
        slot_modes={1: "HP", 2: "SnD", 3: "Control", 4: "HP", 5: "SnD", 6: "Control", 7: "SnD"},
        win_threshold=4,
        ban_modes=frozenset({"HP", "SnD"}),
        expected_bans=4,
    ),
    # S2: map-centric (mode derived from the map), so no slot_modes. expected_bans
    # is 3 per team = 6 total, used only for a soft warning in the bans loader.
    "Ro3": Format(win_threshold=2, expected_bans=6),
    "Bo5": Format(win_threshold=3, expected_bans=6),
    "Bo7": Format(win_threshold=4, expected_bans=6),
}
