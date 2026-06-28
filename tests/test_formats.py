from cdm_stats.ingestion.formats import FORMATS, Format


def test_s2_formats_have_thresholds():
    assert FORMATS["Ro3"].win_threshold == 2
    assert FORMATS["Bo5"].win_threshold == 3
    assert FORMATS["Bo7"].win_threshold == 4


def test_s2_formats_are_slotless():
    # S2 derives mode from the map, so these carry no slot->mode order.
    assert FORMATS["Ro3"].slot_modes == {}
    assert FORMATS["Bo5"].slot_modes == {}
    assert FORMATS["Bo7"].slot_modes == {}


def test_format_slot_modes_is_optional():
    fmt = Format(win_threshold=2)
    assert fmt.slot_modes == {}
    assert fmt.expected_bans == 0


def test_s1_formats_keep_slot_modes():
    assert FORMATS["CDL_BO5"].slot_modes[1] == "SnD"
