from cdm_stats.ingestion.csv_loader import derive_pick_context


def test_slot_5_at_2_2_is_must_win():
    """Slot 5 is always 2-2 in BO5 — both teams need the win."""
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2) == "Must-Win"


def test_slot_1_is_opener():
    assert derive_pick_context(slot=1, picker_score=0, opponent_score=0) == "Opener"


def test_must_win_down_0_2():
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=2) == "Must-Win"


def test_must_win_down_1_2():
    assert derive_pick_context(slot=4, picker_score=1, opponent_score=2) == "Must-Win"


def test_close_out_up_2_0():
    assert derive_pick_context(slot=3, picker_score=2, opponent_score=0) == "Close-Out"


def test_close_out_up_2_1():
    assert derive_pick_context(slot=4, picker_score=2, opponent_score=1) == "Close-Out"


def test_neutral_1_0():
    assert derive_pick_context(slot=2, picker_score=1, opponent_score=0) == "Neutral"


def test_neutral_1_1():
    assert derive_pick_context(slot=3, picker_score=1, opponent_score=1) == "Neutral"


def test_neutral_0_1():
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=1) == "Neutral"


# --- BO7 tests ---

def test_bo7_slot_7_at_3_3_is_must_win():
    assert derive_pick_context(slot=7, picker_score=3, opponent_score=3,
                               win_threshold=4) == "Must-Win"


def test_bo7_slot_5_at_2_2_is_neutral():
    """In BO7, 2-2 is not yet pressure — need 4 wins."""
    result = derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                                 win_threshold=4)
    assert result == "Neutral"


def test_bo7_must_win_when_opponent_has_3():
    """In BO7, Must-Win when opponent needs 1 more win (has 3) and picker has less."""
    assert derive_pick_context(slot=5, picker_score=1, opponent_score=3,
                               win_threshold=4) == "Must-Win"


def test_bo7_close_out_when_picker_has_3():
    """In BO7, Close-Out when picker needs 1 more win (has 3) and opponent has less."""
    assert derive_pick_context(slot=5, picker_score=3, opponent_score=1,
                               win_threshold=4) == "Close-Out"


def test_bo7_neutral_at_2_2():
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=4) == "Neutral"


def test_bo5_slot5_is_must_win():
    """In BO5, slot 5 is always 2-2 — Must-Win for picker."""
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=3) == "Must-Win"


def test_bo5_defaults_backward_compatible():
    """Calling without new params still works for BO5."""
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2) == "Must-Win"
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=2) == "Must-Win"
