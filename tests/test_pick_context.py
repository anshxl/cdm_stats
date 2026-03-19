from cdm_stats.ingestion.csv_loader import derive_pick_context


def test_slot_5_is_coin_toss():
    assert derive_pick_context(slot=5, picker_score=1, opponent_score=2) == "Coin-Toss"


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

def test_bo7_slot_7_is_coin_toss():
    assert derive_pick_context(slot=7, picker_score=3, opponent_score=3,
                               win_threshold=4, last_slot=7) == "Coin-Toss"


def test_bo7_slot_5_is_not_coin_toss():
    """In BO7, slot 5 is a regular slot, not a coin toss."""
    result = derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                                 win_threshold=4, last_slot=7)
    assert result == "Neutral"


def test_bo7_must_win_when_opponent_has_3():
    """In BO7, Must-Win when opponent needs 1 more win (has 3) and picker has less."""
    assert derive_pick_context(slot=5, picker_score=1, opponent_score=3,
                               win_threshold=4, last_slot=7) == "Must-Win"


def test_bo7_close_out_when_picker_has_3():
    """In BO7, Close-Out when picker needs 1 more win (has 3) and opponent has less."""
    assert derive_pick_context(slot=5, picker_score=3, opponent_score=1,
                               win_threshold=4, last_slot=7) == "Close-Out"


def test_bo7_neutral_at_2_2():
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=4, last_slot=7) == "Neutral"


# Ensure existing BO5 behavior still works with explicit params
def test_bo5_explicit_params_slot5_coin_toss():
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2,
                               win_threshold=3, last_slot=5) == "Coin-Toss"


def test_bo5_defaults_backward_compatible():
    """Calling without new params still works for BO5."""
    assert derive_pick_context(slot=5, picker_score=2, opponent_score=2) == "Coin-Toss"
    assert derive_pick_context(slot=3, picker_score=0, opponent_score=2) == "Must-Win"
