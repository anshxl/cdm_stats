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
