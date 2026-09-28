from cdm_stats.metrics.insights import ban_highlight, h2h_tags, map_flag, player_form_flag


# --- map_flag ---

def test_map_flag_low_sample_below_min_n():
    assert map_flag(1.0, 3, 0.5) == {"kind": "low_sample", "label": "n=3"}


def test_map_flag_exactly_min_n_is_eligible():
    assert map_flag(0.75, 4, 0.5) == {"kind": "up", "label": "+25 vs avg"}


def test_map_flag_up_label_rounded():
    assert map_flag(0.9, 10, 0.5667) == {"kind": "up", "label": "+33 vs avg"}


def test_map_flag_down():
    assert map_flag(0.3, 10, 0.5) == {"kind": "down", "label": "-20 vs avg"}


def test_map_flag_exactly_15_pts_up():
    assert map_flag(0.65, 10, 0.5) == {"kind": "up", "label": "+15 vs avg"}


def test_map_flag_exactly_minus_15_pts_down():
    assert map_flag(0.35, 10, 0.5) == {"kind": "down", "label": "-15 vs avg"}


def test_map_flag_just_under_15_pts_none():
    assert map_flag(0.649, 10, 0.5) is None
    assert map_flag(0.351, 10, 0.5) is None


def test_map_flag_zero_gap_none():
    assert map_flag(0.5, 10, 0.5) is None


# --- ban_highlight ---

def test_ban_highlight_most_banned_over_threshold():
    counts = {"Hacienda": 2, "Karachi": 5, "Rio": 1}
    assert ban_highlight(counts, 8) == ("Karachi", {"kind": "down", "label": "Banned 62%"})


def test_ban_highlight_exactly_40_pct():
    assert ban_highlight({"Rio": 2}, 5) == ("Rio", {"kind": "down", "label": "Banned 40%"})


def test_ban_highlight_below_40_pct_none():
    assert ban_highlight({"Rio": 3, "Karachi": 1}, 8) is None


def test_ban_highlight_exactly_min_n_series_eligible():
    assert ban_highlight({"Rio": 2}, 4) == ("Rio", {"kind": "down", "label": "Banned 50%"})


def test_ban_highlight_below_min_n_series_none():
    assert ban_highlight({"Rio": 3}, 3) is None


def test_ban_highlight_empty_or_zero_counts_none():
    assert ban_highlight({}, 10) is None
    assert ban_highlight({"Rio": 0}, 10) is None


def test_ban_highlight_tie_takes_first_in_input_order():
    assert ban_highlight({"Rio": 4, "Karachi": 4}, 8)[0] == "Rio"
    assert ban_highlight({"Karachi": 4, "Rio": 4}, 8)[0] == "Karachi"


# --- player_form_flag ---

def test_player_form_hot():
    series = [(5, 10)] * 5 + [(10, 10)] * 5  # whole 0.75, L5 1.00
    assert player_form_flag(series) == {"kind": "up", "label": "Hot · 1.00 L5"}


def test_player_form_cold():
    series = [(10, 10)] * 5 + [(5, 10)] * 5  # whole 0.75, L5 0.50
    assert player_form_flag(series) == {"kind": "down", "label": "Cold · 0.50 L5"}


def test_player_form_fewer_than_5_series_none():
    assert player_form_flag([(20, 1)] * 4) is None


def test_player_form_exactly_5_series_is_eligible_but_no_gap():
    # With exactly 5 series, L5 == whole selection, so delta is 0.
    assert player_form_flag([(5, 10), (5, 10), (5, 10), (5, 10), (50, 10)]) is None


def test_player_form_exactly_plus_015_is_hot():
    series = [(7, 10), (5, 4), (5, 4), (5, 4), (4, 4), (4, 4)]  # whole 1.00, L5 1.15
    assert player_form_flag(series) == {"kind": "up", "label": "Hot · 1.15 L5"}


def test_player_form_exactly_minus_015_is_cold():
    series = [(13, 10), (4, 4), (4, 4), (3, 4), (3, 4), (3, 4)]  # whole 1.00, L5 0.85
    assert player_form_flag(series) == {"kind": "down", "label": "Cold · 0.85 L5"}


def test_player_form_just_under_015_none():
    series = [(71, 100), (5, 4), (5, 4), (5, 4), (4, 4), (4, 4)]  # whole 94/120, L5 1.15
    assert round(23 / 20 - 94 / 120, 3) > 0.15  # sanity: this one is hot
    assert player_form_flag(series)["kind"] == "up"
    series = [(3, 4), (5, 4), (5, 4), (5, 4), (4, 4), (4, 4)]  # whole 26/24=1.083, L5 1.15
    assert player_form_flag(series) is None  # delta 0.067


def test_player_form_zero_deaths_treated_as_kills():
    series = [(5, 5)] + [(2, 0)] * 5  # L5 deaths 0 -> K/D 10; whole 15/5 = 3.0
    assert player_form_flag(series) == {"kind": "up", "label": "Hot · 10.00 L5"}


def test_player_form_all_zero_none():
    assert player_form_flag([(0, 0)] * 6) is None


# --- h2h_tags ---

def _row(m, our, our_n, their, their_n):
    return {"map": m, "our_rate": our, "our_n": our_n, "their_rate": their, "their_n": their_n}


def test_h2h_pick_and_ban():
    rows = [
        _row("Rio", 0.8, 5, 0.4, 5),      # +0.4 -> pick
        _row("Karachi", 0.3, 6, 0.7, 6),  # -0.4 -> ban
        _row("Hacienda", 0.5, 6, 0.4, 6),
    ]
    assert h2h_tags(rows, {}, 0) == {"Rio": ["pick"], "Karachi": ["ban"]}


def test_h2h_ignores_maps_with_either_n_below_4():
    rows = [
        _row("Rio", 1.0, 3, 0.0, 10),     # our_n < 4
        _row("Karachi", 0.0, 10, 1.0, 3),  # their_n < 4
        _row("Hacienda", 0.6, 4, 0.5, 4),  # exactly 4 -> eligible
    ]
    assert h2h_tags(rows, {}, 0) == {"Hacienda": ["pick"]}


def test_h2h_no_eligible_maps_returns_empty():
    rows = [_row("Rio", 1.0, 3, 0.0, 3)]
    assert h2h_tags(rows, {}, 0) == {}


def test_h2h_zero_gap_gets_no_tag():
    rows = [_row("Rio", 0.5, 5, 0.5, 5), _row("Karachi", 0.6, 5, 0.6, 5)]
    assert h2h_tags(rows, {}, 0) == {}


def test_h2h_all_positive_gaps_gives_no_ban():
    rows = [_row("Rio", 0.8, 5, 0.4, 5), _row("Karachi", 0.6, 5, 0.5, 5)]
    assert h2h_tags(rows, {}, 0) == {"Rio": ["pick"]}


def test_h2h_ties_take_first_in_input_order():
    rows = [
        _row("Rio", 0.7, 5, 0.5, 5),
        _row("Karachi", 0.7, 5, 0.5, 5),
        _row("Hacienda", 0.3, 5, 0.5, 5),
        _row("Vista", 0.3, 5, 0.5, 5),
    ]
    assert h2h_tags(rows, {}, 0) == {"Rio": ["pick"], "Hacienda": ["ban"]}


def test_h2h_they_ban_can_share_map_with_pick():
    rows = [_row("Rio", 0.8, 5, 0.4, 5), _row("Karachi", 0.5, 5, 0.5, 5)]
    tags = h2h_tags(rows, {"Rio": 3, "Karachi": 1}, 10)
    assert tags == {"Rio": ["pick", "they_ban"]}


def test_h2h_they_ban_exactly_30_pct():
    rows = [_row("Rio", 0.5, 5, 0.5, 5)]
    assert h2h_tags(rows, {"Karachi": 3}, 10) == {"Karachi": ["they_ban"]}


def test_h2h_they_ban_below_30_pct_none():
    rows = [_row("Rio", 0.5, 5, 0.5, 5)]
    assert h2h_tags(rows, {"Karachi": 2}, 10) == {}


def test_h2h_they_ban_needs_min_n_series_with_ban_data():
    rows = [_row("Rio", 0.5, 5, 0.5, 5)]
    assert h2h_tags(rows, {"Karachi": 3}, 3) == {}
    assert h2h_tags(rows, {"Karachi": 2}, 4) == {"Karachi": ["they_ban"]}


def test_h2h_they_ban_does_not_need_win_rate_eligibility():
    # they_ban comes from ban data (own MIN_N guard), not from win-rate rows.
    rows = [_row("Rio", 1.0, 3, 0.0, 3)]
    assert h2h_tags(rows, {"Karachi": 4}, 8) == {"Karachi": ["they_ban"]}
