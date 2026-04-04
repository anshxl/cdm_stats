def test_week_pills_builds_options():
    from cdm_stats.dashboard.components.week_pills import week_pills
    component = week_pills("test-pills", [1, 2, 3])
    assert component.id == "test-pills"
    values = [opt["value"] for opt in component.options]
    labels = [opt["label"] for opt in component.options]
    assert values == ["all", 1, 2, 3]
    assert labels == ["All", "W1", "W2", "W3"]
    assert component.value == "all"


def test_week_pills_empty_weeks():
    from cdm_stats.dashboard.components.week_pills import week_pills
    component = week_pills("test-pills", [])
    values = [opt["value"] for opt in component.options]
    assert values == ["all"]
    assert component.value == "all"


def test_pill_value_to_range_all():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range("all") is None


def test_pill_value_to_range_int():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range(3) == (3, 3)


def test_pill_value_to_range_none_defaults_to_all():
    from cdm_stats.dashboard.components.week_pills import pill_value_to_range
    assert pill_value_to_range(None) is None
