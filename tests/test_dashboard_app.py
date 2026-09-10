import json


def test_app_imports_and_initializes():
    from cdm_stats.dashboard.app import app, register_all_callbacks
    register_all_callbacks()
    assert app.layout is not None


TABS = ["team-profile", "head-to-head", "scrim-performance", "elo"]


def test_render_tab_returns_content():
    from cdm_stats.dashboard.app import render_tab
    for tab in TABS:
        assert render_tab(tab) is not None


def test_layout_has_filter_bar_and_no_season_toggle():
    from cdm_stats.dashboard.app import app
    serialized = json.dumps(app.layout.to_plotly_json(), default=str)
    assert "filter-store" in serialized
    assert "filter-families" in serialized
    assert "filter-dates" in serialized
    assert "season-store" not in serialized
    assert "season-tabs" not in serialized
    for tab in TABS:
        assert f"tab_id='{tab}'" in serialized
    assert "player-stats" not in serialized


def test_team_profile_is_landing_tab():
    from cdm_stats.dashboard.app import app
    serialized = json.dumps(app.layout.to_plotly_json(), default=str)
    assert "active_tab='team-profile'" in serialized


def test_sync_filter_builds_store_payload():
    from cdm_stats.dashboard.app import sync_filter
    assert sync_filter(["CDM Summer"], "2026-07-01", None) == {
        "families": ["CDM Summer"], "start": "2026-07-01", "end": None,
    }


def test_unknown_tab_falls_through():
    from cdm_stats.dashboard.app import render_tab
    from dash import html
    assert isinstance(render_tab("map-matrix"), html.Div)
