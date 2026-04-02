def test_app_imports_and_initializes():
    """Verify the Dash app can be imported and all callbacks registered."""
    from cdm_stats.dashboard.app import app, register_all_callbacks
    register_all_callbacks()
    assert app.layout is not None


def test_render_tab_returns_content():
    """Verify each tab renders without error."""
    from cdm_stats.dashboard.app import render_tab
    for tab in ["team-profile", "map-matrix", "matchup-prep", "elo-tracker"]:
        result = render_tab(tab)
        assert result is not None
