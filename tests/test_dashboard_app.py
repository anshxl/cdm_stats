def test_app_imports_and_initializes():
    """Verify the Dash app can be imported and all callbacks registered."""
    from cdm_stats.dashboard.app import app, register_all_callbacks
    register_all_callbacks()
    assert app.layout is not None


def test_render_tab_returns_content():
    """Verify each tab renders without error."""
    from cdm_stats.dashboard.app import render_tab
    for tab in ["matchup-prep", "team-profile", "player-stats", "scrim-performance", "elo-tracker"]:
        result = render_tab(tab)
        assert result is not None


def test_map_matrix_tab_removed():
    """map-matrix is no longer a valid tab id."""
    from cdm_stats.dashboard.app import render_tab
    from dash import html
    result = render_tab("map-matrix")
    assert isinstance(result, html.Div)
    # Falls through to "Select a tab" placeholder
