"""Inline team identifier — logo + abbreviation, with text-only fallback.

Drops in anywhere a team's abbreviation is currently displayed. If a PNG
exists at assets/logos/<abbr>.png the logo is rendered alongside the text;
otherwise only the styled abbreviation appears, so this is safe to use even
when only some teams have logos.
"""
import sqlite3

from dash import html

from cdm_stats.dashboard.helpers import get_all_teams, team_logo_src


def team_badge(
    abbr: str,
    color: str,
    *,
    size: int = 26,
    font_size: str = "1rem",
    show_text: bool = True,
) -> html.Span:
    """Build an inline-flex span containing a logo (if available) and abbr.

    Args:
        abbr: Team abbreviation, e.g. "ATL".
        color: Hex string for the abbreviation text.
        size: Logo height/width in px.
        font_size: CSS font-size for the abbreviation text.
        show_text: If False, hide the abbreviation when a logo is present
            (useful when space is tight). Falls back to text if no logo.
    """
    children: list = []
    src = team_logo_src(abbr)
    if src:
        children.append(
            html.Img(
                src=src,
                alt=abbr,
                style={
                    "height": f"{size}px",
                    "width": f"{size}px",
                    "objectFit": "contain",
                    "marginRight": "8px" if show_text or not src else "0",
                    "filter": "drop-shadow(0 2px 6px rgba(0, 0, 0, 0.45))",
                },
            )
        )
    if show_text or not src:
        children.append(
            html.Span(
                abbr,
                style={
                    "color": color,
                    "fontWeight": "600",
                    "fontSize": font_size,
                    "letterSpacing": "0.02em",
                },
            )
        )
    return html.Span(
        children,
        style={"display": "inline-flex", "alignItems": "center"},
    )


def team_dropdown_options_rich(
    conn: sqlite3.Connection, *, logo_size: int = 20
) -> list[dict]:
    """Return dcc.Dropdown options where each label is a logo + abbr row.

    Falls back to a plain abbreviation label when no logo file exists, so
    this is safe to use with partial logo coverage. The per-option ``search``
    field preserves type-to-find behavior, which dcc.Dropdown otherwise loses
    when option labels are components rather than strings.
    """
    teams = get_all_teams(conn)
    options: list[dict] = []
    for tid, abbr in teams:
        src = team_logo_src(abbr)
        if src:
            label = html.Div(
                [
                    html.Img(
                        src=src,
                        alt=abbr,
                        style={
                            "height": f"{logo_size}px",
                            "width": f"{logo_size}px",
                            "objectFit": "contain",
                            "marginRight": "8px",
                        },
                    ),
                    html.Span(abbr),
                ],
                style={"display": "flex", "alignItems": "center"},
            )
        else:
            label = abbr
        options.append({"label": label, "value": tid, "search": abbr})
    return options
