"""Shared week selector rendered as a Bootstrap button-pill radio group.

Value contract:
    - "all" means no week filter (None downstream)
    - integer N means that specific week, mapped to (N, N) downstream

Use pill_value_to_range() to translate to the week_range tuple that the
query layer expects.
"""
import dash_bootstrap_components as dbc


def week_pills(component_id: str, weeks: list[int]) -> dbc.RadioItems:
    """Return a RadioItems rendered as a btn-check pill group.

    Args:
        component_id: Dash component id.
        weeks: Sorted list of available week numbers (may be empty).
    """
    options = [{"label": "All", "value": "all"}]
    options.extend({"label": f"W{w}", "value": w} for w in weeks)
    return dbc.RadioItems(
        id=component_id,
        options=options,
        value="all",
        inline=True,
        inputClassName="btn-check",
        labelClassName="btn btn-outline-primary btn-sm me-1",
        labelCheckedClassName="active",
        className="mb-0",
    )


def pill_value_to_range(value) -> tuple[int, int] | None:
    """Convert a week-pill value to a (low, high) week range.

    "all" or None -> None (no filter)
    integer w     -> (w, w)
    """
    if value is None or value == "all":
        return None
    return (int(value), int(value))
