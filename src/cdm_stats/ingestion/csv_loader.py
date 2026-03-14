def derive_pick_context(slot: int, picker_score: int, opponent_score: int) -> str:
    """
    Derive the pick context for a map result based on the series state.

    Args:
        slot: Map slot in the Best-of-5 series (1-5)
        picker_score: The picking team's series score before this map (0-2)
        opponent_score: The opponent's series score before this map (0-2)

    Returns:
        One of: "Opener", "Neutral", "Must-Win", "Close-Out", "Coin-Toss"

    Rules:
    - Slot 5 is always a coin toss (regardless of series score)
    - Slot 1 is always the opener (first map)
    - If opponent has 2 wins and picker has < 2: Must-Win
    - If picker has 2 wins and opponent has < 2: Close-Out
    - All other cases: Neutral
    """
    if slot == 5:
        return "Coin-Toss"
    if slot == 1:
        return "Opener"
    if opponent_score == 2 and picker_score < 2:
        return "Must-Win"
    if picker_score == 2 and opponent_score < 2:
        return "Close-Out"
    return "Neutral"
