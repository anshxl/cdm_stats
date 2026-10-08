"""Pydantic response models — the frontend contract (types are generated from these)."""
from typing import Literal

from pydantic import BaseModel


class Flag(BaseModel):
    kind: Literal["up", "down", "low_sample"]
    label: str


class Flagged(BaseModel):
    """A numeric stat with its sample size and optional highlight."""
    value: float | None
    n: int
    flag: Flag | None = None


class TeamInfo(BaseModel):
    abbreviation: str
    team_name: str
    primary_color: str | None
    secondary_color: str | None
    logo: str | None


# --- Team profile ------------------------------------------------------------

class Record(BaseModel):
    series_wins: int
    series_losses: int
    map_wins: int
    map_losses: int


class Strength(BaseModel):
    rating: float | None
    weighted_sample: float
    total_played: int
    low_confidence: bool


class Rate(BaseModel):
    """count of total series (ban/pick data is partial; total = series with data)."""
    count: int
    total: int


class MapResult(BaseModel):
    match_date: str
    opponent: str
    score: str
    pick_context: str | None
    picked_by: str
    result: Literal["W", "L"]
    family: str
    label: str


class ProfileMap(BaseModel):
    map_id: int | None
    map_name: str
    mode: str
    wins: int
    losses: int
    win_rate: Flagged
    strength: Strength
    pick_wins: int
    pick_losses: int
    defend_wins: int
    defend_losses: int
    banned: Rate
    picked: Rate
    opp_banned: Rate
    history: list[MapResult]


class BanHighlight(BaseModel):
    mode: str
    map_name: str
    flag: Flag


class Profile(BaseModel):
    team: TeamInfo
    elo: float
    low_confidence: bool
    record: Record
    overall_map_win_rate: float | None
    maps: list[ProfileMap]
    ban_summary: list[BanHighlight]


# --- Players -------------------------------------------------------------------

class PlayerTile(BaseModel):
    player_name: str
    kills: int
    deaths: int
    assists: int
    games: int
    kd: Flagged
    avg_pos_eng_pct: float
    op_kills: int | None
    op_pulls: int | None
    op_kills_per_pull: Flagged


class KdPoint(BaseModel):
    player_name: str
    match_date: str
    kills: int
    deaths: int
    kd: float


class OpsPoint(BaseModel):
    player_name: str
    match_date: str
    op_kills: int
    op_pulls: int
    maps: int
    kills_per_pull: float | None


class SeriesPlayer(BaseModel):
    player_name: str
    kills: int | None
    deaths: int | None
    assists: int | None
    op_kills: int | None
    op_pulls: int | None


class SeriesMap(BaseModel):
    result_id: int
    slot: int
    map_name: str
    mode: str
    won: bool
    our_score: int
    their_score: int
    players: list[SeriesPlayer]


class PlayerSeries(BaseModel):
    match_id: int
    match_date: str
    opponent: str
    our_maps: int
    their_maps: int
    maps: list[SeriesMap]


class Players(BaseModel):
    players: list[str]
    tiles: list[PlayerTile]
    kd_trend: list[KdPoint]
    ops_trend: list[OpsPoint]
    recent_series: list[PlayerSeries]


# --- Head to head --------------------------------------------------------------

class WL(BaseModel):
    wins: int
    losses: int


class EloSide(BaseModel):
    elo: float
    low_confidence: bool


class H2HElo(BaseModel):
    team: EloSide
    opp: EloSide


class SeriesResult(BaseModel):
    match_date: str
    opponent: str
    result: Literal["W", "L"]
    score: str
    family: str
    label: str


class H2HMap(BaseModel):
    map_id: int
    map_name: str
    delta: float | None
    h2h: WL
    your_wl: WL
    opp_wl: WL
    your_rate: Flagged
    opp_rate: Flagged
    your_strength: Strength
    opp_strength: Strength
    your_pick_wl: WL
    your_defend_wl: WL
    opp_pick_wl: WL
    opp_defend_wl: WL
    opp_bans: Rate
    opp_bans_h2h: Rate
    opp_picks: Rate
    tags: list[Literal["pick", "ban", "they_ban"]]


class H2HMode(BaseModel):
    mode: str
    maps: list[H2HMap]
    not_enough_data: bool


class H2H(BaseModel):
    team: TeamInfo
    opp: TeamInfo
    elo: H2HElo
    h2h_series: WL
    opp_recent_series: list[SeriesResult]
    modes: list[H2HMode]


class Brief(BaseModel):
    text: str


# --- Scrims ----------------------------------------------------------------------

class ScrimWL(BaseModel):
    wins: int
    losses: int
    total: int
    win_pct: float


class ScrimModeWL(ScrimWL):
    mode: str


class ScrimResult(BaseModel):
    date: str
    week: int
    opponent: str
    our_score: int
    opp_score: int
    result: Literal["W", "L"]


class ScrimMap(BaseModel):
    map_name: str
    mode: str
    played: int
    wins: int
    losses: int
    win_pct: float
    avg_margin: float | None
    flag: Flag | None
    recent: list[ScrimResult]


class ScrimTrendPoint(BaseModel):
    match_date: str
    played: int
    wins: int
    win_pct: float


class ScrimKdPoint(KdPoint):
    assists: int
    games: int


class Scrims(BaseModel):
    overall: ScrimWL
    by_mode: list[ScrimModeWL]
    maps: list[ScrimMap]
    trend: list[ScrimTrendPoint]
    # Per player per scrim day; the page sums these for the tiles and the chart.
    kd_trend: list[ScrimKdPoint]


class ScrimOptions(BaseModel):
    maps: list[str]
    opponents: list[TeamInfo]


# --- Elo -------------------------------------------------------------------------

class EloPoint(BaseModel):
    match_date: str
    elo: float
    opponent: str
    result: Literal["W", "L"]
    label: str


class EloSeries(BaseModel):
    team: TeamInfo
    low_confidence: bool
    points: list[EloPoint]


class EloCurrent(BaseModel):
    team: TeamInfo
    elo: float
    low_confidence: bool


class Elo(BaseModel):
    """`trajectory` is filled for view=trajectory, `current` for view=current."""
    view: Literal["trajectory", "current"]
    seed: float
    trajectory: list[EloSeries]
    current: list[EloCurrent]


# --- Training (proxied from the Discord bot) ------------------------------------

class TrainingPlayer(BaseModel):
    username: str
    sessions: int
    days_trained: int


class TrainingDay(BaseModel):
    date: str
    username: str
    sessions: int


class Training(BaseModel):
    """`months` lists months with any submission, ascending; `players` in recap order."""
    month: str
    months: list[str]
    players: list[TrainingPlayer]
    daily: list[TrainingDay]
