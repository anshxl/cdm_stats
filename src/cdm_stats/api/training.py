"""/api/training: participation from the Discord bot's read-only endpoint.

Needs TRAINING_API_URL and TRAINING_API_TOKEN. Responses are cached per month for
TTL_SECONDS; failures are never cached and surface as 503.
"""
import json
import os
import time
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from cdm_stats.api.schemas import Training

router = APIRouter()

TIMEZONE = ZoneInfo("America/New_York")
TTL_SECONDS = 300
TIMEOUT_SECONDS = 5
_cache: dict[str, tuple[float, dict]] = {}
_now = time.monotonic  # replaced in tests


class TrainingUnavailable(Exception):
    """The bot endpoint could not be used; the message is the 503 detail."""


def fetch_training(month: str) -> dict:
    url = os.environ.get("TRAINING_API_URL")
    token = os.environ.get("TRAINING_API_TOKEN")
    if not url or not token:
        raise TrainingUnavailable("Training data is not configured "
                                  "(TRAINING_API_URL / TRAINING_API_TOKEN unset).")
    req = Request(f"{url.rstrip('/')}/training?{urlencode({'month': month})}",
                  headers={"Authorization": f"Bearer {token}"})
    try:
        with urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            if resp.status != 200:
                raise TrainingUnavailable(f"Training bot returned HTTP {resp.status}.")
            try:
                return json.load(resp)
            except ValueError as e:
                raise TrainingUnavailable("Training bot sent an invalid response.") from e
    except HTTPError as e:
        raise TrainingUnavailable(f"Training bot returned HTTP {e.code}.") from e
    except TimeoutError as e:
        raise TrainingUnavailable("Training bot timed out.") from e
    except URLError as e:
        if isinstance(e.reason, TimeoutError):
            raise TrainingUnavailable("Training bot timed out.") from e
        raise TrainingUnavailable(f"Training bot is unreachable: {e.reason}.") from e


def _current_month() -> str:
    return datetime.now(TIMEZONE).strftime("%Y-%m")


@router.get("/training", response_model=Training)
def training(month: str | None = Query(None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$")):
    month = month or _current_month()
    hit = _cache.get(month)
    if hit and _now() - hit[0] < TTL_SECONDS:
        return hit[1]
    try:
        body = fetch_training(month)
    except TrainingUnavailable as e:
        raise HTTPException(503, str(e)) from e
    _cache[month] = (_now(), body)
    return body
