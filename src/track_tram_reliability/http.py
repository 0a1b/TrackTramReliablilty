from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import __version__


def create_session(
    user_agent: str | None = None,
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a configured requests session with retry and a User-Agent.

    Args:
        user_agent: Custom User-Agent header value.
        total_retries: Total retry attempts for transient errors.
        backoff_factor: Exponential backoff factor in seconds.
        status_forcelist: HTTP status codes to trigger retries.

    Returns:
        Configured requests Session.
    """
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods={"GET", "POST"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": user_agent
            or f"TrackTramReliability/{__version__} (+https://example.com/track-tram-reliability)",
            "Accept": "application/json",
        }
    )

    return session
