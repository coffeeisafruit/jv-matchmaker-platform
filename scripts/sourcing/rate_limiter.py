"""
Rate limiter with robots.txt respect for polite scraping.
"""

from __future__ import annotations

import time
import threading
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser


class RateLimiter:
    """Per-source rate limiting with robots.txt caching."""

    def __init__(self):
        self._lock = threading.Lock()
        self._last_request: dict[str, float] = {}
        self._robots_cache: dict[str, RobotFileParser] = {}

    def wait(self, source: str, max_rpm: int) -> None:
        """Block until safe to make the next request for this source."""
        min_interval = 60.0 / max_rpm
        with self._lock:
            last = self._last_request.get(source, 0)
            elapsed = time.time() - last
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request[source] = time.time()

    def is_allowed(self, url: str) -> bool:
        """Check robots.txt for the given URL. Returns True if allowed."""
        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

            if robots_url not in self._robots_cache:
                rp = RobotFileParser()
                rp.set_url(robots_url)
                rp.read()
                self._robots_cache[robots_url] = rp

            return self._robots_cache[robots_url].can_fetch("*", url)
        except Exception:
            # If we can't read robots.txt, allow the request
            return True
