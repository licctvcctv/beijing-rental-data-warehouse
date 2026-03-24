from __future__ import annotations

import time
from typing import Optional

import requests

from .constants import DEFAULT_HEADERS, DEFAULT_TIMEOUT


class HttpClient:
    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        sleep_seconds: float = 0.5,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get(self, url: str, *, referer: Optional[str] = None) -> str:
        headers = {}
        if referer:
            headers["Referer"] = referer
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout, headers=headers)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding or "utf-8"
                if self.sleep_seconds:
                    time.sleep(self.sleep_seconds)
                return response.text
            except requests.RequestException:
                if attempt >= self.max_retries:
                    raise
                retry_sleep = self.sleep_seconds or 0.5
                time.sleep(retry_sleep * attempt)
        raise RuntimeError("Unreachable")
