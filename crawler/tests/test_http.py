import unittest

import requests

from common.http import HttpClient


class StubResponse:
    def __init__(self, text, status_code=200, encoding="utf-8"):
        self.text = text
        self.status_code = status_code
        self.encoding = encoding
        self.apparent_encoding = encoding

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


class StubSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = []

    def get(self, url, timeout=None, headers=None):
        self.calls.append((url, timeout, headers))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class HttpClientTests(unittest.TestCase):
    def test_get_retries_transient_request_errors(self):
        session = StubSession(
            [
                requests.ReadTimeout("slow"),
                StubResponse("ok"),
            ]
        )
        client = HttpClient(session=session, sleep_seconds=0, max_retries=2)

        body = client.get("https://example.com/demo", referer="https://example.com/")

        self.assertEqual(body, "ok")
        self.assertEqual(len(session.calls), 2)


if __name__ == "__main__":
    unittest.main()
