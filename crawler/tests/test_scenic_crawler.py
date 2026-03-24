import unittest

from scenic.crawler import LEISURE_LIST_URL, LIST_URL, ScenicCrawler


class StubClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, referer=None):
        self.calls.append((url, referer))
        return self.responses[url]


LEISURE_PAGE_HTML = """
<html>
  <body>
    <a href="/attraction/2001">景点甲</a>
    <a href="/attraction/2002">景点乙</a>
    <span>1</span>
  </body>
</html>
"""

OFFICIAL_LIST_HTML = """
<html>
  <body>
    <table>
      <tr>
        <td>1</td>
        <td>官方景点</td>
        <td>5A</td>
        <td>东城区</td>
        <td><a href="https://s.visitbeijing.com.cn/attraction/3001">详情</a></td>
      </tr>
    </table>
  </body>
</html>
"""


class ScenicCrawlerTests(unittest.TestCase):
    def test_discover_links_prefers_large_leisure_pool(self):
        client = StubClient({LEISURE_LIST_URL: LEISURE_PAGE_HTML})

        links = ScenicCrawler(client=client)._discover_leisure_links(set())

        self.assertEqual(len(links), 2)
        self.assertEqual(links[0].name, "景点甲")
        self.assertEqual(links[1].detail_url, "https://s.visitbeijing.com.cn/attraction/2002")

    def test_discover_links_falls_back_to_official_source(self):
        client = StubClient(
            {
                LEISURE_LIST_URL: LEISURE_PAGE_HTML,
                LIST_URL: OFFICIAL_LIST_HTML,
            }
        )
        crawler = ScenicCrawler(client=client)
        crawler._discover_leisure_links = lambda seen: []  # type: ignore[method-assign]

        links = crawler.discover_links()

        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].name, "官方景点")
        self.assertEqual(links[0].level, "5A")
        self.assertEqual(links[0].region, "东城区")


if __name__ == "__main__":
    unittest.main()
