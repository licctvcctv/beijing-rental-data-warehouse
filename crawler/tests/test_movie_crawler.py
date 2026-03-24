import unittest
from unittest.mock import patch

from movie.crawler import LIST_URL, MovieCrawler


class StubClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, referer=None):
        self.calls.append((url, referer))
        return self.responses[url]


FIRST_PAGE_HTML = """
<html>
  <body>
    <div class="item">
      <div class="pic"><a href="/subject/1/"></a></div>
      <div class="bd">
        <div class="hd"><span class="title">电影甲</span></div>
        <p>导演: 张三 主演: 李四 / 王五<br>2020 / 中国大陆 / 剧情 喜剧</p>
        <span class="rating_num">8.8</span>
        <span class="inq">简介甲</span>
      </div>
    </div>
    <div class="paginator">
      <span>(共30条)</span>
      <a href="/top250?start=25&filter=">2</a>
    </div>
  </body>
</html>
"""


SECOND_PAGE_HTML = """
<html>
  <body>
    <div class="item">
      <div class="pic"><a href="/subject/2/"></a></div>
      <div class="bd">
        <div class="hd"><span class="title">电影乙</span></div>
        <p>导演: 李导<br>2019 / 美国 英国 / 科幻 冒险</p>
        <span class="rating_num">9.1</span>
      </div>
    </div>
  </body>
</html>
"""


class MovieCrawlerTests(unittest.TestCase):
    def test_crawl_parses_multiple_pages(self):
        client = StubClient(
            {
                LIST_URL: FIRST_PAGE_HTML,
                f"{LIST_URL}?start=25&filter=": SECOND_PAGE_HTML,
            }
        )

        with patch("movie.crawler.write_csv"):
            records = MovieCrawler(client=client).crawl()

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].name, "电影甲")
        self.assertEqual(records[0].score, "8.8")
        self.assertEqual(records[0].country_region, "中国大陆")
        self.assertEqual(records[0].category, "剧情 喜剧")
        self.assertEqual(records[0].director, "张三")
        self.assertEqual(records[0].actors, "李四 / 王五")
        self.assertEqual(records[1].name, "电影乙")
        self.assertEqual(records[1].country_region, "美国 英国")
        self.assertEqual(records[1].category, "科幻 冒险")
        self.assertEqual(records[1].intro, r"\N")

    def test_crawl_respects_sample_limit(self):
        client = StubClient(
            {
                LIST_URL: FIRST_PAGE_HTML,
                f"{LIST_URL}?start=25&filter=": SECOND_PAGE_HTML,
            }
        )

        with patch("movie.crawler.write_csv"):
            records = MovieCrawler(client=client).crawl(sample=1)

        self.assertEqual(len(records), 1)
        self.assertEqual(client.calls, [(LIST_URL, None)])


if __name__ == "__main__":
    unittest.main()
