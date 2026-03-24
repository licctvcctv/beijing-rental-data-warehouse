import unittest
from unittest.mock import patch

from sport.crawler import CITYHUI_CATEGORY_SOURCES, LIST_URL, SportCrawler


class StubClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, referer=None):
        self.calls.append((url, referer))
        return self.responses[url]


VISIT_LIST_HTML = """
<html>
  <body>
    <a href="/attraction/1001">国家游泳中心</a>
    <span>1</span>
  </body>
</html>
"""

VISIT_DETAIL_HTML = """
<html>
  <body>
    <div class="infos">
      <h3 class="title">国家游泳中心</h3>
      <div class="type"><span>游泳馆</span></div>
    </div>
    <div>地址：北京市朝阳区天辰东路11号 百度地图查看路线</div>
    <div>开放时间：09:00-21:00 景区介绍</div>
  </body>
</html>
"""

CITYHUI_LIST_HTML = """
<html>
  <body>
    <a href="/shop/17430.html">Superfit速展飞</a>
  </body>
</html>
"""

CITYHUI_DETAIL_HTML = """
<html>
  <head>
    <title>【Superfit速展飞】怎么样,地址,电话,价格,点评-北京健身房-城市惠</title>
  </head>
  <body>
    <div>地址：工人体育场西路50号 参考价格： ￥199 营业时间： 07:00-23:00 提示信息：说明 综合评分 90 口碑评分 4.5 人气指数 24480</div>
  </body>
</html>
"""

EMPTY_HTML = "<html><body></body></html>"


class SportCrawlerTests(unittest.TestCase):
    def test_crawl_merges_visitbeijing_and_cityhui_sources(self):
        responses = {
            LIST_URL: VISIT_LIST_HTML,
            "https://s.visitbeijing.com.cn/attraction/1001": VISIT_DETAIL_HTML,
            CITYHUI_CATEGORY_SOURCES[0]["list_url"]: CITYHUI_LIST_HTML,
            CITYHUI_CATEGORY_SOURCES[1]["list_url"]: EMPTY_HTML,
            CITYHUI_CATEGORY_SOURCES[2]["list_url"]: EMPTY_HTML,
            "https://www.cityhui.com/shop/17430.html": CITYHUI_DETAIL_HTML,
        }

        with patch("sport.crawler.write_csv"):
            records = SportCrawler(client=StubClient(responses)).crawl()

        self.assertEqual(len(records), 2)
        visit_record = next(record for record in records if record.name == "国家游泳中心")
        self.assertEqual(visit_record.venue_type, "游泳馆")
        self.assertEqual(visit_record.region, "朝阳区")
        cityhui_record = next(record for record in records if record.name == "Superfit速展飞")
        self.assertEqual(cityhui_record.venue_type, "健身房")
        self.assertEqual(cityhui_record.region, r"\N")
        self.assertEqual(cityhui_record.avg_cost, "199")
        self.assertEqual(cityhui_record.score, "90")

    def test_crawl_respects_sample_limit(self):
        responses = {
            LIST_URL: VISIT_LIST_HTML,
            "https://s.visitbeijing.com.cn/attraction/1001": VISIT_DETAIL_HTML,
            CITYHUI_CATEGORY_SOURCES[0]["list_url"]: CITYHUI_LIST_HTML,
            CITYHUI_CATEGORY_SOURCES[1]["list_url"]: EMPTY_HTML,
            CITYHUI_CATEGORY_SOURCES[2]["list_url"]: EMPTY_HTML,
            "https://www.cityhui.com/shop/17430.html": CITYHUI_DETAIL_HTML,
        }
        client = StubClient(responses)

        with patch("sport.crawler.write_csv"):
            records = SportCrawler(client=client).crawl(sample=1)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].name, "Superfit速展飞")


if __name__ == "__main__":
    unittest.main()
