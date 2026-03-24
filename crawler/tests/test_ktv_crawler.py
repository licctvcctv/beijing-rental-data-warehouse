import unittest
from unittest.mock import patch

from common.models import LinkRecord
from ktv.crawler import KtvCrawler, LIST_URL


BUSINESS_LIST_URL = "https://www.cityhui.com/beijing/shangwuktv/"


class StubClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, referer=None):
        self.calls.append((url, referer))
        return self.responses[url]


STANDARD_LIST_HTML = """
<html>
  <body>
    <a href="/shop/17401.html">北京纯K</a>
  </body>
</html>
"""


BUSINESS_LIST_HTML = """
<html>
  <body>
    <a href="/shop/103471.html">北京京浙会KTV</a>
    <a href="/shop/34433.html">北京京浙会KTV会所-朝阳店</a>
  </body>
</html>
"""


STANDARD_DETAIL_HTML = """
<html>
  <head>
    <title>【北京纯K】怎么样,地址,电话,价格,点评-北京KTV-城市惠</title>
  </head>
  <body>
    <div>地址：北京市朝阳区建国路88号 参考价格： ￥180 营业时间： 10:00-02:00 提示信息：说明 综合评分 85 口碑评分 4.5 人气指数 18132</div>
  </body>
</html>
"""


BUSINESS_DETAIL_HTML = """
<html>
  <head>
    <title>【北京京浙会KTV】消费价格,怎么样,预订电话,地址-北京商务KTV-城市惠</title>
  </head>
  <body>
    <div>
      北京京浙会KTV 更新时间：2025-12-31 北京京浙会KTV 地址： 北京市朝阳区东四环中路
      （到店请先提前预订） 预订电话： 13260217387 消费价格： ¥ 4880
      营业时间： 晚上19:00至凌晨3:30 所在区域： 所在榜单： 北京商务KTV排行榜
      提示信息：此页面为用户创建公共信息 综合评分 88 口碑评分 4.5 人气指数 9199
    </div>
  </body>
</html>
"""


class KtvCrawlerTests(unittest.TestCase):
    def test_parse_detail_extracts_actual_business_address(self):
        record = KtvCrawler(client=StubClient({})).parse_detail(
            BUSINESS_DETAIL_HTML,
            LinkRecord(
                detail_url="https://www.cityhui.com/shop/103471.html",
                list_url=BUSINESS_LIST_URL,
                page=1,
                source_site="城市惠",
                name="北京京浙会KTV",
            ),
        )

        self.assertEqual("北京市朝阳区东四环中路", record.address)
        self.assertEqual("朝阳区", record.region)
        self.assertEqual("4880", record.avg_cost)

    def test_crawl_merges_business_ktv_source_and_deduplicates_venues(self):
        client = StubClient(
            {
                LIST_URL: STANDARD_LIST_HTML,
                BUSINESS_LIST_URL: BUSINESS_LIST_HTML,
                "https://www.cityhui.com/shop/17401.html": STANDARD_DETAIL_HTML,
                "https://www.cityhui.com/shop/103471.html": BUSINESS_DETAIL_HTML,
                "https://www.cityhui.com/shop/34433.html": BUSINESS_DETAIL_HTML,
            }
        )

        with patch("ktv.crawler.write_csv"):
            records = KtvCrawler(client=client).crawl()

        self.assertEqual(2, len(records))
        self.assertIn("https://www.cityhui.com/beijing/shangwuktv/", [url for url, _ in client.calls])
        business_records = [record for record in records if "京浙会" in record.name]
        self.assertEqual(1, len(business_records))
        self.assertEqual("朝阳区", business_records[0].region)


if __name__ == "__main__":
    unittest.main()
