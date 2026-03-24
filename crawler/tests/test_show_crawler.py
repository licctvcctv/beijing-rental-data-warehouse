import unittest
from unittest.mock import patch

from show.crawler import LIST_URL, ShowCrawler


DF_BEIJING_LIST_URL = "https://www.df962388.com/yanchu/beijing/"
DF_BEIJING_PAGE_2_URL = "https://www.df962388.com/yanchu/beijing/index_2.html"


class StubClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, referer=None):
        self.calls.append((url, referer))
        return self.responses[url]


DAHE_LIST_HTML = """
<html>
  <body>
    <a href="/yanchupiaowu1/yyh/2017111531299.html">【北京】爱·永恒—理查德·克莱德曼2026钢琴音乐会北京站 售票中</a>
    <a href="/yanchupiaowu1/yyh/2017111531299.html">立即预订</a>
    <div>共1页1条</div>
  </body>
</html>
"""


DAHE_DETAIL_HTML = """
<html>
  <body>
    <h1>爱·永恒—理查德·克莱德曼2026钢琴音乐会北京站</h1>
    <div>演出时间</div>
    <div>2026-07-24 周五 19:30</div>
    <div>演出场馆</div>
    <div>北京展览馆剧场</div>
    <div>票价：380,680,980,1280,1580</div>
    <div>售票中</div>
    <div>关注度</div>
    <div>16</div>
  </body>
</html>
"""


DF_LIST_HTML = """
<html>
  <body>
    <div class="list-archive">
      <ul>
        <li>
          <div class="list-main">
            <h3 class="item-title">
              <a href="https://www.df962388.com/yanchu/351279.html">理查德·克莱德曼钢琴音乐会北京站</a>
            </h3>
            <div class="item-excerpt">
              <p>演出时间：2026.07.24 周五 19:30</p>
              <p>地点：北京展览馆剧场</p>
              <p>门票价格：380-1580</p>
            </div>
          </div>
        </li>
        <li>
          <div class="list-main">
            <h3 class="item-title">
              <a href="https://www.df962388.com/yanchu/351454.html">北京乐见古典打击乐音乐会</a>
            </h3>
            <div class="item-excerpt">
              <p>演出时间：2026.04.05 19:30 周日</p>
              <p>地点：北京音乐厅</p>
              <p>门票价格：99-599</p>
            </div>
          </div>
        </li>
      </ul>
    </div>
    <a href="https://www.df962388.com/yanchu/beijing/index_2.html">下一页</a>
    <a href="https://www.df962388.com/yanchu/beijing/index_3.html">共3页</a>
  </body>
</html>
"""


class ShowCrawlerTests(unittest.TestCase):
    def test_crawl_merges_df_city_source_and_deduplicates_duplicate_events(self):
        client = StubClient(
            {
                LIST_URL: DAHE_LIST_HTML,
                "https://www.dahepiao.com/yanchupiaowu1/yyh/2017111531299.html": DAHE_DETAIL_HTML,
                DF_BEIJING_LIST_URL: DF_LIST_HTML,
            }
        )

        with patch("show.crawler.write_csv"):
            records = ShowCrawler(client=client, df_city_page_limit=1).crawl()

        self.assertEqual(2, len(records))
        self.assertNotIn((DF_BEIJING_PAGE_2_URL, DF_BEIJING_LIST_URL), client.calls)
        records_by_name = {record.name: record for record in records}
        self.assertIn("北京乐见古典打击乐音乐会", records_by_name)
        self.assertEqual("北京音乐厅", records_by_name["北京乐见古典打击乐音乐会"].venue)
        self.assertEqual("99-599", records_by_name["北京乐见古典打击乐音乐会"].price_range)
        self.assertEqual("售票中", records_by_name["爱·永恒—理查德·克莱德曼2026钢琴音乐会北京站"].status)


if __name__ == "__main__":
    unittest.main()
