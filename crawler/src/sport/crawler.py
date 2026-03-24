from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from common.constants import NULL
from common.http import HttpClient
from common.models import LinkRecord, SportRecord
from common.storage import as_row, now_iso, save_links, write_csv
from common.text import clean_text, extract_number, extract_region, first_non_null

LIST_URL = "https://s.visitbeijing.com.cn/attractions?type=8"
SOURCE_SITE = "北京旅游网"
DETAIL_PATH_RE = re.compile(r"/attraction/\d+$")
PAGER_RE = re.compile(r"^\d+$")
CITYHUI_SOURCE_SITE = "城市惠"
CITYHUI_CATEGORY_SOURCES = (
    {
        "list_url": "https://www.cityhui.com/beijing/jianshenfang/",
        "venue_type": "健身房",
    },
    {
        "list_url": "https://www.cityhui.com/beijing/huaxuechang/",
        "venue_type": "滑雪场",
    },
    {
        "list_url": "https://www.cityhui.com/beijing/yujiaguan/",
        "venue_type": "瑜伽馆",
    },
)
CITYHUI_FALLBACK_THRESHOLD = 20
CITYHUI_DETAIL_PATH_RE = re.compile(r"/shop/\d+\.html$")
SPORT_KEYWORDS = (
    "体育馆",
    "体育场",
    "游泳馆",
    "游泳中心",
    "滑冰馆",
    "滑雪场",
    "篮球公园",
    "足球场",
    "网球场",
    "高尔夫",
    "俱乐部",
    "运动中心",
    "体育公园",
    "奥体中心",
    "国家体育场",
    "国家体育馆",
    "国家游泳中心",
    "首都体育馆",
)
EXCLUDED_KEYWORDS = (
    "海底世界",
    "温泉",
    "公园",
    "度假村",
    "景区",
    "风景区",
    "旅游开发",
    "山庄",
    "水世界",
    "乐园",
    "公司",
)
VENUE_TYPE_KEYWORDS = (
    "体育场",
    "体育馆",
    "游泳馆",
    "游泳中心",
    "滑冰馆",
    "滑雪场",
    "篮球公园",
    "足球场",
    "网球场",
    "高尔夫",
    "俱乐部",
    "运动中心",
    "体育公园",
    "奥体中心",
)
ADDRESS_RE = re.compile(r"地址[:：]\s*(.+?)(?=\s+(?:百度地图查看路线|乘车路线|开放时间|门票价格|联系方式|推荐游览时长)|$)")
OPEN_TIME_RE = re.compile(r"开放时间[:：]\s*(.+?)(?=\s+(?:景区介绍|地址|百度地图查看路线|乘车路线|门票价格|联系方式)|$)")
CITYHUI_ADDRESS_RE = re.compile(
    r"地址[:：]\s*(.+?)(?=\s+(?:电话|参考价格|营业时间|提示信息|综合评分|口碑评分|人气指数)[:：]|$)"
)
CITYHUI_OPEN_TIME_RE = re.compile(
    r"营业时间[:：]\s*(.+?)(?=\s+(?:提示信息|综合评分|口碑评分|人气指数)[:：]?|$)"
)
CITYHUI_AVG_COST_RE = re.compile(r"参考价格[:：]\s*([¥￥]?\s*\d+(?:\.\d+)?)")
CITYHUI_SCORE_RE = re.compile(r"综合评分\s*([0-9]+(?:\.\d+)?)")


class SportCrawler:
    def __init__(self, client: HttpClient | None = None) -> None:
        self.client = client or HttpClient()

    def discover_links(self) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        seen: set[str] = set()
        cityhui_links = self._discover_cityhui_links(seen)
        links.extend(cityhui_links)
        if len(cityhui_links) < CITYHUI_FALLBACK_THRESHOLD:
            links.extend(self._discover_visitbeijing_links(seen))
        save_links("sport_links.json", [as_row(item) for item in links])
        return links

    def _discover_visitbeijing_links(self, seen: set[str]) -> list[LinkRecord]:
        first_html = self.client.get(LIST_URL)
        max_page = self._extract_max_page(first_html)
        links: list[LinkRecord] = []

        for page in range(1, max_page + 1):
            list_url = LIST_URL if page == 1 else f"{LIST_URL}&page={page}"
            html = first_html if page == 1 else self.client.get(list_url, referer=LIST_URL)
            soup = BeautifulSoup(html, "lxml")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                full_url = urljoin(list_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc and "visitbeijing.com.cn" not in parsed.netloc:
                    continue
                if not DETAIL_PATH_RE.search(parsed.path):
                    continue
                if full_url in seen:
                    continue
                title = first_non_null(
                    [
                        anchor.get_text(" ", strip=True),
                        anchor.get("title"),
                        anchor.get("aria-label"),
                    ]
                )
                if not self._looks_like_sport(title):
                    continue
                if any(keyword in title for keyword in EXCLUDED_KEYWORDS) and not any(
                    keyword in title for keyword in VENUE_TYPE_KEYWORDS
                ):
                    continue
                seen.add(full_url)
                links.append(
                    LinkRecord(
                        detail_url=full_url,
                        list_url=list_url,
                        page=page,
                        source_site=SOURCE_SITE,
                        name=title if title != "\\N" else "",
                    )
                )

        return links
    
    def _discover_cityhui_links(self, seen: set[str]) -> list[LinkRecord]:
        links: list[LinkRecord] = []

        for source in CITYHUI_CATEGORY_SOURCES:
            list_url = source["list_url"]
            html = self.client.get(list_url)
            soup = BeautifulSoup(html, "lxml")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                full_url = urljoin(list_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc and "cityhui.com" not in parsed.netloc:
                    continue
                if not CITYHUI_DETAIL_PATH_RE.search(parsed.path):
                    continue
                if full_url in seen:
                    continue
                seen.add(full_url)
                name = first_non_null(
                    [
                        anchor.get_text(" ", strip=True),
                        anchor.get("title"),
                    ]
                )
                links.append(
                    LinkRecord(
                        detail_url=full_url,
                        list_url=list_url,
                        page=1,
                        source_site=CITYHUI_SOURCE_SITE,
                        name=name if name != NULL else "",
                        tag=source["venue_type"],
                    )
                )

        return links

    def parse_detail(self, html: str, link: LinkRecord) -> SportRecord:
        if "cityhui.com" in urlparse(link.detail_url).netloc:
            return self._parse_cityhui_detail(html, link)
        return self._parse_visitbeijing_detail(html, link)

    def _parse_visitbeijing_detail(self, html: str, link: LinkRecord) -> SportRecord:
        soup = BeautifulSoup(html, "lxml")
        text_lines = [clean_text(text) for text in soup.stripped_strings]
        joined = " ".join(text for text in text_lines if text != "\\N")

        name = first_non_null(
            [
                soup.select_one(".infos h3.title").get_text(" ", strip=True)
                if soup.select_one(".infos h3.title")
                else None,
                soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
                link.name,
                soup.title.get_text(" ", strip=True) if soup.title else None,
            ]
        )
        address = self._search(joined, ADDRESS_RE)
        open_time = self._search(joined, OPEN_TIME_RE)
        venue_type = self._extract_venue_type(soup, name)
        region = extract_region(address)

        return SportRecord(
            name=name,
            venue_type=venue_type,
            region=region,
            address=address,
            score="\\N",
            comment_count="\\N",
            avg_cost="\\N",
            open_time=open_time,
            source_url=link.detail_url,
            source_site=SOURCE_SITE,
            crawl_time=now_iso(),
        )

    def _parse_cityhui_detail(self, html: str, link: LinkRecord) -> SportRecord:
        soup = BeautifulSoup(html, "lxml")
        text_lines = [clean_text(text) for text in soup.stripped_strings]
        joined = " ".join(text for text in text_lines if text != NULL)

        name = self._extract_cityhui_name(soup, link)
        address = self._search_text(joined, CITYHUI_ADDRESS_RE)
        open_time = self._search_text(joined, CITYHUI_OPEN_TIME_RE)
        avg_cost = extract_number(self._search_text(joined, CITYHUI_AVG_COST_RE))
        score = self._search_text(joined, CITYHUI_SCORE_RE)
        region = extract_region(address)

        return SportRecord(
            name=name,
            venue_type=link.tag or self._extract_venue_type(soup, name),
            region=region,
            address=address,
            score=score,
            comment_count=NULL,
            avg_cost=avg_cost,
            open_time=open_time,
            source_url=link.detail_url,
            source_site=CITYHUI_SOURCE_SITE,
            crawl_time=now_iso(),
        )

    def crawl(self, sample: int | None = None) -> list[SportRecord]:
        links = self.discover_links()
        if sample is not None:
            links = links[:sample]
        records: list[SportRecord] = []
        for link in links:
            html = self.client.get(link.detail_url, referer=link.list_url)
            records.append(self.parse_detail(html, link))
        records = self._deduplicate_records(records)
        write_csv("sport", [as_row(item) for item in records], "sport_raw.csv")
        return records

    def _extract_max_page(self, html: str) -> int:
        soup = BeautifulSoup(html, "lxml")
        values = [int(text) for text in soup.stripped_strings if PAGER_RE.fullmatch(text)]
        return max(values) if values else 1

    def _looks_like_sport(self, value: str) -> bool:
        if not value or value == "\\N":
            return False
        return any(keyword in value for keyword in SPORT_KEYWORDS)

    def _extract_venue_type(self, soup: BeautifulSoup, name: str) -> str:
        candidates: list[str] = []
        for node in soup.select(".infos .tag span, .infos .tip span, .infos .type a, .infos .type span"):
            text = clean_text(node.get_text(" ", strip=True))
            if text == "\\N" or text.startswith("景区等级"):
                continue
            if any(keyword in text for keyword in VENUE_TYPE_KEYWORDS) and text not in candidates:
                candidates.append(text)
        if candidates:
            return candidates[0]
        for keyword in VENUE_TYPE_KEYWORDS:
            if keyword in name:
                return keyword
        return "\\N"

    def _search(self, text: str, pattern: re.Pattern[str]) -> str:
        match = pattern.search(text)
        if not match:
            return "\\N"
        return clean_text(match.group(1))

    def _search_text(self, text: str, pattern: re.Pattern[str]) -> str:
        match = pattern.search(text)
        if not match:
            return NULL
        return clean_text(match.group(1))

    def _extract_cityhui_name(self, soup: BeautifulSoup, link: LinkRecord) -> str:
        title = first_non_null(
            [
                soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
                link.name,
                soup.title.get_text(" ", strip=True) if soup.title else None,
            ]
        )
        if title == NULL:
            return title
        title = re.sub(r"^【", "", title)
        title = re.sub(r"】怎么样,地址,电话,价格,点评-北京.+?-城市惠$", "", title)
        title = re.sub(r"\s+", " ", title).strip(" 】")
        return clean_text(title)

    def _deduplicate_records(self, records: list[SportRecord]) -> list[SportRecord]:
        deduplicated: dict[str, SportRecord] = {}
        for record in records:
            key = clean_text(record.name)
            current = deduplicated.get(key)
            if current is None or self._completeness(record) > self._completeness(current):
                deduplicated[key] = record
        return list(deduplicated.values())

    def _completeness(self, record: SportRecord) -> int:
        return sum(
            value != NULL
            for value in (
                record.name,
                record.venue_type,
                record.region,
                record.address,
                record.score,
                record.avg_cost,
                record.open_time,
            )
        )


def run(sample: int | None = None) -> list[SportRecord]:
    return SportCrawler().crawl(sample=sample)
