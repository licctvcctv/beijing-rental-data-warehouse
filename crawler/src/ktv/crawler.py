from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from common.http import HttpClient
from common.models import KtvRecord, LinkRecord
from common.storage import as_row, now_iso, save_links, write_csv
from common.text import clean_text, extract_number, extract_region, first_non_null

DISTRICT_KEYWORDS = (
    "东城区",
    "西城区",
    "朝阳区",
    "海淀区",
    "丰台区",
    "石景山区",
    "门头沟区",
    "房山区",
    "通州区",
    "顺义区",
    "昌平区",
    "大兴区",
    "怀柔区",
    "平谷区",
    "密云区",
    "延庆区",
)

LIST_URL = "https://www.cityhui.com/beijing/ktv/"
BUSINESS_LIST_URL = "https://www.cityhui.com/beijing/shangwuktv/"
SOURCE_SITE = "城市惠"
DETAIL_PATH_RE = re.compile(r"/shop/\d+\.html$")
LIST_SOURCES = (
    {"list_url": LIST_URL, "tag": "KTV"},
    {"list_url": BUSINESS_LIST_URL, "tag": "商务KTV"},
)
LABEL_VALUE_RE = {
    "avg_cost": re.compile(r"(?:参考价格|消费价格)[:：]\s*([¥￥]?\s*\d+(?:\.\d+)?)"),
    "business_hours": re.compile(r"营业时间[:：]\s*(.+?)(?=\s+(?:提示信息|综合评分|口碑评分|人气指数)[:：]?|$)"),
    "overall_score": re.compile(r"综合评分\s*([0-9]+(?:\.\d+)?)"),
    "service_score": re.compile(r"口碑评分\s*([0-9]+(?:\.\d+)?)"),
    "env_score": re.compile(r"环境评分\s*([0-9]+(?:\.\d+)?)"),
    "popularity": re.compile(r"人气指数\s*([0-9]+(?:\.\d+)?)"),
}
ADDRESS_RE = re.compile(
    r"地址[:：]\s*(.+?)(?=\s+(?:预订电话|电话|参考价格|消费价格|营业时间|所在区域|提示信息|综合评分|口碑评分|环境评分|人气指数)[:：]?|$)"
)


class KtvCrawler:
    def __init__(self, client: HttpClient | None = None) -> None:
        self.client = client or HttpClient()

    def discover_links(self) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        seen: set[str] = set()

        for source in LIST_SOURCES:
            list_url = source["list_url"]
            html = self.client.get(list_url)
            soup = BeautifulSoup(html, "lxml")

            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                full_url = urljoin(list_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc and "cityhui.com" not in parsed.netloc:
                    continue
                if not DETAIL_PATH_RE.search(parsed.path):
                    continue
                if full_url in seen:
                    continue
                seen.add(full_url)
                name = clean_text(anchor.get_text(" ", strip=True))
                if name == "\\N":
                    title = anchor.get("title")
                    name = clean_text(title)
                links.append(
                    LinkRecord(
                        detail_url=full_url,
                        list_url=list_url,
                        page=1,
                        source_site=SOURCE_SITE,
                        name=name if name != "\\N" else "",
                        tag=source["tag"],
                    )
                )

        save_links("ktv_links.json", [as_row(item) for item in links])
        return links

    def parse_detail(self, html: str, link: LinkRecord) -> KtvRecord:
        soup = BeautifulSoup(html, "lxml")
        text_lines = [clean_text(text) for text in soup.stripped_strings]
        joined = " ".join(text for text in text_lines if text != "\\N")

        name = self._extract_name(soup, link)
        address = self._extract_address(joined)
        avg_cost = extract_number(self._search(joined, "avg_cost"))
        service_score = self._search(joined, "service_score")
        env_score = self._search(joined, "env_score")
        overall_score = self._search(joined, "overall_score")
        popularity = extract_number(self._search(joined, "popularity"))
        business_hours = self._search(joined, "business_hours")
        region = self._extract_region(address)

        return KtvRecord(
            name=name,
            region=region,
            address=address,
            avg_cost=avg_cost,
            service_score=service_score,
            env_score=env_score,
            overall_score=overall_score,
            popularity=popularity,
            business_hours=business_hours,
            source_url=link.detail_url,
            source_site=SOURCE_SITE,
            crawl_time=now_iso(),
        )

    def crawl(self, sample: int | None = None) -> list[KtvRecord]:
        links = self.discover_links()
        if sample is not None:
            links = links[:sample]
        records: list[KtvRecord] = []
        for link in links:
            html = self.client.get(link.detail_url, referer=link.list_url)
            records.append(self.parse_detail(html, link))
        records = self._dedupe_records(records)
        write_csv("ktv", [as_row(item) for item in records], "ktv_raw.csv")
        return records

    def _search(self, text: str, key: str) -> str:
        match = LABEL_VALUE_RE[key].search(text)
        if not match:
            return "\\N"
        return clean_text(match.group(1))

    def _extract_name(self, soup: BeautifulSoup, link: LinkRecord) -> str:
        title = first_non_null(
            [
                soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
                soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else None,
                link.name,
                soup.title.get_text(" ", strip=True) if soup.title else None,
            ]
        )
        if title == "\\N":
            return title
        title = re.sub(r"^【", "", title)
        title = re.sub(r"】怎么样,地址,电话,价格,点评-北京KTV-城市惠$", "", title)
        title = re.sub(r"-北京KTV-城市惠$", "", title)
        title = re.sub(r"\s+", " ", title).strip(" 】")
        return clean_text(title)

    def _extract_address(self, text: str) -> str:
        candidates = [clean_text(match.group(1)) for match in ADDRESS_RE.finditer(text)]
        candidates = [candidate for candidate in candidates if candidate != "\\N"]
        if not candidates:
            return "\\N"
        for candidate in reversed(candidates):
            if "北京市" in candidate or extract_region(candidate) in DISTRICT_KEYWORDS:
                return re.sub(r"\s*[（(].*?[）)]\s*$", "", candidate).strip()
        return re.sub(r"\s*[（(].*?[）)]\s*$", "", candidates[-1]).strip()

    def _extract_region(self, address: str) -> str:
        region = extract_region(address)
        if region in DISTRICT_KEYWORDS:
            return region
        return "\\N"

    def _dedupe_records(self, records: list[KtvRecord]) -> list[KtvRecord]:
        deduped: dict[str, KtvRecord] = {}
        for record in records:
            key = self._record_key(record)
            current = deduped.get(key)
            if current is None or self._record_score(record) > self._record_score(current):
                deduped[key] = record
        return list(deduped.values())

    def _record_key(self, record: KtvRecord) -> str:
        address = clean_text(record.address)
        if address != "\\N":
            return address
        return re.sub(r"\s+", "", record.name)

    def _record_score(self, record: KtvRecord) -> tuple[int, float, int]:
        completeness = sum(
            1
            for value in (
                record.address,
                record.avg_cost,
                record.business_hours,
                record.overall_score,
                record.service_score,
                record.env_score,
            )
            if clean_text(value) != "\\N"
        )
        popularity = extract_number(record.popularity)
        return (completeness, popularity, len(record.name))


def run(sample: int | None = None) -> list[KtvRecord]:
    return KtvCrawler().crawl(sample=sample)
