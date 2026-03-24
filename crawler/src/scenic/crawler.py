from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from common.http import HttpClient
from common.models import LinkRecord, ScenicRecord
from common.storage import as_row, now_iso, save_links, write_csv
from common.text import clean_text, extract_region, first_non_null

LIST_URL = "https://whlyj.beijing.gov.cn/ggfw/ly/202511/t20251119_4287815.html"
LEISURE_LIST_URL = "https://s.visitbeijing.com.cn/attractions?type=8"
SOURCE_SITE = "北京市文旅局/北京旅游网"
DETAIL_PATH_RE = re.compile(r"/attraction/\d+$")
PAGER_RE = re.compile(r"^\d+$")


class ScenicCrawler:
    def __init__(self, client: HttpClient | None = None) -> None:
        # Scenic source pages are numerous, so use a shorter built-in delay.
        self.client = client or HttpClient(sleep_seconds=0.05, max_retries=2)

    def discover_links(self) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        seen: set[str] = set()
        links.extend(self._discover_leisure_links(seen))
        if len(links) < 300:
            links.extend(self._discover_official_links(seen))
        save_links("scenic_links.json", [as_row(item) for item in links])
        return links

    def _discover_official_links(self, seen: set[str]) -> list[LinkRecord]:
        html = self.client.get(LIST_URL)
        soup = BeautifulSoup(html, "lxml")
        links: list[LinkRecord] = []
        for row in soup.select("table tr"):
            columns = row.find_all("td")
            if len(columns) < 5:
                continue
            anchor = columns[4].find("a", href=True)
            if not anchor:
                continue
            detail_url = urljoin(LIST_URL, anchor["href"].strip())
            parsed = urlparse(detail_url)
            if "visitbeijing.com.cn" not in parsed.netloc:
                continue
            if detail_url in seen:
                continue
            seen.add(detail_url)
            links.append(
                LinkRecord(
                    detail_url=detail_url,
                    list_url=LIST_URL,
                    page=1,
                    source_site=SOURCE_SITE,
                    name=clean_text(columns[1].get_text(" ", strip=True)),
                    level=clean_text(columns[2].get_text(" ", strip=True)),
                    region=clean_text(columns[3].get_text(" ", strip=True)),
                )
            )
        return links

    def _discover_leisure_links(self, seen: set[str]) -> list[LinkRecord]:
        first_html = self.client.get(LEISURE_LIST_URL)
        max_page = self._extract_max_page(first_html)
        links: list[LinkRecord] = []

        for page in range(1, max_page + 1):
            list_url = LEISURE_LIST_URL if page == 1 else f"{LEISURE_LIST_URL}&page={page}"
            html = first_html if page == 1 else self.client.get(list_url, referer=LEISURE_LIST_URL)
            soup = BeautifulSoup(html, "lxml")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                detail_url = urljoin(list_url, href)
                parsed = urlparse(detail_url)
                if parsed.netloc and "visitbeijing.com.cn" not in parsed.netloc:
                    continue
                if not DETAIL_PATH_RE.search(parsed.path):
                    continue
                if detail_url in seen:
                    continue
                title = clean_text(anchor.get_text(" ", strip=True))
                if title == "\\N":
                    continue
                seen.add(detail_url)
                links.append(
                    LinkRecord(
                        detail_url=detail_url,
                        list_url=list_url,
                        page=page,
                        source_site=SOURCE_SITE,
                        name=title,
                    )
                )
        return links

    def parse_detail(self, html: str, link: LinkRecord) -> ScenicRecord:
        soup = BeautifulSoup(html, "lxml")
        title = first_non_null(
            [
                soup.select_one(".infos h3.title").get_text(" ", strip=True)
                if soup.select_one(".infos h3.title")
                else None,
                soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
                soup.title.get_text(" ", strip=True) if soup.title else None,
            ]
        )
        info_map = self._extract_info_map(soup)
        address = first_non_null([info_map.get("地址"), info_map.get("景区地址")])
        price = first_non_null([info_map.get("门票价格"), info_map.get("门票")])
        open_time = first_non_null([info_map.get("开放时间"), info_map.get("营业时间")])
        visit_duration = first_non_null([info_map.get("推荐游览时长"), info_map.get("游览时长")])
        best_visit_time = first_non_null([info_map.get("最佳游览时间"), info_map.get("最佳旅游时间")])
        region = extract_region(info_map.get("区域"), address)
        level = self._normalize_level(first_non_null([info_map.get("景区等级"), link.level]))
        return ScenicRecord(
            name=title if title != "\\N" else link.name,
            level=level,
            region=region if region != "\\N" else (link.region or "\\N"),
            address=address,
            price=price,
            open_time=open_time,
            visit_duration=visit_duration,
            best_visit_time=best_visit_time,
            source_url=link.detail_url,
            source_site=SOURCE_SITE,
            crawl_time=now_iso(),
        )

    def crawl(self, sample: int | None = None) -> list[ScenicRecord]:
        links = self.discover_links()
        if sample is not None:
            links = links[:sample]
        records: list[ScenicRecord] = []
        for link in links:
            html = self.client.get(link.detail_url, referer=link.list_url)
            record = self.parse_detail(html, link)
            records.append(record)
        write_csv("scenic", [as_row(item) for item in records], "scenic_raw.csv")
        return records

    def _extract_max_page(self, html: str) -> int:
        soup = BeautifulSoup(html, "lxml")
        values = [int(text) for text in soup.stripped_strings if PAGER_RE.fullmatch(text)]
        return max(values) if values else 1

    def _extract_info_map(self, soup: BeautifulSoup) -> dict[str, str]:
        info_map: dict[str, str] = {}

        level_node = soup.select_one(".tip span:last-child i")
        if level_node:
            info_map["景区等级"] = clean_text(level_node.get_text(" ", strip=True))

        for item in soup.select(".phones li"):
            label_node = item.find("p")
            if not label_node:
                continue
            label = clean_text(label_node.get_text(" ", strip=True)).rstrip(":：")
            value_parts: list[str] = []
            for child in item.find_all(["span", "em", "b", "i"], recursive=True):
                text = clean_text(child.get_text(" ", strip=True))
                if text != "\\N":
                    value_parts.append(text)
            value = clean_text(" ".join(value_parts))
            if label and value != "\\N":
                info_map[label] = value

        address_node = soup.select_one(".map .tit .sp2")
        if address_node:
            info_map["地址"] = clean_text(address_node.get_text(" ", strip=True))

        return info_map

    def _normalize_level(self, value: str) -> str:
        if value == "\\N":
            return value
        if value == "AAAAA":
            return "5A"
        if value == "AAAA":
            return "4A"
        if value == "AAA":
            return "3A"
        if value == "AA":
            return "2A"
        if value == "A":
            return "1A"
        return value


def run(sample: int | None = None) -> list[ScenicRecord]:
    return ScenicCrawler().crawl(sample=sample)
