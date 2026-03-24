from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from common.http import HttpClient
from common.models import LinkRecord, ShowRecord
from common.storage import as_row, now_iso, save_links, write_csv
from common.text import clean_text, extract_number, first_non_null

LIST_URL = "https://www.dahepiao.com/yanchupiaowu1_000_3"
DF_BEIJING_LIST_URL = "https://www.df962388.com/yanchu/beijing/"
SOURCE_SITE = "大河票务"
DF_SOURCE_SITE = "东方演出网"
DETAIL_PATH_RE = re.compile(r"/yanchupiaowu1/.+\.html$")
TIME_RE = re.compile(r"(?:\d{4}[-.]\d{2}[-.]\d{2}(?:-\d{2})?(?:\s*(?:周. |星期.)?\s*\d{2}:\d{2})?)")
TOTAL_PAGES_RE = re.compile(r"共\s*(\d+)\s*页\s*(\d+)\s*条")
DF_TOTAL_PAGES_RE = re.compile(r"共\s*(\d+)\s*页")


class ShowCrawler:
    def __init__(
        self,
        client: HttpClient | None = None,
        df_city_page_limit: int = 20,
    ) -> None:
        self.client = client or HttpClient()
        self.df_city_page_limit = df_city_page_limit

    def discover_links(self) -> list[LinkRecord]:
        first_html = self.client.get(LIST_URL)
        total_pages, _ = self._extract_pagination(first_html)
        links: list[LinkRecord] = []
        seen: set[str] = set()

        for page in range(1, total_pages + 1):
            list_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"
            html = first_html if page == 1 else self.client.get(list_url, referer=LIST_URL)
            soup = BeautifulSoup(html, "lxml")
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                parsed = urlparse(href)
                if parsed.netloc and "dahepiao.com" not in parsed.netloc:
                    continue
                full_url = urljoin(list_url, href)
                if not DETAIL_PATH_RE.search(urlparse(full_url).path):
                    continue
                text = clean_text(anchor.get_text(" ", strip=True))
                if "立即预订" not in text and "【北京】" not in text:
                    continue
                if full_url in seen:
                    continue
                seen.add(full_url)
                links.append(
                    LinkRecord(
                        detail_url=full_url,
                        list_url=list_url,
                        page=page,
                        source_site=SOURCE_SITE,
                        name=text if text != "立即预订" else "",
                    )
                )
        save_links("show_links.json", [as_row(item) for item in links])
        return links

    def parse_detail(self, html: str, link: LinkRecord) -> ShowRecord:
        soup = BeautifulSoup(html, "lxml")
        name = first_non_null(
            [
                soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None,
                soup.title.get_text(" ", strip=True) if soup.title else None,
            ]
        )
        text_lines = [clean_text(text) for text in soup.stripped_strings]
        show_time = self._extract_show_time(text_lines)
        venue = self._extract_venue(text_lines)
        price_range = self._extract_price_range(text_lines)
        status = self._extract_status(text_lines)
        attention = self._extract_attention(text_lines)
        if link.name and name == "\\N":
            name = link.name
        return ShowRecord(
            name=name,
            show_time=show_time,
            venue=venue,
            region="北京",
            price_range=price_range,
            status=status,
            attention=attention,
            source_url=link.detail_url,
            source_site=SOURCE_SITE,
            crawl_time=now_iso(),
        )

    def crawl(self, sample: int | None = None) -> list[ShowRecord]:
        links = self.discover_links()
        if sample is not None:
            links = links[:sample]
        records: list[ShowRecord] = []
        for link in links:
            html = self.client.get(link.detail_url, referer=link.list_url)
            records.append(self.parse_detail(html, link))
        records.extend(self._crawl_df_beijing_city())
        records = self._dedupe_records(records)
        if sample is not None:
            records = records[:sample]
        write_csv("show", [as_row(item) for item in records], "show_raw.csv")
        return records

    def _crawl_df_beijing_city(self) -> list[ShowRecord]:
        first_html = self.client.get(DF_BEIJING_LIST_URL)
        total_pages = min(self._extract_df_total_pages(first_html), self.df_city_page_limit)
        records: list[ShowRecord] = []
        seen: set[str] = set()

        for page in range(1, total_pages + 1):
            list_url = (
                DF_BEIJING_LIST_URL
                if page == 1
                else f"https://www.df962388.com/yanchu/beijing/index_{page}.html"
            )
            html = first_html if page == 1 else self.client.get(list_url, referer=DF_BEIJING_LIST_URL)
            records.extend(self._parse_df_list_page(html, list_url, seen))

        return records

    def _parse_df_list_page(self, html: str, list_url: str, seen: set[str]) -> list[ShowRecord]:
        soup = BeautifulSoup(html, "lxml")
        records: list[ShowRecord] = []

        for item in soup.find_all("li"):
            title_link = item.select_one("h3.item-title a[href]")
            excerpt_lines = [
                clean_text(node.get_text(" ", strip=True))
                for node in item.select("div.item-excerpt p")
            ]
            if title_link is None or len(excerpt_lines) < 3:
                continue
            detail_url = urljoin(list_url, title_link["href"].strip())
            if detail_url in seen:
                continue
            seen.add(detail_url)
            records.append(
                ShowRecord(
                    name=clean_text(title_link.get_text(" ", strip=True)),
                    show_time=clean_text(excerpt_lines[0].replace("演出时间：", "", 1)),
                    venue=clean_text(excerpt_lines[1].replace("地点：", "", 1)),
                    region="北京",
                    price_range=clean_text(excerpt_lines[2].replace("门票价格：", "", 1)).replace("、", ","),
                    status="\\N",
                    attention="\\N",
                    source_url=detail_url,
                    source_site=DF_SOURCE_SITE,
                    crawl_time=now_iso(),
                )
            )

        return records

    def _extract_show_time(self, text_lines: list[str]) -> str:
        for text in text_lines:
            match = TIME_RE.search(text)
            if match:
                return clean_text(match.group(0))
        for idx, text in enumerate(text_lines):
            if text in {"时间", "演出时间"} and idx + 1 < len(text_lines):
                return clean_text(text_lines[idx + 1])
        return "\\N"

    def _extract_venue(self, text_lines: list[str]) -> str:
        for idx, text in enumerate(text_lines):
            if text in {"场馆", "演出场馆", "演出地点"} and idx + 1 < len(text_lines):
                return clean_text(text_lines[idx + 1])
        for text in text_lines:
            if "剧场" in text or "音乐堂" in text or "体育馆" in text or "艺术空间" in text:
                return text
        return "\\N"

    def _extract_price_range(self, text_lines: list[str]) -> str:
        candidates: list[str] = []
        for text in text_lines:
            if re.fullmatch(r"\d+(?:,\d+)+", text):
                return text
            if "票价" in text or "价格" in text:
                digits = re.findall(r"\d+", text)
                if len(digits) >= 2:
                    candidates.append(",".join(digits))
        return candidates[0] if candidates else "\\N"

    def _extract_status(self, text_lines: list[str]) -> str:
        for text in text_lines:
            if any(keyword in text for keyword in ("项目结束", "项目已结束", "售票中", "预售", "待定", "缺货")):
                return text.replace("项目结束", "项目已结束")
        return "\\N"

    def _extract_attention(self, text_lines: list[str]) -> str:
        for idx, text in enumerate(text_lines):
            if text in {"关注度", "热度", "关注"} and idx + 1 < len(text_lines):
                return extract_number(text_lines[idx + 1])
            if re.fullmatch(r"\d+(?:\.\d+)?", text):
                if idx > 0 and any(flag in text_lines[idx - 1] for flag in ("关注", "热度")):
                    return text
        return "\\N"

    def _extract_pagination(self, html: str) -> tuple[int, int]:
        text = clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
        match = TOTAL_PAGES_RE.search(text)
        if not match:
            return 1, 0
        return int(match.group(1)), int(match.group(2))

    def _extract_df_total_pages(self, html: str) -> int:
        text = clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
        match = DF_TOTAL_PAGES_RE.search(text)
        if not match:
            return 1
        return int(match.group(1))

    def _dedupe_records(self, records: list[ShowRecord]) -> list[ShowRecord]:
        deduped: dict[tuple[str, str, str], ShowRecord] = {}
        for record in records:
            key = self._record_key(record)
            current = deduped.get(key)
            if current is None:
                deduped[key] = record
                continue
            deduped[key] = self._merge_records(current, record)
        return list(deduped.values())

    def _merge_records(self, left: ShowRecord, right: ShowRecord) -> ShowRecord:
        winner, loser = (
            (left, right)
            if self._record_priority(left) >= self._record_priority(right)
            else (right, left)
        )
        return ShowRecord(
            name=winner.name if clean_text(winner.name) != "\\N" else loser.name,
            show_time=winner.show_time if clean_text(winner.show_time) != "\\N" else loser.show_time,
            venue=winner.venue if clean_text(winner.venue) != "\\N" else loser.venue,
            region=winner.region if clean_text(winner.region) != "\\N" else loser.region,
            price_range=winner.price_range if clean_text(winner.price_range) != "\\N" else loser.price_range,
            status=winner.status if clean_text(winner.status) != "\\N" else loser.status,
            attention=winner.attention if clean_text(winner.attention) != "\\N" else loser.attention,
            source_url=winner.source_url,
            source_site=winner.source_site,
            crawl_time=winner.crawl_time,
        )

    def _record_priority(self, record: ShowRecord) -> tuple[int, int, float]:
        explicit_status = 1 if clean_text(record.status) != "\\N" else 0
        price_detail = self._price_detail_score(record.price_range)
        attention = self._attention_score(record.attention)
        return (explicit_status, price_detail, attention)

    def _record_key(self, record: ShowRecord) -> tuple[str, str, str]:
        time_key = self._normalize_time(record.show_time)
        venue_key = self._normalize_text(record.venue)
        if time_key != "\\N" and venue_key != "\\N":
            return ("", time_key, venue_key)
        return (self._normalize_text(record.name), time_key, venue_key)

    def _normalize_time(self, value: str) -> str:
        normalized = self._normalize_text(value)
        return normalized.replace(".", "-").replace("/", "-")

    def _normalize_text(self, value: str) -> str:
        normalized = clean_text(value)
        if normalized == "\\N":
            return normalized
        normalized = re.sub(r"\s+", "", normalized)
        return normalized.lower()

    def _price_detail_score(self, value: str) -> int:
        return len(re.findall(r"\d+(?:\.\d+)?", clean_text(value)))

    def _attention_score(self, value: str) -> float:
        extracted = extract_number(value)
        if extracted == "\\N":
            return 0.0
        return float(extracted)


def run(sample: int | None = None) -> list[ShowRecord]:
    return ShowCrawler().crawl(sample=sample)
