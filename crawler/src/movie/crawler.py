from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from common.constants import NULL
from common.http import HttpClient
from common.storage import now_iso, write_csv
from common.text import clean_text

SOURCE_SITE = "豆瓣电影"
LIST_URL = "https://movie.douban.com/top250"
PAGE_SIZE = 25
TOTAL_COUNT_RE = re.compile(r"共\s*(\d+)\s*条")
DIRECTOR_RE = re.compile(r"导演:\s*(.*?)(?:\s+主演:|$)")
ACTORS_RE = re.compile(r"主演:\s*(.*)$")


@dataclass(frozen=True)
class MovieRecord:
    name: str
    score: str
    category: str
    country_region: str
    director: str
    actors: str
    intro: str
    source_url: str
    source_site: str
    crawl_time: str


class MovieCrawler:
    def __init__(self, client: HttpClient | None = None) -> None:
        self.client = client or HttpClient()

    def crawl(self, sample: int | None = None) -> list[MovieRecord]:
        first_html = self.client.get(LIST_URL)
        total_count = self._extract_total_count(first_html)
        records = self._parse_list_page(first_html)

        for start in range(PAGE_SIZE, total_count, PAGE_SIZE):
            if sample is not None and len(records) >= sample:
                break
            page_url = f"{LIST_URL}?start={start}&filter="
            html = self.client.get(page_url, referer=LIST_URL)
            records.extend(self._parse_list_page(html))

        if sample is not None:
            records = records[:sample]
        write_csv("movie", [record.__dict__ for record in records], "movie_raw.csv")
        return records

    def _parse_list_page(self, html: str) -> list[MovieRecord]:
        soup = BeautifulSoup(html, "lxml")
        records: list[MovieRecord] = []

        for item in soup.select(".item"):
            title_node = item.select_one(".hd .title") or item.select_one(".title")
            detail_link = item.select_one(".pic a[href], .hd a[href], a[href]")
            meta_node = item.select_one(".bd p")
            score_node = item.select_one(".rating_num")

            if not title_node or not detail_link:
                continue

            meta_lines = []
            if meta_node:
                meta_lines = [clean_text(text) for text in meta_node.stripped_strings]

            director, actors = self._extract_credits(meta_lines[0] if meta_lines else NULL)
            country_region, category = self._extract_summary(meta_lines[1] if len(meta_lines) > 1 else NULL)
            intro_node = item.select_one(".inq")

            records.append(
                MovieRecord(
                    name=clean_text(title_node.get_text(" ", strip=True)),
                    score=clean_text(score_node.get_text(" ", strip=True) if score_node else None),
                    category=category,
                    country_region=country_region,
                    director=director,
                    actors=actors,
                    intro=clean_text(intro_node.get_text(" ", strip=True) if intro_node else None),
                    source_url=urljoin(LIST_URL, detail_link["href"].strip()),
                    source_site=SOURCE_SITE,
                    crawl_time=now_iso(),
                )
            )

        return records

    def _extract_total_count(self, html: str) -> int:
        soup = BeautifulSoup(html, "lxml")
        text = clean_text(soup.get_text(" ", strip=True))
        match = TOTAL_COUNT_RE.search(text)
        if match:
            return int(match.group(1))

        starts = [0]
        for anchor in soup.select(".paginator a[href]"):
            parsed = urlparse(anchor["href"])
            value = parse_qs(parsed.query).get("start")
            if not value:
                continue
            try:
                starts.append(int(value[0]))
            except ValueError:
                continue
        return max(starts) + PAGE_SIZE

    def _extract_credits(self, value: str) -> tuple[str, str]:
        credits = clean_text(value)
        if credits == NULL:
            return NULL, NULL
        director_match = DIRECTOR_RE.search(credits)
        actors_match = ACTORS_RE.search(credits)
        director = clean_text(director_match.group(1) if director_match else None)
        actors = clean_text(actors_match.group(1) if actors_match else None)
        return director, actors

    def _extract_summary(self, value: str) -> tuple[str, str]:
        summary = clean_text(value)
        if summary == NULL:
            return NULL, NULL
        parts = [clean_text(part) for part in summary.split("/") if clean_text(part) != NULL]
        if len(parts) >= 3:
            return " / ".join(parts[1:-1]), parts[-1]
        if len(parts) == 2:
            return parts[1], NULL
        return NULL, NULL


def run(sample: int | None = None) -> list[MovieRecord]:
    return MovieCrawler().crawl(sample=sample)
