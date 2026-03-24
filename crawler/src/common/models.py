from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LinkRecord:
    detail_url: str
    list_url: str
    page: int
    source_site: str
    name: str = ""
    level: str = ""
    region: str = ""
    tag: str = ""


@dataclass(frozen=True)
class ScenicRecord:
    name: str
    level: str
    region: str
    address: str
    price: str
    open_time: str
    visit_duration: str
    best_visit_time: str
    source_url: str
    source_site: str
    crawl_time: str


@dataclass(frozen=True)
class ShowRecord:
    name: str
    show_time: str
    venue: str
    region: str
    price_range: str
    status: str
    attention: str
    source_url: str
    source_site: str
    crawl_time: str


@dataclass(frozen=True)
class KtvRecord:
    name: str
    region: str
    address: str
    avg_cost: str
    service_score: str
    env_score: str
    overall_score: str
    popularity: str
    business_hours: str
    source_url: str
    source_site: str
    crawl_time: str


@dataclass(frozen=True)
class SportRecord:
    name: str
    venue_type: str
    region: str
    address: str
    score: str
    comment_count: str
    avg_cost: str
    open_time: str
    source_url: str
    source_site: str
    crawl_time: str
