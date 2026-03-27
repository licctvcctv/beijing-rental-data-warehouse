from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LinkRecord:
    detail_url: str
    list_url: str
    page: int
    source_site: str
    name: str = ""
    xzq: str = ""
    sq: str = ""


@dataclass(frozen=True)
class RentalRecord:
    fy_id: str
    fy_title: str
    fy_type: str
    fy_status: str
    platform: str
    xzq: str
    sq: str
    jd: str
    wd: str
    month_zj: str
    jzmj: str
    is_dt: str
    zx_qk: str
