from __future__ import annotations

import re
from html import unescape
from typing import Iterable

from bs4 import BeautifulSoup

from .constants import NULL

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
REGION_RE = re.compile(r"([\u4e00-\u9fa5]{2,7}(?:区|县|市))")
DISTRICT_RE = re.compile(
    r"(东城区|西城区|朝阳区|海淀区|丰台区|石景山区|门头沟区|房山区|通州区|顺义区|昌平区|大兴区|怀柔区|平谷区|密云区|延庆区)"
)
NUMBER_RE = re.compile(r"(\d+(?:\.\d+)?)")


def clean_text(value: str | None) -> str:
    if value is None:
        return NULL
    text = unescape(value)
    text = TAG_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text or NULL


def soup_text(value: str | None) -> str:
    if value is None:
        return NULL
    text = BeautifulSoup(value, "lxml").get_text(" ", strip=True)
    return clean_text(text)


def first_non_null(values: Iterable[str | None]) -> str:
    for value in values:
        cleaned = clean_text(value)
        if cleaned != NULL:
            return cleaned
    return NULL


def extract_region(*values: str | None) -> str:
    for value in values:
        text = clean_text(value)
        if text == NULL:
            continue
        district_match = DISTRICT_RE.search(text)
        if district_match:
            return district_match.group(1)
        match = REGION_RE.search(text)
        if match:
            return match.group(1)
    return NULL


def extract_number(value: str | None) -> str:
    text = clean_text(value)
    if text == NULL:
        return NULL
    match = NUMBER_RE.search(text)
    return match.group(1) if match else NULL
