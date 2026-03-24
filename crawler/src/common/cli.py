from __future__ import annotations

import argparse
from typing import Sequence


CATEGORIES = ("scenic", "show", "ktv", "movie", "sport")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="北京娱乐方式原始数据采集工具")
    parser.add_argument("category", choices=CATEGORIES)
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="限制导出条数，不传则抓取全部可发现记录",
    )
    parser.add_argument(
        "--skip-links",
        action="store_true",
        help="跳过链接发现阶段，直接使用已有中间链接文件",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)
