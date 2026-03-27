from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
INTERIM_LINKS_DIR = DATA_DIR / "interim" / "links"
EXPORT_DIR = DATA_DIR / "export"
LOG_DIR = PROJECT_ROOT / "logs"

NULL = r"\N"
DEFAULT_TIMEOUT = 20
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

SCHEMAS = {
    "rental": [
        "fy_id",
        "fy_title",
        "fy_type",
        "fy_status",
        "platform",
        "xzq",
        "sq",
        "jd",
        "wd",
        "month_zj",
        "jzmj",
        "is_dt",
        "zx_qk",
    ],
}
