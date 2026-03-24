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
    "scenic": [
        "name",
        "level",
        "region",
        "address",
        "price",
        "open_time",
        "visit_duration",
        "best_visit_time",
        "source_url",
        "source_site",
        "crawl_time",
    ],
    "show": [
        "name",
        "show_time",
        "venue",
        "region",
        "price_range",
        "status",
        "attention",
        "source_url",
        "source_site",
        "crawl_time",
    ],
    "ktv": [
        "name",
        "region",
        "address",
        "avg_cost",
        "service_score",
        "env_score",
        "overall_score",
        "popularity",
        "business_hours",
        "source_url",
        "source_site",
        "crawl_time",
    ],
    "movie": [
        "name",
        "score",
        "category",
        "country_region",
        "director",
        "actors",
        "intro",
        "source_url",
        "source_site",
        "crawl_time",
    ],
    "sport": [
        "name",
        "venue_type",
        "region",
        "address",
        "score",
        "comment_count",
        "avg_cost",
        "open_time",
        "source_url",
        "source_site",
        "crawl_time",
    ],
}
