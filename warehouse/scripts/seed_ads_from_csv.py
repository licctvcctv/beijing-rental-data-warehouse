#!/usr/bin/env python3
import argparse
import csv
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path


NULL_VALUE = "\\N"
ADS_TABLES = [
    "ads_region_entertainment_count",
    "ads_movie_score_distribution",
    "ads_show_price_top10",
    "ads_show_status_ratio",
    "ads_ktv_region_hotspot",
    "ads_ktv_cost_performance_top5",
    "ads_sport_type_ratio_top5",
    "ads_scenic_free_ratio",
]
REGIONS = [
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
]
PRICE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")
SCENIC_PRICE_SEGMENT_PATTERN = re.compile(r"(?:门票|票价|联票|成人票|通票)[:：]?([^。；;]*)")


def normalize_text(raw):
    if raw is None:
        return NULL_VALUE
    value = (
        str(raw)
        .replace("\u00A0", " ")
        .replace("\r", " ")
        .replace("\n", " ")
        .strip()
    )
    value = re.sub(r"<[^>]*>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value or value.lower() == "null" or value == "暂无":
        return NULL_VALUE
    return value


def is_null_value(value):
    return normalize_text(value) == NULL_VALUE


def extract_region(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return NULL_VALUE
    for region in REGIONS:
        if region in normalized or region.replace("区", "") in normalized:
            return region
    if "北京" in normalized:
        return "北京市"
    return NULL_VALUE


def normalize_region(region, address):
    normalized = extract_region(region)
    if normalized != NULL_VALUE:
        return normalized
    return extract_region(address)


def extract_prices(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return []
    return sorted(float(match.group(1)) for match in PRICE_PATTERN.finditer(normalized.replace(",", " ")))


def extract_scenic_prices(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return []
    values = []
    for match in SCENIC_PRICE_SEGMENT_PATTERN.finditer(normalized):
        values.extend(extract_prices(match.group(1)))
    if values:
        return sorted(values)
    if "免费" in normalized:
        values.append(0.0)
    values.extend(extract_prices(normalized))
    return sorted(values)


def extract_scenic_min_price(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return 0.0
    prices = extract_scenic_prices(normalized)
    if not prices and "免费" in normalized:
        return 0.0
    return prices[0] if prices else 0.0


def extract_scenic_max_price(text):
    prices = extract_scenic_prices(text)
    return prices[-1] if prices else 0.0


def extract_decimal(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return 0.0
    match = PRICE_PATTERN.search(normalized)
    return float(match.group(1)) if match else 0.0


def extract_integer(text):
    normalized = normalize_text(text)
    if normalized == NULL_VALUE:
        return 0
    match = re.search(r"(\d+)", normalized.replace(",", ""))
    return int(match.group(1)) if match else 0


def normalize_show_status(status):
    normalized = normalize_text(status)
    if normalized == NULL_VALUE:
        return "待定"
    normalized = normalized.strip("[]【】")
    if normalized in {"售票中", "预售中", "已结束"}:
        return normalized
    return normalized


def normalize_score_level(score_num):
    if score_num >= 9:
        return "9分及以上"
    if score_num >= 8:
        return "8-9分"
    if score_num >= 7:
        return "7-8分"
    if score_num > 0:
        return "7分以下"
    return "暂无评分"


def normalize_sport_type(venue_type, name):
    normalized = normalize_text(venue_type)
    if normalized != NULL_VALUE:
        return normalized
    name_text = normalize_text(name)
    if "游泳" in name_text:
        return "游泳馆"
    if "滑冰" in name_text:
        return "滑冰馆"
    if "足球" in name_text:
        return "足球场"
    if "篮球" in name_text:
        return "篮球馆"
    return "其他场馆"


def read_csv_rows(export_dir, filename):
    path = Path(export_dir) / filename
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def compute_ads_rows(export_dir):
    scenic_rows = []
    for row in read_csv_rows(export_dir, "scenic_raw.csv"):
        scenic_rows.append(
            {
                "name": normalize_text(row.get("name")),
                "region": normalize_region(row.get("region"), row.get("address")),
                "price_min": round(extract_scenic_min_price(row.get("price")), 2),
                "price_max": round(extract_scenic_max_price(row.get("price")), 2),
            }
        )

    show_rows = []
    for row in read_csv_rows(export_dir, "show_raw.csv"):
        prices = extract_prices(row.get("price_range"))
        show_rows.append(
            {
                "name": normalize_text(row.get("name")),
                "venue": normalize_text(row.get("venue")),
                "region": normalize_region(row.get("region"), row.get("venue")),
                "price_min": round(prices[0], 2) if prices else 0.0,
                "price_max": round(prices[-1], 2) if prices else 0.0,
                "status_std": normalize_show_status(row.get("status")),
                "attention_num": round(extract_decimal(row.get("attention")), 2),
            }
        )

    ktv_rows = []
    for row in read_csv_rows(export_dir, "ktv_raw.csv"):
        avg_cost = round(extract_decimal(row.get("avg_cost")), 2)
        overall_score = round(extract_decimal(row.get("overall_score")), 2)
        ktv_rows.append(
            {
                "name": normalize_text(row.get("name")),
                "region": normalize_region(row.get("region"), row.get("address")),
                "avg_cost": avg_cost,
                "overall_score": overall_score,
                "cost_performance": round(overall_score / avg_cost, 4) if avg_cost else 0.0,
                "popularity_num": extract_integer(row.get("popularity")),
            }
        )

    movie_rows = []
    for row in read_csv_rows(export_dir, "movie_raw.csv"):
        score_num = round(extract_decimal(row.get("score")), 2)
        movie_rows.append(
            {
                "name": normalize_text(row.get("name")),
                "score_num": score_num,
                "score_level": normalize_score_level(score_num),
            }
        )

    sport_rows = []
    for row in read_csv_rows(export_dir, "sport_raw.csv"):
        sport_rows.append(
            {
                "name": normalize_text(row.get("name")),
                "venue_type": normalize_sport_type(row.get("venue_type"), row.get("name")),
                "region": normalize_region(row.get("region"), row.get("address")),
                "score_num": round(extract_decimal(row.get("score")), 2),
            }
        )

    region_counter = Counter()
    for row in scenic_rows:
        if row["region"] in REGIONS:
            region_counter[row["region"]] += 1
    for row in show_rows:
        if row["region"] in REGIONS:
            region_counter[row["region"]] += 1
    for row in ktv_rows:
        if row["region"] in REGIONS:
            region_counter[row["region"]] += 1
    for row in sport_rows:
        if row["region"] in REGIONS:
            region_counter[row["region"]] += 1

    movie_groups = defaultdict(list)
    for row in movie_rows:
        movie_groups[row["score_level"]].append(row["score_num"])

    show_ratio_rows = [row for row in show_rows if row["status_std"] != "待定"]
    if not show_ratio_rows:
        show_ratio_rows = show_rows
    show_status_counter = Counter(row["status_std"] for row in show_ratio_rows)
    show_total = sum(show_status_counter.values()) or 1

    ktv_region_metrics = defaultdict(list)
    for row in ktv_rows:
        ktv_region_metrics[row["region"]].append(row)

    sport_groups = defaultdict(list)
    for row in sport_rows:
        sport_groups[row["venue_type"]].append(row["score_num"])
    sport_total = len(sport_rows) or 1

    scenic_total = len(scenic_rows) or 1
    scenic_free_count = sum(1 for row in scenic_rows if row["price_min"] == 0)
    scenic_paid_count = sum(1 for row in scenic_rows if row["price_min"] > 0)

    datasets = {
        "ads_region_entertainment_count": [
            {"region": region, "entertainment_count": count}
            for region, count in sorted(
                region_counter.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
        "ads_movie_score_distribution": [
            {
                "score_level": score_level,
                "movie_count": len(scores),
                "avg_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
            }
            for score_level, scores in sorted(
                movie_groups.items(),
                key=lambda item: (-len(item[1]), item[0]),
            )
        ],
        "ads_show_price_top10": sorted(
            [
                {
                    "name": row["name"],
                    "venue": row["venue"],
                    "region": row["region"],
                    "price_max": row["price_max"],
                    "price_min": row["price_min"],
                    "status_std": row["status_std"],
                    "attention_num": row["attention_num"],
                }
                for row in show_rows
            ],
            key=lambda item: (-item["price_max"], -item["attention_num"], item["name"]),
        )[:10],
        "ads_show_status_ratio": [
            {
                "status_std": status,
                "show_count": count,
                "status_ratio": round(count / show_total, 4),
            }
            for status, count in sorted(
                show_status_counter.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
        "ads_ktv_region_hotspot": [
            {
                "region": region,
                "ktv_count": len(rows),
                "avg_cost": round(sum(row["avg_cost"] for row in rows) / len(rows), 2),
                "avg_score": round(sum(row["overall_score"] for row in rows) / len(rows), 2),
            }
            for region, rows in sorted(
                ktv_region_metrics.items(),
                key=lambda item: (-len(item[1]), item[0]),
            )
        ],
        "ads_ktv_cost_performance_top5": sorted(
            [
                {
                    "name": row["name"],
                    "region": row["region"],
                    "avg_cost": row["avg_cost"],
                    "overall_score": row["overall_score"],
                    "cost_performance": row["cost_performance"],
                    "popularity_num": row["popularity_num"],
                }
                for row in ktv_rows
                if row["avg_cost"] > 0
            ],
            key=lambda item: (-item["cost_performance"], -item["popularity_num"], item["name"]),
        )[:5],
        "ads_sport_type_ratio_top5": sorted(
            [
                {
                    "venue_type": venue_type,
                    "venue_count": len(scores),
                    "venue_ratio": round(len(scores) / sport_total, 4),
                    "avg_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
                }
                for venue_type, scores in sport_groups.items()
            ],
            key=lambda item: (-item["venue_count"], item["venue_type"]),
        )[:5],
        "ads_scenic_free_ratio": [
            {
                "scenic_type": "免费景点",
                "scenic_count": scenic_free_count,
                "scenic_ratio": round(scenic_free_count / scenic_total, 4),
            },
            {
                "scenic_type": "收费景点",
                "scenic_count": scenic_paid_count,
                "scenic_ratio": round(scenic_paid_count / scenic_total, 4),
            },
        ],
    }
    return datasets


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    return str(value)


def build_seed_sql(datasets):
    statements = []
    for table in ADS_TABLES:
        rows = datasets[table]
        statements.append(f"DELETE FROM {table};")
        if not rows:
            continue
        columns = list(rows[0].keys())
        values = []
        for row in rows:
            values.append(
                "(" + ", ".join(sql_literal(row[column]) for column in columns) + ")"
            )
        statements.append(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n  " + ",\n  ".join(values) + ";"
        )
    return "\n".join(statements) + "\n"


def run_mysql(container, user, password, database, sql, capture_output=False):
    command = [
        "docker",
        "exec",
        "-i",
        container,
        "mysql",
        "--default-character-set=utf8mb4",
        f"-u{user}",
        f"-p{password}",
        "-D",
        database,
    ]
    return subprocess.run(
        command,
        input=sql,
        text=True,
        capture_output=capture_output,
        check=True,
    )


def wait_for_mysql(container, user, password, retries=30, interval_seconds=2):
    for _ in range(retries):
        result = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "mysqladmin",
                "ping",
                "-h",
                "127.0.0.1",
                f"-u{user}",
                f"-p{password}",
                "--silent",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        time.sleep(interval_seconds)
    raise RuntimeError("MySQL did not become ready in time.")


def ads_tables_have_data(container, user, password, database):
    query = (
        "SELECT "
        + " + ".join(f"(SELECT COUNT(*) FROM {table})" for table in ADS_TABLES)
        + " AS total_rows;"
    )
    result = run_mysql(container, user, password, database, query, capture_output=True)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return False
    return int(lines[-1]) > 0


def main():
    parser = argparse.ArgumentParser(description="Seed Metabase ADS tables from raw CSV exports.")
    parser.add_argument(
        "--export-dir",
        default=str(Path(__file__).resolve().parents[2] / "crawler" / "data" / "export"),
    )
    parser.add_argument("--mysql-container", default="wenyu-mysql")
    parser.add_argument("--mysql-user", default="wenyu")
    parser.add_argument("--mysql-password", default="wenyu123")
    parser.add_argument("--mysql-database", default="wenyu_result")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    wait_for_mysql(args.mysql_container, args.mysql_user, args.mysql_password)
    if not args.force and ads_tables_have_data(
        args.mysql_container,
        args.mysql_user,
        args.mysql_password,
        args.mysql_database,
    ):
        print("ADS tables already contain data, skipping CSV fallback seeding.")
        return

    datasets = compute_ads_rows(args.export_dir)
    seed_sql = build_seed_sql(datasets)
    run_mysql(
        args.mysql_container,
        args.mysql_user,
        args.mysql_password,
        args.mysql_database,
        seed_sql,
    )
    total_rows = sum(len(rows) for rows in datasets.values())
    print(f"Seeded ADS tables from CSV fallback with {total_rows} rows.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[seed-ads-from-csv] {exc}", file=sys.stderr)
        sys.exit(1)
