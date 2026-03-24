import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "seed_ads_from_csv.py"


def load_module():
    spec = importlib.util.spec_from_file_location("seed_ads_from_csv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class SeedAdsFromCsvTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def make_export_dir(self):
        temp_dir = tempfile.TemporaryDirectory()
        export_dir = Path(temp_dir.name)

        write_csv(
            export_dir / "scenic_raw.csv",
            [
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
            [
                {
                    "name": "景点A",
                    "level": "5A",
                    "region": "北京市东城区",
                    "address": "北京市东城区测试路1号",
                    "price": "门票：50元",
                    "open_time": "全天",
                    "visit_duration": "2小时",
                    "best_visit_time": "春季",
                    "source_url": "https://example.com/scenic-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "景点B",
                    "level": "4A",
                    "region": "北京市朝阳区",
                    "address": "北京市朝阳区测试路2号",
                    "price": "免费开放",
                    "open_time": "全天",
                    "visit_duration": "3小时",
                    "best_visit_time": "秋季",
                    "source_url": "https://example.com/scenic-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        write_csv(
            export_dir / "show_raw.csv",
            [
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
            [
                {
                    "name": "演出A",
                    "show_time": "2026-03-24 19:30",
                    "venue": "剧场A",
                    "region": "北京",
                    "price_range": "100,200,300",
                    "status": "【售票中】",
                    "attention": "10",
                    "source_url": "https://example.com/show-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "演出B",
                    "show_time": "2026-03-25 19:30",
                    "venue": "剧场B",
                    "region": "\\N",
                    "price_range": "80,120",
                    "status": "预售中",
                    "attention": "5",
                    "source_url": "https://example.com/show-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        write_csv(
            export_dir / "ktv_raw.csv",
            [
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
            [
                {
                    "name": "KTV A",
                    "region": "\\N",
                    "address": "北京市朝阳区望京路88号",
                    "avg_cost": "100",
                    "service_score": "4.5",
                    "env_score": "4.0",
                    "overall_score": "80",
                    "popularity": "1000",
                    "business_hours": "全天",
                    "source_url": "https://example.com/ktv-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "KTV B",
                    "region": "\\N",
                    "address": "北京市海淀区中关村大街1号",
                    "avg_cost": "200",
                    "service_score": "4.0",
                    "env_score": "4.0",
                    "overall_score": "90",
                    "popularity": "1500",
                    "business_hours": "全天",
                    "source_url": "https://example.com/ktv-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        write_csv(
            export_dir / "movie_raw.csv",
            [
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
            [
                {
                    "name": "电影A",
                    "score": "8.5",
                    "category": "剧情",
                    "country_region": "中国大陆",
                    "director": "导演A",
                    "actors": "演员A",
                    "intro": "简介A",
                    "source_url": "https://example.com/movie-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "电影B",
                    "score": "6.8",
                    "category": "喜剧",
                    "country_region": "中国大陆",
                    "director": "导演B",
                    "actors": "演员B",
                    "intro": "简介B",
                    "source_url": "https://example.com/movie-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        write_csv(
            export_dir / "sport_raw.csv",
            [
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
            [
                {
                    "name": "游泳馆A",
                    "venue_type": "游泳馆",
                    "region": "北京市朝阳区",
                    "address": "北京市朝阳区测试路9号",
                    "score": "4.5",
                    "comment_count": "10",
                    "avg_cost": "80",
                    "open_time": "全天",
                    "source_url": "https://example.com/sport-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "运动场B",
                    "venue_type": "\\N",
                    "region": "\\N",
                    "address": "北京市海淀区学院路1号",
                    "score": "\\N",
                    "comment_count": "\\N",
                    "avg_cost": "\\N",
                    "open_time": "全天",
                    "source_url": "https://example.com/sport-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        self.addCleanup(temp_dir.cleanup)
        return export_dir

    def test_compute_ads_rows_builds_dashboard_datasets(self):
        export_dir = self.make_export_dir()

        datasets = self.module.compute_ads_rows(export_dir)

        region_counts = {
            row["region"]: row["entertainment_count"]
            for row in datasets["ads_region_entertainment_count"]
        }
        self.assertEqual(3, region_counts["朝阳区"])
        self.assertEqual(2, region_counts["海淀区"])
        self.assertEqual(1, region_counts["东城区"])
        self.assertNotIn("北京市", region_counts)
        self.assertNotIn("\\N", region_counts)

        top_show = datasets["ads_show_price_top10"][0]
        self.assertEqual("演出A", top_show["name"])
        self.assertEqual(300.0, top_show["price_max"])
        self.assertEqual("售票中", top_show["status_std"])

        top_ktv = datasets["ads_ktv_cost_performance_top5"][0]
        self.assertEqual("KTV A", top_ktv["name"])
        self.assertEqual(0.8, top_ktv["cost_performance"])

    def test_compute_ads_rows_builds_ratio_tables(self):
        export_dir = self.make_export_dir()

        datasets = self.module.compute_ads_rows(export_dir)

        movie_distribution = {
            row["score_level"]: row["movie_count"]
            for row in datasets["ads_movie_score_distribution"]
        }
        self.assertEqual(1, movie_distribution["8-9分"])
        self.assertEqual(1, movie_distribution["7分以下"])

        scenic_ratio = {
            row["scenic_type"]: row["scenic_ratio"]
            for row in datasets["ads_scenic_free_ratio"]
        }
        self.assertEqual(0.5, scenic_ratio["免费景点"])
        self.assertEqual(0.5, scenic_ratio["收费景点"])

        show_ratio = {
            row["status_std"]: row["status_ratio"]
            for row in datasets["ads_show_status_ratio"]
        }
        self.assertEqual(0.5, show_ratio["售票中"])
        self.assertEqual(0.5, show_ratio["预售中"])

    def test_show_status_ratio_ignores_unknown_status_when_explicit_status_exists(self):
        export_dir = self.make_export_dir()

        write_csv(
            export_dir / "show_raw.csv",
            [
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
            [
                {
                    "name": "演出A",
                    "show_time": "2026-03-24 19:30",
                    "venue": "剧场A",
                    "region": "北京",
                    "price_range": "100,200,300",
                    "status": "【售票中】",
                    "attention": "10",
                    "source_url": "https://example.com/show-a",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "演出B",
                    "show_time": "2026-03-25 19:30",
                    "venue": "剧场B",
                    "region": "\\N",
                    "price_range": "80,120",
                    "status": "预售中",
                    "attention": "5",
                    "source_url": "https://example.com/show-b",
                    "source_site": "test",
                    "crawl_time": "2026-03-24T00:00:00",
                },
                {
                    "name": "演出C",
                    "show_time": "2026-04-01 19:30",
                    "venue": "剧场C",
                    "region": "北京",
                    "price_range": "99,199",
                    "status": "\\N",
                    "attention": "3",
                    "source_url": "https://example.com/show-c",
                    "source_site": "df",
                    "crawl_time": "2026-03-24T00:00:00",
                },
            ],
        )

        datasets = self.module.compute_ads_rows(export_dir)
        statuses = {row["status_std"] for row in datasets["ads_show_status_ratio"]}

        self.assertEqual({"售票中", "预售中"}, statuses)

    def test_run_mysql_uses_utf8mb4_client_charset(self):
        with mock.patch.object(self.module.subprocess, "run") as run:
            self.module.run_mysql(
                "wenyu-mysql",
                "wenyu",
                "wenyu123",
                "wenyu_result",
                "SELECT 1;",
            )

        command = run.call_args.kwargs["args"] if "args" in run.call_args.kwargs else run.call_args.args[0]
        self.assertIn("--default-character-set=utf8mb4", command)


if __name__ == "__main__":
    unittest.main()
