import importlib.util
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "init_metabase_dashboard.py"


def load_module():
    spec = importlib.util.spec_from_file_location("init_metabase_dashboard", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class InitMetabaseDashboardTests(unittest.TestCase):
    def setUp(self):
        self.module = load_module()

    def test_ensure_admin_falls_back_to_login_when_setup_is_forbidden(self):
        with mock.patch.object(
            self.module,
            "get_session_properties",
            return_value={"setup-token": "setup-token"},
        ), mock.patch.object(self.module, "request_json") as request_json:
            request_json.side_effect = [
                RuntimeError(
                    "POST /api/setup failed: HTTP 403 The /api/setup route can only be used to create the first user, however a user currently exists."
                ),
                {"id": "session-from-login"},
            ]

            session_id = self.module.ensure_admin()

        self.assertEqual("session-from-login", session_id)

    def test_ensure_database_accepts_wrapped_database_lists(self):
        with mock.patch.object(
            self.module,
            "request_json",
            return_value={
                "data": [
                    {
                        "id": 7,
                        "name": self.module.DB_NAME,
                        "details": {"db": self.module.MYSQL_DB},
                    }
                ]
            },
        ):
            database_id = self.module.ensure_database("session-id")

        self.assertEqual(7, database_id)

    def test_ensure_card_updates_existing_card_when_configuration_changes(self):
        spec = {
            "name": "娱乐资源总数",
            "description": "来自 ads_region_entertainment_count",
            "display": "progress",
            "query": "SELECT 267 AS value, 300 AS goal;",
        }
        existing_card = {"id": 42, "name": "娱乐资源总数", "display": "scalar"}
        with mock.patch.object(
            self.module,
            "list_cards",
            return_value=[existing_card],
        ), mock.patch.object(self.module, "request_json", return_value={"id": 42}) as request_json:
            card_id = self.module.ensure_card("session-id", 2, spec)

        self.assertEqual(42, card_id)
        request_json.assert_called_once_with(
            "PUT",
            "/api/card/42",
            {
                "name": spec["name"],
                "description": spec["description"],
                "display": spec["display"],
                "visualization_settings": {},
                "dataset_query": {
                    "type": "native",
                    "database": 2,
                    "native": {
                        "query": spec["query"],
                        "template-tags": {},
                    },
                },
            },
            session_id="session-id",
        )

    def test_ensure_dashboard_updates_width_to_full_for_existing_dashboard(self):
        with mock.patch.object(
            self.module,
            "request_json",
            side_effect=[
                [
                    {
                        "id": 9,
                        "name": self.module.DASHBOARD_NAME,
                        "width": "fixed",
                    }
                ],
                {"id": 9},
            ],
        ) as request_json:
            dashboard_id = self.module.ensure_dashboard("session-id")

        self.assertEqual(9, dashboard_id)
        request_json.assert_any_call(
            "PUT",
            "/api/dashboard/9",
            {"width": "full"},
            session_id="session-id",
        )

    def test_card_configuration_uses_richer_display_mix(self):
        displays = {card["display"] for card in self.module.CARDS}

        self.assertIn("progress", displays)
        self.assertIn("table", displays)
        self.assertIn("bar", displays)
        self.assertIn("pie", displays)

    def test_card_layout_stays_within_single_screen_height_budget(self):
        max_bottom = max(card["layout"]["row"] + card["layout"]["size_y"] for card in self.module.CARDS)

        self.assertLessEqual(max_bottom, 15)

    def test_top_kpi_cards_use_progress_style_with_goals(self):
        kpi_names = {"娱乐资源总数", "覆盖区域数", "在售演出数", "免费景点占比(%)"}
        cards = [card for card in self.module.CARDS if card["name"] in kpi_names]

        self.assertEqual(4, len(cards))
        for card in cards:
            self.assertEqual("progress", card["display"])
            self.assertIn(" AS goal", card["query"])


if __name__ == "__main__":
    unittest.main()
