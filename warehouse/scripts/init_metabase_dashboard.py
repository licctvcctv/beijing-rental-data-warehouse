#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@wenyu.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "Admin@123456")
ADMIN_FIRST_NAME = os.environ.get("METABASE_ADMIN_FIRST_NAME", "Wenyu")
ADMIN_LAST_NAME = os.environ.get("METABASE_ADMIN_LAST_NAME", "Admin")
DB_NAME = os.environ.get("METABASE_DB_NAME", "Wenyu MySQL")
DASHBOARD_NAME = os.environ.get("METABASE_DASHBOARD_NAME", "北京娱乐方式离线数仓 BI 看板")
MYSQL_HOST = os.environ.get("METABASE_MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("METABASE_MYSQL_PORT", "3306"))
MYSQL_DB = os.environ.get("METABASE_MYSQL_DATABASE", "wenyu_result")
MYSQL_USER = os.environ.get("METABASE_MYSQL_USER", "wenyu")
MYSQL_PASSWORD = os.environ.get("METABASE_MYSQL_PASSWORD", "wenyu123")
WAIT_TIMEOUT_SECONDS = int(os.environ.get("METABASE_WAIT_TIMEOUT_SECONDS", "180"))

CARDS = [
    {
        "name": "娱乐资源总数",
        "description": "来自 ads_region_entertainment_count",
        "display": "progress",
        "query": "SELECT SUM(entertainment_count) AS value, 300 AS goal FROM ads_region_entertainment_count WHERE region <> '\\\\N';",
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 3},
    },
    {
        "name": "覆盖区域数",
        "description": "来自 ads_region_entertainment_count",
        "display": "progress",
        "query": "SELECT COUNT(*) AS value, 16 AS goal FROM ads_region_entertainment_count WHERE region NOT IN ('北京市', '\\\\N');",
        "layout": {"row": 0, "col": 6, "size_x": 6, "size_y": 3},
    },
    {
        "name": "在售演出数",
        "description": "来自 ads_show_status_ratio",
        "display": "progress",
        "query": "SELECT COALESCE(MAX(CASE WHEN status_std = '售票中' THEN show_count END), 0) AS value, 60 AS goal FROM ads_show_status_ratio;",
        "layout": {"row": 0, "col": 12, "size_x": 6, "size_y": 3},
    },
    {
        "name": "免费景点占比(%)",
        "description": "来自 ads_scenic_free_ratio",
        "display": "progress",
        "query": "SELECT ROUND(COALESCE(MAX(CASE WHEN scenic_type = '免费景点' THEN scenic_ratio END), 0) * 100, 2) AS value, 100 AS goal FROM ads_scenic_free_ratio;",
        "layout": {"row": 0, "col": 18, "size_x": 6, "size_y": 3},
    },
    {
        "name": "各区娱乐资源总量",
        "description": "来自 ads_region_entertainment_count",
        "display": "bar",
        "query": "SELECT region, entertainment_count FROM ads_region_entertainment_count WHERE region <> '\\\\N' ORDER BY entertainment_count DESC;",
        "layout": {"row": 3, "col": 0, "size_x": 12, "size_y": 5},
    },
    {
        "name": "娱乐类别分布",
        "description": "来自 ADS 汇总表",
        "display": "bar",
        "query": "SELECT category, total_count FROM (SELECT '景点' AS category, COALESCE(SUM(scenic_count), 0) AS total_count FROM ads_scenic_free_ratio UNION ALL SELECT '电影' AS category, COALESCE(SUM(movie_count), 0) AS total_count FROM ads_movie_score_distribution UNION ALL SELECT '演出' AS category, COALESCE(SUM(show_count), 0) AS total_count FROM ads_show_status_ratio UNION ALL SELECT 'KTV' AS category, COALESCE(SUM(ktv_count), 0) AS total_count FROM ads_ktv_region_hotspot UNION ALL SELECT '体育' AS category, COALESCE(SUM(venue_count), 0) AS total_count FROM ads_sport_type_ratio_top5) category_summary ORDER BY total_count DESC;",
        "layout": {"row": 3, "col": 12, "size_x": 6, "size_y": 5},
    },
    {
        "name": "演出价格榜单",
        "description": "来自 ads_show_price_top10",
        "display": "table",
        "query": "SELECT name, venue, price_max, status_std FROM ads_show_price_top10 ORDER BY price_max DESC LIMIT 6;",
        "layout": {"row": 3, "col": 18, "size_x": 6, "size_y": 5},
    },
    {
        "name": "电影评分分布",
        "description": "来自 ads_movie_score_distribution",
        "display": "pie",
        "query": "SELECT score_level, movie_count FROM ads_movie_score_distribution ORDER BY movie_count DESC;",
        "layout": {"row": 8, "col": 0, "size_x": 6, "size_y": 5},
    },
    {
        "name": "演出售票状态占比",
        "description": "来自 ads_show_status_ratio",
        "display": "pie",
        "query": "SELECT status_std, status_ratio FROM ads_show_status_ratio ORDER BY status_ratio DESC;",
        "layout": {"row": 8, "col": 6, "size_x": 6, "size_y": 5},
    },
    {
        "name": "KTV 性价比 Top5",
        "description": "来自 ads_ktv_cost_performance_top5",
        "display": "bar",
        "query": "SELECT name, cost_performance FROM ads_ktv_cost_performance_top5 ORDER BY cost_performance DESC LIMIT 5;",
        "layout": {"row": 8, "col": 12, "size_x": 6, "size_y": 5},
    },
    {
        "name": "体育场馆类型占比",
        "description": "来自 ads_sport_type_ratio_top5",
        "display": "pie",
        "query": "SELECT venue_type, venue_ratio FROM ads_sport_type_ratio_top5 ORDER BY venue_ratio DESC;",
        "layout": {"row": 8, "col": 18, "size_x": 6, "size_y": 5},
    },
]


def request_json(method, path, payload=None, session_id=None):
    url = path if path.startswith("http") else f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if session_id:
        headers["X-Metabase-Session"] = session_id
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            if not body:
                return None
            return json.loads(body)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {error.code} {body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"{method} {path} failed: {error}") from error


def wait_for_metabase():
    deadline = time.time() + WAIT_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            health = request_json("GET", "/api/health")
            if isinstance(health, dict) and health.get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("Metabase did not become ready in time.")


def get_session_properties():
    return request_json("GET", "/api/session/properties") or {}


def ensure_admin():
    properties = get_session_properties()
    setup_token = properties.get("setup-token")
    if setup_token:
        print("Metabase is not initialized. Creating admin user...")
        payload = {
            "token": setup_token,
            "user": {
                "first_name": ADMIN_FIRST_NAME,
                "last_name": ADMIN_LAST_NAME,
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "site_name": "Wenyu BI",
            },
            "prefs": {
                "site_name": "Wenyu BI",
                "allow_tracking": False,
            },
            "database": None,
        }
        try:
            response = request_json("POST", "/api/setup", payload)
            session_id = None
            if isinstance(response, dict):
                session_id = response.get("id") or response.get("session_id")
            if session_id:
                return session_id
        except RuntimeError as error:
            if "HTTP 403" not in str(error):
                raise
    print("Logging into Metabase...")
    response = request_json(
        "POST",
        "/api/session",
        {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    session_id = response.get("id") if isinstance(response, dict) else None
    if not session_id:
        raise RuntimeError("Metabase login did not return a session id.")
    return session_id


def ensure_database(session_id):
    databases = request_json("GET", "/api/database", session_id=session_id) or []
    if isinstance(databases, dict):
        databases = databases.get("data") or []
    for database in databases:
        details = database.get("details") or {}
        if database.get("name") == DB_NAME:
            return database["id"]
        if details.get("db") == MYSQL_DB or details.get("dbname") == MYSQL_DB:
            return database["id"]

    print("Creating Metabase MySQL connection...")
    payload = {
        "engine": "mysql",
        "name": DB_NAME,
        "details": {
            "host": MYSQL_HOST,
            "port": MYSQL_PORT,
            "db": MYSQL_DB,
            "dbname": MYSQL_DB,
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
        },
        "is_full_sync": True,
        "is_on_demand": False,
        "auto_run_queries": True,
    }
    database = request_json("POST", "/api/database", payload, session_id=session_id)
    return database["id"]


def list_cards(session_id):
    cards = request_json("GET", "/api/card", session_id=session_id) or []
    return cards if isinstance(cards, list) else []


def ensure_card(session_id, database_id, spec):
    payload = {
        "name": spec["name"],
        "description": spec["description"],
        "display": spec["display"],
        "visualization_settings": spec.get("visualization_settings", {}),
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {
                "query": spec["query"],
                "template-tags": {},
            },
        },
    }
    for card in list_cards(session_id):
        if card.get("name") == spec["name"]:
            request_json(
                "PUT",
                f"/api/card/{card['id']}",
                payload,
                session_id=session_id,
            )
            return card["id"]

    print(f"Creating card: {spec['name']}")
    card = request_json("POST", "/api/card", payload, session_id=session_id)
    return card["id"]


def list_dashboards(session_id):
    dashboards = request_json("GET", "/api/dashboard", session_id=session_id) or []
    return dashboards if isinstance(dashboards, list) else []


def ensure_dashboard(session_id):
    for dashboard in list_dashboards(session_id):
        if dashboard.get("name") == DASHBOARD_NAME:
            if dashboard.get("width") != "full":
                request_json(
                    "PUT",
                    f"/api/dashboard/{dashboard['id']}",
                    {"width": "full"},
                    session_id=session_id,
                )
            return dashboard["id"]

    print("Creating dashboard...")
    dashboard = request_json(
        "POST",
        "/api/dashboard",
        {
            "name": DASHBOARD_NAME,
            "description": "自动初始化的北京娱乐方式离线数仓 BI 面板",
            "parameters": [],
            "width": "full",
        },
        session_id=session_id,
    )
    return dashboard["id"]


def dashboard_details(session_id, dashboard_id):
    return request_json("GET", f"/api/dashboard/{dashboard_id}", session_id=session_id) or {}


def sync_dashboard_cards(session_id, dashboard_id, card_specs_with_ids):
    dashboard = dashboard_details(session_id, dashboard_id)
    existing_dashcards = dashboard.get("dashcards") or []
    existing_by_card_id = {
        dashcard.get("card_id"): dashcard for dashcard in existing_dashcards if dashcard.get("card_id")
    }

    payload_cards = []
    for index, spec in enumerate(card_specs_with_ids, start=1):
        existing = existing_by_card_id.get(spec["card_id"])
        payload_cards.append(
            {
                "id": existing.get("id") if existing else -index,
                "card_id": spec["card_id"],
                "row": spec["layout"]["row"],
                "col": spec["layout"]["col"],
                "size_x": spec["layout"]["size_x"],
                "size_y": spec["layout"]["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
                "series": [],
            }
        )

    print("Syncing dashboard layout...")
    request_json(
        "PUT",
        f"/api/dashboard/{dashboard_id}/cards",
        {"cards": payload_cards},
        session_id=session_id,
    )


def main():
    wait_for_metabase()
    session_id = ensure_admin()
    database_id = ensure_database(session_id)

    cards = []
    for spec in CARDS:
        card_id = ensure_card(session_id, database_id, spec)
        card_spec = dict(spec)
        card_spec["card_id"] = card_id
        cards.append(card_spec)

    dashboard_id = ensure_dashboard(session_id)
    sync_dashboard_cards(session_id, dashboard_id, cards)

    print("Metabase dashboard is ready.")
    print(f"Dashboard URL: {METABASE_URL}/dashboard/{dashboard_id}")
    print(f"Admin login: {ADMIN_EMAIL} / {ADMIN_PASSWORD}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[metabase-init] {exc}", file=sys.stderr)
        sys.exit(1)
