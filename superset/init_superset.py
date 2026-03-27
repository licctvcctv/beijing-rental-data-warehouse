#!/usr/bin/env python3
"""
Apache Superset auto-bootstrap script.
Creates database connection, charts, and dashboard via REST API.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
ADMIN_USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "rental_result")
MYSQL_USER = os.environ.get("MYSQL_USER", "rental")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "rental123")
WAIT_TIMEOUT = int(os.environ.get("WAIT_TIMEOUT_SECONDS", "300"))
MARKER_PATH = os.environ.get("SUPERSET_BOOTSTRAP_MARKER_PATH", "")
DASHBOARD_TITLE = "北京租房数据离线数仓 BI 看板"

# ── Chart definitions (name, viz_type, sql, description) ──
CHARTS = [
    {
        "name": "各行政区平均租金",
        "viz_type": "echarts_bar",
        "sql": "SELECT xzq AS 行政区, pj_zj AS 平均租金 FROM ads_xzq_avg_rent ORDER BY pj_zj DESC",
        "description": "横向柱状图：对比各行政区租金差异",
    },
    {
        "name": "商圈房源数量TOP10",
        "viz_type": "echarts_bar",
        "sql": "SELECT sq AS 商圈, fysl AS 房源数量, pj_zj AS 平均租金 FROM ads_sq_top10 ORDER BY fysl DESC",
        "description": "柱状图：展示热门商圈房源供给情况",
    },
    {
        "name": "房源类型占比",
        "viz_type": "pie",
        "sql": "SELECT fy_type AS 房源类型, fysl AS 数量 FROM ads_fy_type_ratio ORDER BY fysl DESC",
        "description": "饼图：呈现北京租房市场房源类型结构",
    },
    {
        "name": "地铁房vs非地铁房租金对比",
        "viz_type": "echarts_bar",
        "sql": "SELECT xzq AS 行政区, MAX(CASE WHEN is_dt='是' THEN pj_zj END) AS 地铁房均价, MAX(CASE WHEN is_dt='否' THEN pj_zj END) AS 非地铁房均价 FROM ads_metro_rent_compare GROUP BY xzq ORDER BY 地铁房均价 DESC",
        "description": "分组柱状图：量化地铁因素对租金的影响",
    },
    {
        "name": "各装修情况平均租金",
        "viz_type": "echarts_bar",
        "sql": "SELECT zx_qk AS 装修情况, pj_zj AS 平均租金, pj_dj AS 平均单价 FROM ads_zx_avg_rent ORDER BY pj_zj DESC",
        "description": "柱状图：多维度展示装修情况与租金的关联",
    },
    {
        "name": "各平台房源分布",
        "viz_type": "pie",
        "sql": "SELECT platform AS 平台, fysl AS 房源数量 FROM ads_platform_distribution ORDER BY fysl DESC",
        "description": "饼图：明确各平台房源供给规模",
    },
    {
        "name": "各行政区房源数量",
        "viz_type": "echarts_bar",
        "sql": "SELECT xzq AS 行政区, fysl AS 房源数量 FROM ads_xzq_avg_rent ORDER BY fysl DESC",
        "description": "柱状图：展示各区房源供给",
    },
    {
        "name": "租金热力图数据",
        "viz_type": "table",
        "sql": "SELECT sq AS 商圈, xzq AS 行政区, pj_zj AS 平均租金, fysl AS 房源数量, center_jd AS 经度, center_wd AS 纬度 FROM ads_fy_heatmap ORDER BY pj_zj DESC LIMIT 20",
        "description": "表格：商圈维度租金与坐标数据",
    },
]


def api(method, path, data=None, token=None, csrf=None):
    """Make HTTP request to Superset API."""
    url = f"{SUPERSET_URL}{path}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if csrf:
        headers["X-CSRFToken"] = csrf
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        print(f"  API error {e.code}: {method} {path} → {err_body[:300]}")
        raise


def wait_for_superset():
    """Wait until Superset health endpoint responds."""
    deadline = time.time() + WAIT_TIMEOUT
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{SUPERSET_URL}/health")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    print("Superset is ready.")
                    return
        except Exception as e:
            print(f"Waiting for Superset: {e}")
        time.sleep(3)
    raise RuntimeError("Superset did not become ready in time.")


def login():
    """Login and return (access_token, csrf_token, cookies)."""
    resp = api("POST", "/api/v1/security/login", {
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": True,
    })
    token = resp["access_token"]
    # Get CSRF token
    csrf_resp = api("GET", "/api/v1/security/csrf_token/", token=token)
    csrf = csrf_resp.get("result", "")
    print(f"Logged in as {ADMIN_USER}, got tokens.")
    return token, csrf


def ensure_database(token, csrf):
    """Create MySQL database connection if not exists, return database_id."""
    existing = api("GET", "/api/v1/database/", token=token)
    for db in existing.get("result", []):
        if db.get("database_name") == "Rental MySQL":
            print(f"Database connection already exists: id={db['id']}")
            return db["id"]

    sqlalchemy_uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    resp = api("POST", "/api/v1/database/", {
        "database_name": "Rental MySQL",
        "engine": "mysql",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
    }, token=token, csrf=csrf)
    db_id = resp["id"]
    print(f"Created database connection: id={db_id}")
    return db_id


def ensure_charts(token, csrf, database_id):
    """Create charts, return list of chart ids."""
    existing = api("GET", "/api/v1/chart/?q=(page_size:100)", token=token)
    existing_names = {}
    for c in existing.get("result", []):
        existing_names[c["slice_name"]] = c["id"]

    chart_ids = []
    for spec in CHARTS:
        if spec["name"] in existing_names:
            cid = existing_names[spec["name"]]
            print(f"  Chart already exists: '{spec['name']}' id={cid}")
            chart_ids.append(cid)
            continue

        payload = {
            "slice_name": spec["name"],
            "description": spec["description"],
            "viz_type": spec["viz_type"],
            "datasource_type": "query",
            "database": database_id,
            "params": json.dumps({
                "datasource": f"__{database_id}__query",
                "viz_type": spec["viz_type"],
            }),
            "query_context": json.dumps({
                "datasource": {"type": "query"},
                "queries": [{"sql": spec["sql"]}],
            }),
        }
        try:
            resp = api("POST", "/api/v1/chart/", payload, token=token, csrf=csrf)
            cid = resp.get("id")
            print(f"  Created chart: '{spec['name']}' id={cid}")
            chart_ids.append(cid)
        except Exception as e:
            print(f"  Failed to create chart '{spec['name']}': {e}")
    return chart_ids


def ensure_dashboard(token, csrf, chart_ids):
    """Create dashboard and add charts."""
    existing = api("GET", "/api/v1/dashboard/?q=(page_size:50)", token=token)
    dash_id = None
    for d in existing.get("result", []):
        if d.get("dashboard_title") == DASHBOARD_TITLE:
            dash_id = d["id"]
            print(f"Dashboard already exists: id={dash_id}")
            break

    if dash_id is None:
        resp = api("POST", "/api/v1/dashboard/", {
            "dashboard_title": DASHBOARD_TITLE,
            "published": True,
        }, token=token, csrf=csrf)
        dash_id = resp["id"]
        print(f"Created dashboard: id={dash_id}")

    # Build position_json layout — 12-column grid
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    # Layout: 2 charts per row, each 6 cols wide, 8 rows high
    row_idx = 0
    for i, cid in enumerate(chart_ids):
        if cid is None:
            continue
        col = (i % 2) * 6
        if i % 2 == 0:
            row_id = f"ROW-{row_idx}"
            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            position["GRID_ID"]["children"].append(row_id)

        row_id = f"ROW-{row_idx}"
        chart_key = f"CHART-{cid}"
        position[chart_key] = {
            "type": "CHART",
            "id": chart_key,
            "children": [],
            "meta": {
                "width": 6,
                "height": 50,
                "chartId": cid,
                "sliceName": CHARTS[i]["name"] if i < len(CHARTS) else "",
            },
        }
        position[row_id]["children"].append(chart_key)

        if i % 2 == 1:
            row_idx += 1
    if len(chart_ids) % 2 == 1:
        row_idx += 1

    api("PUT", f"/api/v1/dashboard/{dash_id}", {
        "position_json": json.dumps(position),
        "json_metadata": json.dumps({
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "color_scheme": "",
            "label_colors": {},
        }),
    }, token=token, csrf=csrf)
    print(f"Dashboard layout updated with {len(chart_ids)} charts.")
    return dash_id


def write_marker(dash_id):
    if not MARKER_PATH:
        return
    os.makedirs(os.path.dirname(MARKER_PATH) or ".", exist_ok=True)
    public_url = os.environ.get("SUPERSET_PUBLIC_URL", "http://localhost:8088")
    with open(MARKER_PATH, "w") as f:
        f.write(f"superset_status=SUCCESS\n")
        f.write(f"dashboard_url={public_url}/superset/dashboard/{dash_id}/\n")
        f.write(f"admin_user={ADMIN_USER}\n")


def main():
    print("=" * 60)
    print("  Superset Auto-Bootstrap for 北京租房数据离线数仓")
    print("=" * 60)

    wait_for_superset()
    token, csrf = login()
    db_id = ensure_database(token, csrf)
    chart_ids = ensure_charts(token, csrf, db_id)
    dash_id = ensure_dashboard(token, csrf, chart_ids)
    write_marker(dash_id)

    public_url = os.environ.get("SUPERSET_PUBLIC_URL", "http://localhost:8088")
    print()
    print("=" * 60)
    print(f"  Dashboard: {public_url}/superset/dashboard/{dash_id}/")
    print(f"  Login: {ADMIN_USER} / {ADMIN_PASS}")
    print("=" * 60)


if __name__ == "__main__":
    main()
