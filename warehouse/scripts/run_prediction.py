#!/usr/bin/env python3
"""北京租房租金预测模块 - 基于随机森林回归"""
import pymysql
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

DB_CONFIG = dict(host="localhost", port=3306, user="root", password="123456",
                 database="rental_result", charset="utf8mb4")


def load_data():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT fy_id, xzq, unit_dj, jzmj, month_zj, is_dt, fy_type, zx_qk
        FROM ads_price_area_scatter a
        JOIN (SELECT xzq AS x2, is_dt, fy_type, zx_qk, fy_id AS fid
              FROM rental_result.ads_metro_rent_compare m
              RIGHT JOIN (SELECT fy_id, xzq, unit_dj, jzmj, month_zj
                          FROM ads_price_area_scatter) t ON 1=0
             ) b ON 1=0
        LIMIT 0
    """)
    # Actually read from the detail scatter table + get extra features from Hive exported data
    # Since we only have scatter data in MySQL, we'll use what's available
    cur.execute("SELECT fy_id, xzq, unit_dj, jzmj, month_zj FROM ads_price_area_scatter WHERE jzmj > 0 AND month_zj > 0")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def run_prediction():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load data with all features we have
    cur.execute("SELECT fy_id, xzq, unit_dj, jzmj, month_zj FROM ads_price_area_scatter WHERE jzmj > 0 AND month_zj > 0")
    rows = cur.fetchall()
    print(f"Loaded {len(rows)} records")

    fy_ids = [r[0] for r in rows]
    xzqs = [r[1] for r in rows]
    unit_djs = [float(r[2]) for r in rows]
    jzmjs = [float(r[3]) for r in rows]
    month_zjs = [int(r[4]) for r in rows]

    # Encode categorical: xzq
    le_xzq = LabelEncoder()
    xzq_encoded = le_xzq.fit_transform(xzqs)

    # Feature matrix: [jzmj, unit_dj, xzq_encoded]
    # Target: month_zj
    # Note: unit_dj = month_zj / jzmj, so to avoid data leakage we only use jzmj + xzq
    X = np.column_stack([jzmjs, xzq_encoded])
    y = np.array(month_zjs)

    feature_names = ["建筑面积(jzmj)", "行政区(xzq)"]

    # Split
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, np.arange(len(y)), test_size=0.2, random_state=42
    )

    # Train Random Forest
    model = RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    y_pred_all = model.predict(X)
    y_pred_test = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred_test)
    r2 = r2_score(y_test, y_pred_test)
    print(f"MAE: {mae:.2f}, R2: {r2:.4f}")

    # 1. Feature importance
    importances = model.feature_importances_
    cur.execute("DELETE FROM ads_rent_feature_importance")
    for fname, imp in zip(feature_names, importances):
        cur.execute(
            "INSERT INTO ads_rent_feature_importance (feature_name, importance, feature_desc) VALUES (%s, %s, %s)",
            (fname, round(float(imp), 4), fname)
        )
    print("Feature importance saved")

    # 2. Prediction by district
    from collections import defaultdict
    district_actual = defaultdict(list)
    district_predict = defaultdict(list)
    for i in range(len(y)):
        district_actual[xzqs[i]].append(month_zjs[i])
        district_predict[xzqs[i]].append(int(y_pred_all[i]))

    cur.execute("DELETE FROM ads_rent_predict_by_xzq")
    for xzq in district_actual:
        actual_avg = np.mean(district_actual[xzq])
        predict_avg = np.mean(district_predict[xzq])
        diff_pct = (predict_avg - actual_avg) / actual_avg * 100
        cur.execute(
            "INSERT INTO ads_rent_predict_by_xzq (xzq, actual_avg_zj, predict_avg_zj, diff_pct, fysl) VALUES (%s,%s,%s,%s,%s)",
            (xzq, round(actual_avg, 2), round(predict_avg, 2), round(diff_pct, 2), len(district_actual[xzq]))
        )
    print("District prediction saved")

    # 3. Scatter: sample 500 points for visualization
    cur.execute("DELETE FROM ads_rent_predict_scatter")
    sample_idx = np.random.RandomState(42).choice(len(y), min(500, len(y)), replace=False)
    for i in sample_idx:
        cur.execute(
            "INSERT INTO ads_rent_predict_scatter (fy_id, xzq, actual_zj, predict_zj, jzmj) VALUES (%s,%s,%s,%s,%s)",
            (fy_ids[i], xzqs[i], month_zjs[i], int(y_pred_all[i]), jzmjs[i])
        )
    print("Prediction scatter saved")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nPrediction complete. MAE={mae:.0f}元, R2={r2:.4f}")


if __name__ == "__main__":
    run_prediction()
