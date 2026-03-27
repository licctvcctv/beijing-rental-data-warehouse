#!/usr/bin/env python3
"""Generate ~140,000 synthetic Beijing rental housing records."""
import csv
import random
import uuid
from pathlib import Path

random.seed(42)

# --- 北京行政区及商圈、经纬度范围 ---
DISTRICTS = {
    "朝阳区": {
        "center": (116.4435, 39.9210),
        "sq": ["望京", "国贸", "三里屯", "亚运村", "大望路", "十里堡", "双井", "劲松", "潘家园", "常营", "北苑", "管庄", "酒仙桥", "太阳宫", "安贞"],
    },
    "海淀区": {
        "center": (116.2980, 39.9593),
        "sq": ["中关村", "五道口", "西二旗", "上地", "学院路", "万寿路", "公主坟", "北太平庄", "牡丹园", "知春路", "清河", "西三旗"],
    },
    "西城区": {
        "center": (116.3660, 39.9120),
        "sq": ["金融街", "西单", "德胜门", "新街口", "陶然亭", "广安门", "牛街", "月坛", "白纸坊"],
    },
    "东城区": {
        "center": (116.4160, 39.9285),
        "sq": ["王府井", "东直门", "崇文门", "安定门", "和平里", "北新桥", "天坛", "磁器口"],
    },
    "丰台区": {
        "center": (116.2870, 39.8585),
        "sq": ["方庄", "丰台科技园", "草桥", "角门", "马家堡", "刘家窑", "宋家庄", "大红门", "西罗园", "青塔"],
    },
    "石景山区": {
        "center": (116.2227, 39.9056),
        "sq": ["鲁谷", "苹果园", "古城", "八角", "老山", "金顶街"],
    },
    "通州区": {
        "center": (116.6571, 39.9095),
        "sq": ["梨园", "北苑", "九棵树", "果园", "土桥", "马驹桥", "临河里"],
    },
    "大兴区": {
        "center": (116.3413, 39.7268),
        "sq": ["黄村", "亦庄", "旧宫", "枣园", "高米店", "西红门", "生物医药基地"],
    },
    "昌平区": {
        "center": (116.2312, 40.2207),
        "sq": ["回龙观", "天通苑", "沙河", "昌平镇", "龙泽", "霍营", "北七家"],
    },
    "顺义区": {
        "center": (116.6545, 40.1302),
        "sq": ["后沙峪", "马坡", "顺义城", "石门", "天竺", "李桥"],
    },
    "房山区": {
        "center": (116.1433, 39.7530),
        "sq": ["良乡", "长阳", "房山城关", "阎村", "窦店"],
    },
    "门头沟区": {
        "center": (116.1020, 39.9404),
        "sq": ["大峪", "城子", "石门营", "龙泉"],
    },
    "平谷区": {
        "center": (117.1212, 40.1406),
        "sq": ["平谷城区", "马坊", "兴谷"],
    },
    "怀柔区": {
        "center": (116.6316, 40.3161),
        "sq": ["怀柔城区", "雁栖", "庙城"],
    },
    "密云区": {
        "center": (116.8430, 40.3762),
        "sq": ["密云城区", "果园街道", "鼓楼街道"],
    },
    "延庆区": {
        "center": (115.9750, 40.4567),
        "sq": ["延庆城区", "康庄", "儒林街道"],
    },
}

# 房源类型权重
FY_TYPES = [("整租", 0.45), ("合租", 0.50), ("独栋", 0.05)]
FY_TYPE_VALS = [t[0] for t in FY_TYPES]
FY_TYPE_WEIGHTS = [t[1] for t in FY_TYPES]

# 平台权重
PLATFORMS = [("链家", 0.30), ("贝壳找房", 0.25), ("58同城", 0.20), ("安居客", 0.15), ("自如", 0.10)]
PLAT_VALS = [p[0] for p in PLATFORMS]
PLAT_WEIGHTS = [p[1] for p in PLATFORMS]

# 装修情况
ZX_OPTIONS = [("精装", 0.40), ("简装", 0.35), ("毛坯", 0.15), ("豪装", 0.10)]
ZX_VALS = [z[0] for z in ZX_OPTIONS]
ZX_WEIGHTS = [z[1] for z in ZX_OPTIONS]

# 行政区权重 (朝阳/海淀占大头)
DISTRICT_WEIGHTS = {
    "朝阳区": 0.22, "海淀区": 0.16, "西城区": 0.08, "东城区": 0.07,
    "丰台区": 0.12, "通州区": 0.08, "大兴区": 0.07, "昌平区": 0.08,
    "石景山区": 0.03, "顺义区": 0.03, "房山区": 0.02, "门头沟区": 0.01,
    "平谷区": 0.01, "怀柔区": 0.01, "密云区": 0.005, "延庆区": 0.005,
}

# 各区租金基准 (元/月, 整租)
RENT_BASE = {
    "朝阳区": 6800, "海淀区": 6500, "西城区": 7500, "东城区": 7200,
    "丰台区": 5000, "石景山区": 4800, "通州区": 4200, "大兴区": 4000,
    "昌平区": 3800, "顺义区": 4500, "房山区": 3200, "门头沟区": 3500,
    "平谷区": 2500, "怀柔区": 2800, "密云区": 2400, "延庆区": 2200,
}

# 面积分布参数 (均值, 标准差)
AREA_PARAMS = {"整租": (65, 25), "合租": (18, 6), "独栋": (180, 60)}

TOTAL = 10000

def gen_coord(center, spread=0.04):
    return (
        round(center[0] + random.uniform(-spread, spread), 6),
        round(center[1] + random.uniform(-spread, spread), 6),
    )

def gen_records():
    dist_names = list(DISTRICT_WEIGHTS.keys())
    dist_weights = [DISTRICT_WEIGHTS[d] for d in dist_names]

    for i in range(TOTAL):
        xzq = random.choices(dist_names, weights=dist_weights, k=1)[0]
        info = DISTRICTS[xzq]
        sq = random.choice(info["sq"])
        jd, wd = gen_coord(info["center"])

        fy_type = random.choices(FY_TYPE_VALS, weights=FY_TYPE_WEIGHTS, k=1)[0]
        platform = random.choices(PLAT_VALS, weights=PLAT_WEIGHTS, k=1)[0]
        zx_qk = random.choices(ZX_VALS, weights=ZX_WEIGHTS, k=1)[0]

        # 面积
        mean, std = AREA_PARAMS[fy_type]
        jzmj = max(8, round(random.gauss(mean, std), 1))

        # 月租金 = 基准 * 类型系数 * 随机波动
        base = RENT_BASE[xzq]
        if fy_type == "合租":
            base_rent = base * 0.35
        elif fy_type == "独栋":
            base_rent = base * 3.5
        else:
            base_rent = base
        # 装修加成
        zx_mult = {"精装": 1.10, "简装": 1.0, "毛坯": 0.85, "豪装": 1.25}[zx_qk]
        month_zj = max(500, int(base_rent * zx_mult * random.uniform(0.7, 1.4)))

        # 是否地铁房 (核心区概率更高)
        metro_prob = 0.7 if xzq in ("朝阳区", "海淀区", "西城区", "东城区") else 0.4
        is_dt = "是" if random.random() < metro_prob else "否"
        if is_dt == "是":
            month_zj = int(month_zj * random.uniform(1.05, 1.25))

        fy_id = f"BJ{i+1:07d}"
        fy_title = f"{sq}{jzmj}㎡{zx_qk}{fy_type}"
        fy_status = random.choices(["在租", "已租", "待审核"], weights=[0.7, 0.2, 0.1], k=1)[0]

        yield {
            "fy_id": fy_id,
            "fy_title": fy_title,
            "fy_type": fy_type,
            "fy_status": fy_status,
            "platform": platform,
            "xzq": xzq,
            "sq": sq,
            "jd": jd,
            "wd": wd,
            "month_zj": month_zj,
            "jzmj": jzmj,
            "is_dt": is_dt,
            "zx_qk": zx_qk,
        }

def main():
    out_dir = Path(__file__).parent / "data" / "export"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "rental_raw.csv"

    fields = ["fy_id", "fy_title", "fy_type", "fy_status", "platform",
              "xzq", "sq", "jd", "wd", "month_zj", "jzmj", "is_dt", "zx_qk"]

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for rec in gen_records():
            writer.writerow(rec)

    print(f"Generated {TOTAL} records -> {out_path}")

if __name__ == "__main__":
    main()
