import json
from collections import Counter

# 分段规则（第一个区间从 1 开始）
ranges = [
    (1, 9999, "1 - 9999"),
    (10000, 49999, "10000 - 49999"),
    (50000, 99999, "50000 - 99999"),
    (100000, 199999, "100000 - 199999"),
    (200000, 299999, "200000 - 299999"),
    (300000, float("inf"), "300000+"),
]

def get_range_label(xp: int) -> str:
    for low, high, label in ranges:
        if low <= xp <= high:
            return label
    return None  # 不符合区间

def count_wallets_by_xp(file_path: str):
    counter = Counter()
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)

            xp = data.get("totalXp", 0)
            xp_rank = data.get("xpRank")

            # 过滤条件
            if xp == 0 or xp_rank is None:
                continue

            label = get_range_label(xp)
            if label:
                counter[label] += 1
    return counter

if __name__ == "__main__":
    result = count_wallets_by_xp("20250929_leaderboard.json")
    print("📊 XP 分布统计（过滤 totalXp=0, xpRank!=null）：")
    for _, _, label in ranges:
        print(f"{label}: {result.get(label, 0)}")
