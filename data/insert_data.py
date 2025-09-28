import os
import json
import pymysql
from datetime import datetime, timedelta
from tqdm import tqdm
import time

# ================= 数据库配置 =================
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "123456",
    "database": "plume_db",
    "port": 3306,
    "charset": "utf8mb4",
    "autocommit": False,
    "connect_timeout": 10
}

BASE_BATCH_SIZE = 2000
MAX_RETRY = 3

# ================= SQL 常量 =================
SQL_USER = """
    INSERT INTO users (wallet_address, referred_by, referral_count)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE 
        id=LAST_INSERT_ID(id),
        referred_by=VALUES(referred_by),
        referral_count=VALUES(referral_count)
"""

SQL_SNAPSHOT = """
    INSERT INTO user_snapshots (
        user_id, snapshot_date, bridged_total, swap_volume, swap_count,
        tvl_total_usd, real_tvl_usd, protocols_used, longest_swap_streak_weeks,
        adjustment_points, protectors_points, badge_points, user_self_xp,
        referral_bonus_xp, total_xp, xp_rank, longest_tvl_streak,
        plume_staking_points, plume_staking_bonus, plume_staking_total_tokens
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        bridged_total=VALUES(bridged_total),
        swap_volume=VALUES(swap_volume),
        swap_count=VALUES(swap_count),
        tvl_total_usd=VALUES(tvl_total_usd),
        real_tvl_usd=VALUES(real_tvl_usd),
        protocols_used=VALUES(protocols_used),
        longest_swap_streak_weeks=VALUES(longest_swap_streak_weeks),
        adjustment_points=VALUES(adjustment_points),
        protectors_points=VALUES(protectors_points),
        badge_points=VALUES(badge_points),
        user_self_xp=VALUES(user_self_xp),
        referral_bonus_xp=VALUES(referral_bonus_xp),
        total_xp=VALUES(total_xp),
        xp_rank=VALUES(xp_rank),
        longest_tvl_streak=VALUES(longest_tvl_streak),
        plume_staking_points=VALUES(plume_staking_points),
        plume_staking_bonus=VALUES(plume_staking_bonus),
        plume_staking_total_tokens=VALUES(plume_staking_total_tokens)
"""

SQL_CHANGE = """
    INSERT INTO user_daily_changes (user_id, snapshot_date, xp_change, tvl_change)
    VALUES (%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        xp_change=VALUES(xp_change),
        tvl_change=VALUES(tvl_change)
"""

# ================= 工具函数 =================
def get_connection():
    for attempt in range(MAX_RETRY):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.MySQLError:
            print(f"⚠️ 数据库连接失败，重试 {attempt + 1}/{MAX_RETRY} …")
            time.sleep(1)
    raise ConnectionError("❌ 数据库连接失败，请检查配置")

def clean_tvl(value, max_value=1e12):
    """清理 tvl 数值，保证插入数据库不会溢出"""
    try:
        val = float(value)
    except (ValueError, TypeError):
        return 0
    if val < 0:
        return 0
    if val > max_value:  # 避免超出数据库范围
        return max_value
    return val

def snapshot_values(user_id, snapshot_date, data):
    tvl_total_usd = data.get("tvlTotalUsd", 0)
    if tvl_total_usd is None:
        tvl_total_usd = 0
    else:
        try:
            tvl_total_usd = float(tvl_total_usd)
        except (ValueError, TypeError):
            tvl_total_usd = 0
    if tvl_total_usd < 0:
        tvl_total_usd = 0
    return (
        user_id,
        snapshot_date,
        data.get("bridgedTotal", 0),
        data.get("swapVolume", 0),
        data.get("swapCount", 0),
        clean_tvl(data.get("tvlTotalUsd", 0)),     # ✅ 已清洗
        clean_tvl(data.get("realTvlUsd", 0)),      # ✅ 建议 real_tvl_usd 也清洗
        data.get("protocolsUsed", 0),
        data.get("longestSwapStreakWeeks", 0),
        data.get("adjustmentPoints", 0),
        data.get("protectorsOfPlumePoints", 0),
        data.get("badgePoints", 0),
        data.get("userSelfXp", 0),
        data.get("referralBonusXp", 0),
        data.get("totalXp", 0),
        data.get("xpRank"),
        data.get("longestTvlStreak", 0),
        data.get("plumeStakingPointsEarned", 0),
        data.get("plumeStakingBonusPointsEarned", 0),
        data.get("currentPlumeStakingTotalTokens", 0),
    )


def daily_change_values(user_id, snapshot_date, xp_change, tvl_change):
    return (user_id, snapshot_date, xp_change, tvl_change)


def get_batch_size(total_lines):
    if total_lines > 200_000:
        return 4000
    elif total_lines > 100_000:
        return 3000
    else:
        return BASE_BATCH_SIZE

# ================= 批次处理 =================
def process_batch(batch, attempt=1):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ---- 批量解析 JSON ----
        parsed = [json.loads(line.strip()) for line in batch if line.strip()]

        # ---- 批量 upsert 用户 ----
        wallets = [(d["walletAddress"], d.get("referredBy"), d.get("referralCount", 0)) for d in parsed]
        cursor.executemany(SQL_USER, wallets)
        conn.commit()  # 确保 id 映射可用

        # 获取 user_id 映射
        cursor.execute(
            f"SELECT id, wallet_address FROM users WHERE wallet_address IN ({','.join(['%s']*len(wallets))})",
            [w[0] for w in wallets]
        )
        user_map = {w: uid for uid, w in cursor.fetchall()}

        # ---- 批量查询昨天的快照 ----
        snapshot_date = datetime.strptime(parsed[0]["dateStr"].split("_")[0], "%Y-%m-%d").date()
        yesterday = snapshot_date - timedelta(days=1)
        cursor.execute(
            f"SELECT user_id, total_xp, tvl_total_usd FROM user_snapshots "
            f"WHERE snapshot_date=%s AND user_id IN ({','.join(['%s']*len(user_map))})",
            [yesterday] + list(user_map.values())
        )
        yesterday_map = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

        # ---- 生成快照 & 日变化数据 ----
        snapshots_batch, changes_batch = [], []
        for data in parsed:
            user_id = user_map[data["walletAddress"]]

            # 快照
            snapshots_batch.append(snapshot_values(user_id, snapshot_date, data))

            # 日变化
            y_xp, y_tvl = yesterday_map.get(user_id, (0, 0))
            xp_change = int(data.get("totalXp", 0)) - int(y_xp)
            tvl_change = float(data.get("tvlTotalUsd", 0)) - float(y_tvl)
            
            # ✅ 清理 tvl_change，避免溢出
            tvl_change = clean_tvl(tvl_change)

            if xp_change or tvl_change:
                changes_batch.append(daily_change_values(user_id, snapshot_date, xp_change, tvl_change))

        # ---- 批量插入 ----
        if snapshots_batch:
            cursor.executemany(SQL_SNAPSHOT, snapshots_batch)
        if changes_batch:
            cursor.executemany(SQL_CHANGE, changes_batch)

        conn.commit()
        return True

    except pymysql.err.OperationalError as e:
        if attempt < MAX_RETRY and (1213 in str(e) or "MySQL server has gone away" in str(e)):
            print(f"⚠️ 第{attempt}次重试批次，原因: {e}")
            time.sleep(0.5 * attempt)
            return process_batch(batch, attempt + 1)
        else:
            return f"❌ 出错: {e}\n数据示例: {batch[0].strip() if batch else '空'}"
    except Exception as e:
        return f"❌ 出错: {e}\n数据示例: {batch[0].strip() if batch else '空'}"
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# ================= 批量导入入口 =================
def bulk_insert(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    batch_size = get_batch_size(len(lines))
    batches = [lines[i:i + batch_size] for i in range(0, len(lines), batch_size)]
    print(f"🚀 开始导入，总数据={len(lines)}, 批次数={len(batches)}, 批次大小={batch_size}")

    for batch in tqdm(batches, desc="插入数据"):
        result = process_batch(batch)
        if result is not True:
            print(result)

    print("✅ 单天增量数据插入完成")

# ================= 平台统计 =================
def update_platform_stats(snapshot_date):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                COUNT(*) AS total_wallets,
                SUM(total_xp) AS total_xp
            FROM user_snapshots
            WHERE snapshot_date=%s
              AND xp_rank IS NOT NULL
              AND total_xp <> 0
        """, (snapshot_date,))
        row = cursor.fetchone()
        if not row or row[0] == 0:
            print(f"⚠️ {snapshot_date} 没有符合条件的快照数据，统计跳过")
            return

        total_wallets, total_xp = row

        yesterday = snapshot_date - timedelta(days=1)
        cursor.execute("""
            SELECT total_wallets, total_xp
            FROM platform_stats
            WHERE snapshot_date=%s
        """, (yesterday,))
        prev = cursor.fetchone()
        if prev:
            prev_wallets, prev_xp = prev
        else:
            prev_wallets, prev_xp = 0, 0

        new_wallets = total_wallets - prev_wallets
        new_xp = int(total_xp) - int(prev_xp)

        cursor.execute("""
            INSERT INTO platform_stats
                (snapshot_date, total_wallets, total_xp, new_wallets, new_xp)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                total_wallets=VALUES(total_wallets),
                total_xp=VALUES(total_xp),
                new_wallets=VALUES(new_wallets),
                new_xp=VALUES(new_xp)
        """, (snapshot_date, total_wallets, total_xp, new_wallets, new_xp))

        conn.commit()
        print(f"✅ {snapshot_date} 平台统计已更新 | 总钱包={total_wallets}, 新增钱包={new_wallets}")

    except Exception as e:
        conn.rollback()
        print(f"❌ 更新 {snapshot_date} 平台统计失败: {e}")
    finally:
        cursor.close()
        conn.close()

# ================= 主程序入口 =================
if __name__ == "__main__":
    json_file = "20250928_leaderboard.json"  # 文件名表示当天快照

    base_name = os.path.basename(json_file).split("_")[0]  # 20250903
    record_date = datetime.strptime(base_name, "%Y%m%d").date()

    bulk_insert(json_file)

    platform_date = record_date - timedelta(days=1)
    update_platform_stats(platform_date)

    print(f"🎉 {record_date} 单天增量数据 & {platform_date} 平台统计完成")
