from database import get_connection
from datetime import date, timedelta

# ========== Platform Stats ==========
# def create_platform_stats(stats: schemas.PlatformStatsCreate):
#     conn = get_connection()
#     try:
#         with conn.cursor() as cursor:
#             sql = """INSERT INTO platform_stats (snapshot_date, total_wallets, total_tvl, total_xp)
#                      VALUES (%s, %s, %s, %s)
#                      ON DUPLICATE KEY UPDATE
#                         total_wallets=VALUES(total_wallets),
#                         total_tvl=VALUES(total_tvl),
#                         total_xp=VALUES(total_xp)"""
#             cursor.execute(sql, (stats.snapshot_date, stats.total_wallets, stats.total_tvl, stats.total_xp))
#         conn.commit()
#         return {"message": "Platform stats created/updated"}
#     finally:
#         conn.close()


def get_platform_stats(date: str = None):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            if date:
                sql = "SELECT id, snapshot_date, total_wallets, total_xp, new_wallets, new_xp FROM platform_stats WHERE snapshot_date=%s"
                cursor.execute(sql, (date,))
            else:
                sql = "SELECT id, snapshot_date, total_wallets, total_xp, new_wallets, new_xp FROM platform_stats ORDER BY snapshot_date DESC LIMIT 1"
                cursor.execute(sql)
            row = cursor.fetchone()
            if not row:  # 先判断
                return None
            return {
                "id": row['id'],
                "snapshot_date": row['snapshot_date'],
                "total_wallets": row['total_wallets'],
                "total_xp": int(row['total_xp'] or 0),
                "new_wallets": int(row['new_wallets'] or 0),
                "new_xp": int(row['new_xp'] or 0)
            }
    finally:
        conn.close()
        
def get_all_platform_stats():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT id, snapshot_date, total_wallets, total_xp, new_wallets, new_xp
                FROM platform_stats
                ORDER BY snapshot_date DESC
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:  # 先判断
                return []
            result = []
            for row in rows:
                result.append({
                    "id": row['id'],
                    "snapshot_date": row['snapshot_date'],
                    "total_wallets": row['total_wallets'],
                    "total_xp": int(row['total_xp'] or 0),
                    "new_wallets": int(row['new_wallets'] or 0),
                    "new_xp": int(row['new_xp'] or 0)
                })
            return result
    finally:
        conn.close()
        
# ========== 获取每日用户总排行 ==========
def get_global_rank(snapshot_date: date = None, limit: int = 100, debug: bool = False):
    if snapshot_date is None:
        snapshot_date = date.today() - timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT u.wallet_address, us.total_xp, us.xp_rank
                FROM users u
                JOIN user_snapshots us ON u.id = us.user_id
                WHERE us.snapshot_date = %s
                  AND us.xp_rank IS NOT NULL
                ORDER BY us.total_xp DESC
                LIMIT %s
            """
            # 调试用：查看索引是否生效
            if debug:
                cursor.execute("EXPLAIN " + sql, (snapshot_date, limit))
                print("🔍 EXPLAIN get_top_users:", cursor.fetchall())

            cursor.execute(sql, (snapshot_date, limit))
            rows = cursor.fetchall()
            return [
                {
                    "wallet_address": r["wallet_address"],
                    "total_xp": int(r["total_xp"] or 0),
                    "xp_rank": r["xp_rank"]
                }
                for r in rows
            ]
    finally:
        conn.close()


# ========== 获取每日 XP 增量排行 ==========
def get_top_daily_xp_changes(snapshot_date: date = None, limit: int = 100, debug: bool = False):
    if snapshot_date is None:
        snapshot_date = date.today() - timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT u.wallet_address, uc.xp_change
                FROM users u
                JOIN user_daily_changes uc ON u.id = uc.user_id
                JOIN user_snapshots us ON u.id = us.user_id
                WHERE uc.snapshot_date = %s
                  AND us.snapshot_date = %s
                  AND us.xp_rank IS NOT NULL
                ORDER BY uc.xp_change DESC
                LIMIT %s
            """
            # 调试用：查看索引是否生效
            if debug:
                cursor.execute("EXPLAIN " + sql, (snapshot_date, snapshot_date, limit))
                print("🔍 EXPLAIN get_top_daily_xp_changes:", cursor.fetchall())

            cursor.execute(sql, (snapshot_date, snapshot_date, limit))
            rows = cursor.fetchall()
            return [
                {
                    "wallet_address": r["wallet_address"],
                    "xp_change": int(r["xp_change"] or 0),
                    "rank": idx
                }
                for idx, r in enumerate(rows, start=1)
            ]
    finally:
        conn.close()

def get_new_wallets(snapshot_date: date = None, offset: int = 0, limit: int = 100):
    """
    获取每日新增钱包数据（分页 + 总数） - MySQL 5.7 兼容
    """
    if snapshot_date is None:
        snapshot_date = date.today() - timedelta(days=1)

    yesterday = snapshot_date - timedelta(days=1)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT t.wallet_address, t.total_xp, t.xp_rank, t.snapshot_date, counts.total_count
                FROM (
                    SELECT u.wallet_address,
                           us.total_xp,
                           us.xp_rank,
                           us.snapshot_date,
                           1 AS is_new
                    FROM user_snapshots us
                    JOIN users u ON u.id = us.user_id
                    LEFT JOIN user_snapshots us_prev
                           ON us_prev.user_id = us.user_id
                          AND us_prev.snapshot_date = %s
                    WHERE us.snapshot_date = %s
                      AND us.xp_rank IS NOT NULL
                      AND us.total_xp > 0
                      AND us_prev.user_id IS NULL
                ) t
                JOIN (
                    SELECT COUNT(*) AS total_count
                    FROM user_snapshots us
                    LEFT JOIN user_snapshots us_prev
                           ON us_prev.user_id = us.user_id
                          AND us_prev.snapshot_date = %s
                    WHERE us.snapshot_date = %s
                      AND us.xp_rank IS NOT NULL
                      AND us.total_xp > 0
                      AND us_prev.user_id IS NULL
                ) counts
                ORDER BY t.total_xp DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, (yesterday, snapshot_date, yesterday, snapshot_date, limit, offset))
            rows = cursor.fetchall()

            if not rows:
                return {"total": 0, "items": []}

            total = rows[0]["total_count"]

            items = [
                {
                    "wallet_address": r["wallet_address"],
                    "total_xp": int(r["total_xp"] or 0),
                    "xp_rank": r["xp_rank"],
                    "snapshot_date": r["snapshot_date"]
                }
                for r in rows
            ]

            return {"total": total, "items": items}

    finally:
        conn.close()
