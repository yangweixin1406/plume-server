import pymysql

# 数据库连接配置
DB_CONFIG = {
    "host": "127.0.0.1",   # 数据库地址
    "user": "root",        # 用户名
    "password": "123456",  # 密码
    "port": 3306,          # 端口
    "charset": "utf8mb4"
}

DB_NAME = "plume_db"


# 建表语句
TABLES = {}

TABLES["users"] = """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    wallet_address VARCHAR(100) NOT NULL UNIQUE,
    referred_by VARCHAR(100) NULL,
    referral_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_wallet_address (wallet_address)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

TABLES["user_snapshots"] = """
CREATE TABLE IF NOT EXISTS user_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,

    bridged_total DECIMAL(30,10) DEFAULT 0,
    swap_volume DECIMAL(30,10) DEFAULT 0,
    swap_count BIGINT DEFAULT 0,
    tvl_total_usd DECIMAL(30,10) DEFAULT 0,
    real_tvl_usd DECIMAL(30,10) DEFAULT 0,
    protocols_used INT DEFAULT 0,
    longest_swap_streak_weeks INT DEFAULT 0,
    adjustment_points BIGINT DEFAULT 0,
    protectors_points BIGINT DEFAULT 0,
    badge_points BIGINT DEFAULT 0,
    user_self_xp BIGINT DEFAULT 0,
    referral_bonus_xp BIGINT DEFAULT 0,
    total_xp BIGINT DEFAULT 0,
    xp_rank INT NULL,
    longest_tvl_streak INT DEFAULT 0,
    plume_staking_points BIGINT DEFAULT 0,
    plume_staking_bonus BIGINT DEFAULT 0,
    plume_staking_total_tokens BIGINT DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_user_date (user_id, snapshot_date),
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_user_date (user_id, snapshot_date),
    INDEX idx_total_xp_date (snapshot_date, total_xp DESC),
    INDEX idx_xp_rank_date (snapshot_date, xp_rank),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

TABLES["user_daily_changes"] = """
CREATE TABLE IF NOT EXISTS user_daily_changes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    xp_change BIGINT DEFAULT 0,
    tvl_change DECIMAL(30,10) DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_user_date (user_id, snapshot_date),
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_user_date (user_id, snapshot_date),
    INDEX idx_xp_change_date (snapshot_date, xp_change DESC),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

TABLES["platform_stats"] = """
CREATE TABLE IF NOT EXISTS platform_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL,

    total_wallets BIGINT DEFAULT 0,
    total_xp BIGINT DEFAULT 0,

    new_wallets BIGINT DEFAULT 0,
    new_xp BIGINT DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uniq_date (snapshot_date),
    INDEX idx_snapshot_date (snapshot_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def create_database_and_tables():
    # 先连接到 MySQL，不指定数据库
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 创建数据库（如果不存在）
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8mb4'")
    conn.commit()
    cursor.close()
    conn.close()

    # 重新连接到目标数据库
    conn = pymysql.connect(**DB_CONFIG, database=DB_NAME)
    cursor = conn.cursor()

    # 创建表
    for name, ddl in TABLES.items():
        print(f"Creating table {name}...")
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 数据库和数据表初始化完成！")


if __name__ == "__main__":
    create_database_and_tables()
