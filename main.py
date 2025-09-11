from fastapi import FastAPI, HTTPException, Query
import crud, schemas
from typing import List

app = FastAPI(title="Demo API")

# ========== Platform Stats ==========
@app.post("/platform-stats/")
def create_platform_stats(stats: schemas.PlatformStatsCreate):
    return crud.create_platform_stats(stats)

@app.get("/platform-stats/", response_model=schemas.PlatformStatsResponse)
def read_platform_stats(date: str = Query(None, description="日期 YYYY-MM-DD, 不填则返回最新数据")):
    stats = crud.get_platform_stats(date)
    if not stats:
        raise HTTPException(status_code=404, detail="Platform stats not found")
    return stats

@app.get("/platform-stats-all", response_model=List[schemas.PlatformStatsResponse])
def read_all_platform_stats():
    """
    获取平台所有统计数据，按日期倒序排序
    """
    stats_list = crud.get_all_platform_stats()
    if not stats_list:
        raise HTTPException(status_code=404, detail="No platform stats found")
    return stats_list

# ======== 用户总排行 ========
@app.get("/global-rank", response_model=List[schemas.UserRank])
def rankings_total(snapshot_date: str = Query(None, description="日期 YYYY-MM-DD, 默认昨天"),
                   limit: int = Query(100, le=500)):
    """用户单日总排行"""
    return crud.get_global_rank(snapshot_date, limit)

# ======== 用户每日新增 XP 排行 ========
@app.get("/daily-rank", response_model=List[schemas.UserDailyXpChange])
def rankings_daily(snapshot_date: str = Query(None, description="日期 YYYY-MM-DD, 默认昨天"),
                   limit: int = Query(100, le=500)):
    """用户每日新增 XP 排行"""
    return crud.get_top_daily_xp_changes(snapshot_date, limit)