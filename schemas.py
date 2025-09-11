from pydantic import BaseModel
from typing import Optional
from datetime import date

# ========== Users ==========
class UserCreate(BaseModel):
    wallet_address: str
    referred_by: Optional[str] = None

class UserResponse(BaseModel):
    id: int
    wallet_address: str
    referred_by: Optional[str] = None
    referral_count: int

# ========== User Snapshots ==========
class UserSnapshotCreate(BaseModel):
    user_id: int
    snapshot_date: date
    total_xp: int

class UserSnapshotResponse(BaseModel):
    id: int
    user_id: int
    snapshot_date: date
    total_xp: int

# ========== Platform Stats ==========
class PlatformStatsCreate(BaseModel):
    snapshot_date: date
    total_wallets: int
    total_tvl: float
    total_xp: int

class PlatformStatsResponse(BaseModel):
    id: int
    snapshot_date: date
    total_wallets: int
    total_xp: int
    new_wallets: int
    new_xp: int
    
# ========== 用户总排行 ==========
class UserRank(BaseModel):
    wallet_address: str
    total_xp: int
    xp_rank: Optional[int] = None

# ========== 用户每日新增 ==========
class UserDailyXpChange(BaseModel):
    wallet_address: str
    xp_change: int
    rank: int

