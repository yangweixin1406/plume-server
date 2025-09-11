from typing import Optional, List
from pydantic import BaseModel

class UserOut(BaseModel):
    wallet_address: str
    total_xp: int  # 强制转换为 int
    xp_rank: Optional[int] = None  # xp_rank 可能为空
