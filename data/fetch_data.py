import os
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple
import requests
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://portal-api.plume.org/api/v1/stats/leaderboard"
COUNT_PER_REQUEST = 5000

# 并发与容错参数
MAX_WORKERS = 8                 # 并发线程数（建议 4~10）
WINDOW_PAGES = 20               # 每个窗口拉取多少页（总窗口量 = WINDOW_PAGES * COUNT_PER_REQUEST）
HTTP_MAX_RETRIES = 5            # requests 的底层重试（连接/5xx）
LOGIC_MAX_RETRIES = 3           # 单页逻辑重试（JSON 解析/空响应等）
TIMEOUT = 20                    # 单次请求超时时间（秒）
BACKOFF_BASE = 0.8              # 逻辑重试回退基数（指数退避）

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "leaderboard-fetcher/1.0"
}

def make_session() -> requests.Session:
    """创建带 HTTP 重试的 Session。"""
    s = requests.Session()
    retries = Retry(
        total=HTTP_MAX_RETRIES,
        connect=HTTP_MAX_RETRIES,
        read=HTTP_MAX_RETRIES,
        status=HTTP_MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def fetch_one_page(session: requests.Session, offset: int) -> Tuple[int, List[dict]]:
    """获取单页（带逻辑重试）。返回 (offset, leaderboard_list)。"""
    params = {
        "offset": offset,
        "count": COUNT_PER_REQUEST,
        "walletAddress": "",
        "overrideDay1Override": "false",
        "preview": "false"
    }
    for attempt in range(1, LOGIC_MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            leaderboard = data.get("data", {}).get("leaderboard", [])
            # leaderboard 应该是 list；否则视为异常并重试
            if not isinstance(leaderboard, list):
                raise ValueError("Unexpected payload structure: 'leaderboard' is not a list")
            print(f"[OK] offset={offset} -> {len(leaderboard)} 条")
            return offset, leaderboard
        except Exception as e:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"[WARN] offset={offset} 第{attempt}/{LOGIC_MAX_RETRIES}次失败：{e}，{wait:.1f}s后重试")
            time.sleep(wait)
    # 所有重试失败，返回空列表并记录
    print(f"[ERROR] offset={offset} 多次失败，返回空结果以避免阻塞")
    return offset, []

def write_jsonl_append(filename: str, rows: List[dict]):
    with open(filename, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        f.flush()

def load_progress(progress_file: str) -> int:
    """读取已完成写入的最后 offset。若无进度则返回 0。"""
    if not os.path.exists(progress_file):
        return 0
    try:
        with open(progress_file, "r") as pf:
            val = int(pf.read().strip())
            # 保护：必须是 COUNT_PER_REQUEST 的倍数
            if val % COUNT_PER_REQUEST == 0 and val >= 0:
                return val
    except Exception:
        pass
    return 0

def save_progress(progress_file: str, last_written_offset: int):
    with open(progress_file, "w") as pf:
        pf.write(str(last_written_offset))

def fetch_leaderboard_concurrent_windowed():
    today = datetime.now().strftime("%Y%m%d")
    out_file = f"{today}_leaderboard.json"   # JSONL
    progress_file = f"{today}_leaderboard.progress"

    # 断点续抓：如果已有输出文件，继续 append；从 progress 读取下一个 offset
    next_offset = load_progress(progress_file)
    last_written_offset = max(0, next_offset - COUNT_PER_REQUEST)
    print(f"[INFO] 断点续抓：从 offset={next_offset} 开始（上次完成到 {last_written_offset}）")

    # 去重集合（防止接口变动导致的重复）
    seen_wallets = set()

    # 如果是续抓且文件存在，可以选择是否把已写入的钱包加载到内存去重
    # 为避免大文件占内存，这里默认不回读历史，只对本次运行期间去重
    # ——如果一定要全量去重，可以自行遍历 out_file 先加载 walletAddress。

    session = make_session()
    done = False

    # 确保输出文件存在（append 模式）
    if not os.path.exists(out_file):
        open(out_file, "w", encoding="utf-8").close()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        while not done:
            # 构造当前窗口的连续 offsets
            offsets = list(range(next_offset,
                                 next_offset + WINDOW_PAGES * COUNT_PER_REQUEST,
                                 COUNT_PER_REQUEST))

            # 提交任务
            future_map = {
                pool.submit(fetch_one_page, session, off): off for off in offsets
            }

            # 收集结果
            results: Dict[int, List[dict]] = {}
            for fut in as_completed(future_map):
                off = future_map[fut]
                o, page = fut.result()
                results[o] = page

            # 窗口必须完整收到所有页（若个别页空，说明失败/最后一页）
            # 统一按 offset 升序写入，保持顺序不乱
            for off in sorted(results.keys()):
                page = results[off]

                # 如果这一页是真失败（多次失败后仍空），我们不直接跳过：
                # 这里再给一次“窗口级别”的补救重试（避免单次逻辑重试全部失败）
                if len(page) == 0:
                    print(f"[RETRY] offset={off} 触发窗口级别补救重试")
                    _, page = fetch_one_page(session, off)

                # 仍为空则中止，避免出现缺页（你也可以改成 continue 跳过，但会造成数据缺失）
                if len(page) == 0:
                    print(f"[FATAL] offset={off} 页面多次失败，停止以避免数据缺失。你可重试运行以续抓。")
                    done = True
                    break

                # 去重写入
                to_write = []
                for item in page:
                    # 假定 walletAddress 是唯一键；若接口另有唯一键，请替换这里
                    wa = item.get("walletAddress")
                    if wa is None:
                        # 如果某些记录没有 walletAddress，可选择：直接写入或跳过
                        # 这里选择直接写入（极少数）
                        to_write.append(item)
                    else:
                        if wa not in seen_wallets:
                            seen_wallets.add(wa)
                            to_write.append(item)

                write_jsonl_append(out_file, to_write)
                last_written_offset = off
                save_progress(progress_file, last_written_offset + COUNT_PER_REQUEST)

                print(f"[WRITE] offset={off} 写入 {len(to_write)} 条（原页 {len(page)} 条）")

                # 判断是否最后一页
                if len(page) < COUNT_PER_REQUEST:
                    print(f"[DONE] 检测到最后一页：offset={off}，size={len(page)}")
                    done = True
                    break

            # 准备下一个窗口
            next_offset = last_written_offset + COUNT_PER_REQUEST

    print(f"[OK] 全部完成，文件：{out_file}")
    # 成功后可删除进度文件（保留也行，方便追加）
    # try: os.remove(progress_file)
    # except OSError: pass


if __name__ == "__main__":
    fetch_leaderboard_concurrent_windowed()
