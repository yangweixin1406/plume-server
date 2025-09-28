const fs = require("fs");

// 1. 读取本地 JSON 文件（逐行解析）
const wallets = fs.readFileSync("20250926_leaderboard.json", "utf-8")
  .split("\n")
  .filter(line => line.trim())
  .map(line => JSON.parse(line))
  .filter(_ => _.totalXp > 10000 && !!_.xpRank)

// 2. 定义函数，请求 API 并返回是否符合条件
async function checkWallet(walletAddress) {
  const url = `https://portal-api.plume.org/api/v1/stats/dailySpinData?walletAddress=${walletAddress}`;
  try {
    const res = await fetch(url); // Node v20 内置 fetch
    if (!res.ok) {
      throw new Error(`请求失败: ${res.status}`);
    }
    const data = await res.json();
    const length = data?.data?.spinHistory?.length || 0;

    if (length >= 100) {
      console.log(`${walletAddress} ✅ spinHistory = ${length} （计入统计）`);
      return 1;
    } else {
      console.log(`${walletAddress} ❌ spinHistory = ${length} （不计入）`);
      return 0;
    }
  } catch (err) {
    console.error(`请求 ${walletAddress} 出错:`, err.message);
    return 0;
  }
}

// 3. 批量并发处理函数
async function runInBatches(items, batchSize = 10) {
  let total = 0;

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);

    // 并发执行一批请求
    const results = await Promise.allSettled(
      batch.map(w => checkWallet(w.walletAddress))
    );

    // 累加结果
    for (const r of results) {
      if (r.status === "fulfilled") {
        total += r.value;
      }
    }
  }

  return total;
}

// 4. 主流程
(async () => {
  const total = await runInBatches(wallets, 800); // 每批 10 个
  console.log(`\n🎯 最终结果：共有 ${total} 个钱包 spinHistory >= 100`);
})();
