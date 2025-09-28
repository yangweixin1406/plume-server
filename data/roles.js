const fs = require("fs");

// 1. 读取本地 JSON 文件（逐行解析，取前10000条）
const wallets = fs.readFileSync("test.json", "utf-8")
  .split("\n")
  .filter(line => line.trim())
  .slice(0, 10000)  // ✅ 只保留前10000条
  .map(line => JSON.parse(line));

// 2. 检查用户的社交信息
async function checkSocial(wallet) {
  const url = `https://portal-api.plume.org/api/v1/user/social-connections?walletAddress=${wallet.walletAddress}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`请求失败: ${res.status}`);
    }
    const data = await res.json();
    console.log('discord >>>>>>>>', data)
    const discord = data?.data?.discord;
    if (discord?.hasGoonfluencerRole || discord?.hasMoonGoonRole) {
      console.log(`${wallet.walletAddress} ✅ 满足条件`);
      return wallet; // 返回符合条件的钱包
    } else {
      console.log(`${wallet.walletAddress} ❌ 不满足条件`);
      return null;
    }
  } catch (err) {
    console.error(`请求 ${wallet.walletAddress} 出错:`, err.message);
    return null;
  }
}

// 3. 批量并发处理
async function runInBatches(items, batchSize = 10) {
  let qualified = [];

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);

    const results = await Promise.allSettled(
      batch.map(w => checkSocial(w))
    );

    for (const r of results) {
      if (r.status === "fulfilled" && r.value) {
        qualified.push(r.value);
      }
    }
  }

  return qualified;
}

// 4. 主流程
(async () => {
  const qualifiedWallets = await runInBatches(wallets, 10);

  // 保存为 JSON 文件
  fs.writeFileSync(
    "qualified_wallets.json",
    JSON.stringify(qualifiedWallets, null, 2),
    "utf-8"
  );

  console.log(`\n🎯 共找到 ${qualifiedWallets.length} 个满足条件的钱包，已保存到 qualified_wallets.json`);
})();
