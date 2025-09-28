const fs = require("fs");
const readline = require("readline");

async function sumTotalXp(filePath) {
  let totalXpSum = 0;

  // 创建文件流
  const fileStream = fs.createReadStream(filePath);

  // 使用 readline 按行读取
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue; // 跳过空行
    try {
      const obj = JSON.parse(line);
      totalXpSum += obj.xpRank && obj.totalXp || 0;
    } catch (err) {
      console.error("解析失败:", line);
    }
  }

  console.log("✅ Total XP:", totalXpSum);
  return totalXpSum;
}

// 使用示例
sumTotalXp("20250925_leaderboard.json");
