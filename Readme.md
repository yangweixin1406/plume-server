## PLUME 拉取LeaderBoard数据

### 安装插件
pip install -r requirements.txt

### 数据操作
cd ./data

* 拉取数据
python fetch_data.py

* 插入数据库
修改 insert_data.py json_file为对应的文件名
python insert_data.py

### 启动方法服务
uvicorn main:app --reload

### 已经实现的接口
1. /platform-stats 平台每日数据汇总
2. /platform-stats-all 平台汇总数据列表
3. /global-rank  用户pp排行
4. /daily-rank  单日新增pp排行