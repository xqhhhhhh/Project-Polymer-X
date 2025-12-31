# Project-Polymer-X

本项目实现了测试题中的完整数据工程链路：海量抓取 → 解析清洗 → Schema 对齐 → 训练数据构建。以下按评分点逐条说明“如何满足要求”。

## 1. 爬虫模块（Spider）
目标：从公开网站批量获取材料数据，并保存 HTML。

对应要求与实现：
- **高并发架构**：`spider/crawler.py` 支持并发抓取，`--concurrency` 可配置（默认 16），并配合 `DOWNLOAD_DELAY` 实现吞吐与礼貌抓取平衡。
- **动态渲染（AJAX/JS）**：支持 `--use-playwright` 启用 Scrapy-Playwright 下载处理器，展示可抓取 JS 渲染页面。
- **断点续传**：默认开启 `resume`，已下载文件会跳过；`--no-resume` 可强制重抓。
- **代理池策略**：`spider/middlewares.py` 提供 `ProxyPoolMiddleware`，从 `--proxy-pool` 或 `PROXY_POOL` 环境变量随机选择代理；403/429 自动清理代理并重试。
- **翻页抓取**：搜索结果超过 50 条时自动翻页继续抓取。
- **落盘 HTML**：详情页 HTML 以 URL 编码文件名保存到 `data/html_pages/`。

运行示例：
```bash
python spider/crawler.py --query "Polyethylene ExxonMobil" --count 50 --out-dir data/html_pages

# 使用代理池
python spider/crawler.py --proxy-pool "http://1.2.3.4:8000,http://5.6.7.8:8000"

# 启用动态渲染
python spider/crawler.py --use-playwright --query "Polyethylene ExxonMobil" --count 50
```

## 2. 解析模块（Parser）
目标：解析本地 PDF 与网络 HTML，处理复杂表格，并对齐 Schema。

对应要求与实现：
- **PDF 表格复原**：`parser/pdf_extractor.py` 使用 `pdfplumber.extract_tables()` 做结构化表格恢复，并兼容文本行回退解析。
- **ExxonMobil 双列英制/公制**：表格解析优先提取 Metric 列；行解析则通过单位优先级筛选公制。
- **Shell 中英混排**：针对中英文混排与空白列结构做兜底逻辑。
- **HTML 解析复用引擎**：`parser/html_cleaner.py` 将 `<tr>` 拍平成文本行，复用 PDF 的“文本行解析 + 正则提取 + 校验”逻辑。
- **Schema 对齐**：`PROPERTY_MAP` 统一字段命名（Density/密度等），输出统一 JSON。
- **单位换算**：psi→MPa，°F→°C，单位归一化处理。

运行示例：
```bash
python parser/pdf_extractor.py --input-dir data_src --out data/pdf_data.json --dirty-log data/dirty_data.log
python parser/html_cleaner.py --input-dir data/html_pages --out data/html_data.json
```

## 3. 数据清洗与校验（ETL）
目标：防止“毒数据”进入训练集。

对应要求与实现：
- **数值范围校验**：`validate_value_with_reason()` 对密度、熔指、温度、伸长率做范围过滤。
- **异常可观测性**：脏数据会写入 `data/dirty_data.log`（JSONL），不再静默丢弃。

## 4. 大模型训练数据构建（LLM Data Formatting）
目标：生成符合 Alpaca 格式的 JSONL，并做自然语言扩写。

对应要求与实现：
- **数据增强**：`parser/sft_builder.py` 使用多模板 + 物性描述扩写。
- **伪专家推理**：在输出末尾追加“专家推理”段，基于密度/熔指/强度/伸长率组合推理。
- **格式输出**：生成 `data/train.jsonl`（Alpaca 格式）。

运行示例：
```bash
python parser/sft_builder.py --pdf data/pdf_data.json --html data/html_data.json \
  --merged-out data/merged_data.json --out data/train.jsonl --count 100
```

## 5. 系统设计
分布式架构方案见 `system_design.md`，覆盖 Scrapy-Redis、Kafka、Spark、Airflow、对象存储、监控与数据质量等组件。

## 目录结构
```
spider/
  crawler.py           # Scrapy 爬虫（并发/翻页/断点续传/可选 Playwright）
  middlewares.py       # UA 轮换 + 代理池 + 403/429 重试
parser/
  pdf_extractor.py     # PDF 表格解析 + Schema 对齐 + 脏数据日志
  html_cleaner.py      # HTML 表格解析（复用 PDF 引擎）
  sft_builder.py       # Alpaca JSONL 生成 + 专家推理
data/
  html_pages/          # 爬取 HTML
  pdf_data.json        # PDF 解析输出
  html_data.json       # HTML 解析输出
  merged_data.json     # 融合后结构化数据
  train.jsonl          # 最终训练集
system_design.md       # 分布式爬虫与清洗架构图
```

## 环境依赖
```bash
python -m pip install -r requirements.txt
```
