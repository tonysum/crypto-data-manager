# 加密货币数据管理系统 (Crypto Data Manager)

一套健壮、全自动的加密货币 K 线数据采集与管理基础设施，专为量化交易设计。

---

## 🚀 核心特性

- **稳健调度系统**：基于 `asyncio.Queue` 的串行任务队列，彻底杜绝瞬间并发导致的 IP 封禁。
- **智能差量补全**：自动对比本地数据库与交易所数据，仅下载缺失片段，支持大跨度时间切片。
- **自动冲突愈合**：内置主键冲突自愈逻辑，遇到重复数据自动跳过，确保程序 24/7 不间断运行。
- **极限制速保护**：严格遵守交易所 API 权重限制，内置静默休眠机制。
- **高性能存储**：采用 PostgreSQL 存储海量 K 线数据，支持多维度查询与完整性校验。
- **现代 Web 界面**：基于 Next.js + TailwindCSS 的可视化控制台，实时监控下载进度与数据库状态。

---

## 🛠 技术文档 (Documentation)

项目包含详细的技术方案演进与开发指南：
- 📄 **[技术方案全景图 (TECH_DESIGN.md)](./TECH_DESIGN.md)**：记录了系统从脚本化到稳健串行架构的演进历程。
- 🤖 **[跨交易所开发提示词 (EXCHANGE_SYSTEM_PROMPT.md)](./EXCHANGE_SYSTEM_PROMPT.md)**：用于快速将本系统克隆到其他交易所的 AI 提示词模板。
- 📖 **[快速开始指南 (QUICKSTART.md)](./QUICKSTART.md)**：更详细的安装与配置手册。

---

## 🚦 快速开始

### 1. 环境准备
复制 `.env.example` 为 `.env` 并配置你的币安 API Key 及数据库连接。

### 2. 一键启动 (推荐)
```bash
./scripts/start.sh
```
你可以选择 **Local Development** (本地开发) 或 **Docker Compose** (容器化) 模式。

### 3. 分步启动

#### 后端服务 (FastAPI)
```bash
cd backend
uv run python main.py
```

#### 自动化调度器 (关键)
```bash
cd backend
pm2 start "uv run python scheduler_klines.py" --name "crypto-scheduler"
```

#### 前端界面 (Next.js)
```bash
cd frontend
pnpm install
pnpm dev
```

---

## 🏗 系统架构

```ascii
[交易所 API] <--> [接入层 (Binance Client)] <--> [逻辑层 (Klines Logic)]
                                                     |
                                                     v
[Web 控制台] <--> [API 门户 (FastAPI)] <--> [存储层 (PostgreSQL)]
                                                     ^
                                                     |
                                          [自动化调度器 (Queue Scheduler)]
```

---

## 🔗 相关资源

- **API 文档**: 启动后访问 `http://localhost:8001/docs`
- **PM2 监控**: 使用 `pm2 logs crypto-scheduler` 实时查看下载节奏。

---
*Powered by Trae IDE & Senior Pair-Programming*
