# 加密货币数据管理系统

## 功能特性

- K线数据下载和管理
- 数据查询和检索
- 数据完整性检查
- 数据修复和重检
- 使用 PostgreSQL 数据库存储数据

## 快速开始

### 后端

#### 使用 uv（推荐）

```bash
cd backend
uv pip install -r requirements.txt  # 安装依赖
uv run python main.py               # 运行服务
```

#### 使用传统方式

```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

> 📖 更多 uv 使用方法请查看 [backend/UV_USAGE.md](./backend/UV_USAGE.md)

### 前端

```bash
cd frontend
npm install
npm run dev
```

## API文档

启动后端服务后，访问 http://localhost:8001/docs 查看API文档。

## 环境配置

复制 `.env.example` 为 `.env` 并配置相关参数。
