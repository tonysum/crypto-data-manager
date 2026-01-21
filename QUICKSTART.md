# 快速开始指南

## 🚀 快速启动

### 1. 配置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env 文件，填入你的配置
# 至少需要配置币安API密钥（如果需要下载数据）
```

### 2. 启动后端服务

#### 方法 A: 使用 uv（推荐，更快）

```bash
cd backend

# 安装依赖（会自动创建虚拟环境）
uv pip install -r requirements.txt

# 启动服务
uv run python main.py
```

#### 方法 B: 使用传统方式

```bash
cd backend

# 创建虚拟环境（推荐）
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 启动服务
python main.py
```

> 💡 **提示**: 如果还没有安装 `uv`，可以运行 `curl -LsSf https://astral.sh/uv/install.sh | sh` 安装（macOS/Linux）或查看 [UV_USAGE.md](./backend/UV_USAGE.md) 了解更多。

后端服务将在 http://localhost:8001 启动

API文档：http://localhost:8001/docs

### 3. 启动前端应用

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端应用将在 http://localhost:3001 启动

## 🐳 使用Docker（推荐）

### 启动所有服务

```bash
# 确保已创建 .env 文件
cp .env.example .env
# 编辑 .env 文件

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

## 📝 环境变量说明

### 必需配置

- `BINANCE_API_KEY` - 币安API密钥（下载数据时需要）
- `BINANCE_API_SECRET` - 币安API密钥（下载数据时需要）

### 网络配置（可选，如果遇到连接问题）

- `BINANCE_TIMEOUT` - 连接超时时间（秒，默认30）
- `BINANCE_MAX_RETRIES` - 最大重试次数（默认3）
- `BINANCE_RETRY_DELAY` - 重试延迟（秒，默认2.0）
- `BINANCE_PROXY` - 代理设置（格式: `http://proxy_host:proxy_port` 或 `socks5://proxy_host:proxy_port`）

> 💡 **提示**: 如果遇到连接超时错误，请查看 [BINANCE_NETWORK_TROUBLESHOOTING.md](./backend/BINANCE_NETWORK_TROUBLESHOOTING.md)

### 可选配置

- `PG_HOST` - PostgreSQL 主机地址（默认：`localhost`）
- `PG_PORT` - PostgreSQL 端口（默认：`5432`）
- `PG_DB` - PostgreSQL 数据库名（默认：`crypto_data`）
- `PG_USER` - PostgreSQL 用户名（默认：`postgres`）
- `PG_PASSWORD` - PostgreSQL 密码（必需）
- `DATA_SERVICE_PORT` - 后端服务端口（默认：8001）
- `NEXT_PUBLIC_API_URL` - 前端API地址（默认：`http://localhost:8001`）

## ✅ 验证安装

1. **检查后端服务**
   - 访问 http://localhost:8001/docs
   - 应该能看到API文档页面

2. **检查前端应用**
   - 访问 http://localhost:3001
   - 应该能看到数据管理界面

3. **测试API**
   ```bash
   # 获取本地交易对列表
   curl http://localhost:8001/api/symbols?interval=1d
   ```

## 🔧 常见问题

### 问题1: 端口被占用

**错误：** `Address already in use`

**解决：** 
- 修改 `.env` 中的 `DATA_SERVICE_PORT`
- 或停止占用端口的进程

### 问题2: 无法连接 PostgreSQL 数据库

**错误：** `无法连接到 PostgreSQL` 或 `connection refused`

**解决：**
1. 确保 PostgreSQL 服务正在运行
2. 检查 `.env` 文件中的 PostgreSQL 配置是否正确
3. 确保数据库已创建：`CREATE DATABASE crypto_data;`
4. 检查 PostgreSQL 用户权限和密码
5. 检查防火墙设置，确保端口 5432 可访问

### 问题3: 前端无法连接后端

**错误：** CORS错误或连接失败

**解决：**
1. 检查后端服务是否运行
2. 检查 `backend/config.py` 中的 `ALLOWED_ORIGINS`
3. 检查前端 `lib/api-config.ts` 中的API地址

## 📚 下一步

- 查看 [README.md](./README.md) 了解完整功能
- 查看 [API文档](http://localhost:8001/docs) 了解API接口
- 开始下载和管理你的K线数据！
