"""
数据管理服务配置
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载 .env 文件
backend_dir = Path(__file__).parent
project_root = backend_dir.parent
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

# 数据库配置 - PostgreSQL
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crypto_data")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# 数据库连接URL
# 对密码进行 URL 编码以处理特殊字符
from urllib.parse import quote_plus

if PG_PASSWORD:
    encoded_password = quote_plus(PG_PASSWORD)
    DATABASE_URL = f"postgresql://{PG_USER}:{encoded_password}@{PG_HOST}:{PG_PORT}/{PG_DB}"
else:
    DATABASE_URL = f"postgresql://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# SSL 模式配置（如果需要）
PG_SSLMODE = os.getenv("PG_SSLMODE", "")  # 可选值: disable, allow, prefer, require, verify-ca, verify-full

# 保持向后兼容（已废弃，使用 DATABASE_URL）
DB_PATH = os.getenv("DB_PATH", str(project_root / "data" / "crypto_data.db"))

# 服务配置
SERVICE_PORT = int(os.getenv("DATA_SERVICE_PORT", "8001"))
SERVICE_HOST = os.getenv("DATA_SERVICE_HOST", "0.0.0.0")

# CORS配置
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
    # 添加生产环境域名
]

# 币安API配置
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_BASE_PATH = os.getenv("BINANCE_BASE_PATH", "")

# 币安API网络配置
BINANCE_TIMEOUT = int(os.getenv("BINANCE_TIMEOUT", "30"))  # 连接超时时间（秒），默认30秒
BINANCE_MAX_RETRIES = int(os.getenv("BINANCE_MAX_RETRIES", "3"))  # 最大重试次数，默认3次
BINANCE_RETRY_DELAY = float(os.getenv("BINANCE_RETRY_DELAY", "2.0"))  # 重试延迟（秒），默认2秒
BINANCE_PROXY = os.getenv("BINANCE_PROXY", "")  # 代理设置，格式: http://proxy_host:proxy_port 或 socks5://proxy_host:proxy_port

