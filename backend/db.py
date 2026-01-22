from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
import logging

# 从配置文件获取数据库连接
try:
    from config import DATABASE_URL
except ImportError:
    # 如果config模块不可用，使用环境变量构建连接URL
    import os
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = int(os.getenv("PG_PORT", "5432"))
    PG_DB = os.getenv("PG_DB", "crypto_data")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "")
    
    if PG_PASSWORD:
        DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    else:
        DATABASE_URL = f"postgresql://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# 创建 PostgreSQL 数据库引擎
# 检查是否需要 SSL 连接
connect_args = {
    "connect_timeout": 10,  # 连接超时10秒
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5
}

# 如果环境变量要求 SSL，添加 SSL 参数
import os
if os.getenv("PG_SSLMODE"):
    connect_args["sslmode"] = os.getenv("PG_SSLMODE")
elif "192.168" in DATABASE_URL or "localhost" not in DATABASE_URL.lower():
    # 对于远程连接，尝试使用 prefer SSL 模式
    # 如果服务器不支持 SSL，会自动降级到非 SSL
    connect_args["sslmode"] = "prefer"

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # 自动检测并重连断开的连接
    echo=False,
    connect_args=connect_args
)


# 1. 查询表是否存在，没有则创建
def create_table(table_name):
    """创建K线数据表（如果不存在）"""
    with engine.connect() as conn:
        # PostgreSQL 使用 information_schema 查询表是否存在
        # 注意：PostgreSQL中，如果表名用引号创建，会保持大小写；否则会转换为小写
        # 所以需要检查两种情况：原始大小写和小写
        result = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND (table_name = :table_name OR table_name = LOWER(:table_name))
                );
            """),
            {"table_name": table_name}
        )
        
        table_exists = result.fetchone()[0]

        if not table_exists:
            # PostgreSQL 表创建语句
            # 注意：PostgreSQL 使用 SERIAL 而不是 AUTOINCREMENT
            # REAL 在 PostgreSQL 中是单精度，使用 DOUBLE PRECISION 或 NUMERIC
            text_create = f"""
            CREATE TABLE "{table_name}" (
                trade_date VARCHAR(50) PRIMARY KEY,
                open_time BIGINT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                close_time BIGINT,
                quote_volume DOUBLE PRECISION,
                trade_count INTEGER,
                active_buy_volume DOUBLE PRECISION,
                active_buy_quote_volume DOUBLE PRECISION,
                reserved_field TEXT,
                diff DOUBLE PRECISION,
                pct_chg DOUBLE PRECISION
            );
            """
            conn.execute(text(text_create))
            conn.commit()
            logging.info(f"Table '{table_name}' created successfully.")
        return table_exists
    
# 2. 删除表    
def delete_table(table_name):
    """删除指定的表"""
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}";'))
        conn.commit()
        logging.info(f"Table '{table_name}' deleted successfully.")


# 3. 创建交易记录表（用于回测结果存储）
def create_trade_table():
    """创建交易记录表"""
    table_name = 'backtrade_records'
    with engine.connect() as conn:
        # PostgreSQL 使用 information_schema 查询表是否存在
        result = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                );
            """),
            {"table_name": table_name}
        )
        table_exists = result.fetchone()[0]
        
        if not table_exists:
            # PostgreSQL 表创建语句
            # 使用 SERIAL 或 BIGSERIAL 作为自增主键
            text_create = f"""
            CREATE TABLE "{table_name}" (
                id BIGSERIAL PRIMARY KEY,
                entry_date VARCHAR(50) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                entry_pct_chg DOUBLE PRECISION,
                position_size DOUBLE PRECISION NOT NULL,
                leverage INTEGER NOT NULL,
                exit_date VARCHAR(50),
                exit_price DOUBLE PRECISION,
                exit_reason TEXT,
                profit_loss DOUBLE PRECISION,
                profit_loss_pct DOUBLE PRECISION,
                max_profit DOUBLE PRECISION,
                max_loss DOUBLE PRECISION,
                hold_hours INTEGER,
                has_added_position INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(text_create))
            conn.commit()
            logging.info(f"交易记录表 '{table_name}' 创建成功")
        else:
            # 检查是否需要添加 has_added_position 字段
            # PostgreSQL 使用 information_schema.columns 查询列信息
            result = conn.execute(
                text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name;
                """),
                {"table_name": table_name}
            )
            columns = [row[0] for row in result.fetchall()]
            if 'has_added_position' not in columns:
                logging.info(f"添加 has_added_position 字段到表 '{table_name}'")
                conn.execute(
                    text(f'ALTER TABLE "{table_name}" ADD COLUMN has_added_position INTEGER DEFAULT 0;')
                )
                conn.commit()
            logging.info(f"交易记录表 '{table_name}' 已存在")
        
        return table_exists


# 4. 创建交易对表
def create_symbols_table():
    """创建交易对表（如果不存在）"""
    from symbols import create_symbols_table as _create_symbols_table
    return _create_symbols_table()
