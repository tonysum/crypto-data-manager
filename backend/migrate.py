#!/usr/bin/env python3
"""
SQLite 到 PostgreSQL 数据迁移脚本

功能：
1. 从 SQLite 数据库读取所有表和数据
2. 在 PostgreSQL 中创建对应的表结构
3. 迁移数据到 PostgreSQL
4. 支持断点续传（记录已迁移的表）
5. 支持批量迁移和进度显示

使用方法（推荐使用 .env 文件）：
    1. 在项目根目录创建 .env 文件，配置 PostgreSQL 连接信息：
       PG_HOST=localhost
       PG_PORT=5432
       PG_DB=crypto_data
       PG_USER=crypto_user
       PG_PASSWORD=your_password
       SQLITE_PATH=data/crypto_data.db  # 可选
    
    2. 直接运行（会自动从 .env 读取配置）：
       python migrate.py
    
    3. 或使用命令行参数（会覆盖 .env 配置）：
       python migrate.py --pg-host localhost --pg-password your_password
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# 配置日志（先配置，以便后续日志能正常输出）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 🔧 加载 .env 文件（优先使用项目根目录，其次 backend 目录）
backend_dir = Path(__file__).parent
project_root = backend_dir.parent
env_path = project_root / '.env'
if not env_path.exists():
    env_path = backend_dir / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.info(f"✅ 已加载环境变量文件: {env_path}")
else:
    logging.warning(f"⚠️  未找到 .env 文件，将使用环境变量或默认值。查找路径: {project_root / '.env'}")


class SQLiteToPostgreSQLMigrator:
    """SQLite 到 PostgreSQL 数据迁移器"""
    
    def __init__(
        self,
        sqlite_path: str,
        pg_host: str = "localhost",
        pg_port: int = 5432,
        pg_db: str = "crypto_data",
        pg_user: str = "crypto_user",
        pg_password: str = "",
        batch_size: int = 10000
    ):
        """
        初始化迁移器
        
        Args:
            sqlite_path: SQLite 数据库文件路径
            pg_host: PostgreSQL 主机地址
            pg_port: PostgreSQL 端口
            pg_db: PostgreSQL 数据库名
            pg_user: PostgreSQL 用户名
            pg_password: PostgreSQL 密码
            batch_size: 批量插入大小
        """
        self.sqlite_path = sqlite_path
        self.batch_size = batch_size
        self.migrated_tables = set()
        
        # 连接 SQLite
        if not os.path.exists(sqlite_path):
            # 提供更详细的错误信息
            abs_path = os.path.abspath(sqlite_path)
            cwd = os.getcwd()
            raise FileNotFoundError(
                f"SQLite 数据库文件不存在: {sqlite_path}\n"
                f"  绝对路径: {abs_path}\n"
                f"  当前工作目录: {cwd}\n"
                f"  请检查路径是否正确，或使用 --sqlite-path 参数指定完整路径"
            )
        
        self.sqlite_engine = create_engine(f'sqlite:///{sqlite_path}')
        logging.info(f"已连接 SQLite 数据库: {sqlite_path}")
        
        # 连接 PostgreSQL（带重试机制）
        pg_url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        max_retries = 3
        retry_delay = 5  # 秒
        
        for attempt in range(max_retries):
            try:
                self.pg_engine = create_engine(
                    pg_url,
                    poolclass=QueuePool,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True,  # 自动检测并重连断开的连接
                    echo=False,
                    connect_args={
                        "connect_timeout": 10,  # 连接超时10秒
                        "keepalives": 1,
                        "keepalives_idle": 30,
                        "keepalives_interval": 10,
                        "keepalives_count": 5
                    }
                )
                # 测试连接
                with self.pg_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logging.info(f"✅ 已连接 PostgreSQL 数据库: {pg_host}:{pg_port}/{pg_db}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"⚠️  连接 PostgreSQL 失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                    logging.info(f"    {retry_delay} 秒后重试...")
                    import time
                    time.sleep(retry_delay)
                else:
                    raise ConnectionError(
                        f"无法连接到 PostgreSQL (已重试 {max_retries} 次): {e}\n"
                        f"  请检查:\n"
                        f"  1. 数据库服务器是否运行\n"
                        f"  2. 网络连接是否正常\n"
                        f"  3. 连接参数是否正确 (主机: {pg_host}, 端口: {pg_port})"
                    )
    
    def get_sqlite_tables(self) -> List[str]:
        """获取 SQLite 中所有表名"""
        with self.sqlite_engine.connect() as conn:
            result = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ))
            tables = [row[0] for row in result]
        return tables
    
    def get_postgresql_tables(self) -> List[str]:
        """获取 PostgreSQL 中所有表名"""
        with self.pg_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """))
            tables = [row[0] for row in result]
        return tables
    
    def compare_table_counts(self, table_filter: Optional[str] = None) -> Dict:
        """
        对比SQLite和PostgreSQL的表数量
        
        Args:
            table_filter: 表名过滤（可选）
        
        Returns:
            对比结果字典
        """
        # 获取SQLite表列表
        sqlite_tables = self.get_sqlite_tables()
        
        # 应用过滤
        if table_filter:
            if table_filter.startswith('K'):
                sqlite_tables = [t for t in sqlite_tables if t.startswith(table_filter)]
            else:
                sqlite_tables = [t for t in sqlite_tables if table_filter in t]
        
        # 获取PostgreSQL表列表
        pg_tables = self.get_postgresql_tables()
        
        # 应用相同的过滤
        if table_filter:
            if table_filter.startswith('K'):
                pg_tables = [t for t in pg_tables if t.startswith(table_filter)]
            else:
                pg_tables = [t for t in pg_tables if table_filter in t]
        
        sqlite_count = len(sqlite_tables)
        pg_count = len(pg_tables)
        
        # 找出差异
        sqlite_set = set(sqlite_tables)
        pg_set = set(pg_tables)
        
        only_in_sqlite = sorted(sqlite_set - pg_set)
        only_in_pg = sorted(pg_set - sqlite_set)
        common = sorted(sqlite_set & pg_set)
        
        is_consistent = sqlite_count == pg_count and len(only_in_sqlite) == 0 and len(only_in_pg) == 0
        
        return {
            'sqlite_count': sqlite_count,
            'pg_count': pg_count,
            'is_consistent': is_consistent,
            'only_in_sqlite': only_in_sqlite,
            'only_in_pg': only_in_pg,
            'common_count': len(common),
            'sqlite_tables': sorted(sqlite_tables),
            'pg_tables': sorted(pg_tables)
        }
    
    def get_table_schema(self, table_name: str) -> Dict:
        """获取 SQLite 表的架构信息"""
        with self.sqlite_engine.connect() as conn:
            # 获取列信息
            result = conn.execute(text(f'PRAGMA table_info("{table_name}")'))
            columns = []
            for row in result:
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'not_null': row[3],
                    'default_value': row[4],
                    'primary_key': row[5]
                })
            
            # 获取主键信息
            primary_keys = [col['name'] for col in columns if col['primary_key']]
            
            # 获取行数
            count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            row_count = count_result.scalar()
        
        return {
            'columns': columns,
            'primary_keys': primary_keys,
            'row_count': row_count
        }
    
    def sqlite_type_to_postgresql(self, sqlite_type: str, column_name: str = None) -> str:
        """
        将 SQLite 数据类型转换为 PostgreSQL 数据类型
        
        Args:
            sqlite_type: SQLite 数据类型
            column_name: 列名（用于特殊处理，如 has_added_position）
        """
        sqlite_type_upper = sqlite_type.upper()
        
        # 🔧 特殊处理：某些列名暗示应该是boolean类型
        if column_name and 'has_added_position' in column_name.lower():
            # 如果列名包含 has_added_position，且SQLite中是INTEGER，检查PostgreSQL中是否已经是boolean
            # 这里先返回INTEGER，如果PostgreSQL中已经是boolean，会在数据迁移时转换
            pass  # 继续正常处理
        
        # SQLite 类型映射到 PostgreSQL
        type_mapping = {
            'INTEGER': 'BIGINT',
            'REAL': 'DOUBLE PRECISION',
            'TEXT': 'TEXT',
            'BLOB': 'BYTEA',
            'NUMERIC': 'NUMERIC',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP'
        }
        
        # 处理带长度的类型（如 VARCHAR(255)）
        if '(' in sqlite_type:
            base_type = sqlite_type_upper.split('(')[0].strip()
            length = sqlite_type.split('(')[1].split(')')[0]
            if base_type in ['VARCHAR', 'CHAR']:
                return f'VARCHAR({length})'
            elif base_type == 'DECIMAL':
                return f'NUMERIC({length})'
        
        # 处理常见类型
        for sqlite_key, pg_type in type_mapping.items():
            if sqlite_key in sqlite_type_upper:
                return pg_type
        
        # 默认返回 TEXT
        return 'TEXT'
    
    def create_postgresql_table(self, table_name: str, schema: Dict) -> bool:
        """在 PostgreSQL 中创建表"""
        # PostgreSQL 表名需要用引号包裹（保持大小写）
        safe_table_name = f'"{table_name}"'
        
        try:
            with self.pg_engine.connect() as conn:
                # 使用事务确保操作的原子性
                trans = conn.begin()
                try:
                    # 使用系统表检查表是否存在（更安全，不会导致事务失败）
                    # PostgreSQL 中，如果表名用引号创建，会保持大小写；否则会转换为小写
                    # 所以需要检查两种情况：原始大小写和小写
                    check_sql = text("""
                        SELECT EXISTS (
                            SELECT 1 
                            FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND (table_name = :table_name OR table_name = LOWER(:table_name))
                        )
                    """)
                    result = conn.execute(check_sql, {"table_name": table_name})
                    table_exists = result.scalar()
                    
                    if table_exists:
                        trans.commit()
                        logging.info(f"表 {table_name} 已存在，跳过创建")
                        return True
                    
                    # 构建 CREATE TABLE 语句
                    column_defs = []
                    for col in schema['columns']:
                        pg_type = self.sqlite_type_to_postgresql(col['type'], col['name'])
                        col_def = f'"{col["name"]}" {pg_type}'
                        
                        if col['not_null']:
                            col_def += ' NOT NULL'
                        
                        if col['default_value'] is not None:
                            default_val = col['default_value']
                            # 处理默认值
                            if isinstance(default_val, str):
                                if default_val.upper() == 'CURRENT_TIMESTAMP':
                                    col_def += ' DEFAULT CURRENT_TIMESTAMP'
                                else:
                                    # 转义单引号
                                    escaped_val = default_val.replace("'", "''")
                                    col_def += f" DEFAULT '{escaped_val}'"
                            else:
                                col_def += f' DEFAULT {default_val}'
                        
                        column_defs.append(col_def)
                    
                    # 添加主键约束
                    if schema['primary_keys']:
                        pk_cols = ', '.join([f'"{pk}"' for pk in schema['primary_keys']])
                        column_defs.append(f'PRIMARY KEY ({pk_cols})')
                    
                    create_sql = f"""
                        CREATE TABLE {safe_table_name} (
                            {', '.join(column_defs)}
                        );
                    """
                    
                    conn.execute(text(create_sql))
                    trans.commit()
                    logging.info(f"✅ 已创建表: {table_name}")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    error_msg = str(e).lower()
                    # 检查是否是连接错误
                    if ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                        'host is down' in error_msg or 'could not receive data' in error_msg or 
                        'operation timed out' in error_msg or 'server closed' in error_msg):
                        raise ConnectionError(f"创建表时连接失败: {e}")
                    # 其他错误
                    logging.error(f"❌ 创建表 {table_name} 失败: {e}")
                    import traceback
                    logging.debug(traceback.format_exc())
                    return False
        except ConnectionError:
            # 重新抛出连接错误
            raise
        except Exception as e:
            error_msg = str(e).lower()
            # 检查是否是连接错误
            if ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                'host is down' in error_msg or 'could not receive data' in error_msg or 
                'operation timed out' in error_msg or 'server closed' in error_msg):
                raise ConnectionError(f"创建表时连接失败: {e}")
            # 其他错误
            logging.error(f"❌ 创建表 {table_name} 失败: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            return False
    
    def migrate_table_data(self, table_name: str, schema: Dict) -> int:
        """迁移表数据（支持断点续传）"""
        safe_table_name = f'"{table_name}"'
        row_count = schema['row_count']
        
        if row_count == 0:
            logging.info(f"表 {table_name} 为空，跳过数据迁移")
            return 0
        
        # 🔧 检查已迁移的行数（断点续传）
        already_migrated = 0
        try:
            with self.pg_engine.connect() as conn:
                try:
                    count_result = conn.execute(text(f'SELECT COUNT(*) FROM {safe_table_name}'))
                    already_migrated = count_result.scalar()
                    if already_migrated > 0:
                        logging.info(f"📊 表 {table_name} 已存在 {already_migrated:,} 行数据，将从断点继续迁移...")
                except Exception as e:
                    # 表不存在或无法查询，从头开始
                    error_msg = str(e).lower()
                    # 如果是连接错误，立即抛出
                    if 'connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 'host is down' in error_msg:
                        raise ConnectionError(f"检查已迁移数据时连接失败: {e}")
                    # 其他错误（如表不存在），从头开始
                    already_migrated = 0
        except ConnectionError:
            # 重新抛出连接错误
            raise
        except Exception as e:
            error_msg = str(e).lower()
            # 检查是否是连接错误
            if 'connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 'host is down' in error_msg:
                raise ConnectionError(f"检查已迁移数据时连接失败: {e}")
            # 其他错误，从头开始
            logging.warning(f"⚠️  检查已迁移数据时出错: {e}，将从头开始迁移")
            already_migrated = 0
        
        if already_migrated >= row_count:
            logging.info(f"✅ 表 {table_name} 数据已完全迁移 ({already_migrated:,}/{row_count:,} 行)，跳过")
            return already_migrated
        
        logging.info(f"开始迁移表 {table_name}，共 {row_count:,} 行 (已迁移: {already_migrated:,}, 剩余: {row_count - already_migrated:,})...")
        
        # 分批读取和插入（从断点继续）
        migrated_rows = already_migrated
        offset = already_migrated
        
        while offset < row_count:
            try:
                # 从 SQLite 读取数据
                with self.sqlite_engine.connect() as sqlite_conn:
                    query = f'SELECT * FROM "{table_name}" LIMIT {self.batch_size} OFFSET {offset}'
                    df = pd.read_sql(query, sqlite_conn)
                
                if df.empty:
                    break
                
                # 🔧 数据类型转换：检查PostgreSQL表结构，转换需要的数据类型
                with self.pg_engine.connect() as pg_conn:
                    # 获取PostgreSQL表的列信息
                    column_info = pg_conn.execute(text("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = :table_name
                    """), {"table_name": table_name})
                    
                    pg_columns = {row[0]: row[1] for row in column_info}
                    
                    # 转换boolean类型的列（SQLite中可能是integer，需要转换为boolean）
                    for col_name, pg_type in pg_columns.items():
                        if pg_type == 'boolean' and col_name in df.columns:
                            # 将integer (0/1) 转换为boolean (False/True)
                            if df[col_name].dtype in ['int64', 'int32', 'int', 'Int64']:
                                # 将0转换为False，1转换为True，其他值保持原样或转换为None
                                df[col_name] = df[col_name].apply(
                                    lambda x: bool(x) if pd.notna(x) else None
                                )
                            elif df[col_name].dtype in ['float64', 'float32', 'float']:
                                # 处理浮点数类型（可能是NaN）
                                df[col_name] = df[col_name].apply(
                                    lambda x: bool(int(x)) if pd.notna(x) else None
                                )
                            elif df[col_name].dtype == 'object':
                                # 处理可能为None或字符串的情况
                                df[col_name] = df[col_name].apply(
                                    lambda x: bool(int(x)) if pd.notna(x) and str(x).isdigit() else (bool(x) if pd.notna(x) and x != '' else None)
                                )
                            logging.debug(f"已将列 {col_name} 从 {df[col_name].dtype} 转换为 boolean")
                
                # 写入 PostgreSQL
                # 使用 to_sql 批量插入（会自动提交）
                df.to_sql(
                    name=table_name,
                    con=self.pg_engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                migrated_rows += len(df)
                offset += self.batch_size
                
                if migrated_rows % 10000 == 0 or offset >= row_count:
                    progress_pct = (migrated_rows / row_count * 100) if row_count > 0 else 0
                    logging.info(f"  进度: {migrated_rows:,}/{row_count:,} 行 ({progress_pct:.1f}%)...")
            
            except Exception as e:
                error_msg = str(e).lower()
                # 检查是否是因为重复数据（可能是并发插入或部分数据已存在）
                if 'duplicate key' in error_msg or 'unique constraint' in error_msg:
                    logging.warning(f"  ⚠️  检测到重复数据 (offset={offset})，跳过当前批次...")
                    # 跳过当前批次，继续下一批
                    offset += self.batch_size
                    continue
                # 检查是否是连接错误
                elif ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                      'host is down' in error_msg or 'could not receive data' in error_msg or 
                      'operation timed out' in error_msg or 'server closed' in error_msg):
                    logging.error(f"❌ 数据库连接错误 (offset={offset}): {e}")
                    logging.info(f"   已迁移 {migrated_rows:,}/{row_count:,} 行")
                    logging.info(f"   请检查数据库连接，重新运行程序将从断点继续迁移")
                    raise ConnectionError(f"数据库连接失败: {e}")
                else:
                    logging.error(f"❌ 迁移表 {table_name} 数据失败 (offset={offset}): {e}")
                    raise
        
        logging.info(f"✅ 表 {table_name} 迁移完成，共迁移 {migrated_rows:,} 行 (本次新增: {migrated_rows - already_migrated:,} 行)")
        return migrated_rows
    
    def migrate_table(self, table_name: str, skip_existing: bool = True) -> bool:
        """迁移单个表（包括结构和数据）"""
        # 检查表是否已迁移
        if skip_existing:
            safe_table_name = f'"{table_name}"'
            try:
                with self.pg_engine.connect() as conn:
                    try:
                        # 检查表是否存在
                        check_sql = text("""
                            SELECT EXISTS (
                                SELECT 1 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND (table_name = :table_name OR table_name = LOWER(:table_name))
                            )
                        """)
                        table_exists = conn.execute(check_sql, {"table_name": table_name}).scalar()
                        
                        if table_exists:
                            # 获取已迁移的行数
                            count_result = conn.execute(text(f'SELECT COUNT(*) FROM {safe_table_name}'))
                            pg_row_count = count_result.scalar()
                            
                            # 获取源表的行数
                            schema = self.get_table_schema(table_name)
                            sqlite_row_count = schema['row_count']
                            
                            if pg_row_count >= sqlite_row_count:
                                logging.info(f"⏭️  表 {table_name} 已完全迁移 ({pg_row_count:,}/{sqlite_row_count:,} 行)，跳过")
                                return True
                            else:
                                logging.info(f"🔄 表 {table_name} 部分迁移 ({pg_row_count:,}/{sqlite_row_count:,} 行)，将继续迁移剩余数据")
                    except Exception as e:
                        # 检查是否是连接错误
                        error_msg = str(e).lower()
                        if ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                            'host is down' in error_msg or 'could not receive data' in error_msg or 
                            'operation timed out' in error_msg or 'server closed' in error_msg):
                            raise ConnectionError(f"检查表状态时连接失败: {e}")
                        # 表不存在或查询失败，继续迁移
                        logging.debug(f"检查表 {table_name} 时出错: {e}，将创建新表")
            except ConnectionError:
                # 重新抛出连接错误
                raise
            except Exception as e:
                # 检查是否是连接错误
                error_msg = str(e).lower()
                if ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                    'host is down' in error_msg or 'could not receive data' in error_msg or 
                    'operation timed out' in error_msg or 'server closed' in error_msg):
                    raise ConnectionError(f"检查表状态时连接失败: {e}")
                # 其他错误，记录但继续尝试
                logging.warning(f"⚠️  检查表 {table_name} 状态时出错: {e}，将继续尝试迁移")
        
        try:
            # 获取表结构
            schema = self.get_table_schema(table_name)
            
            # 创建表
            if not self.create_postgresql_table(table_name, schema):
                return False
            
            # 迁移数据
            self.migrate_table_data(table_name, schema)
            
            self.migrated_tables.add(table_name)
            return True
        
        except Exception as e:
            logging.error(f"❌ 迁移表 {table_name} 失败: {e}")
            return False
    
    def migrate_all(
        self, 
        table_filter: Optional[str] = None,
        table_names: Optional[List[str]] = None,
        skip_existing: bool = True
    ) -> Dict:
        """
        迁移所有表
        
        Args:
            table_filter: 表名过滤字符串（支持前缀匹配或包含匹配）
            table_names: 指定要迁移的表名列表（精确匹配，优先级高于table_filter）
            skip_existing: 是否跳过已存在的表
        """
        # 获取所有表
        all_tables = self.get_sqlite_tables()
        
        # 如果指定了表名列表，优先使用精确匹配
        if table_names:
            # 精确匹配指定的表名
            filtered_tables = []
            for table_name in table_names:
                if table_name in all_tables:
                    filtered_tables.append(table_name)
                else:
                    logging.warning(f"⚠️  表 '{table_name}' 不存在于SQLite数据库中，跳过")
            all_tables = filtered_tables
            logging.info(f"指定迁移 {len(all_tables)} 个表: {', '.join(all_tables)}")
        # 否则使用过滤字符串
        elif table_filter:
            if table_filter.startswith('K'):
                # K线表过滤：匹配以该前缀开头的所有表
                all_tables = [t for t in all_tables if t.startswith(table_filter)]
                logging.info(f"使用前缀过滤 '{table_filter}'，找到 {len(all_tables)} 个表")
            else:
                # 包含匹配：匹配包含该字符串的所有表
                all_tables = [t for t in all_tables if table_filter in t]
                logging.info(f"使用包含过滤 '{table_filter}'，找到 {len(all_tables)} 个表")
        
        total_tables = len(all_tables)
        if total_tables == 0:
            logging.warning("⚠️  没有找到需要迁移的表")
            return {
                'total_tables': 0,
                'success_count': 0,
                'fail_count': 0,
                'total_rows': 0,
                'duration_seconds': 0
            }
        
        logging.info(f"找到 {total_tables} 个表需要迁移")
        
        success_count = 0
        fail_count = 0
        total_rows = 0
        
        start_time = datetime.now()
        
        for i, table_name in enumerate(all_tables, 1):
            logging.info(f"\n[{i}/{total_tables}] 处理表: {table_name}")
            
            try:
                if self.migrate_table(table_name, skip_existing=skip_existing):
                    success_count += 1
                    schema = self.get_table_schema(table_name)
                    total_rows += schema['row_count']
                else:
                    fail_count += 1
            except ConnectionError as e:
                # 连接错误：停止迁移，提示用户修复连接后重新运行
                logging.error(f"❌ 处理表 {table_name} 时数据库连接失败: {e}")
                logging.warning(f"\n⚠️  迁移已停止在表: {table_name} ({i}/{total_tables})")
                logging.info(f"💡 已成功迁移 {success_count} 个表，失败 {fail_count} 个表")
                logging.info(f"💡 修复数据库连接后，重新运行程序将从断点继续迁移")
                raise  # 重新抛出异常，让主函数处理
            except ConnectionError:
                # 连接错误：立即停止迁移
                logging.error(f"❌ 处理表 {table_name} 时数据库连接失败")
                logging.warning(f"\n⚠️  迁移已停止在表: {table_name} ({i}/{total_tables})")
                logging.info(f"💡 已成功迁移 {success_count} 个表，失败 {fail_count} 个表")
                logging.info(f"💡 修复数据库连接后，重新运行程序将从断点继续迁移")
                raise  # 重新抛出异常，让主函数处理
            except Exception as e:
                error_msg = str(e).lower()
                # 检查是否是连接相关错误
                if ('connection' in error_msg or 'network' in error_msg or 'timeout' in error_msg or 
                    'host is down' in error_msg or 'could not receive data' in error_msg or 
                    'operation timed out' in error_msg or 'server closed' in error_msg):
                    logging.error(f"❌ 处理表 {table_name} 时发生连接错误: {e}")
                    logging.warning(f"\n⚠️  迁移已停止在表: {table_name} ({i}/{total_tables})")
                    logging.info(f"💡 已成功迁移 {success_count} 个表，失败 {fail_count} 个表")
                    logging.info(f"💡 修复数据库连接后，重新运行程序将从断点继续迁移")
                    raise ConnectionError(f"数据库连接失败: {e}")
                else:
                    # 其他错误：记录但继续处理下一个表
                    logging.error(f"❌ 处理表 {table_name} 时发生错误: {e}")
                    fail_count += 1
                    # 继续处理下一个表
                    continue
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            'total_tables': total_tables,
            'success_count': success_count,
            'fail_count': fail_count,
            'total_rows': total_rows,
            'duration_seconds': duration
        }


def main():
    """主函数"""
    # 🔧 获取项目路径和 .env 路径（与文件开头保持一致）
    backend_dir = Path(__file__).parent
    project_root = backend_dir.parent
    env_path = project_root / '.env'
    if not env_path.exists():
        env_path = backend_dir / '.env'
    
    # 🔧 优先从 .env 文件读取配置（已在文件开头加载）
    pg_host = os.getenv('PG_HOST', '')
    pg_port = int(os.getenv('PG_PORT', ''))
    pg_db = os.getenv('PG_DB', '')
    pg_user = os.getenv('PG_USER', '')
    pg_password = os.getenv('PG_PASSWORD', '')
    sqlite_path_env = os.getenv('SQLITE_PATH', '')
    
    parser = argparse.ArgumentParser(
        description='SQLite 到 PostgreSQL 数据迁移工具（优先使用 .env 文件配置）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
配置优先级（从高到低）：
  1. 命令行参数
  2. .env 文件（{'已找到: ' + str(env_path) if env_path.exists() else '未找到'}）
  3. 默认值

当前配置（从 .env 或默认值）：
  SQLite 路径: {sqlite_path_env or 'data/crypto_data.db (默认)'}
  PostgreSQL 主机: {pg_host}
  PostgreSQL 端口: {pg_port}
  PostgreSQL 数据库: {pg_db}
  PostgreSQL 用户: {pg_user}
  PostgreSQL 密码: {'已设置' if pg_password else '未设置（需要提供）'}
        """
    )
    
    parser.add_argument(
        '--sqlite-path',
        type=str,
        default=sqlite_path_env if sqlite_path_env else None,
        help=f'SQLite 数据库文件路径（默认: {sqlite_path_env or "data/crypto_data.db"} 或从 .env 读取）'
    )
    parser.add_argument(
        '--pg-host',
        type=str,
        default=pg_host,
        help=f'PostgreSQL 主机地址（默认: {pg_host}，从 .env 或默认值）'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        default=pg_port,
        help=f'PostgreSQL 端口（默认: {pg_port}，从 .env 或默认值）'
    )
    parser.add_argument(
        '--pg-db',
        type=str,
        default=pg_db,
        help=f'PostgreSQL 数据库名（默认: {pg_db}，从 .env 或默认值）'
    )
    parser.add_argument(
        '--pg-user',
        type=str,
        default=pg_user,
        help=f'PostgreSQL 用户名（默认: {pg_user}，从 .env 或默认值）'
    )
    parser.add_argument(
        '--pg-password',
        type=str,
        default=pg_password,
        help='PostgreSQL 密码（默认: 从 .env 读取，如果未设置则提示输入）'
    )
    parser.add_argument(
        '--table-filter',
        type=str,
        default=None,
        help='表名过滤（例如: K1d 只迁移日线表，K5m 只迁移5分钟表，backtrade 匹配包含backtrade的表）'
    )
    parser.add_argument(
        '--tables',
        type=str,
        nargs='+',
        default=None,
        help='指定要迁移的表名列表（精确匹配，多个表名用空格分隔，例如: --tables backtrade_records K1dBTCUSDT）'
    )
    parser.add_argument(
        '--table-file',
        type=str,
        default=None,
        help='从文件读取要迁移的表名列表（每行一个表名）'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='批量插入大小（默认: 10000）'
    )
    parser.add_argument(
        '--no-skip-existing',
        action='store_true',
        help='不跳过已存在的表（会重新迁移）'
    )
    parser.add_argument(
        '--compare-only',
        action='store_true',
        help='仅对比SQLite和PostgreSQL的表数量，不执行迁移'
    )
    
    args = parser.parse_args()
    
    # 确定 SQLite 路径（确保使用绝对路径）
    if not args.sqlite_path:
        args.sqlite_path = str(project_root / "data" / "crypto_data.db")
    else:
        # 如果路径是相对路径，转换为绝对路径（相对于项目根目录）
        sqlite_path_obj = Path(args.sqlite_path)
        if not sqlite_path_obj.is_absolute():
            # 相对路径：先尝试相对于项目根目录，如果不存在则相对于当前工作目录
            abs_path = project_root / args.sqlite_path
            if not abs_path.exists():
                # 如果项目根目录下不存在，尝试相对于当前工作目录
                abs_path = Path(args.sqlite_path).resolve()
            args.sqlite_path = str(abs_path)
    
    # 显示配置信息
    logging.info("=" * 80)
    logging.info("数据迁移配置")
    logging.info("=" * 80)
    # 确保路径是绝对路径并显示
    sqlite_abs_path = os.path.abspath(args.sqlite_path)
    logging.info(f"SQLite 数据库: {args.sqlite_path}")
    logging.info(f"SQLite 绝对路径: {sqlite_abs_path}")
    logging.info(f"PostgreSQL 主机: {args.pg_host}:{args.pg_port}")
    logging.info(f"PostgreSQL 数据库: {args.pg_db}")
    logging.info(f"PostgreSQL 用户: {args.pg_user}")
    
    # 检查配置来源
    config_source = []
    if env_path.exists():
        config_source.append(f".env 文件 ({env_path})")
    # 检查是否使用了非默认值（可能是从 .env 或命令行参数）
    if args.pg_host != 'localhost' or args.pg_port != 5432 or args.pg_db != 'crypto_data' or args.pg_user != 'crypto_user':
        if not env_path.exists():
            config_source.append("环境变量或命令行参数")
    if not config_source:
        config_source.append("默认值")
    
    logging.info(f"配置来源: {', '.join(config_source)}")
    
    # 如果没有提供密码，提示输入
    if not args.pg_password:
        import getpass
        logging.warning("⚠️  PostgreSQL 密码未在 .env 文件中设置")
        args.pg_password = getpass.getpass("请输入 PostgreSQL 密码: ")
    else:
        logging.info("✅ PostgreSQL 密码已从 .env 文件加载")
    
    # 创建迁移器
    try:
        migrator = SQLiteToPostgreSQLMigrator(
            sqlite_path=args.sqlite_path,
            pg_host=args.pg_host,
            pg_port=args.pg_port,
            pg_db=args.pg_db,
            pg_user=args.pg_user,
            pg_password=args.pg_password,
            batch_size=args.batch_size
        )
    except Exception as e:
        logging.error(f"初始化迁移器失败: {e}")
        sys.exit(1)
    
    # 处理表名列表
    table_names = None
    if args.tables:
        # 从命令行参数获取表名列表
        table_names = args.tables
        logging.info(f"从命令行参数指定了 {len(table_names)} 个表: {', '.join(table_names)}")
    elif args.table_file:
        # 从文件读取表名列表
        table_file_path = Path(args.table_file)
        if not table_file_path.is_absolute():
            # 相对路径：先尝试相对于项目根目录
            table_file_path = project_root / args.table_file
            if not table_file_path.exists():
                # 如果项目根目录下不存在，尝试相对于当前工作目录
                table_file_path = Path(args.table_file).resolve()
        
        if not table_file_path.exists():
            logging.error(f"❌ 表名列表文件不存在: {table_file_path}")
            sys.exit(1)
        
        try:
            with open(table_file_path, 'r', encoding='utf-8') as f:
                table_names = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            logging.info(f"从文件 {table_file_path} 读取了 {len(table_names)} 个表名")
        except Exception as e:
            logging.error(f"❌ 读取表名列表文件失败: {e}")
            sys.exit(1)
    
    # 如果同时指定了 --tables/--table-file 和 --table-filter，提示用户
    if (args.tables or args.table_file) and args.table_filter:
        logging.warning("⚠️  同时指定了 --tables/--table-file 和 --table-filter，将优先使用 --tables/--table-file（精确匹配）")
    
    # 如果只是对比表数量，执行对比后退出
    if args.compare_only:
        logging.info("=" * 80)
        logging.info("对比SQLite和PostgreSQL的表数量")
        logging.info("=" * 80)
        
        try:
            comparison = migrator.compare_table_counts(table_filter=args.table_filter)
            
            logging.info("\n" + "=" * 80)
            logging.info("对比结果")
            logging.info("=" * 80)
            logging.info(f"SQLite 表数量: {comparison['sqlite_count']}")
            logging.info(f"PostgreSQL 表数量: {comparison['pg_count']}")
            logging.info(f"共同表数量: {comparison['common_count']}")
            
            if comparison['is_consistent']:
                logging.info("✅ 表数量一致！")
            else:
                logging.warning("⚠️  表数量不一致！")
                
                if comparison['only_in_sqlite']:
                    logging.info(f"\n仅在SQLite中的表 ({len(comparison['only_in_sqlite'])} 个):")
                    for table in comparison['only_in_sqlite'][:20]:  # 只显示前20个
                        logging.info(f"  - {table}")
                    if len(comparison['only_in_sqlite']) > 20:
                        logging.info(f"  ... 还有 {len(comparison['only_in_sqlite']) - 20} 个表")
                
                if comparison['only_in_pg']:
                    logging.info(f"\n仅在PostgreSQL中的表 ({len(comparison['only_in_pg'])} 个):")
                    for table in comparison['only_in_pg'][:20]:  # 只显示前20个
                        logging.info(f"  - {table}")
                    if len(comparison['only_in_pg']) > 20:
                        logging.info(f"  ... 还有 {len(comparison['only_in_pg']) - 20} 个表")
            
            # 返回适当的退出码
            sys.exit(0 if comparison['is_consistent'] else 1)
            
        except Exception as e:
            logging.error(f"\n❌ 对比表数量时发生错误: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            sys.exit(1)
    
    # 执行迁移
    logging.info("=" * 80)
    logging.info("开始数据迁移")
    logging.info("=" * 80)
    
    try:
        results = migrator.migrate_all(
            table_filter=args.table_filter,
            table_names=table_names,
            skip_existing=not args.no_skip_existing
        )
        
        # 输出统计信息
        logging.info("\n" + "=" * 80)
        logging.info("迁移完成！")
        logging.info("=" * 80)
        logging.info(f"总表数: {results['total_tables']}")
        logging.info(f"成功: {results['success_count']}")
        logging.info(f"失败: {results['fail_count']}")
        logging.info(f"总行数: {results['total_rows']:,}")
        logging.info(f"耗时: {results['duration_seconds']:.2f} 秒")
        
        if results['fail_count'] > 0:
            logging.warning(f"⚠️  有 {results['fail_count']} 个表迁移失败，请检查日志")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logging.warning("\n⚠️  用户中断迁移")
        logging.info("💡 提示: 重新运行程序将从断点继续迁移（已迁移的表和数据会被跳过）")
        sys.exit(1)
    except ConnectionError as e:
        logging.error(f"\n❌ 数据库连接错误: {e}")
        logging.info("\n💡 处理建议:")
        logging.info("  1. 检查数据库服务器是否运行")
        logging.info("  2. 检查网络连接是否正常")
        logging.info("  3. 检查连接参数是否正确")
        logging.info("  4. 修复连接问题后，重新运行程序将从断点继续迁移")
        sys.exit(1)
    except Exception as e:
        logging.error(f"\n❌ 迁移过程中发生错误: {e}")
        import traceback
        logging.debug(traceback.format_exc())
        error_msg = str(e).lower()
        if 'connection' in error_msg or 'network' in error_msg:
            logging.info("\n💡 这可能是数据库连接问题，修复后重新运行程序将从断点继续迁移")
        sys.exit(1)


if __name__ == "__main__":
    main()
