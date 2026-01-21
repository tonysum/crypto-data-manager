"""
PostgreSQL 到 PostgreSQL 数据迁移脚本

功能：
1. 从本地 PostgreSQL 数据库导出数据
2. 导入到云服务器 PostgreSQL 数据库
3. 支持表过滤、数据验证、增量迁移等

使用方法：
1. 使用 pg_dump/pg_restore（推荐，速度快）：
   python migrate_pg_to_pg.py --method dump

2. 使用 Python 脚本（更灵活，支持过滤）：
   python migrate_pg_to_pg.py --method python

3. 只迁移特定表：
   python migrate_pg_to_pg.py --tables K1dBTCUSDT K1dETHUSDT

4. 只迁移K线数据表：
   python migrate_pg_to_pg.py --table-filter K1d

5. 比较两个数据库的表数量：
   python migrate_pg_to_pg.py --compare-only
"""

import os
import sys
import logging
import argparse
import subprocess
import time
from pathlib import Path
from typing import List, Optional, Dict, Set
from datetime import datetime
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, DisconnectionError
from dotenv import load_dotenv

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 加载 .env 文件（优先使用项目根目录，其次 backend 目录）
backend_dir = Path(__file__).parent
env_path = project_root / '.env'
if not env_path.exists():
    env_path = backend_dir / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.info(f"✅ 已加载 .env 文件: {env_path}")
else:
    logging.warning(f"⚠️  未找到 .env 文件，将使用环境变量或默认值。查找路径: {project_root / '.env'}")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class PostgreSQLToPostgreSQLMigrator:
    """PostgreSQL 到 PostgreSQL 数据迁移器"""
    
    def __init__(
        self,
        source_host: str,
        source_port: int,
        source_db: str,
        source_user: str,
        source_password: str,
        target_host: str,
        target_port: int,
        target_db: str,
        target_user: str,
        target_password: str,
        batch_size: int = 10000
    ):
        """
        初始化迁移器
        
        Args:
            source_host: 源数据库主机地址
            source_port: 源数据库端口
            source_db: 源数据库名
            source_user: 源数据库用户名
            source_password: 源数据库密码
            target_host: 目标数据库主机地址
            target_port: 目标数据库端口
            target_db: 目标数据库名
            target_user: 目标数据库用户名
            target_password: 目标数据库密码
            batch_size: 批量插入大小
        """
        self.batch_size = batch_size
        self.migrated_tables = set()
        
        # 构建连接URL（对密码进行URL编码以处理特殊字符）
        if source_password:
            encoded_source_password = quote_plus(source_password)
            source_url = f"postgresql://{source_user}:{encoded_source_password}@{source_host}:{source_port}/{source_db}"
        else:
            source_url = f"postgresql://{source_user}@{source_host}:{source_port}/{source_db}"
        
        if target_password:
            encoded_target_password = quote_plus(target_password)
            target_url = f"postgresql://{target_user}:{encoded_target_password}@{target_host}:{target_port}/{target_db}"
        else:
            target_url = f"postgresql://{target_user}@{target_host}:{target_port}/{target_db}"
        
        # 连接源数据库
        logging.info(f"正在连接源数据库: {source_host}:{source_port}/{source_db}")
        self.source_engine = create_engine(
            source_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5
            }
        )
        
        # 测试源数据库连接
        try:
            with self.source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info(f"✅ 已连接源数据库: {source_host}:{source_port}/{source_db}")
        except Exception as e:
            raise ConnectionError(f"无法连接到源数据库: {e}")
        
        # 连接目标数据库
        logging.info(f"正在连接目标数据库: {target_host}:{target_port}/{target_db}")
        self.target_engine = create_engine(
            target_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5
            }
        )
        
        # 测试目标数据库连接
        try:
            with self.target_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info(f"✅ 已连接目标数据库: {target_host}:{target_port}/{target_db}")
        except Exception as e:
            raise ConnectionError(f"无法连接到目标数据库: {e}")
        
        # 保存连接信息（用于 pg_dump）
        self.source_config = {
            'host': source_host,
            'port': source_port,
            'db': source_db,
            'user': source_user,
            'password': source_password
        }
        self.target_config = {
            'host': target_host,
            'port': target_port,
            'db': target_db,
            'user': target_user,
            'password': target_password
        }
    
    def get_source_tables(self) -> List[str]:
        """获取源数据库中所有表名"""
        with self.source_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
        return tables
    
    def get_target_tables(self) -> List[str]:
        """获取目标数据库中所有表名"""
        with self.target_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
        return tables
    
    def get_table_row_count(self, engine, table_name: str) -> int:
        """获取表的行数"""
        try:
            safe_table_name = f'"{table_name}"'
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {safe_table_name}'))
                return result.fetchone()[0]
        except Exception as e:
            logging.warning(f"获取表 {table_name} 行数失败: {e}")
            return 0
    
    def compare_table_counts(self, table_filter: Optional[str] = None) -> Dict:
        """对比源数据库和目标数据库的表数量"""
        source_tables = self.get_source_tables()
        target_tables = self.get_target_tables()
        
        if table_filter:
            if table_filter.startswith('K'):
                source_tables = [t for t in source_tables if t.startswith(table_filter)]
                target_tables = [t for t in target_tables if t.startswith(table_filter)]
            else:
                source_tables = [t for t in source_tables if table_filter in t]
                target_tables = [t for t in target_tables if table_filter in t]
        
        source_count = len(source_tables)
        target_count = len(target_tables)
        
        # 找出差异
        source_set = set(source_tables)
        target_set = set(target_tables)
        
        only_in_source = sorted(source_set - target_set)
        only_in_target = sorted(target_set - source_set)
        common = sorted(source_set & target_set)
        
        # 对比共同表的行数
        row_count_diff = {}
        for table in common:
            source_rows = self.get_table_row_count(self.source_engine, table)
            target_rows = self.get_table_row_count(self.target_engine, table)
            if source_rows != target_rows:
                row_count_diff[table] = {
                    'source': source_rows,
                    'target': target_rows,
                    'diff': source_rows - target_rows
                }
        
        is_consistent = (
            source_count == target_count and 
            len(only_in_source) == 0 and 
            len(only_in_target) == 0 and
            len(row_count_diff) == 0
        )
        
        return {
            'source_count': source_count,
            'target_count': target_count,
            'is_consistent': is_consistent,
            'only_in_source': only_in_source,
            'only_in_target': only_in_target,
            'common_count': len(common),
            'row_count_diff': row_count_diff,
            'source_tables': sorted(source_tables),
            'target_tables': sorted(target_tables)
        }
    
    def migrate_table_schema(self, table_name: str) -> bool:
        """迁移表结构（如果不存在）"""
        try:
            # 检查目标表是否存在
            with self.target_engine.connect() as conn:
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
            
            if table_exists:
                logging.info(f"表 {table_name} 已存在于目标数据库")
                return True
            
            # 从源数据库获取表结构
            inspector = inspect(self.source_engine)
            columns = inspector.get_columns(table_name)
            primary_keys = inspector.get_pk_constraint(table_name)
            
            # 构建 CREATE TABLE 语句
            safe_table_name = f'"{table_name}"'
            column_defs = []
            
            for col in columns:
                col_name = col['name']
                col_type = str(col['type'])
                
                # 转换数据类型（如果需要）
                if 'VARCHAR' in col_type or 'TEXT' in col_type:
                    pg_type = 'TEXT'
                elif 'INTEGER' in col_type or 'INT' in col_type:
                    pg_type = 'INTEGER'
                elif 'BIGINT' in col_type:
                    pg_type = 'BIGINT'
                elif 'REAL' in col_type or 'FLOAT' in col_type or 'DOUBLE' in col_type:
                    pg_type = 'DOUBLE PRECISION'
                elif 'BOOLEAN' in col_type:
                    pg_type = 'BOOLEAN'
                elif 'TIMESTAMP' in col_type or 'DATETIME' in col_type:
                    pg_type = 'TIMESTAMP'
                else:
                    pg_type = col_type
                
                nullable = 'NULL' if col.get('nullable', True) else 'NOT NULL'
                column_defs.append(f'"{col_name}" {pg_type} {nullable}')
            
            # 添加主键约束
            if primary_keys.get('constrained_columns'):
                pk_cols = ', '.join([f'"{col}"' for col in primary_keys['constrained_columns']])
                column_defs.append(f'PRIMARY KEY ({pk_cols})')
            
            create_sql = f'CREATE TABLE {safe_table_name} (\n    ' + ',\n    '.join(column_defs) + '\n);'
            
            # 在目标数据库创建表
            with self.target_engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logging.info(f"✅ 已创建表结构: {table_name}")
            return True
            
        except Exception as e:
            logging.error(f"❌ 迁移表结构失败 {table_name}: {e}")
            return False
    
    def migrate_table_data(
        self,
        table_name: str,
        skip_existing: bool = False
    ) -> Dict:
        """迁移表数据"""
        safe_table_name = f'"{table_name}"'
        
        try:
            # 检查目标表是否存在
            with self.target_engine.connect() as conn:
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
                if not result.fetchone()[0]:
                    # 先迁移表结构
                    if not self.migrate_table_schema(table_name):
                        return {'success': False, 'rows': 0, 'error': '无法创建表结构'}
            
            # 获取源表行数
            source_rows = self.get_table_row_count(self.source_engine, table_name)
            if source_rows == 0:
                logging.info(f"表 {table_name} 没有数据，跳过")
                return {'success': True, 'rows': 0}
            
            logging.info(f"开始迁移表 {table_name}，共 {source_rows} 行")
            
            # 如果跳过已存在的数据，先获取目标表中已存在的键
            existing_keys = set()
            if skip_existing:
                try:
                    # 假设主键是 trade_date（K线表）或 id（其他表）
                    with self.target_engine.connect() as conn:
                        result = conn.execute(text(f'SELECT trade_date FROM {safe_table_name}'))
                        existing_keys = {row[0] for row in result.fetchall()}
                    logging.info(f"目标表中已有 {len(existing_keys)} 条记录")
                except:
                    # 如果没有 trade_date 列，尝试 id
                    try:
                        with self.target_engine.connect() as conn:
                            result = conn.execute(text(f'SELECT id FROM {safe_table_name}'))
                            existing_keys = {row[0] for row in result.fetchall()}
                    except:
                        pass
            
            # 分批读取和插入数据
            migrated_rows = 0
            offset = 0
            max_retries = 3
            retry_delay = 2.0  # 初始重试延迟（秒）
            
            while offset < source_rows:
                # 从源数据库读取一批数据（带重试）
                limit = min(self.batch_size, source_rows - offset)
                df = None
                
                for retry in range(max_retries + 1):
                    try:
                        with self.source_engine.connect() as conn:
                            query = f'SELECT * FROM {safe_table_name} ORDER BY trade_date LIMIT {limit} OFFSET {offset}'
                            df = pd.read_sql(query, conn)
                        break  # 成功读取，跳出重试循环
                    except (OperationalError, DisconnectionError) as e:
                        error_msg = str(e).lower()
                        is_network_error = (
                            'connection' in error_msg or 
                            'network' in error_msg or 
                            'timeout' in error_msg or
                            'could not translate host' in error_msg or
                            'could not receive data' in error_msg or
                            'server closed' in error_msg or
                            'connection refused' in error_msg
                        )
                        
                        if is_network_error and retry < max_retries:
                            wait_time = retry_delay * (2 ** retry)  # 指数退避
                            logging.warning(f"  网络错误（尝试 {retry + 1}/{max_retries + 1}）: {str(e)[:100]}")
                            logging.info(f"  等待 {wait_time:.1f} 秒后重试...")
                            time.sleep(wait_time)
                            continue
                        else:
                            # 不是网络错误或已达到最大重试次数
                            raise
                
                if df is None or df.empty:
                    break
                
                # 过滤已存在的数据
                if skip_existing and existing_keys:
                    if 'trade_date' in df.columns:
                        df = df[~df['trade_date'].isin(existing_keys)]
                    elif 'id' in df.columns:
                        df = df[~df['id'].isin(existing_keys)]
                
                if not df.empty:
                    # 插入到目标数据库（带重试）
                    for retry in range(max_retries + 1):
                        try:
                            df.to_sql(
                                name=table_name,
                                con=self.target_engine,
                                if_exists='append',
                                index=False,
                                method='multi',
                                chunksize=min(1000, len(df))
                            )
                            migrated_rows += len(df)
                            
                            # 更新已存在的键集合
                            if skip_existing:
                                if 'trade_date' in df.columns:
                                    existing_keys.update(df['trade_date'].tolist())
                                elif 'id' in df.columns:
                                    existing_keys.update(df['id'].tolist())
                            break  # 成功插入，跳出重试循环
                        except (OperationalError, DisconnectionError) as e:
                            error_msg = str(e).lower()
                            is_network_error = (
                                'connection' in error_msg or 
                                'network' in error_msg or 
                                'timeout' in error_msg or
                                'could not translate host' in error_msg or
                                'could not receive data' in error_msg or
                                'server closed' in error_msg or
                                'connection refused' in error_msg
                            )
                            
                            if is_network_error and retry < max_retries:
                                wait_time = retry_delay * (2 ** retry)  # 指数退避
                                logging.warning(f"  网络错误（尝试 {retry + 1}/{max_retries + 1}）: {str(e)[:100]}")
                                logging.info(f"  等待 {wait_time:.1f} 秒后重试...")
                                time.sleep(wait_time)
                                continue
                            else:
                                # 不是网络错误或已达到最大重试次数
                                raise
                
                offset += limit
                
                if offset % (self.batch_size * 10) == 0 or offset >= source_rows:
                    logging.info(f"  进度: {offset}/{source_rows} ({offset*100//source_rows}%)，已迁移: {migrated_rows} 行")
            
            logging.info(f"✅ 表 {table_name} 迁移完成，共迁移 {migrated_rows} 行")
            return {'success': True, 'rows': migrated_rows}
            
        except (OperationalError, DisconnectionError) as e:
            error_msg = str(e).lower()
            is_network_error = (
                'connection' in error_msg or 
                'network' in error_msg or 
                'timeout' in error_msg or
                'could not translate host' in error_msg or
                'could not receive data' in error_msg or
                'server closed' in error_msg or
                'connection refused' in error_msg
            )
            
            logging.error(f"❌ 迁移表 {table_name} 数据失败: {e}")
            if is_network_error:
                logging.warning(f"⚠️  这是网络连接错误（已迁移 {migrated_rows} 行，进度: {offset}/{source_rows}）")
                logging.info(f"💡 修复网络连接后，使用 --skip-existing 参数重新运行将从断点继续迁移")
            import traceback
            logging.error(traceback.format_exc())
            return {'success': False, 'rows': migrated_rows, 'error': str(e), 'is_network_error': is_network_error}
        except Exception as e:
            logging.error(f"❌ 迁移表 {table_name} 数据失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return {'success': False, 'rows': migrated_rows, 'error': str(e)}
    
    def migrate_all(
        self,
        table_filter: Optional[str] = None,
        tables: Optional[List[str]] = None,
        table_file: Optional[str] = None,
        skip_existing: bool = False
    ) -> Dict:
        """迁移所有表"""
        # 获取要迁移的表列表
        if tables:
            tables_to_migrate = tables
        elif table_file:
            with open(table_file, 'r') as f:
                tables_to_migrate = [line.strip() for line in f if line.strip()]
        else:
            tables_to_migrate = self.get_source_tables()
            if table_filter:
                if table_filter.startswith('K'):
                    tables_to_migrate = [t for t in tables_to_migrate if t.startswith(table_filter)]
                else:
                    tables_to_migrate = [t for t in tables_to_migrate if table_filter in t]
        
        logging.info(f"准备迁移 {len(tables_to_migrate)} 个表")
        
        stats = {
            'total': len(tables_to_migrate),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total_rows': 0
        }
        
        for i, table_name in enumerate(tables_to_migrate, 1):
            logging.info(f"\n[{i}/{len(tables_to_migrate)}] 迁移表: {table_name}")
            
            result = self.migrate_table_data(table_name, skip_existing=skip_existing)
            
            if result['success']:
                stats['success'] += 1
                stats['total_rows'] += result['rows']
                if result['rows'] == 0:
                    stats['skipped'] += 1
            else:
                stats['failed'] += 1
                # 如果是网络错误，记录并提示
                if result.get('is_network_error'):
                    logging.warning(f"⚠️  表 {table_name} 因网络错误中断，已迁移 {result['rows']} 行")
                    logging.info(f"💡 修复网络后使用 --skip-existing 重新运行可继续迁移此表")
        
        logging.info("\n" + "=" * 80)
        logging.info("迁移完成！")
        logging.info("=" * 80)
        logging.info(f"总表数: {stats['total']}")
        logging.info(f"✓ 成功: {stats['success']}")
        logging.info(f"✗ 失败: {stats['failed']}")
        logging.info(f"○ 跳过（无数据）: {stats['skipped']}")
        logging.info(f"总迁移行数: {stats['total_rows']}")
        logging.info("=" * 80)
        
        return stats
    
    def migrate_with_pg_dump(self, tables: Optional[List[str]] = None) -> bool:
        """使用 pg_dump 和 pg_restore 迁移（推荐方法，速度快）"""
        try:
            import tempfile
            
            # 创建临时文件
            dump_file = tempfile.NamedTemporaryFile(mode='w+b', suffix='.sql', delete=False)
            dump_path = dump_file.name
            dump_file.close()
            
            try:
                # 构建 pg_dump 命令
                dump_cmd = [
                    'pg_dump',
                    '-h', self.source_config['host'],
                    '-p', str(self.source_config['port']),
                    '-U', self.source_config['user'],
                    '-d', self.source_config['db'],
                    '-F', 'c',  # 自定义格式（压缩）
                    '-f', dump_path
                ]
                
                # 如果指定了表，只导出这些表
                if tables:
                    for table in tables:
                        dump_cmd.extend(['-t', table])
                
                # 设置密码环境变量
                env = os.environ.copy()
                if self.source_config['password']:
                    env['PGPASSWORD'] = self.source_config['password']
                
                logging.info(f"正在导出数据到: {dump_path}")
                logging.info(f"执行命令: {' '.join(dump_cmd)}")
                
                result = subprocess.run(
                    dump_cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                logging.info("✅ 数据导出成功")
                
                # 构建 pg_restore 命令
                restore_cmd = [
                    'pg_restore',
                    '-h', self.target_config['host'],
                    '-p', str(self.target_config['port']),
                    '-U', self.target_config['user'],
                    '-d', self.target_config['db'],
                    '--clean',  # 清理目标数据库中的对象
                    '--if-exists',  # 如果对象不存在也不报错
                    dump_path
                ]
                
                # 设置密码环境变量
                if self.target_config['password']:
                    env['PGPASSWORD'] = self.target_config['password']
                
                logging.info(f"正在导入数据到目标数据库...")
                logging.info(f"执行命令: {' '.join(restore_cmd)}")
                
                result = subprocess.run(
                    restore_cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                logging.info("✅ 数据导入成功")
                return True
                
            finally:
                # 清理临时文件
                if os.path.exists(dump_path):
                    os.unlink(dump_path)
                    logging.info(f"已删除临时文件: {dump_path}")
                    
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ pg_dump/pg_restore 失败: {e}")
            if e.stderr:
                logging.error(f"错误输出: {e.stderr}")
            return False
        except Exception as e:
            logging.error(f"❌ 迁移失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return False


def main():
    # 从 .env 文件读取本地数据库配置（源数据库）
    source_host_default = os.getenv('PG_HOST', 'localhost')
    source_port_default = int(os.getenv('PG_PORT', '5432'))
    source_db_default = os.getenv('PG_DB', 'crypto_data')
    source_user_default = os.getenv('PG_USER', 'postgres')
    source_password_default = os.getenv('PG_PASSWORD', '')
    
    parser = argparse.ArgumentParser(description='PostgreSQL 到 PostgreSQL 数据迁移工具')
    
    # 源数据库配置（默认从 .env 文件读取）
    parser.add_argument('--source-host', default=source_host_default, 
                       help=f'源数据库主机地址（默认: {source_host_default}，从 .env 读取）')
    parser.add_argument('--source-port', type=int, default=source_port_default, 
                       help=f'源数据库端口（默认: {source_port_default}，从 .env 读取）')
    parser.add_argument('--source-db', default=source_db_default, 
                       help=f'源数据库名（默认: {source_db_default}，从 .env 读取）')
    parser.add_argument('--source-user', default=source_user_default, 
                       help=f'源数据库用户名（默认: {source_user_default}，从 .env 读取）')
    parser.add_argument('--source-password', default=source_password_default, 
                       help='源数据库密码（默认: 从 .env 读取）')
    
    # 目标数据库配置
    parser.add_argument('--target-host', required=True, help='目标数据库主机地址（云服务器）')
    parser.add_argument('--target-port', type=int, default=5432, help='目标数据库端口')
    parser.add_argument('--target-db', default='crypto_data', help='目标数据库名')
    parser.add_argument('--target-user', default='postgres', help='目标数据库用户名')
    parser.add_argument('--target-password', required=True, help='目标数据库密码')
    
    # 迁移选项
    parser.add_argument('--method', choices=['dump', 'python'], default='dump',
                       help='迁移方法：dump（使用pg_dump，推荐）或 python（使用Python脚本）')
    parser.add_argument('--table-filter', help='表名过滤（如 K1d 表示只迁移K1d开头的表）')
    parser.add_argument('--tables', nargs='+', help='指定要迁移的表名列表')
    parser.add_argument('--table-file', help='从文件读取要迁移的表名列表（每行一个）')
    parser.add_argument('--skip-existing', action='store_true',
                       help='跳过目标数据库中已存在的数据（增量迁移）')
    parser.add_argument('--compare-only', action='store_true',
                       help='只对比两个数据库的表数量，不执行迁移')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='批量插入大小（仅用于python方法）')
    
    args = parser.parse_args()
    
    # 配置优先级：命令行参数 > 环境变量（SOURCE_PG_*）> .env文件（PG_*）> 默认值
    # 源数据库（本地）：优先使用命令行参数，其次环境变量，最后 .env 文件中的 PG_* 配置
    source_host = os.getenv('SOURCE_PG_HOST') or args.source_host
    source_port = int(os.getenv('SOURCE_PG_PORT') or args.source_port)
    source_db = os.getenv('SOURCE_PG_DB') or args.source_db
    source_user = os.getenv('SOURCE_PG_USER') or args.source_user
    source_password = os.getenv('SOURCE_PG_PASSWORD') or args.source_password
    
    # 目标数据库（云服务器）：优先使用命令行参数，其次环境变量
    target_host = os.getenv('TARGET_PG_HOST') or args.target_host
    target_port = int(os.getenv('TARGET_PG_PORT') or args.target_port)
    target_db = os.getenv('TARGET_PG_DB') or args.target_db
    target_user = os.getenv('TARGET_PG_USER') or args.target_user
    target_password = os.getenv('TARGET_PG_PASSWORD') or args.target_password
    
    # 显示配置来源
    # 重新获取 env_path（在函数作用域内）
    env_path_check = project_root / '.env'
    if not env_path_check.exists():
        env_path_check = backend_dir / '.env'
    
    logging.info("=" * 80)
    logging.info("数据库配置")
    logging.info("=" * 80)
    logging.info(f"源数据库（本地）: {source_user}@{source_host}:{source_port}/{source_db}")
    if env_path_check.exists():
        logging.info(f"  ✓ 已从 .env 文件读取本地数据库配置: {env_path_check}")
    else:
        logging.info(f"  ⚠️  未找到 .env 文件，使用默认值或命令行参数")
    logging.info(f"目标数据库（云服务器）: {target_user}@{target_host}:{target_port}/{target_db}")
    logging.info("=" * 80)
    
    # 创建迁移器
    migrator = PostgreSQLToPostgreSQLMigrator(
        source_host=source_host,
        source_port=source_port,
        source_db=source_db,
        source_user=source_user,
        source_password=source_password,
        target_host=target_host,
        target_port=target_port,
        target_db=target_db,
        target_user=target_user,
        target_password=target_password,
        batch_size=args.batch_size
    )
    
    # 如果只是对比
    if args.compare_only:
        logging.info("=" * 80)
        logging.info("对比数据库表数量")
        logging.info("=" * 80)
        comparison = migrator.compare_table_counts(args.table_filter)
        
        print(f"\n源数据库表数量: {comparison['source_count']}")
        print(f"目标数据库表数量: {comparison['target_count']}")
        print(f"共同表数量: {comparison['common_count']}")
        
        if comparison['only_in_source']:
            print(f"\n仅在源数据库中的表 ({len(comparison['only_in_source'])}):")
            for table in comparison['only_in_source'][:10]:
                print(f"  - {table}")
            if len(comparison['only_in_source']) > 10:
                print(f"  ... 还有 {len(comparison['only_in_source']) - 10} 个表")
        
        if comparison['only_in_target']:
            print(f"\n仅在目标数据库中的表 ({len(comparison['only_in_target'])}):")
            for table in comparison['only_in_target'][:10]:
                print(f"  - {table}")
            if len(comparison['only_in_target']) > 10:
                print(f"  ... 还有 {len(comparison['only_in_target']) - 10} 个表")
        
        return
    
    # 执行迁移
    logging.info("=" * 80)
    logging.info("开始迁移数据")
    logging.info("=" * 80)
    
    # 确定要迁移的表
    tables_to_migrate = None
    if args.tables:
        tables_to_migrate = args.tables
        logging.info(f"指定迁移表: {tables_to_migrate}")
    elif args.table_file:
        with open(args.table_file, 'r') as f:
            tables_to_migrate = [line.strip() for line in f if line.strip()]
        logging.info(f"从文件读取 {len(tables_to_migrate)} 个表")
    elif args.table_filter:
        source_tables = migrator.get_source_tables()
        if args.table_filter.startswith('K'):
            tables_to_migrate = [t for t in source_tables if t.startswith(args.table_filter)]
        else:
            tables_to_migrate = [t for t in source_tables if args.table_filter in t]
        logging.info(f"过滤后需要迁移的表: {len(tables_to_migrate)} 个")
    
    # 选择迁移方法
    if args.method == 'dump':
        # 使用 pg_dump/pg_restore（推荐）
        logging.info("使用 pg_dump/pg_restore 方法迁移（推荐）")
        success = migrator.migrate_with_pg_dump(tables_to_migrate)
        if success:
            logging.info("=" * 80)
            logging.info("✅ 迁移完成！")
            logging.info("=" * 80)
        else:
            logging.error("=" * 80)
            logging.error("❌ 迁移失败！")
            logging.error("=" * 80)
            sys.exit(1)
    else:
        # 使用 Python 脚本方法
        logging.info("使用 Python 脚本方法迁移")
        stats = migrator.migrate_all(
            table_filter=args.table_filter,
            tables=tables_to_migrate,
            table_file=args.table_file,
            skip_existing=args.skip_existing
        )
        
        logging.info("=" * 80)
        logging.info("迁移完成！")
        logging.info("=" * 80)
        logging.info(f"总表数: {stats['total']}")
        logging.info(f"✓ 成功: {stats['success']}")
        logging.info(f"✗ 失败: {stats['failed']}")
        logging.info(f"⏭️  跳过: {stats['skipped']}")
        logging.info("=" * 80)
        
        if stats['failed'] > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
       