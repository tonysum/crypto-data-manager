"""
币安U本位合约K线数据下载脚本

功能：
1. 获取所有USDT交易对
2. 🔧 交易对校验：下载前自动校验交易对是否在交易所正常交易，跳过已下架或暂停的交易对
3. 下载每个交易对的K线数据
4. 保存到本地PostgreSQL数据库，表名格式：K{interval}{symbol}（例如：K1dBTCUSDT, K1hETHUSDT）
5. 支持增量更新(避免重复下载)
   - 日线及以上：按日期去重，不更新最后一天
   - 小时线及以下：按时间点去重，不更新最后一条
6. 智能跳过：下载前检查本地数据最后时间，如果 >= end_time则跳过该交易对（除非使用--update）
7. 支持指定开始和结束时间，确保不同时间间隔的数据时间范围一致
8. 默认不下载当天数据（因为当天数据不完整）
9. 自动分段下载：当数据条数超过1500条时，自动分段下载，每段最多1500条
10. 请求频率控制：每次API请求之间自动延迟，避免触发API频率限制
    - 每次请求延迟：默认0.1秒（可通过--request-delay调整）
    - 批次暂停：每处理指定数量的交易对后暂停（默认30个后暂停3秒）

使用方法举例：

1. 下载所有交易对的日线数据（默认）：
   python download_klines.py

2. 下载指定时间范围的日线数据：
   python download_klines.py --interval 1d --start-time 2025-01-01 --end-time 2025-12-31

3. 下载1小时K线数据，指定时间范围：
   python download_klines.py --interval 1h --start-time 2025-01-01 --end-time 2025-12-31

4. 下载4小时K线数据，指定时间范围（自动分段下载）：
   python download_klines.py --interval 4h --start-time 2022-01-01 --end-time 2025-12-31

5. 下载5分钟K线数据，指定时间范围：
   python download_klines.py --interval 5m --start-time 2025-01-01 --end-time 2025-12-31

6. 下载指定交易对的数据：
   python download_klines.py --interval 1d --start-time 2025-01-01 --end-time 2025-12-31 --symbols BTCUSDT ETHUSDT

7. 下载最近30天的数据：
   python download_klines.py --interval 1d --days 30

8. 只下载缺失的交易对：
   python download_klines.py --interval 1d --missing-only

9. 更新已存在的数据：
   python download_klines.py --interval 1d --update

10. 使用精确时间（包含时分秒），自动分段下载：
    python download_klines.py --interval 1h --start-time "2025-01-01 00:00:00" --end-time "2025-12-31 23:59:59"

11. 自定义请求延迟和批次设置：
    python download_klines.py --interval 1h --start-time 2024-01-01 --end-time 2025-12-31 --request-delay 0.2 --batch-size 20 --batch-delay 5.0

12. 禁用自动分段下载（使用原有单次下载逻辑）：
    python download_klines.py --interval 4h --start-time 2022-01-01 --end-time 2025-12-31 --no-auto-split

命令行参数：
  --interval: K线间隔 (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
  --start-time: 开始时间 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
  --end-time: 结束时间 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
  --days: 回溯天数（如果提供了--start-time和--end-time则忽略此参数）
  --limit: 每次请求的最大条数（默认None，自动使用1500。如果只提供start-time和end-time会自动计算）
  --auto-split: 当数据条数超过限制时自动分段下载（默认: True）
  --no-auto-split: 禁用自动分段下载
  --request-delay: 每次API请求之间的延迟时间（秒），避免频率限制（默认: 0.1）
  --batch-size: 每处理多少个交易对后暂停（默认: 30）
  --batch-delay: 每批处理后的暂停时间（秒）（默认: 3.0）
  --update: 更新已存在的数据
  --missing-only: 只下载缺失的交易对
  --symbols: 指定要下载的交易对列表

注意事项：
- 表名格式：K{interval}{symbol}，例如日线数据存储在 K1dBTCUSDT 表中
- 🔧 交易对校验：下载前会自动校验交易对是否在交易所正常交易（状态为TRADING）
  * 如果交易对已下架或暂停交易，会自动跳过并记录警告日志
  * 交易对列表会缓存1小时，避免重复查询交易所
  * 如果无法获取交易所交易对列表（网络问题等），会记录警告但允许继续下载
- 默认不下载当天的数据（因为当天数据不完整）
- 增量更新规则：
  * 日线及以上（1d, 3d, 1w, 1M）：按日期去重，不更新最后一天
  * 小时线及以下（1h, 4h, 5m等）：按时间点去重，不更新最后一条
- 如果提供了--start-time和--end-time，会自动计算数据条数
- 当数据条数超过1500条时，会自动分段下载，每段最多1500条
- 每次API请求之间会自动延迟（默认0.1秒），避免触发频率限制
- 每处理指定数量的交易对后会暂停（默认30个后暂停3秒）
- 如果提供了--start-time和--end-time，会优先使用这些参数，忽略--days参数

#3. 代码解读：一步步看懂管理员如何工作
让我们跟着上面的流程图，看看代码是如何实现的：

1. 准备工作 (文件开头) 脚本首先导入所有需要的工具，比如 pandas (用于整理数据)、 sqlalchemy (用于和数据库沟通) 
   以及项目内其他模块如 binance_client (负责和币安API打交道)。同时，定义了一些重要的规则，比如API请求的限制、缓存时间等。
2. 指令解析 ( main 函数) 当您运行这个脚本时， main 函数（通常在文件的最下方）会首先启动，负责解析您在命令行输入的指令，
   比如 --interval 1d 或 --symbols BTCUSDT 。这些指令告诉管理员具体要下载什么。
3. 获取并验证交易对 ( get_valid_trading_symbols 和 validate_symbol ) 在开始下载前，
   脚本会调用 get_valid_trading_symbols 从交易所获取一份所有“仍在发行”的交易对列表。为了效率，这份列表会被 缓存一个小时 ，
   避免每次都去麻烦交易所。接着，在处理每个交易对时， validate_symbol 会核对一下，确保它在这份有效列表里。
4. 增量更新 ( get_last_trade_date ) 为了不重复下载，脚本会通过 get_last_trade_date 查询数据库，看看这个交易对的数据
   已经下载到哪个时间点了。这样它就能精确地计算出下一次应该从哪里开始。
5. 自动分段 ( split_time_range ) 币安API不允许一次请求太多数据（比如超过1500条）。如果计算发现需要下载的数据量超过了
   这个限制， split_time_range 函数就会像切蛋糕一样，把一个大的时间范围切成多个小段，确保每一段的请求都不会超限。
6. 下载与存储 ( kline_candlestick_data 和 _insert_with_skip_duplicates ) 一切准备就绪后，脚本会为每一个小时间段
   调用 kline_candlestick_data 函数，这才是真正去币安API获取数据的步骤。拿到数据后，会转换成 pandas 的 DataFrame 格式，
   这是一种非常便于处理的表格形式。最后，通过 to_sql 或类似方法（如此文件中的 _insert_with_skip_duplicates ）存入PostgreSQL数据库。
   表名是动态生成的，例如 K1dBTCUSDT ，清晰明了。

#4 容易忽略的“坑”：时区与“未完成”的数据

一个常见的误解和陷阱是关于 时间和数据的完整性 。

- 陷阱是什么？ 您可能会想：“为什么脚本默认不下载当天的数据？” 假设现在是1月24日中午12点，您想获取 BTCUSDT 的日线 ( 1d ) 数据。
  您可能会期望拿到1月24日这根K线。但问题是，这根“天”K线要到午夜UTC时间24:00才算真正“收盘”，它的最高价、最低价、收盘价在这一天内
  都还在不断变化。如果您在中午12点就把它下载并保存了，您存储的就是一个 不完整、不准确 的“半成品”。
- 脚本如何避免这个陷阱？ 这个脚本设计得非常严谨，它默认只下载已经 完全走完 的时间周期的数据。对于日线，它通常会下载到“昨天”为止。
  这样可以确保存入数据库的每一条记录都是最终的、不会再改变的。
- 代码中的体现 您会看到代码在计算起止时间时，常常使用 datetime.now() - timedelta(days=1) 这样的逻辑。
  此外， ensure_utc_timezone 函数的存在至关重要。金融数据API几乎总是以**UTC（协调世界时）**为标准。如果在处理时间时不统一时区，
  很容易因为时差导致请求错误的时间范围（比如“差一天”问题）。该脚本强制将所有时间对象转换为UTC，从根源上避免了这类混乱。

"""

import os
import sys
import logging
import time
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd      # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from sqlalchemy import text  # pyright: ignore[reportMissingImports]
from sqlalchemy.exc import IntegrityError  # pyright: ignore[reportMissingImports]

from binance_client import (
    in_exchange_trading_symbols,
    kline_candlestick_data,
    kline2df
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
    KlineCandlestickDataIntervalEnum
)
from db import engine, create_table

# 🔧 缓存交易所正常交易的交易对列表（避免重复查询）
_valid_trading_symbols_cache: Optional[List[str]] = None
_cache_timestamp: Optional[datetime] = None
CACHE_TTL_SECONDS = 3600
DEFAULT_REQUEST_DELAY = 0.3
DEFAULT_BATCH_SIZE = 30
DEFAULT_BATCH_DELAY = 3.0
BATCH_SIZE = 50  # PostgreSQL 批量插入大小
API_DATA_LIMIT = 1500
DISK_SPACE_REQUIRED_GB = 1.0


def get_valid_trading_symbols(force_refresh: bool = False) -> List[str]:
    """
    获取交易所正常交易的交易对列表（带缓存）
    
    Args:
        force_refresh: 是否强制刷新缓存，默认False
    
    Returns:
        正常交易的交易对列表
    """
    global _valid_trading_symbols_cache, _cache_timestamp
    
    now = datetime.now()
    
    # 检查缓存是否有效
    if (
        not force_refresh
        and _valid_trading_symbols_cache is not None
        and _cache_timestamp is not None
        and (now - _cache_timestamp).total_seconds() < CACHE_TTL_SECONDS
    ):
        return _valid_trading_symbols_cache
    
    # 从交易所获取交易对列表
    logging.info("正在从交易所获取正常交易的交易对列表...")
    try:
        valid_symbols = in_exchange_trading_symbols(status="TRADING")
        if valid_symbols:
            _valid_trading_symbols_cache = valid_symbols
            _cache_timestamp = now
            logging.info(f"获取到 {len(valid_symbols)} 个正常交易的交易对")
            return valid_symbols
        else:
            logging.warning("无法从交易所获取交易对列表，返回空列表")
            return []
    except Exception as e:
        logging.error(f"获取交易所交易对列表失败: {e}")
        # 如果获取失败，返回缓存（如果有）
        if _valid_trading_symbols_cache is not None:
            logging.warning("使用缓存的交易对列表")
            return _valid_trading_symbols_cache
        return []


def validate_symbol(symbol: str, skip_validation: bool = False) -> bool:
    """
    校验交易对是否在交易所正常交易
    
    Args:
        symbol: 交易对符号
        skip_validation: 是否跳过校验（用于测试或特殊情况），默认False
    
    Returns:
        bool: 如果交易对正常交易返回True，否则返回False
    """
    if skip_validation:
        return True
    
    valid_symbols = get_valid_trading_symbols()
    
    if not valid_symbols:
        # 如果无法获取交易对列表，记录警告但允许继续（避免网络问题导致无法下载）
        logging.warning(f"⚠️ 无法获取交易所交易对列表，跳过 {symbol} 的校验（允许继续下载）")
        return True
    
    if symbol not in valid_symbols:
        logging.warning(
            f"⚠️ 交易对 {symbol} 不在交易所正常交易列表中，跳过下载。"
            f"（可能已下架或暂停交易）"
        )
        return False
    
    return True

# 注意：日志配置在 main.py 中统一配置，这里不再重复配置
# 这样可以确保所有日志都输出到同一个地方（终端）


def check_disk_space(required_gb: float = 1.0) -> bool:
    """
    检查磁盘可用空间（仅供参考，PostgreSQL 数据库存储在服务器上）
    
    注意：由于使用 PostgreSQL，数据库实际存储在服务器上，本地磁盘空间检查仅供参考。
    实际应该检查 PostgreSQL 服务器所在磁盘的空间。
    
    Args:
        required_gb: 需要的最小可用空间（GB），默认 1GB（此参数已不再使用）
    
    Returns:
        bool: 始终返回 True（不阻止下载）
    """
    try:
        # PostgreSQL 数据库存储在服务器上，本地磁盘检查仅供参考
        # 实际应该检查 PostgreSQL 服务器所在磁盘的空间
        import shutil
        # 获取当前工作目录的磁盘使用情况（作为参考）
        stat = shutil.disk_usage(os.getcwd())
        free_gb = stat.free / (1024 ** 3)  # 转换为 GB
        total_gb = stat.total / (1024 ** 3)
        used_percent = (stat.used / stat.total) * 100
        
        # 仅记录信息，不发出警告（因为数据库在服务器上）
        logging.debug(f"本地磁盘空间（仅供参考）: 总容量 {total_gb:.2f}GB, 已用 {used_percent:.1f}%, 可用 {free_gb:.2f}GB")
        logging.debug("注意：PostgreSQL 数据库存储在服务器上，本地磁盘空间仅供参考")
        
        # 始终返回 True，不阻止下载
        return True
    except Exception as e:
        logging.debug(f"无法检查磁盘空间: {e}，继续执行...")
        return True  # 如果检查失败，允许继续执行


def get_local_symbols(interval: str = "1d") -> List[str]:
    """
    获取本地数据库中已存在的交易对列表
    
    优先从交易对表获取，如果没有交易对表则从表名推断
    """
    try:
        # 优先从交易对表获取
        from symbols import get_trading_symbols
        trading_symbols = get_trading_symbols()
        
        # 检查这些交易对是否有对应interval的数据表
        prefix = f'K{interval}'
        valid_symbols = []
        
        with engine.connect() as conn:
            for symbol in trading_symbols:
                table_name = f"{prefix}{symbol}"
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
                if result.fetchone()[0]:
                    valid_symbols.append(symbol)
        
        if valid_symbols:
            logging.info(f"从交易对表获取到 {len(valid_symbols)} 个交易对（interval: {interval}）")
            return valid_symbols
    except Exception as e:
        logging.warning(f"从交易对表获取交易对失败，回退到表名推断方式: {e}")
    
    # 回退到原来的方式：从表名推断
    prefix = f'K{interval}'
    stmt = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE :prefix
    """
    with engine.connect() as conn:
        result = conn.execute(text(stmt), {"prefix": f"{prefix}%"})
        table_names = result.fetchall()
    # 去掉前缀 'K{interval}', 例如 'K1d' -> ''
    prefix_len = len(prefix)
    local_symbols = [name[0][prefix_len:] for name in table_names]
    logging.info(f"从表名推断获取到 {len(local_symbols)} 个交易对（interval: {interval}）")
    return local_symbols


def calculate_interval_seconds(interval: str) -> int:
    """
    计算K线间隔对应的秒数
    
    Args:
        interval: K线间隔字符串，如 '1m', '1h', '1d' 等
    
    Returns:
        int: 对应的秒数
    """
    interval_map = {
        '1m': 60,
        '3m': 180,
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '2h': 7200,
        '4h': 14400,
        '6h': 21600,
        '8h': 28800,
        '12h': 43200,
        '1d': 86400,
        '3d': 259200,
        '1w': 604800,
        '1M': 2592000,  # 假设1个月=30天
    }
    return interval_map.get(interval, 86400)


def ensure_utc_timezone(*args: datetime) -> tuple:
    """
    确保datetime对象具有UTC时区信息

    Args:
        *args: 需要处理的datetime对象

    Returns:
        tuple: 处理后的datetime对象列表（都带有UTC时区）
    """
    result = []
    for dt in args:
        if dt.tzinfo is None:
            result.append(dt.replace(tzinfo=timezone.utc))
        else:
            result.append(dt)
    return tuple(result) if len(result) > 1 else result[0]


def calculate_data_count(start_time: datetime, end_time: datetime, interval: str) -> int:
    """
    计算指定时间范围内的数据条数
    
    Args:
        start_time: 开始时间
        end_time: 结束时间
        interval: K线间隔
    
    Returns:
        int: 数据条数
    """
    if not start_time or not end_time:
        return 0

    start_time, end_time = ensure_utc_timezone(start_time, end_time)

    interval_seconds = calculate_interval_seconds(interval)
    total_seconds = int((end_time - start_time).total_seconds())
    count = total_seconds // interval_seconds + 1
    return count


def split_time_range(start_time: datetime, end_time: datetime, interval: str, max_count: int = API_DATA_LIMIT) -> List[tuple]:
    """
    将时间范围分割成多个段，每段不超过max_count条数据
    
    Args:
        start_time: 开始时间
        end_time: 结束时间
        interval: K线间隔
        max_count: 每段最大数据条数，默认1500
    
    Returns:
        List[tuple]: [(start1, end1), (start2, end2), ...] 时间范围列表
    """
    if not start_time or not end_time:
        return []

    start_time, end_time = ensure_utc_timezone(start_time, end_time)

    interval_seconds = calculate_interval_seconds(interval)
    max_seconds = (max_count - 1) * interval_seconds  # 减1是因为包含起始和结束时间
    
    ranges = []
    current_start = start_time
    
    while current_start < end_time:
        # 计算当前段的结束时间
        current_end = current_start + timedelta(seconds=max_seconds)
        if current_end > end_time:
            current_end = end_time
        
        ranges.append((current_start, current_end))
        current_start = current_end + timedelta(seconds=interval_seconds)
    
    return ranges


def get_existing_dates(symbol: str, interval: str = "1d") -> set:
    """获取指定交易对在数据库中已存在的日期集合"""
    table_name = f'K{interval}{symbol}'
    # PostgreSQL 表名需要用引号包裹（保持大小写）
    safe_table_name = f'"{table_name}"'
    try:
        stmt = f'SELECT trade_date FROM {safe_table_name}'
        with engine.connect() as conn:
            result = conn.execute(text(stmt))
            dates = result.fetchall()
        return {date[0] for date in dates}
    except Exception as e:
        # 如果查询失败，尝试检查表名大小写问题
        logging.warning(f"获取 {symbol} 已存在日期失败: {e}")
        try:
            with engine.connect() as conn:
                # 检查是否存在大小写不匹配的表名
                result = conn.execute(
                    text("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND (table_name = :table_name OR LOWER(table_name) = LOWER(:table_name))
                    """),
                    {"table_name": table_name}
                )
                actual_table_name = result.fetchone()
                if actual_table_name:
                    actual_name = actual_table_name[0]
                    safe_actual_name = f'"{actual_name}"'
                    stmt_retry = f'SELECT trade_date FROM {safe_actual_name}'
                    result_retry = conn.execute(text(stmt_retry))
                    dates_retry = result_retry.fetchall()
                    logging.info(f"使用实际表名 {actual_name} 成功获取 {len(dates_retry)} 个日期")
                    return {date[0] for date in dates_retry}
        except Exception as e2:
            logging.debug(f"检查表名时出错: {e2}")
        return set()


def _insert_with_skip_duplicates(df: pd.DataFrame, table_name: str, engine) -> int:
    """
    逐条插入数据，跳过重复的trade_date
    
    Args:
        df: 要插入的DataFrame
        table_name: 表名
        engine: 数据库引擎
    
    Returns:
        int: 成功插入的行数
    """
    saved_count = 0
    skipped_count = 0
    total_rows = len(df)
    
    # 🔧 修复：表名用双引号括起来，避免包含特殊字符时SQL语法错误
    quoted_table_name = f'"{table_name}"'
    
    for idx, (_, row) in enumerate(df.iterrows(), 1):
        try:
            # 将row转换为字典
            row_dict = row.to_dict()
            
            # 构建INSERT语句，使用命名参数（:param）
            # 🔧 修复：列名也用双引号括起来，避免特殊字符问题
            columns = ', '.join([f'"{col}"' for col in df.columns])
            placeholders = ', '.join([f':{col}' for col in df.columns])
            
            stmt = f"INSERT INTO {quoted_table_name} ({columns}) VALUES ({placeholders})"
            with engine.connect() as conn:
                # SQLAlchemy的execute方法使用字典作为参数
                conn.execute(text(stmt), row_dict)
                conn.commit()
            saved_count += 1
            
            # 每处理100条输出一次进度
            if idx % 100 == 0:
                logging.info(f"逐条插入进度: {idx}/{total_rows}, 已保存: {saved_count}, 跳过: {skipped_count}")
        except Exception as e:
            # 如果是UNIQUE constraint错误，跳过这条数据
            error_msg = str(e)
            is_unique_error = any(keyword in error_msg for keyword in ["UniqueViolation", "duplicate key", "IntegrityError"]) or "unique" in error_msg.lower()
            
            if is_unique_error:
                skipped_count += 1
                continue
            else:
                trade_date = row_dict.get('trade_date', 'unknown') if 'row_dict' in locals() else 'unknown'
                logging.error(f"插入数据失败: {e}, trade_date: {trade_date}")
                logging.error(f"SQL语句: {stmt}")
                raise
    
    logging.info(f"逐条插入完成: 总计 {total_rows} 条，成功保存 {saved_count} 条，跳过 {skipped_count} 条重复数据")
    return saved_count


def get_last_trade_date(symbol: str, interval: str = "1d") -> Optional[str]:
    """
    获取指定交易对在数据库中的最后一条数据的trade_date
    
    Args:
        symbol: 交易对符号
        interval: K线间隔
    
    Returns:
        Optional[str]: 最后一条数据的trade_date，如果表不存在或没有数据则返回None
    """
    table_name = f'K{interval}{symbol}'
    # PostgreSQL 表名需要用引号包裹（保持大小写）
    safe_table_name = f'"{table_name}"'
    try:
        stmt = f'SELECT trade_date FROM {safe_table_name} ORDER BY open_time DESC LIMIT 1'
        with engine.connect() as conn:
            result = conn.execute(text(stmt))
            row = result.fetchone()
            if row:
                return row[0]
        return None
    except Exception as e:
        # 如果查询失败，尝试检查表名大小写问题
        try:
            with engine.connect() as conn:
                # 检查是否存在大小写不匹配的表名
                result = conn.execute(
                    text("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND (table_name = :table_name OR LOWER(table_name) = LOWER(:table_name))
                    """),
                    {"table_name": table_name}
                )
                actual_table_name = result.fetchone()
                if actual_table_name:
                    actual_name = actual_table_name[0]
                    safe_actual_name = f'"{actual_name}"'
                    stmt_retry = f'SELECT trade_date FROM {safe_actual_name} ORDER BY trade_date DESC LIMIT 1'
                    result_retry = conn.execute(text(stmt_retry))
                    row_retry = result_retry.fetchone()
                    if row_retry:
                        logging.debug(f"使用实际表名 {actual_name} 成功获取最后交易日期")
                        return row_retry[0]
        except Exception as e2:
            logging.debug(f"检查表名时出错: {e2}")
        # 表不存在或其他错误，返回None
        return None


def compare_trade_dates(last_date: str, end_time: datetime, interval: str) -> bool:
    """
    比较本地最后一条数据的时间与end_time
    
    Args:
        last_date: 本地最后一条数据的trade_date（字符串格式）
        end_time: 要下载的结束时间
        interval: K线间隔
    
    Returns:
        bool: 如果last_date >= end_time对应的日期/时间，返回True（表示已是最新数据）
    """
    try:
        if interval in ['1d', '3d', '1w', '1M']:
            # 日线及以上，比较日期
            last_date_obj = datetime.strptime(last_date, '%Y-%m-%d').date()
            # 确保end_time有时区信息，然后转换为date
            if end_time.tzinfo is None:
                end_date = end_time.date()
            else:
                end_date = end_time.astimezone(timezone.utc).date()
            result = last_date_obj >= end_date
            comparison_op = ">=" if result else "<"
            logging.info(f"日期比较: 本地最后日期={last_date_obj}, 结束日期={end_date}, 结果={result} (本地{comparison_op}结束)")
            return result
        else:
            # 小时线及以下，比较完整时间
            last_date_obj = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
            # 确保两个datetime对象都有相同的时区信息
            if end_time.tzinfo is not None:
                # end_time有时区信息，将last_date_obj也转换为UTC时区
                last_date_obj = last_date_obj.replace(tzinfo=timezone.utc)
            elif last_date_obj.tzinfo is not None:
                # last_date_obj有时区信息，将end_time也转换为UTC时区
                end_time = end_time.replace(tzinfo=timezone.utc)
            
            result = last_date_obj >= end_time
            end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
            comparison_op = ">=" if result else "<"
            logging.info(f"时间比较: 本地最后时间={last_date}, 结束时间={end_time_str}, 结果={result} (本地{comparison_op}结束)")
            return result
    except Exception as e:
        logging.warning(f"比较日期失败: {e}, last_date={last_date}, end_time={end_time}, interval={interval}")
        return False


def _get_default_end_time(interval: str, reference_time: Optional[datetime] = None) -> datetime:
    """
    获取指定K线间隔的默认结束时间

    Args:
        interval: K线间隔
        reference_time: 参考时间，默认为当前时间

    Returns:
        datetime: 默认结束时间
    """
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    if interval in ['1d', '3d', '1w', '1M']:
        today = reference_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(seconds=1)
    else:
        interval_seconds = calculate_interval_seconds(interval)
        now_utc = reference_time if reference_time.tzinfo is not None else reference_time.replace(tzinfo=timezone.utc)
        current_timestamp = int(now_utc.timestamp())
        kline_index = current_timestamp // interval_seconds
        current_kline_start_timestamp = kline_index * interval_seconds
        latest_complete_kline_start_timestamp = current_kline_start_timestamp - interval_seconds
        return datetime.fromtimestamp(latest_complete_kline_start_timestamp, tz=timezone.utc)


def _get_latest_complete_kline_time(interval: str) -> datetime:
    """
    获取当前时间之前最新完整K线的开始时间

    Args:
        interval: K线间隔

    Returns:
        datetime: 最新完整K线的开始时间（UTC时区）
    """
    interval_seconds = calculate_interval_seconds(interval)
    now_utc = datetime.now(timezone.utc)
    current_timestamp = int(now_utc.timestamp())
    kline_index = current_timestamp // interval_seconds
    current_kline_start_timestamp = kline_index * interval_seconds
    latest_complete_kline_start_timestamp = current_kline_start_timestamp - interval_seconds
    return datetime.fromtimestamp(latest_complete_kline_start_timestamp, tz=timezone.utc)


def download_kline_data(
    symbol: str,
    interval: str = "1d",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: Optional[int] = API_DATA_LIMIT,
    update_existing: bool = False,
    auto_split: bool = True,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    skip_symbol_validation: bool = False
) -> bool:
    """
    下载指定交易对的K线数据并保存到数据库
    
    注意：默认不会下载当天的数据, 因为当天数据不完整(还在交易中)。
    只有在第二天更新前一天的数据才准确。
    
    Args:
        symbol: 交易对符号, 如 'BTCUSDT'
        interval: K线间隔, 默认 '1d'(日线)
        start_time: 开始时间, 默认None(从最早开始)
        end_time: 结束时间, 默认None(到昨天的结束时间, 不包含今天)
        limit: 每次请求的最大条数, 默认1500。如果为None且提供了start_time和end_time，会自动计算
        update_existing: 是否更新已存在的数据, 默认False
        auto_split: 当数据条数超过limit时是否自动分段下载, 默认True
        request_delay: 每次API请求之间的延迟时间（秒），避免频率限制, 默认0.3秒
        skip_symbol_validation: 是否跳过交易对校验（用于测试或特殊情况），默认False
    
    Returns:
        bool: 是否成功下载
    """
    logging.info(f"开始下载 {symbol} 的 {interval} K线数据...")
    if start_time:
        logging.info(f"  开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if end_time:
        logging.info(f"  结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 🔧 校验交易对是否在交易所正常交易
    if not validate_symbol(symbol, skip_validation=skip_symbol_validation):
        logging.warning(f"{symbol} 交易对校验失败，跳过下载")
        return False
    
    table_name = f'K{interval}{symbol}'
    
    # 检查磁盘空间（仅供参考，PostgreSQL 数据库在服务器上）
    # 此检查不会阻止下载，仅用于记录参考信息
    check_disk_space(required_gb=DISK_SPACE_REQUIRED_GB)
    
    try:
        # 如果未启用update_existing，先检查本地最后一条数据的时间
        if not update_existing:
            check_end_time = end_time if end_time is not None else _get_default_end_time(interval)

            last_trade_date = get_last_trade_date(symbol, interval)
            if last_trade_date:
                if compare_trade_dates(last_trade_date, check_end_time, interval):
                    end_time_str = check_end_time.strftime('%Y-%m-%d' if interval in ['1d', '3d', '1w', '1M'] else '%Y-%m-%d %H:%M:%S')
                    logging.info(f"{symbol} 本地数据最后时间({last_trade_date}) >= 结束时间({end_time_str})，跳过下载（使用--update可强制更新）")
                    return True
        
        # 创建表(如果不存在)
        create_table(table_name)
        
        # 获取已存在的日期
        existing_dates = get_existing_dates(symbol, interval) if not update_existing else set()
        
        # 转换时间间隔
        interval_enum = KlineCandlestickDataIntervalEnum[f"INTERVAL_{interval}"].value

        # 转换时间格式(如果需要)
        if end_time is None:
            end_time = _get_default_end_time(interval)
            logging.info(f"{symbol} 默认结束时间设置为最新完整K线时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
        # 如果提供了start_time和end_time，检查是否需要分段下载
        max_limit = limit if limit is not None else API_DATA_LIMIT
        
        if start_time and end_time and auto_split:
            # 计算预计数据条数
            data_count = calculate_data_count(start_time, end_time, interval)
            logging.info(f"{symbol} 预计数据条数: {data_count}, 限制: {max_limit}")
            
            if data_count > max_limit:
                # 需要分段下载
                logging.info(f"{symbol} 数据条数({data_count})超过限制({max_limit})，将分段下载")
                time_ranges = split_time_range(start_time, end_time, interval, max_limit)
                logging.info(f"{symbol} 将分为 {len(time_ranges)} 段下载")
                
                all_dfs = []
                for idx, (seg_start, seg_end) in enumerate(time_ranges, 1):
                    logging.info(f"{symbol} 正在下载第 {idx}/{len(time_ranges)} 段: {seg_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {seg_end.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    seg_start_ts = int(seg_start.timestamp() * 1000)
                    seg_end_ts = int(seg_end.timestamp() * 1000)
                    
                    # 请求前暂停，避免频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)
                    
                    try:
                        klines = kline_candlestick_data(
                            symbol=symbol,
                            interval=interval_enum,
                            starttime=seg_start_ts,
                            endtime=seg_end_ts,
                            limit=max_limit
                        )
                        
                        if klines:
                            seg_df = kline2df(klines)
                            if not seg_df.empty:
                                all_dfs.append(seg_df)
                                logging.info(f"{symbol} 第 {idx} 段下载成功，获得 {len(seg_df)} 条数据")
                            else:
                                logging.warning(f"{symbol} 第 {idx} 段转换后的DataFrame为空")
                        else:
                            logging.warning(f"{symbol} 第 {idx} 段没有获取到K线数据")
                    except Exception as e:
                        error_msg = str(e)
                        logging.error(f"{symbol} 第 {idx} 段下载失败: {e}")
                        
                        # 检查是否是API频率限制错误
                        if 'Way too many requests' in error_msg or 'banned until' in error_msg:
                            # 尝试从错误信息中提取封禁时间
                            import re
                            banned_match = re.search(r'banned until (\d+)', error_msg)
                            if banned_match:
                                banned_until = int(banned_match.group(1))
                                current_time = int(time.time() * 1000)  # 转换为毫秒
                                wait_time = max(0, (banned_until - current_time) / 1000)  # 转换为秒
                                if wait_time > 0:
                                    logging.warning(f"{symbol} 检测到API频率限制，等待 {wait_time:.1f} 秒...")
                                    time.sleep(min(wait_time + 5, 300))  # 最多等待5分钟
                            else:
                                # 如果没有提取到封禁时间，等待60秒
                                logging.warning(f"{symbol} 检测到API频率限制，等待 60 秒...")
                                time.sleep(60)
                        
                        continue
                
                if not all_dfs:
                    logging.warning(f"{symbol} 所有分段都没有获取到数据")
                    return False
                
                # 合并所有分段的数据
                df = pd.concat(all_dfs, ignore_index=True)
                
                # 将trade_date转换为字符串格式(用于数据库存储和去重)
                # 根据K线间隔选择合适的日期格式
                if interval in ['1d', '3d', '1w', '1M']:
                    # 日线及以上, 使用日期格式
                    df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
                else:
                    # 小时线及以下, 使用完整时间格式
                    df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # 去重（按trade_date）
                df = df.drop_duplicates(subset=['trade_date'], keep='first')
                logging.info(f"{symbol} 分段下载完成，合并后共 {len(df)} 条数据（去重前: {sum(len(d) for d in all_dfs)} 条）")
            else:
                # 不需要分段，直接下载
                start_time, end_time = ensure_utc_timezone(start_time, end_time)

                start_timestamp = int(start_time.timestamp() * 1000)
                end_timestamp = int(end_time.timestamp() * 1000)

                # 请求前暂停
                if request_delay > 0:
                    time.sleep(request_delay)
                
                logging.info(f"正在下载 {symbol} 的K线数据...")
                klines = kline_candlestick_data(
                    symbol=symbol,
                    interval=interval_enum,
                    starttime=start_timestamp,
                    endtime=end_timestamp,
                    limit=max_limit
                )
                
                if not klines:
                    logging.warning(f"{symbol} 没有获取到K线数据")
                    return False
                
                df = kline2df(klines)
        else:
            # 原有逻辑：单次下载（不自动分段或没有提供时间范围）
            start_timestamp = None
            end_timestamp = None
            if start_time:
                start_time = ensure_utc_timezone(start_time)
                start_timestamp = int(start_time.timestamp() * 1000)
            if end_time:
                end_time = ensure_utc_timezone(end_time)
                end_timestamp = int(end_time.timestamp() * 1000)

            # 请求前暂停
            if request_delay > 0:
                time.sleep(request_delay)
            
            # 下载K线数据
            logging.info(f"正在下载 {symbol} 的K线数据...")
            klines = kline_candlestick_data(
                symbol=symbol,
                interval=interval_enum,
                starttime=start_timestamp,
                endtime=end_timestamp,
                limit=max_limit
            )
            
            if not klines:
                logging.warning(f"{symbol} 没有获取到K线数据")
                return False
            
            # 转换为DataFrame
            df = kline2df(klines)
        
        if df.empty:
            logging.warning(f"{symbol} 转换后的DataFrame为空")
            return False
        
        # 将trade_date转换为字符串格式(用于数据库存储和去重)
        # 注意：分段下载时已经在合并前转换过了，这里需要检查是否已转换
        if df['trade_date'].dtype == 'object':
            # 已经是字符串格式，跳过转换
            pass
        else:
            # 根据K线间隔选择合适的日期格式
            if interval in ['1d', '3d', '1w', '1M']:
                # 日线及以上, 使用日期格式
                df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            else:
                # 小时线及以下, 使用完整时间格式
                df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 过滤掉不完整的数据
        now_utc = datetime.now(timezone.utc)
        today_str = now_utc.strftime('%Y-%m-%d')
        before_filter = len(df)

        if interval in ['1d', '3d', '1w', '1M']:
            df = df[df['trade_date'] != today_str]
        else:
            latest_complete_time = _get_latest_complete_kline_time(interval)

            def is_complete_kline(trade_date_str: str) -> bool:
                try:
                    trade_date_obj = datetime.strptime(trade_date_str, '%Y-%m-%d %H:%M:%S')
                    trade_date_utc = trade_date_obj.replace(tzinfo=timezone.utc)
                    return trade_date_utc <= latest_complete_time
                except (ValueError, TypeError):
                    return True

            df = df[df['trade_date'].apply(is_complete_kline)]

            if before_filter > len(df):
                logging.info(f"{symbol} 过滤掉 {before_filter - len(df)} 条不完整的K线数据（最新完整K线时间: {latest_complete_time.strftime('%Y-%m-%d %H:%M:%S')}）")

        after_filter = len(df)
        if after_filter < before_filter:
            logging.info(f"{symbol} 共过滤掉 {before_filter - after_filter} 条不完整数据")
        
        # 再次去重（确保DataFrame内部没有重复的trade_date）
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['trade_date'], keep='first')
        after_dedup = len(df)
        if after_dedup < before_dedup:
            logging.info(f"{symbol} DataFrame内部去重，移除 {before_dedup - after_dedup} 条重复数据")
        
        # 过滤已存在的数据
        if existing_dates and not update_existing:
            before_count = len(df)
            df = df[~df['trade_date'].isin(existing_dates)]
            after_count = len(df)
            if after_count < before_count:
                logging.info(f"{symbol} 过滤掉 {before_count - after_count} 条已存在的数据")
        
        if df.empty:
            logging.info(f"{symbol} 没有新数据需要保存")
            return True
        
        # 保存到数据库前，再次获取最新的已存在数据（防止并发插入）
        if not update_existing:
            current_existing_dates = get_existing_dates(symbol, interval)
            if current_existing_dates:
                before_final_check = len(df)
                df = df[~df['trade_date'].isin(current_existing_dates)]
                after_final_check = len(df)
                if after_final_check < before_final_check:
                    logging.info(f"{symbol} 最终检查过滤掉 {before_final_check - after_final_check} 条已存在的数据")
                if df.empty:
                    logging.info(f"{symbol} 最终检查后没有新数据需要保存")
                    return True
        
        BATCH_SIZE = 50  # PostgreSQL 批量插入大小
        total_rows = len(df)
        saved_count = 0

        if total_rows <= BATCH_SIZE:
            try:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                saved_count = len(df)
            except Exception as e:
                # 🔧 增强：如果是唯一约束冲突，降级到逐条插入
                error_msg = str(e)
                is_unique_error = any(keyword in error_msg for keyword in ["UniqueViolation", "duplicate key", "IntegrityError"]) or "unique" in error_msg.lower()
                
                if is_unique_error:
                    logging.warning(f"⚠️ {symbol} 批量插入发生冲突，尝试降级到逐条插入自愈模式...")
                    saved_count = _insert_with_skip_duplicates(df, table_name, engine)
                else:
                    logging.error(f"{symbol} 批量插入失败: {e}")
                    raise
        else:
            for i in range(0, total_rows, BATCH_SIZE):
                batch_df = df.iloc[i:i+BATCH_SIZE]
                try:
                    batch_df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    saved_count += len(batch_df)
                except Exception as e:
                    # 🔧 增强：批量插入失败时尝试逐条插入该批次
                    error_msg = str(e)
                    is_unique_error = any(keyword in error_msg for keyword in ["UniqueViolation", "duplicate key", "IntegrityError"]) or "unique" in error_msg.lower()
                    
                    if is_unique_error:
                        logging.warning(f"⚠️ {symbol} 第 {i//BATCH_SIZE + 1} 批插入冲突，尝试逐条插入自愈...")
                        saved_batch_count = _insert_with_skip_duplicates(batch_df, table_name, engine)
                        saved_count += saved_batch_count
                    else:
                        logging.error(f"{symbol} 第 {i//BATCH_SIZE + 1} 批插入失败: {e}")
                        raise

                if (i + BATCH_SIZE) % (BATCH_SIZE * 10) == 0 or (i + BATCH_SIZE) >= total_rows:
                    logging.info(f"{symbol} 已保存 {saved_count}/{total_rows} 条数据")
        
        if saved_count < total_rows:
            logging.info(f"{symbol} 成功保存 {saved_count} 条K线数据（共 {total_rows} 条，跳过 {total_rows - saved_count} 条重复数据）")
        else:
            logging.info(f"{symbol} 成功保存 {saved_count} 条K线数据")
        return True
        
    except Exception as e:
        logging.error(f"下载 {symbol} K线数据失败: {e}")
        return False


def download_all_symbols(
    interval: str = "1d",
    days_back: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: Optional[int] = API_DATA_LIMIT,
    update_existing: bool = False,
    symbols: Optional[List[str]] = None,
    auto_split: bool = True,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_delay: float = DEFAULT_BATCH_DELAY
):
    """
    下载所有交易对的K线数据
    
    Args:
        interval: K线间隔, 默认 '1d'
        days_back: 回溯天数, 默认None(下载所有数据), 如果提供了start_time和end_time则忽略此参数
        start_time: 开始时间, 默认None(根据days_back计算或下载所有数据)
        end_time: 结束时间, 默认None(昨天的结束时间)
        limit: 每次请求的最大条数, 默认1500
        update_existing: 是否更新已存在的数据, 默认False
        symbols: 指定要下载的交易对列表, 默认None(下载所有)
    """
    logging.info("=" * 80)
    logging.info(f"开始下载所有交易对的K线数据，间隔: {interval}")
    if start_time:
        logging.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if end_time:
        logging.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if days_back:
        logging.info(f"回溯天数: {days_back}")
    logging.info("=" * 80)
    
    # 获取交易对列表
    if symbols is None:
        logging.info("正在获取所有交易对...")
        all_symbols = in_exchange_trading_symbols()
        if not all_symbols:
            logging.error("无法获取交易对列表")
            return
        # 从交易所获取的交易对列表已经是正常交易的，不需要额外校验
        logging.info(f"从交易所获取到 {len(all_symbols)} 个正常交易的交易对")
    else:
        # 🔧 如果用户提供了自定义交易对列表，需要校验每个交易对
        all_symbols = symbols
        logging.info(f"用户指定了 {len(all_symbols)} 个交易对，将进行校验...")
        
        # 过滤掉不在交易所正常交易的交易对
        valid_symbols = []
        invalid_symbols = []
        valid_trading_list = get_valid_trading_symbols()
        
        for symbol in all_symbols:
            if valid_trading_list and symbol not in valid_trading_list:
                invalid_symbols.append(symbol)
                logging.warning(f"⚠️ 交易对 {symbol} 不在交易所正常交易列表中，将跳过")
            else:
                valid_symbols.append(symbol)
        
        if invalid_symbols:
            logging.warning(f"⚠️ 共 {len(invalid_symbols)} 个交易对不在交易所正常交易列表中，已跳过")
            logging.info(f"✅ 共 {len(valid_symbols)} 个有效交易对将进行下载")
        
        all_symbols = valid_symbols
        
        if not all_symbols:
            logging.error("没有有效的交易对可以下载")
            return
    
    logging.info(f"共找到 {len(all_symbols)} 个交易对")
    
    # 计算时间范围
    # 如果提供了start_time和end_time，优先使用；否则使用默认逻辑
    if end_time is None:
        now_utc = datetime.now(timezone.utc)
        if interval in ['1d', '3d', '1w', '1M']:
            # 日线及以上, 默认结束时间为昨天的结束时间(不包含今天)
            today = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = today - timedelta(seconds=1)  # 昨天的23:59:59
        else:
            # 小时线及以下, 设置为当前时间之前的最新完整K线时间
            interval_seconds = calculate_interval_seconds(interval)
            current_timestamp = int(now_utc.timestamp())
            kline_index = current_timestamp // interval_seconds
            current_kline_start_timestamp = kline_index * interval_seconds
            latest_complete_kline_start_timestamp = current_kline_start_timestamp - interval_seconds
            end_time = datetime.fromtimestamp(latest_complete_kline_start_timestamp, tz=timezone.utc)
            logging.info(f"默认结束时间设置为最新完整K线时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    if start_time is None:
        # 如果没有提供start_time，根据days_back计算
        if days_back:
            start_time = end_time - timedelta(days=days_back)
        # 如果days_back也为None，则start_time保持为None（下载所有数据）
    
    # 下载每个交易对的数据
    success_count = 0
    fail_count = 0
    
    for i, symbol in enumerate(all_symbols, 1):
        logging.info(f"[{i}/{len(all_symbols)}] 处理交易对: {symbol}")
        if download_kline_data(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            update_existing=update_existing,
            auto_split=auto_split,
            request_delay=request_delay
        ):
            success_count += 1
        else:
            fail_count += 1
        
        # 每处理指定数量的交易对后暂停，避免触发交易所API限制
        if i % batch_size == 0:
            logging.info(f"已处理 {i} 个交易对, 暂停 {batch_delay} 秒以避免API限制...")
            time.sleep(batch_delay)
    
    logging.info(f"下载完成！成功: {success_count}, 失败: {fail_count}")


def download_missing_symbols(
    interval: str = "1d",
    days_back: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: Optional[int] = API_DATA_LIMIT,
    auto_split: bool = True,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_delay: float = DEFAULT_BATCH_DELAY
):
    """只下载本地数据库中缺失的交易对数据"""
    logging.info("=" * 80)
    logging.info(f"开始下载缺失的交易对数据，间隔: {interval}")
    if start_time:
        logging.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if end_time:
        logging.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if days_back:
        logging.info(f"回溯天数: {days_back}")
    logging.info("=" * 80)
    logging.info("正在检查缺失的交易对...")
    
    # 获取交易所所有交易对
    exchange_symbols = in_exchange_trading_symbols()
    if not exchange_symbols:
        logging.error("无法获取交易所交易对列表")
        return
    
    # 获取本地已有交易对
    local_symbols = get_local_symbols(interval)
    
    # 找出缺失的交易对
    missing_symbols = [s for s in exchange_symbols if s not in local_symbols]
    
    if not missing_symbols:
        logging.info("没有缺失的交易对")
        return
    
    logging.info(f"找到 {len(missing_symbols)} 个缺失的交易对")
    
    # 下载缺失的交易对数据
    download_all_symbols(
        interval=interval,
        days_back=days_back,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        update_existing=False,
        symbols=missing_symbols,
        auto_split=auto_split,
        request_delay=request_delay,
        batch_size=batch_size,
        batch_delay=batch_delay
    )


def _update_single_symbol(
    symbol: str, 
    i: int, 
    total: int, 
    interval: str, 
    end_time: datetime, 
    limit: Optional[int], 
    auto_split: bool, 
    request_delay: float
):
    """
    处理单个交易对的更新逻辑（用于多线程并行）
    """
    try:
        logging.info(f"[{i}/{total}] 处理交易对: {symbol}")
        
        # 🔧 先检查交易对是否在交易所正常交易
        is_valid = validate_symbol(symbol, skip_validation=False)
        if not is_valid:
            logging.info(f"⏭️  跳过 {symbol}（已下架或暂停交易）")
            return 'skipped', symbol
        
        # 获取最后更新日期
        last_trade_date = get_last_trade_date(symbol, interval)
        
        status = 'updated'
        if last_trade_date:
            # 有数据，计算开始时间（最后日期的下一个K线）
            if interval in ['1d', '3d', '1w', '1M']:
                last_date_obj = datetime.strptime(last_trade_date, '%Y-%m-%d').date()
                if interval == '1d':
                    next_date = last_date_obj + timedelta(days=1)
                elif interval == '3d':
                    next_date = last_date_obj + timedelta(days=3)
                elif interval == '1w':
                    next_date = last_date_obj + timedelta(weeks=1)
                elif interval == '1M':
                    if last_date_obj.month == 12:
                        next_date = last_date_obj.replace(year=last_date_obj.year + 1, month=1)
                    else:
                        next_date = last_date_obj.replace(month=last_date_obj.month + 1)
                else:
                    next_date = last_date_obj + timedelta(days=1)
                start_time = datetime.combine(next_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            else:
                last_datetime_obj = datetime.strptime(last_trade_date, '%Y-%m-%d %H:%M:%S')
                last_datetime_obj = ensure_utc_timezone(last_datetime_obj)
                interval_seconds = calculate_interval_seconds(interval)
                last_timestamp = int(last_datetime_obj.timestamp())
                next_timestamp = ((last_timestamp // interval_seconds) + 1) * interval_seconds
                start_time = datetime.fromtimestamp(next_timestamp, tz=timezone.utc)
            
            if compare_trade_dates(last_trade_date, end_time, interval):
                logging.info(f"{symbol} 数据已是最新 ({last_trade_date})")
                return 'no_data_needed', symbol
            
            logging.info(f"{symbol} 最后更新日期: {last_trade_date}, 开始补全数据")
        else:
            start_time = end_time - timedelta(days=365)
            logging.info(f"{symbol} 没有本地数据，从 {start_time.strftime('%Y-%m-%d')} 开始下载")
            status = 'new'
        
        # 下载数据
        success = download_kline_data(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            update_existing=False,
            auto_split=auto_split,
            request_delay=request_delay,
            skip_symbol_validation=True
        )
        
        if success:
            return status, symbol
        else:
            return 'failed', symbol
            
    except Exception as e:
        logging.error(f"处理 {symbol} 失败: {e}")
        return 'failed', symbol


def auto_update_all_symbols(
    interval: str = "1d",
    limit: Optional[int] = API_DATA_LIMIT,
    auto_split: bool = True,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_delay: float = DEFAULT_BATCH_DELAY,
    max_workers: int = 1  # 默认 1 表示保持原有单线程行为
):
    """
    自动补全所有交易对的数据：从最后更新日期到现在
    
    功能：
    1. 获取指定interval的所有交易对
    2. 对于每个交易对，获取最后更新日期
    3. 从最后更新日期的下一天/下一个K线开始，补全到当前时间
    4. 对于没有数据的交易对，从默认开始时间下载
    
    Args:
        interval: K线间隔
        limit: 每次请求的最大条数
        auto_split: 是否自动分段下载
        request_delay: 每次API请求之间的延迟时间（秒）
        batch_size: 每处理多少个交易对后暂停
        batch_delay: 每批处理后的暂停时间（秒）
    """
    logging.info("=" * 80)
    logging.info(f"开始自动补全 {interval} 数据")
    logging.info("=" * 80)
    
    # 导入线程锁
    import threading
    stats_lock = threading.Lock()
    
    # 统计信息
    stats = {
        'total': 0,
        'updated': 0,
        'new': 0,
        'skipped': 0,
        'failed': 0,
        'no_data_needed': 0
    }
    
    # 修改辅助函数以支持锁
    def safe_stats_increment(key, delta=1):
        with stats_lock:
            stats[key] += delta
    
    # 只获取交易所的交易对列表（忽略本地已下架的交易对）
    # 🔧 优化：使用带缓存的 get_valid_trading_symbols，避免重复请求 exchange_info
    try:
        exchange_symbols = get_valid_trading_symbols()
    except Exception as e:
        logging.error(f"获取交易所交易对列表失败: {e}")
        logging.info("=" * 80)
        logging.info("自动补全失败：无法获取交易所交易对列表")
        logging.info("=" * 80)
        return stats
    
    if not exchange_symbols:
        logging.error("无法获取交易所交易对列表（返回空列表）")
        logging.info("=" * 80)
        logging.info("自动补全失败：无法获取交易所交易对列表")
        logging.info("=" * 80)
        return stats
    
    all_symbols = exchange_symbols
    
    logging.info(f"共找到 {len(all_symbols)} 个交易所正常交易的交易对")
    
    if not all_symbols:
        logging.warning("没有找到任何交易对，退出自动补全")
        logging.info("=" * 80)
        logging.info("自动补全完成：没有找到任何交易对")
        logging.info("=" * 80)
        return stats
    
    stats['total'] = len(all_symbols)
    
    # 计算当前时间作为结束时间
    now = datetime.now(timezone.utc)
    if interval in ['1d', '3d', '1w', '1M']:
        # 日线及以上，使用昨天的结束时间（不包含今天）
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = today - timedelta(seconds=1)  # 昨天的23:59:59
    else:
        # 小时线及以下，使用当前时间之前的最新完整K线时间
        interval_seconds = calculate_interval_seconds(interval)
        current_timestamp = int(now.timestamp())
        kline_index = current_timestamp // interval_seconds
        current_kline_start_timestamp = kline_index * interval_seconds
        latest_complete_kline_start_timestamp = current_kline_start_timestamp - interval_seconds
        end_time = datetime.fromtimestamp(latest_complete_kline_start_timestamp, tz=timezone.utc)
    
    logging.info(f"结束时间设置为: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logging.info(f"开始处理 {len(all_symbols)} 个交易对...")
    logging.info("")
    
    # 处理每个交易对
    if max_workers <= 1:
        for i, symbol in enumerate(all_symbols, 1):
            status, _ = _update_single_symbol(symbol, i, len(all_symbols), interval, end_time, limit, auto_split, request_delay)
            if status in stats:
                stats[status] += 1
            
            # 批次暂停 (仅在单线程模式下有效)
            if i % batch_size == 0:
                logging.info(f"已处理 {i} 个交易对，暂停 {batch_delay} 秒...")
                time.sleep(batch_delay)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        logging.info(f"使用 {max_workers} 个线程进行并行更新...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(
                _update_single_symbol, 
                symbol, i, len(all_symbols), interval, end_time, limit, auto_split, request_delay
            ): symbol for i, symbol in enumerate(all_symbols, 1)}
            
            completed_count = 0
            for future in as_completed(futures):
                symbol = futures[future]
                completed_count += 1
                try:
                    status, _ = future.result()
                    if status in stats:
                        with stats_lock:
                            stats[status] += 1
                except Exception as e:
                    logging.error(f"并发处理 {symbol} 时发生未捕获错误: {e}")
                    with stats_lock:
                        stats['failed'] += 1
                
                # 每10个输出一次进度
                if completed_count % 10 == 0:
                    with stats_lock:
                        logging.info(f"进度: {completed_count}/{len(all_symbols)} ({completed_count*100//len(all_symbols)}%) | 成功: {stats['updated']+stats['new']} | 跳过: {stats['skipped']} | 无需更新: {stats['no_data_needed']} | 失败: {stats['failed']}")
    
    # 输出最终进度（如果还没有输出过，或者不是10的倍数）
    total_processed = stats['updated'] + stats['new'] + stats['no_data_needed'] + stats['skipped'] + stats['failed']
    if total_processed < len(all_symbols):
        logging.info(f"进度: {total_processed}/{len(all_symbols)} | 成功: {stats['updated']+stats['new']} | 跳过: {stats['skipped']} | 无需更新: {stats['no_data_needed']} | 失败: {stats['failed']}")
    
    # 输出统计信息
    logging.info("")
    logging.info("=" * 80)
    logging.info("自动补全完成！")
    logging.info("=" * 80)
    logging.info(f"总交易对数: {stats['total']}")
    logging.info(f"✓ 更新已有数据: {stats['updated']}")
    logging.info(f"✓ 新增交易对: {stats['new']}")
    logging.info(f"○ 无需更新（数据已是最新）: {stats['no_data_needed']}")
    logging.info(f"⏭️  跳过（已下架或暂停交易）: {stats['skipped']}")
    logging.info(f"✗ 失败: {stats['failed']}")
    logging.info(f"")
    logging.info(f"总计处理: {stats['updated'] + stats['new'] + stats['no_data_needed'] + stats['skipped'] + stats['failed']} 个交易对")
    logging.info("=" * 80)
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='下载币安U本位合约K线数据')
    parser.add_argument(
        '--interval',
        type=str,
        default='1d',
        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'],
        help='K线间隔(默认: 1d)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='回溯天数(默认: None, 下载所有数据), 如果提供了--start-time和--end-time则忽略此参数'
    )
    parser.add_argument(
        '--start-time',
        type=str,
        default=None,
        help='开始时间, 格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS (默认: None, 根据--days计算或下载所有数据)'
    )
    parser.add_argument(
        '--end-time',
        type=str,
        default=None,
        help='结束时间, 格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS (默认: None, 昨天的结束时间)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='每次请求的最大条数(默认: None, 自动使用1500。如果只提供start-time和end-time会自动计算)'
    )
    parser.add_argument(
        '--auto-split',
        action='store_true',
        default=True,
        help='当数据条数超过限制时自动分段下载(默认: True)'
    )
    parser.add_argument(
        '--no-auto-split',
        action='store_false',
        dest='auto_split',
        help='禁用自动分段下载'
    )
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.1,
        help='每次API请求之间的延迟时间（秒），避免频率限制(默认: 0.1)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=30,
        help='每处理多少个交易对后暂停(默认: 30)'
    )
    parser.add_argument(
        '--batch-delay',
        type=float,
        default=3.0,
        help='每批处理后的暂停时间（秒）(默认: 3.0)'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='更新已存在的数据'
    )
    parser.add_argument(
        '--missing-only',
        action='store_true',
        help='只下载缺失的交易对'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        help='指定要下载的交易对列表, 例如: --symbols BTCUSDT ETHUSDT'
    )
    
    args = parser.parse_args()
    
    # 解析时间参数
    start_time = None
    end_time = None
    
    if args.start_time:
        try:
            # 尝试解析日期时间格式
            if len(args.start_time) == 10:  # YYYY-MM-DD
                start_time = datetime.strptime(args.start_time, '%Y-%m-%d')
            else:  # YYYY-MM-DD HH:MM:SS
                start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logging.error(f"开始时间格式错误: {args.start_time}, 错误: {e}")
            logging.error("请使用格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
            sys.exit(1)
    
    if args.end_time:
        try:
            # 尝试解析日期时间格式
            if len(args.end_time) == 10:  # YYYY-MM-DD
                end_time = datetime.strptime(args.end_time, '%Y-%m-%d')
                # 如果是日期格式，设置为当天的23:59:59
                end_time = end_time.replace(hour=23, minute=59, second=59)
            else:  # YYYY-MM-DD HH:MM:SS
                end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logging.error(f"结束时间格式错误: {args.end_time}, 错误: {e}")
            logging.error("请使用格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
            sys.exit(1)
    
    if args.missing_only:
        # 只下载缺失的交易对
        download_missing_symbols(
            interval=args.interval,
            days_back=args.days,
            start_time=start_time,
            end_time=end_time,
            limit=args.limit,
            auto_split=args.auto_split,
            request_delay=args.request_delay,
            batch_size=args.batch_size,
            batch_delay=args.batch_delay
        )
    else:
        # 下载所有或指定的交易对
        download_all_symbols(
            interval=args.interval,
            days_back=args.days,
            start_time=start_time,
            end_time=end_time,
            limit=args.limit,
            update_existing=args.update,
            symbols=args.symbols,
            auto_split=args.auto_split,
            request_delay=args.request_delay,
            batch_size=args.batch_size,
            batch_delay=args.batch_delay
        )

