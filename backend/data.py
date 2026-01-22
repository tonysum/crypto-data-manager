from db import engine
from sqlalchemy import text  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingImports]
from typing import Union, Dict, List, Optional
from datetime import datetime, timedelta
import logging

from binance_client import in_exchange_trading_symbols, kline_candlestick_data, kline2df

# 获取币安交易所所有合约交易对
IN_EXCHANGE_SYMBOLS = in_exchange_trading_symbols()

# 获取本地全部交易对数据
def get_local_symbols(interval: str = "1d"):
    """获取本地数据库中指定时间间隔的交易对列表"""
    try:
        # 表名格式: K{interval}{symbol}, 例如: K1dBTCUSDT
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
        return local_symbols
    except Exception as e:
        logging.warning(f"无法连接到数据库获取交易对列表: {e}")
        logging.warning("将使用空列表，某些功能可能不可用")
        return []

# 延迟初始化，避免启动时连接失败
try:
    LOCAL_SYMBOLS = get_local_symbols(interval="1d")  # 默认使用日线数据
except Exception as e:
    logging.warning(f"初始化本地交易对列表失败: {e}")
    LOCAL_SYMBOLS = []

#比较本地和交易所交易对，找出缺失的交易对
def find_missing_symbols():
    
    if not IN_EXCHANGE_SYMBOLS:
        return []
    
    missing_symbols = [
        symbol for symbol in IN_EXCHANGE_SYMBOLS
        if symbol not in LOCAL_SYMBOLS
    ]
    return missing_symbols

MISSING_SYMBOLS = find_missing_symbols()
# print(f"Missing symbols: {MISSING_SYMBOLS}")  # 注释掉，避免每次导入时都打印

def get_local_kline_data(symbol: str, interval: str = "1d") -> pd.DataFrame:
    """获取本地数据库中指定交易对的K线数据"""
    table_name = f'K{interval}{symbol}'
    # PostgreSQL 表名需要用引号包裹（保持大小写）
    safe_table_name = f'"{table_name}"'
    stmt = f'SELECT * FROM {safe_table_name} ORDER BY trade_date ASC'
    try:
        with engine.connect() as conn:
            result = conn.execute(text(stmt))
            data = result.fetchall()
            columns = result.keys()
        df = pd.DataFrame(data, columns=columns)
        logging.debug(f"成功从表 {table_name} 获取 {len(df)} 条数据")
        return df
    except Exception as e:
        # 如果表不存在或其他数据库错误，返回空DataFrame
        # 不抛出异常，让调用者处理空数据的情况
        logging.warning(f"获取本地K线数据失败（表 {table_name} 可能不存在）: {e}")
        # 尝试检查表是否存在（使用大小写不敏感的查询）
        try:
            with engine.connect() as conn:
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
                    logging.info(f"发现表名大小写不匹配: 查询的是 {table_name}，实际表名是 {actual_table_name[0]}")
                    # 使用实际表名重试
                    actual_name = actual_table_name[0]
                    safe_actual_name = f'"{actual_name}"'
                    stmt_retry = f'SELECT * FROM {safe_actual_name} ORDER BY trade_date ASC'
                    result_retry = conn.execute(text(stmt_retry))
                    data_retry = result_retry.fetchall()
                    columns_retry = result_retry.keys()
                    df_retry = pd.DataFrame(data_retry, columns=columns_retry)
                    logging.info(f"使用实际表名 {actual_name} 成功获取 {len(df_retry)} 条数据")
                    return df_retry
        except Exception as e2:
            logging.debug(f"检查表名时出错: {e2}")
        return pd.DataFrame()


def get_kline_data_for_date(symbol: str, date: str) -> Optional[pd.Series]:
    """
    获取指定交易对在指定日期的K线数据
    
    Args:
        symbol: 交易对符号
        date: 日期字符串 'YYYY-MM-DD'
    
    Returns:
        Series包含该日期的K线数据，或None
    """
    try:
        df = get_local_kline_data(symbol)
        if df.empty:
            return None
        
        # 将trade_date转换为日期字符串格式进行比较（处理多种日期格式）
        if df['trade_date'].dtype == 'object':
            # 字符串格式，提取日期部分
            df['trade_date_str'] = df['trade_date'].str[:10]
        else:
            # datetime格式
            df['trade_date_str'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        
        date_data = df[df['trade_date_str'] == date]
        if date_data.empty:
            return None
        
        return date_data.iloc[0]
    except Exception as e:
        logging.error(f"获取 {symbol} 在 {date} 的K线数据失败: {e}")
        return None



def get_24h_quote_volume(symbol: str, entry_datetime: str) -> float:
    """
    获取建仓时刻往前24小时的成交额（quote_volume）
    
    用于判断主力是否已经出货：
    - 高涨幅 + 低成交额(<3亿)：主力还没出完货，继续拉盘风险高
    - 高涨幅 + 高成交额(>=3亿)：FOMO充分，主力可以出货，做空更安全
    
    Args:
        symbol: 交易对符号
        entry_datetime: 建仓时间（格式：'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD'）
    
    Returns:
        24小时成交额（USDT），失败返回-1
    """
    table_name = f'K1h{symbol}'
    try:
        # 解析建仓时间
        if ' ' in entry_datetime:
            entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d %H:%M:%S')
        else:
            entry_dt = datetime.strptime(entry_datetime, '%Y-%m-%d')
        
        # 计算24小时前的时间
        start_dt = entry_dt - timedelta(hours=24)
        
        # 查询24小时内的成交额总和
        query = f'''
            SELECT SUM(quote_volume) as total_volume
            FROM {table_name}
            WHERE trade_date >= "{start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            AND trade_date < "{entry_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        '''
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            if row and row[0]:
                return float(row[0])
            return -1
    except Exception as e:
        logging.warning(f"获取 {symbol} 24小时成交额失败: {e}")
        return -1


def get_top_gainer_by_date(date: str) -> Optional[tuple]:
    """
    获取指定日期涨幅第一的交易对
    
    Args:
        date: 日期字符串，格式 'YYYY-MM-DD'
    
    Returns:
        Tuple[symbol, pct_chg] 或 None
    """
    from typing import Tuple
    symbols = get_local_symbols()
    top_gainer = None
    max_pct_chg = float('-inf')
    
    for symbol in symbols:
        try:
            df = get_local_kline_data(symbol)
            if df.empty:
                continue
            
            # 将trade_date转换为字符串格式进行比较（处理多种日期格式）
            if df['trade_date'].dtype == 'object':
                # 字符串格式，提取日期部分
                df['trade_date_str'] = df['trade_date'].str[:10]
            else:
                # datetime格式
                df['trade_date_str'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            # 查找指定日期的数据
            date_data = df[df['trade_date_str'] == date]
            if date_data.empty:
                continue
            
            row = date_data.iloc[0]
            pct_chg = row['pct_chg']
            
            # 如果pct_chg是NaN，尝试使用收盘价和开盘价计算涨幅
            if pd.isna(pct_chg):
                # 查找前一天的收盘价
                date_dt = datetime.strptime(date, '%Y-%m-%d')
                prev_date = (date_dt - timedelta(days=1)).strftime('%Y-%m-%d')
                prev_data = df[df['trade_date_str'] == prev_date]
                
                if not prev_data.empty and not pd.isna(prev_data.iloc[0]['close']):
                    prev_close = prev_data.iloc[0]['close']
                    current_close = row['close']
                    if not pd.isna(current_close) and prev_close > 0:
                        # 计算涨幅
                        pct_chg = (current_close - prev_close) / prev_close * 100
                    else:
                        continue
                else:
                    continue
            
            if pct_chg > max_pct_chg:
                max_pct_chg = pct_chg
                top_gainer = symbol
        except Exception as e:
            logging.debug(f"获取 {symbol} 在 {date} 的数据失败: {e}")
            continue
    
    if top_gainer:
        return (top_gainer, max_pct_chg)
    return None


def get_all_top_gainers(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取指定日期范围内所有涨幅第一的交易对（优化版本）
    
    Args:
        start_date: 开始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'
    
    Returns:
        DataFrame包含日期、交易对、涨幅
    """
    symbols = get_local_symbols()
    all_data = []
    
    # 一次性读取所有交易对的数据
    logging.info(f"正在读取 {len(symbols)} 个交易对的数据...")
    for symbol in symbols:
        try:
            df = get_local_kline_data(symbol)
            if df.empty:
                continue
            
            # 标准化trade_date格式
            if df['trade_date'].dtype == 'object':
                df['trade_date_str'] = df['trade_date'].str[:10]
            else:
                df['trade_date_str'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            # 筛选日期范围
            date_mask = (df['trade_date_str'] >= start_date) & (df['trade_date_str'] <= end_date)
            df_filtered = df[date_mask].copy()
            
            if df_filtered.empty:
                continue
            
            # 添加symbol列
            df_filtered['symbol'] = symbol
            
            # 处理NaN的pct_chg
            for idx, row in df_filtered.iterrows():
                if pd.isna(row['pct_chg']):
                    # 尝试计算涨幅
                    date_str = row['trade_date_str']
                    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                    prev_date = (date_dt - timedelta(days=1)).strftime('%Y-%m-%d')
                    prev_data = df[df['trade_date_str'] == prev_date]
                    
                    if not prev_data.empty and not pd.isna(prev_data.iloc[0]['close']):
                        prev_close = prev_data.iloc[0]['close']
                        current_close = row['close']
                        if not pd.isna(current_close) and prev_close > 0:
                            df_filtered.at[idx, 'pct_chg'] = (current_close - prev_close) / prev_close * 100
            
            # 只保留需要的列
            df_filtered = df_filtered[['trade_date_str', 'symbol', 'pct_chg']].copy()
            all_data.append(df_filtered)
        except Exception as e:
            logging.debug(f"读取 {symbol} 数据失败: {e}")
            continue
    
    if not all_data:
        logging.warning("未找到任何数据")
        return pd.DataFrame(columns=['date', 'symbol', 'pct_chg'])
    
    # 合并所有数据
    logging.info("正在合并数据并计算涨幅第一...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 过滤掉pct_chg为NaN的行
    combined_df = combined_df[combined_df['pct_chg'].notna()]
    
    # 按日期分组，使用nlargest找出每天涨幅最大的交易对
    top_gainers = (
        combined_df.groupby('trade_date_str', group_keys=False)
        .apply(lambda x: x.nlargest(1, 'pct_chg'))
        .reset_index(drop=True)
    )
    
    # 重命名列
    top_gainers = top_gainers.rename(columns={'trade_date_str': 'date'})
    
    # 按日期排序
    top_gainers = top_gainers.sort_values('date').reset_index(drop=True)
    
    # 记录日志
    for _, row in top_gainers.iterrows():
        logging.info(f"{row['date']}: 涨幅第一 {row['symbol']}, 涨幅 {row['pct_chg']:.2f}%")
    
    return top_gainers[['date', 'symbol', 'pct_chg']]


def delete_all_tables(confirm: bool = False) -> int:
    """
    删除数据库中所有的表
    
    Args:
        confirm: 是否确认删除，默认False（需要显式设置为True才会执行）
    
    Returns:
        删除的表数量
    """
    if not confirm:
        print("警告：删除所有表需要设置 confirm=True 参数")
        return 0
    
    with engine.connect() as conn:
        # 获取所有表名
        stmt = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        result = conn.execute(text(stmt))
        table_names = [row[0] for row in result.fetchall()]
        
        if not table_names:
            print("数据库中没有表")
            return 0
        
        # 删除所有表
        deleted_count = 0
        for table_name in table_names:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
                print(f"已删除表: {table_name}")
                deleted_count += 1
            except Exception as e:
                print(f"删除表 {table_name} 失败: {e}")
        
        conn.commit()
        print(f"共删除 {deleted_count} 个表")
        return deleted_count


def delete_kline_data(
    symbol: str,
    interval: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    verbose: bool = True
) -> Dict:
    """
    删除指定交易对和interval的K线数据
    
    Args:
        symbol: 交易对符号，例如 'BTCUSDT'
        interval: K线间隔，例如 '1d', '1h', '4h'
        start_time: 开始时间（格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'），如果为None则删除全部
        end_time: 结束时间（格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'），如果为None则删除全部
        verbose: 是否输出详细信息
    
    Returns:
        Dict: 删除结果统计
    """
    from datetime import datetime
    from sqlalchemy import text
    
    table_name = f'K{interval}{symbol}'
    
    # 检查表是否存在
    with engine.connect() as conn:
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
            if verbose:
                print(f"表 {table_name} 不存在")
            return {
                'success': False,
                'message': f'表 {table_name} 不存在',
                'deleted_count': 0
            }
        
        # 如果没有指定时间范围，删除整个表
        if start_time is None and end_time is None:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            conn.commit()
            if verbose:
                print(f"已删除整个表: {table_name}")
            return {
                'success': True,
                'message': f'已删除整个表: {table_name}',
                'deleted_count': -1  # -1 表示删除整个表
            }
        
        # 删除指定时间范围内的数据
        # 先获取删除前的记录数
        count_stmt = f"SELECT COUNT(*) FROM {table_name}"
        count_result = conn.execute(text(count_stmt))
        before_count = count_result.fetchone()[0]
        
        # 构建WHERE条件
        conditions = []
        if start_time:
            try:
                # 尝试解析完整时间格式
                start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                # 如果失败，尝试日期格式
                try:
                    start_dt = datetime.strptime(start_time, '%Y-%m-%d')
                    start_str = start_dt.strftime('%Y-%m-%d')
                except ValueError:
                    if verbose:
                        print(f"无效的开始时间格式: {start_time}")
                    return {
                        'success': False,
                        'message': f'无效的开始时间格式: {start_time}',
                        'deleted_count': 0
                    }
            conditions.append(f"trade_date >= '{start_str}'")
        
        if end_time:
            try:
                # 尝试解析完整时间格式
                end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                # 如果失败，尝试日期格式
                try:
                    end_dt = datetime.strptime(end_time, '%Y-%m-%d')
                    # 对于日期格式，需要包含当天的所有时间
                    end_str = end_dt.strftime('%Y-%m-%d 23:59:59')
                except ValueError:
                    if verbose:
                        print(f"无效的结束时间格式: {end_time}")
                    return {
                        'success': False,
                        'message': f'无效的结束时间格式: {end_time}',
                        'deleted_count': 0
                    }
            conditions.append(f"trade_date <= '{end_str}'")
        
        where_clause = " AND ".join(conditions)
        delete_stmt = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        try:
            conn.execute(text(delete_stmt))
            conn.commit()
            
            # 获取删除后的记录数
            count_result = conn.execute(text(count_stmt))
            after_count = count_result.fetchone()[0]
            deleted_count = before_count - after_count
            
            if verbose:
                print(f"已从表 {table_name} 删除 {deleted_count} 条记录")
            
            return {
                'success': True,
                'message': f'已删除 {deleted_count} 条记录',
                'deleted_count': deleted_count,
                'before_count': before_count,
                'after_count': after_count
            }
        except Exception as e:
            conn.rollback()
            if verbose:
                print(f"删除数据失败: {e}")
            return {
                'success': False,
                'message': f'删除失败: {str(e)}',
                'deleted_count': 0
            }


def check_data_integrity(
    symbol: Optional[str] = None,
    interval: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    check_duplicates: bool = True,
    check_missing_dates: bool = True,
    check_data_quality: bool = True,
    verbose: bool = True
) -> Dict:
    """
    检查数据完整性
    
    Args:
        symbol: 交易对符号，如果为None则检查所有交易对
        interval: K线间隔，默认"1d"
        start_date: 开始日期（可选），格式: YYYY-MM-DD
        end_date: 结束日期（可选），格式: YYYY-MM-DD
        check_duplicates: 是否检查重复数据，默认True
        check_missing_dates: 是否检查缺失日期，默认True
        check_data_quality: 是否检查数据质量（空值、异常值等），默认True
        verbose: 是否输出详细信息，默认True
    
    Returns:
        Dict: 包含检查结果的字典
    """
    results = {
        'total_symbols': 0,
        'checked_symbols': 0,
        'symbols_with_issues': [],
        'summary': {
            'duplicates': 0,
            'missing_dates': 0,
            'data_quality_issues': 0,
            'empty_tables': 0
        },
        'details': {}
    }
    
    # 获取要检查的交易对列表
    if symbol:
        symbols_to_check = [symbol]
    else:
        symbols_to_check = get_local_symbols(interval=interval)
    
    results['total_symbols'] = len(symbols_to_check)
    
    if verbose:
        print(f"开始检查数据完整性...")
        print(f"时间间隔: {interval}")
        print(f"待检查交易对数量: {len(symbols_to_check)}")
        if start_date:
            print(f"开始日期: {start_date}")
        if end_date:
            print(f"结束日期: {end_date}")
        print("-" * 60)
    
    for symbol in symbols_to_check:
        symbol_results = {
            'symbol': symbol,
            'table_name': f'K{interval}{symbol}',
            'record_count': 0,
            'date_range': None,
            'issues': [],
            'duplicate_count': 0,
            'missing_dates': [],
            'data_quality_issues': []
        }
        
        try:
            # 获取数据
            df = get_local_kline_data(symbol, interval=interval)
            
            if df.empty:
                symbol_results['issues'].append('表为空')
                results['summary']['empty_tables'] += 1
                results['details'][symbol] = symbol_results
                if verbose:
                    print(f"⚠️  {symbol}: 表为空")
                continue
            
            symbol_results['record_count'] = len(df)
            
            # 处理日期格式
            if df['trade_date'].dtype == 'object':
                df['trade_date_dt'] = pd.to_datetime(df['trade_date'].str[:10])
            else:
                df['trade_date_dt'] = pd.to_datetime(df['trade_date'])
            
            # 按日期排序
            df = df.sort_values('trade_date_dt').reset_index(drop=True)
            
            # 日期范围
            min_date = df['trade_date_dt'].min()
            max_date = df['trade_date_dt'].max()
            symbol_results['date_range'] = {
                'start': min_date.strftime('%Y-%m-%d'),
                'end': max_date.strftime('%Y-%m-%d'),
                'days': (max_date - min_date).days + 1
            }
            
            # 如果指定了日期范围，进行过滤
            if start_date:
                start_dt = pd.to_datetime(start_date)
                df = df[df['trade_date_dt'] >= start_dt]
            if end_date:
                end_dt = pd.to_datetime(end_date)
                df = df[df['trade_date_dt'] <= end_dt]
            
            # 1. 检查重复数据
            if check_duplicates:
                duplicate_mask = df.duplicated(subset=['trade_date'], keep=False)
                duplicate_count = duplicate_mask.sum()
                if duplicate_count > 0:
                    symbol_results['duplicate_count'] = duplicate_count
                    symbol_results['issues'].append(f'发现 {duplicate_count} 条重复数据')
                    results['summary']['duplicates'] += duplicate_count
                    if verbose:
                        print(f"⚠️  {symbol}: 发现 {duplicate_count} 条重复数据")
            
            # 2. 检查缺失日期
            if check_missing_dates and len(df) > 1:
                # 确定检查的起始日期
                # 如果用户提供了start_date，使用用户指定的日期
                # 否则，使用数据中的最早日期作为起始日期（因为数据可能不是从交易所开始就有的）
                if start_date:
                    check_start_date = pd.to_datetime(start_date)
                else:
                    # 使用数据中的最早日期作为起始日期
                    check_start_date = df['trade_date_dt'].min()
                    if verbose:
                        logging.debug(f"{symbol} 未指定开始日期，使用数据最早日期: {check_start_date.strftime('%Y-%m-%d')}")
                
                # 确定检查的结束日期
                if end_date:
                    check_end_date = pd.to_datetime(end_date)
                else:
                    # 使用数据中的最晚日期作为结束日期
                    check_end_date = df['trade_date_dt'].max()
                
                # 转换interval为pandas频率
                freq_map = {
                    '1d': 'D',
                    '1h': 'h',  # 使用小写 'h' 替代已弃用的 'H'
                    '4h': '4h',  # 使用小写 'h' 替代已弃用的 'H'
                    '1m': '1min',
                    '5m': '5min',
                    '15m': '15min',
                    '30m': '30min'
                }
                freq = freq_map.get(interval, 'D')
                
                # 生成期望的日期序列（从检查起始日期到结束日期）
                if freq != 'D':
                    date_range = pd.date_range(
                        start=check_start_date,
                        end=check_end_date,
                        freq=freq
                    )
                else:
                    date_range = pd.date_range(
                        start=check_start_date,
                        end=check_end_date,
                        freq='D'
                    )
                
                # 获取实际存在的日期
                existing_dates = set(df['trade_date_dt'].dt.date)
                
                # 只检查在检查范围内的日期
                check_date_range = pd.date_range(start=check_start_date, end=check_end_date, freq=freq)
                check_date_set = set(check_date_range.date)
                
                # 找出在检查范围内但不存在的数据
                missing_dates = sorted(check_date_set - existing_dates)
                
                if missing_dates:
                    symbol_results['missing_dates'] = [d.strftime('%Y-%m-%d') for d in missing_dates[:10]]  # 只保存前10个
                    missing_count = len(missing_dates)
                    symbol_results['issues'].append(f'缺失 {missing_count} 个日期')
                    results['summary']['missing_dates'] += missing_count
                    if verbose:
                        print(f"⚠️  {symbol}: 缺失 {missing_count} 个日期（显示前10个: {symbol_results['missing_dates']}）")
            
            # 3. 检查数据质量
            if check_data_quality:
                quality_issues = []
                
                # 检查关键字段是否有空值
                required_fields = ['open', 'high', 'low', 'close', 'volume']
                for field in required_fields:
                    null_count = df[field].isna().sum()
                    if null_count > 0:
                        quality_issues.append(f'{field} 字段有 {null_count} 个空值')
                
                # 检查价格数据的合理性
                invalid_price_mask = (
                    (df['high'] < df['low']) |
                    (df['open'] > df['high']) |
                    (df['open'] < df['low']) |
                    (df['close'] > df['high']) |
                    (df['close'] < df['low'])
                )
                invalid_price_count = invalid_price_mask.sum()
                if invalid_price_count > 0:
                    # 获取具体的问题数据
                    invalid_rows = df[invalid_price_mask].copy()
                    # 只保留前20条问题数据，避免输出过多
                    invalid_rows_display = invalid_rows.head(20)
                    
                    invalid_data_list = []
                    for idx, row in invalid_rows_display.iterrows():
                        issues = []
                        if row['high'] < row['low']:
                            issues.append(f"high({row['high']}) < low({row['low']})")
                        if row['open'] > row['high']:
                            issues.append(f"open({row['open']}) > high({row['high']})")
                        if row['open'] < row['low']:
                            issues.append(f"open({row['open']}) < low({row['low']})")
                        if row['close'] > row['high']:
                            issues.append(f"close({row['close']}) > high({row['high']})")
                        if row['close'] < row['low']:
                            issues.append(f"close({row['close']}) < low({row['low']})")
                        
                        trade_date = row.get('trade_date', 'N/A')
                        if isinstance(trade_date, pd.Timestamp):
                            trade_date = trade_date.strftime('%Y-%m-%d %H:%M:%S')
                        elif pd.isna(trade_date):
                            trade_date = 'N/A'
                        
                        invalid_data_list.append({
                            'trade_date': str(trade_date),
                            'open': float(row['open']) if pd.notna(row['open']) else None,
                            'high': float(row['high']) if pd.notna(row['high']) else None,
                            'low': float(row['low']) if pd.notna(row['low']) else None,
                            'close': float(row['close']) if pd.notna(row['close']) else None,
                            'issues': issues
                        })
                    
                    quality_issues.append(f'发现 {invalid_price_count} 条价格数据不合理（high < low 或 open/close 超出范围）')
                    # 将具体的问题数据添加到symbol_results中
                    if 'invalid_price_data' not in symbol_results:
                        symbol_results['invalid_price_data'] = []
                    symbol_results['invalid_price_data'].extend(invalid_data_list)
                    
                    if verbose:
                        print(f"⚠️  {symbol}: 发现 {invalid_price_count} 条价格数据不合理")
                        for data in invalid_data_list[:5]:  # 只显示前5条
                            print(f"   日期: {data['trade_date']}, open={data['open']}, high={data['high']}, low={data['low']}, close={data['close']}, 问题: {', '.join(data['issues'])}")
                        if invalid_price_count > 5:
                            print(f"   ... 还有 {invalid_price_count - 5} 条问题数据未显示")
                
                # 检查价格是否为0或负数
                price_fields = ['open', 'high', 'low', 'close']
                for field in price_fields:
                    invalid_count = (df[field] <= 0).sum()
                    if invalid_count > 0:
                        quality_issues.append(f'{field} 字段有 {invalid_count} 个无效值（<=0）')
                
                # 检查成交量是否为负数
                if 'volume' in df.columns:
                    invalid_volume_count = (df['volume'] < 0).sum()
                    if invalid_volume_count > 0:
                        quality_issues.append(f'volume 字段有 {invalid_volume_count} 个负数')
                
                if quality_issues:
                    symbol_results['data_quality_issues'] = quality_issues
                    symbol_results['issues'].extend(quality_issues)
                    results['summary']['data_quality_issues'] += len(quality_issues)
                    if verbose:
                        for issue in quality_issues:
                            print(f"⚠️  {symbol}: {issue}")
            
            # 如果没有问题，标记为通过
            if not symbol_results['issues']:
                results['checked_symbols'] += 1
                if verbose:
                    print(f"✅ {symbol}: 数据完整性检查通过（{symbol_results['record_count']} 条记录，日期范围: {symbol_results['date_range']['start']} 至 {symbol_results['date_range']['end']}）")
            else:
                results['symbols_with_issues'].append(symbol)
                results['checked_symbols'] += 1
            
            results['details'][symbol] = symbol_results
            
        except Exception as e:
            symbol_results['issues'].append(f'检查失败: {str(e)}')
            results['symbols_with_issues'].append(symbol)
            results['details'][symbol] = symbol_results
            if verbose:
                print(f"❌ {symbol}: 检查失败 - {str(e)}")
    
    # 输出总结
    if verbose:
        print("-" * 60)
        print("数据完整性检查总结:")
        print(f"总交易对数: {results['total_symbols']}")
        print(f"已检查: {results['checked_symbols']}")
        print(f"有问题的交易对: {len(results['symbols_with_issues'])}")
        print(f"空表数量: {results['summary']['empty_tables']}")
        print(f"重复数据总数: {results['summary']['duplicates']}")
        print(f"缺失日期总数: {results['summary']['missing_dates']}")
        print(f"数据质量问题总数: {results['summary']['data_quality_issues']}")
        
        if results['symbols_with_issues']:
            print(f"\n有问题的交易对列表: {', '.join(results['symbols_with_issues'][:20])}")
            if len(results['symbols_with_issues']) > 20:
                print(f"... 还有 {len(results['symbols_with_issues']) - 20} 个交易对")
    
    return results


def generate_integrity_report(
    check_results: Dict,
    interval: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    check_duplicates: bool = True,
    check_missing_dates: bool = True,
    check_data_quality: bool = True,
    output_format: str = "text",
    output_file: Optional[str] = None
) -> str:
    """
    生成数据完整性检查报告
    
    Args:
        check_results: check_data_integrity() 返回的检查结果
        interval: K线间隔
        start_date: 检查的开始日期（可选）
        end_date: 检查的结束日期（可选）
        check_duplicates: 是否检查了重复数据
        check_missing_dates: 是否检查了缺失日期
        check_data_quality: 是否检查了数据质量
        output_format: 输出格式，可选: "text", "json", "html", "markdown"
        output_file: 输出文件路径（可选），如果提供则保存到文件
    
    Returns:
        str: 报告内容
    """
    from datetime import datetime
    
    report_lines = []
    
    # 报告头部
    report_lines.append("=" * 80)
    report_lines.append("数据完整性检查报告")
    report_lines.append("=" * 80)
    report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"K线间隔: {interval}")
    if start_date:
        report_lines.append(f"开始日期: {start_date}")
    if end_date:
        report_lines.append(f"结束日期: {end_date}")
    report_lines.append("")
    
    # 检查配置
    report_lines.append("检查配置:")
    report_lines.append(f"  - 检查重复数据: {'是' if check_duplicates else '否'}")
    report_lines.append(f"  - 检查缺失日期: {'是' if check_missing_dates else '否'}")
    report_lines.append(f"  - 检查数据质量: {'是' if check_data_quality else '否'}")
    report_lines.append("")
    
    # 总体统计
    report_lines.append("=" * 80)
    report_lines.append("总体统计")
    report_lines.append("=" * 80)
    report_lines.append(f"总交易对数: {check_results['total_symbols']}")
    report_lines.append(f"已检查交易对数: {check_results['checked_symbols']}")
    report_lines.append(f"有问题的交易对数: {len(check_results['symbols_with_issues'])}")
    report_lines.append(f"数据完整性: {((check_results['checked_symbols'] - len(check_results['symbols_with_issues'])) / check_results['checked_symbols'] * 100):.2f}%" if check_results['checked_symbols'] > 0 else "N/A")
    report_lines.append("")
    
    # 问题分类统计
    report_lines.append("问题分类统计:")
    report_lines.append(f"  - 空表数量: {check_results['summary']['empty_tables']}")
    report_lines.append(f"  - 重复数据总数: {check_results['summary']['duplicates']}")
    report_lines.append(f"  - 缺失日期总数: {check_results['summary']['missing_dates']}")
    report_lines.append(f"  - 数据质量问题总数: {check_results['summary']['data_quality_issues']}")
    report_lines.append("")
    
    # 数据质量评分
    total_issues = (
        check_results['summary']['duplicates'] +
        check_results['summary']['missing_dates'] +
        check_results['summary']['data_quality_issues']
    )
    total_records = sum(details.get('record_count', 0) for details in check_results['details'].values())
    
    # 初始化质量评分变量
    quality_score = None
    quality_level = None
    
    if total_records > 0:
        issue_rate = (total_issues / total_records) * 100 if total_records > 0 else 0
        quality_score = max(0, 100 - issue_rate * 10)  # 每个问题扣10分，最低0分
        report_lines.append("数据质量评分:")
        report_lines.append(f"  - 总记录数: {total_records:,}")
        report_lines.append(f"  - 问题总数: {total_issues}")
        report_lines.append(f"  - 问题率: {issue_rate:.4f}%")
        report_lines.append(f"  - 质量评分: {quality_score:.2f}/100")
        
        # 质量等级
        if quality_score >= 95:
            quality_level = "优秀"
        elif quality_score >= 85:
            quality_level = "良好"
        elif quality_score >= 70:
            quality_level = "一般"
        elif quality_score >= 60:
            quality_level = "较差"
        else:
            quality_level = "很差"
        report_lines.append(f"  - 质量等级: {quality_level}")
        report_lines.append("")
    
    # 有问题的交易对详情
    if check_results['symbols_with_issues']:
        report_lines.append("=" * 80)
        report_lines.append("有问题的交易对详情")
        report_lines.append("=" * 80)
        
        # 按问题类型分类
        empty_tables = []
        duplicate_issues = []
        missing_date_issues = []
        quality_issues = []
        
        for symbol in check_results['symbols_with_issues']:
            details = check_results['details'][symbol]
            if details['record_count'] == 0:
                empty_tables.append(symbol)
            if details['duplicate_count'] > 0:
                duplicate_issues.append((symbol, details))
            if details['missing_dates']:
                missing_date_issues.append((symbol, details))
            if details['data_quality_issues']:
                quality_issues.append((symbol, details))
        
        # 空表
        if empty_tables:
            report_lines.append(f"\n空表交易对 ({len(empty_tables)} 个):")
            for symbol in empty_tables[:20]:  # 只显示前20个
                report_lines.append(f"  - {symbol}")
            if len(empty_tables) > 20:
                report_lines.append(f"  ... 还有 {len(empty_tables) - 20} 个空表")
        
        # 重复数据
        if duplicate_issues:
            report_lines.append(f"\n有重复数据的交易对 ({len(duplicate_issues)} 个):")
            for symbol, details in duplicate_issues[:10]:
                report_lines.append(f"  - {symbol}: {details['duplicate_count']} 条重复数据")
            if len(duplicate_issues) > 10:
                report_lines.append(f"  ... 还有 {len(duplicate_issues) - 10} 个交易对有重复数据")
        
        # 缺失日期
        if missing_date_issues:
            report_lines.append(f"\n有缺失日期的交易对 ({len(missing_date_issues)} 个):")
            for symbol, details in missing_date_issues[:10]:
                missing_count = len(details['missing_dates'])
                date_range = details.get('date_range', {})
                if date_range:
                    report_lines.append(
                        f"  - {symbol}: 缺失 {missing_count} 个日期 "
                        f"(数据范围: {date_range['start']} 至 {date_range['end']}, "
                        f"共 {date_range['days']} 天, 实际有 {details['record_count']} 条记录)"
                    )
                    if details['missing_dates']:
                        missing_dates_str = ', '.join(details['missing_dates'][:5])
                        if missing_count > 5:
                            missing_dates_str += f" ... (还有 {missing_count - 5} 个)"
                        report_lines.append(f"    缺失日期示例: {missing_dates_str}")
                else:
                    report_lines.append(f"  - {symbol}: 缺失 {missing_count} 个日期")
            if len(missing_date_issues) > 10:
                report_lines.append(f"  ... 还有 {len(missing_date_issues) - 10} 个交易对有缺失日期")
        
        # 数据质量问题
        if quality_issues:
            report_lines.append(f"\n有数据质量问题的交易对 ({len(quality_issues)} 个):")
            for symbol, details in quality_issues[:10]:
                report_lines.append(f"  - {symbol}:")
                for issue in details['data_quality_issues']:
                    report_lines.append(f"    * {issue}")
                
                # 如果有价格数据不合理的问题，显示具体的问题数据
                if 'invalid_price_data' in details and details['invalid_price_data']:
                    invalid_data_list = details['invalid_price_data']
                    report_lines.append(f"    价格数据不合理详情 (共 {len(invalid_data_list)} 条):")
                    # 显示所有问题数据（报告中应该包含完整信息）
                    for idx, data in enumerate(invalid_data_list, 1):
                        report_lines.append(f"      [{idx}] 日期: {data['trade_date']}")
                        report_lines.append(f"          open={data['open']}, high={data['high']}, low={data['low']}, close={data['close']}")
                        report_lines.append(f"          问题: {', '.join(data['issues'])}")
            if len(quality_issues) > 10:
                report_lines.append(f"  ... 还有 {len(quality_issues) - 10} 个交易对有数据质量问题")
    
    # 正常交易对统计
    normal_symbols = [
        symbol for symbol, details in check_results['details'].items()
        if symbol not in check_results['symbols_with_issues']
    ]
    if normal_symbols:
        report_lines.append("\n" + "=" * 80)
        report_lines.append("数据正常的交易对")
        report_lines.append("=" * 80)
        report_lines.append(f"正常交易对数: {len(normal_symbols)}")
        if len(normal_symbols) <= 20:
            for symbol in normal_symbols:
                details = check_results['details'][symbol]
                date_range = details.get('date_range', {})
                if date_range:
                    report_lines.append(
                        f"  - {symbol}: {details['record_count']} 条记录 "
                        f"({date_range['start']} 至 {date_range['end']})"
                    )
                else:
                    report_lines.append(f"  - {symbol}: {details['record_count']} 条记录")
        else:
            report_lines.append(f"  (前20个)")
            for symbol in normal_symbols[:20]:
                details = check_results['details'][symbol]
                date_range = details.get('date_range', {})
                if date_range:
                    report_lines.append(
                        f"  - {symbol}: {details['record_count']} 条记录 "
                        f"({date_range['start']} 至 {date_range['end']})"
                    )
                else:
                    report_lines.append(f"  - {symbol}: {details['record_count']} 条记录")
            report_lines.append(f"  ... 还有 {len(normal_symbols) - 20} 个正常交易对")
    
    # 建议和修复方案
    report_lines.append("\n" + "=" * 80)
    report_lines.append("建议和修复方案")
    report_lines.append("=" * 80)
    
    if check_results['summary']['empty_tables'] > 0:
        report_lines.append(f"\n1. 发现 {check_results['summary']['empty_tables']} 个空表:")
        report_lines.append("   建议: 使用数据下载功能下载这些交易对的数据")
        report_lines.append(f"   命令: python download_klines.py --interval {interval} --missing-only")
    
    if check_results['summary']['missing_dates'] > 0:
        report_lines.append(f"\n2. 发现 {check_results['summary']['missing_dates']} 个缺失日期:")
        report_lines.append("   建议: 使用自动下载缺失数据功能补充缺失的日期")
        report_lines.append("   方法: 在前端点击'自动下载缺失数据'按钮，或使用命令行:")
        report_lines.append(f"         python data.py --interval {interval} --auto-download")
    
    if check_results['summary']['duplicates'] > 0:
        report_lines.append(f"\n3. 发现 {check_results['summary']['duplicates']} 条重复数据:")
        report_lines.append("   建议: 清理重复数据，可以使用数据库工具删除重复记录")
        report_lines.append("   注意: 重复数据可能影响分析结果的准确性")
    
    if check_results['summary']['data_quality_issues'] > 0:
        report_lines.append(f"\n4. 发现 {check_results['summary']['data_quality_issues']} 个数据质量问题:")
        report_lines.append("   建议: 检查数据来源，可能需要重新下载有问题的数据")
        report_lines.append("   注意: 数据质量问题可能导致回测和分析结果不准确")
    
    if total_issues == 0:
        report_lines.append("\n✓ 恭喜！所有数据检查通过，数据完整性良好。")
    
    # 报告尾部
    report_lines.append("\n" + "=" * 80)
    report_lines.append("报告结束")
    report_lines.append("=" * 80)
    
    report_content = "\n".join(report_lines)
    
    # 根据格式输出
    if output_format == "json":
        import json
        report_dict = {
            "report_time": datetime.now().isoformat(),
            "config": {
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date,
                "check_duplicates": check_duplicates,
                "check_missing_dates": check_missing_dates,
                "check_data_quality": check_data_quality
            },
            "summary": check_results['summary'],
            "statistics": {
                "total_symbols": check_results['total_symbols'],
                "checked_symbols": check_results['checked_symbols'],
                "symbols_with_issues": len(check_results['symbols_with_issues']),
                "quality_score": quality_score if total_records > 0 else None,
                "quality_level": quality_level if total_records > 0 else None
            },
            "details": check_results['details'],
            "text_report": report_content
        }
        report_content = json.dumps(report_dict, ensure_ascii=False, indent=2)
    
    elif output_format == "html":
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>数据完整性检查报告</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .stat {{ background: #f9f9f9; padding: 15px; margin: 10px 0; border-left: 4px solid #2196F3; }}
        .issue {{ background: #fff3cd; padding: 10px; margin: 5px 0; border-left: 4px solid #ffc107; }}
        .success {{ background: #d4edda; padding: 10px; margin: 5px 0; border-left: 4px solid #28a745; }}
        .error {{ background: #f8d7da; padding: 10px; margin: 5px 0; border-left: 4px solid #dc3545; }}
        pre {{ background: #f4f4f4; padding: 10px; border-radius: 4px; overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #4CAF50; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>数据完整性检查报告</h1>
        <p><strong>生成时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>K线间隔:</strong> {interval}</p>
        {f"<p><strong>开始日期:</strong> {start_date}</p>" if start_date else ""}
        {f"<p><strong>结束日期:</strong> {end_date}</p>" if end_date else ""}
        
        <h2>总体统计</h2>
        <div class="stat">
            <p><strong>总交易对数:</strong> {check_results['total_symbols']}</p>
            <p><strong>已检查交易对数:</strong> {check_results['checked_symbols']}</p>
            <p><strong>有问题的交易对数:</strong> {len(check_results['symbols_with_issues'])}</p>
        </div>
        
        <h2>问题分类</h2>
        <div class="issue">
            <p><strong>空表数量:</strong> {check_results['summary']['empty_tables']}</p>
            <p><strong>重复数据总数:</strong> {check_results['summary']['duplicates']}</p>
            <p><strong>缺失日期总数:</strong> {check_results['summary']['missing_dates']}</p>
            <p><strong>数据质量问题总数:</strong> {check_results['summary']['data_quality_issues']}</p>
        </div>
        
        <h2>详细结果</h2>
        <pre>{report_content.replace('<', '&lt;').replace('>', '&gt;')}</pre>
    </div>
</body>
</html>
        """
        report_content = html_content
    
    elif output_format == "markdown":
        md_content = f"""# 数据完整性检查报告

**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**K线间隔:** {interval}  
{f"**开始日期:** {start_date}  " if start_date else ""}
{f"**结束日期:** {end_date}  " if end_date else ""}

## 检查配置

- 检查重复数据: {'是' if check_duplicates else '否'}
- 检查缺失日期: {'是' if check_missing_dates else '否'}
- 检查数据质量: {'是' if check_data_quality else '否'}

## 总体统计

| 项目 | 数量 |
|------|------|
| 总交易对数 | {check_results['total_symbols']} |
| 已检查交易对数 | {check_results['checked_symbols']} |
| 有问题的交易对数 | {len(check_results['symbols_with_issues'])} |

## 问题分类统计

| 问题类型 | 数量 |
|----------|------|
| 空表数量 | {check_results['summary']['empty_tables']} |
| 重复数据总数 | {check_results['summary']['duplicates']} |
| 缺失日期总数 | {check_results['summary']['missing_dates']} |
| 数据质量问题总数 | {check_results['summary']['data_quality_issues']} |

## 详细结果

{report_content.replace('=', '#').replace('  -', '-')}
"""
        report_content = md_content
    
    # 保存到文件
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"报告已保存到: {output_file}")
    
    return report_content


def generate_download_script_from_check(
    check_results: Dict,
    interval: str,
    output_file: Optional[str] = None,
    auto_execute: bool = False
) -> str:
    """
    根据数据完整性检查结果生成下载脚本
    
    Args:
        check_results: check_data_integrity() 返回的检查结果
        interval: K线间隔
        output_file: 输出脚本文件路径（可选），如果为None则只返回脚本内容
        auto_execute: 是否自动执行下载（默认False）
    
    Returns:
        str: 生成的下载脚本内容
    """
    from datetime import datetime, timedelta
    
    script_lines = [
        "#!/bin/bash",
        f"# 根据数据完整性检查结果自动生成的下载脚本",
        f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"# K线间隔: {interval}",
        "",
        "# 下载缺失的交易对和缺失日期的数据",
        ""
    ]
    
    # 收集需要下载的交易对和日期范围
    symbols_to_download = []
    empty_tables = []
    symbols_with_missing_dates = {}
    
    for symbol, details in check_results['details'].items():
        if details['record_count'] == 0:
            # 空表，需要完整下载
            empty_tables.append(symbol)
        elif details['missing_dates']:
            # 有缺失日期
            symbols_with_missing_dates[symbol] = {
                'missing_dates': details['missing_dates'],
                'date_range': details['date_range']
            }
    
    # 生成下载命令
    if empty_tables:
        script_lines.append("# 下载空表的数据")
        for symbol in empty_tables:
            details = check_results['details'][symbol]
            if details.get('date_range'):
                start_date = details['date_range']['start']
                end_date = details['date_range']['end']
            else:
                # 默认下载最近1年的数据
                end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            
            script_lines.append(
                f"python download_klines.py --interval {interval} --symbols {symbol} "
                f"--start-time {start_date} --end-time {end_date}"
            )
        script_lines.append("")
    
    if symbols_with_missing_dates:
        script_lines.append("# 下载缺失日期的数据")
        for symbol, info in symbols_with_missing_dates.items():
            missing_dates = info['missing_dates']
            if missing_dates:
                # 计算缺失日期的范围
                missing_dates_sorted = sorted(missing_dates)
                start_date = missing_dates_sorted[0]
                end_date = missing_dates_sorted[-1]
                
                script_lines.append(
                    f"python download_klines.py --interval {interval} --symbols {symbol} "
                    f"--start-time {start_date} --end-time {end_date}"
                )
        script_lines.append("")
    
    # 如果有缺失的交易对（在交易所但不在本地）
    if check_results['summary']['empty_tables'] > 0:
        script_lines.append("# 下载缺失的交易对（如果存在）")
        script_lines.append(f"python download_klines.py --interval {interval} --missing-only")
        script_lines.append("")
    
    script_content = "\n".join(script_lines)
    
    # 如果指定了输出文件，写入文件
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        import os
        os.chmod(output_file, 0o755)  # 添加执行权限
        print(f"下载脚本已保存到: {output_file}")
    
    # 如果启用自动执行
    if auto_execute:
        from download_klines import download_kline_data, download_all_symbols
        from datetime import datetime as dt
        
        print("开始自动下载缺失数据...")
        
        # 下载空表
        for symbol in empty_tables:
            details = check_results['details'][symbol]
            if details['date_range']:
                start_date = dt.strptime(details['date_range']['start'], '%Y-%m-%d')
                end_date = dt.strptime(details['date_range']['end'], '%Y-%m-%d')
            else:
                end_date = dt.now() - timedelta(days=1)
                start_date = end_date - timedelta(days=365)
            
            print(f"下载 {symbol} 的数据...")
            download_kline_data(
                symbol=symbol,
                interval=interval,
                start_time=start_date,
                end_time=end_date,
                update_existing=True  # 强制更新，确保下载缺失的数据
            )
        
        # 下载缺失日期
        for symbol, info in symbols_with_missing_dates.items():
            missing_dates = info['missing_dates']
            if missing_dates:
                missing_dates_sorted = sorted(missing_dates)
                start_date = dt.strptime(missing_dates_sorted[0], '%Y-%m-%d')
                end_date = dt.strptime(missing_dates_sorted[-1], '%Y-%m-%d')
                
                print(f"下载 {symbol} 缺失日期的数据 ({start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')})...")
                download_kline_data(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_date,
                    end_time=end_date,
                    update_existing=True  # 强制更新，确保下载缺失的数据
                )
        
        print("自动下载完成！")
    
    return script_content


def download_missing_data_from_check(
    check_results: Dict,
    interval: str,
    verbose: bool = True
) -> Dict:
    """
    根据数据完整性检查结果直接下载缺失的数据
    
    Args:
        check_results: check_data_integrity() 返回的检查结果
        interval: K线间隔
        verbose: 是否输出详细信息
    
    Returns:
        Dict: 下载结果统计
    """
    from download_klines import download_kline_data
    from datetime import datetime, timedelta
    
    download_stats = {
        'empty_tables_downloaded': 0,
        'missing_dates_downloaded': 0,
        'failed': [],
        'success': []
    }
    
    # 下载空表
    for symbol, details in check_results['details'].items():
        if details['record_count'] == 0:
            try:
                if details['date_range']:
                    start_date = datetime.strptime(details['date_range']['start'], '%Y-%m-%d')
                    end_date = datetime.strptime(details['date_range']['end'], '%Y-%m-%d')
                else:
                    # 默认下载最近1年的数据
                    end_date = datetime.now() - timedelta(days=1)
                    start_date = end_date - timedelta(days=365)
                
                if verbose:
                    print(f"下载空表 {symbol} 的数据 ({start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')})...")
                
                success = download_kline_data(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_date,
                    end_time=end_date,
                    update_existing=True  # 强制更新，确保下载缺失的数据
                )
                
                if success:
                    download_stats['empty_tables_downloaded'] += 1
                    download_stats['success'].append(symbol)
                else:
                    download_stats['failed'].append(symbol)
            except Exception as e:
                if verbose:
                    print(f"下载 {symbol} 失败: {e}")
                download_stats['failed'].append(symbol)
    
    # 下载缺失日期
    for symbol, details in check_results['details'].items():
        if details['missing_dates']:
            try:
                missing_dates = sorted(details['missing_dates'])
                start_date = datetime.strptime(missing_dates[0], '%Y-%m-%d')
                end_date = datetime.strptime(missing_dates[-1], '%Y-%m-%d')
                
                if verbose:
                    print(f"下载 {symbol} 缺失日期的数据 ({start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')})...")
                
                success = download_kline_data(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_date,
                    end_time=end_date,
                    update_existing=True  # 强制更新，确保下载缺失的数据
                )
                
                if success:
                    download_stats['missing_dates_downloaded'] += 1
                    if symbol not in download_stats['success']:
                        download_stats['success'].append(symbol)
                    # 等待数据库写入完成
                    import time
                    time.sleep(0.5)
                else:
                    if symbol not in download_stats['failed']:
                        download_stats['failed'].append(symbol)
            except Exception as e:
                if verbose:
                    print(f"下载 {symbol} 缺失日期失败: {e}")
                if symbol not in download_stats['failed']:
                    download_stats['failed'].append(symbol)
    
    if verbose:
        print("\n下载统计:")
        print(f"空表下载: {download_stats['empty_tables_downloaded']}")
        print(f"缺失日期下载: {download_stats['missing_dates_downloaded']}")
        print(f"成功: {len(download_stats['success'])}")
        print(f"失败: {len(download_stats['failed'])}")
        if download_stats['failed']:
            print(f"失败的交易对: {', '.join(download_stats['failed'])}")
    
    return download_stats


def recheck_problematic_symbols(
    check_results: Dict,
    interval: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    verbose: bool = True,
    output_file: Optional[str] = None
) -> Dict:
    """
    复检有问题的交易对，对比交易所API数据和本地数据
    
    Args:
        check_results: check_data_integrity返回的检查结果
        interval: K线间隔
        start_date: 开始日期（可选），格式: YYYY-MM-DD
        end_date: 结束日期（可选），格式: YYYY-MM-DD
        verbose: 是否输出详细信息，默认True
        output_file: 输出文件路径（可选），如果指定则生成TXT报告文件
    
    Returns:
        Dict: 包含复检结果的字典
    """
    import time
    from datetime import datetime as dt
    
    recheck_results = {
        'total_rechecked': 0,
        'exchange_api_issues': [],  # 交易所API数据有问题
        'local_data_issues': [],     # 本地数据有问题
        'both_issues': [],           # 两边都有问题
        'fixed_by_redownload': [],   # 重新下载后修复
        'details': {}
    }
    
    problematic_symbols = check_results.get('symbols_with_issues', [])
    if not problematic_symbols:
        if verbose:
            print("没有需要复检的交易对")
        return recheck_results
    
    recheck_results['total_rechecked'] = len(problematic_symbols)
    
    if verbose:
        print(f"\n开始复检 {len(problematic_symbols)} 个有问题的交易对...")
        print("=" * 80)
    
    # 转换日期为datetime对象
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = dt.strptime(start_date, '%Y-%m-%d')
    if end_date:
        end_dt = dt.strptime(end_date, '%Y-%m-%d')
    
    for idx, symbol in enumerate(problematic_symbols, 1):
        # 从check_results的details中获取该交易对的详细信息
        symbol_details = check_results.get('details', {}).get(symbol, {})
        issues = symbol_details.get('issues', [])
        
        if verbose:
            print(f"\n[{idx}/{len(problematic_symbols)}] 复检 {symbol}...")
            if issues:
                print(f"  问题: {', '.join(issues)}")
        
        symbol_detail = {
            'symbol': symbol,
            'issues': issues,
            'local_data': {},
            'exchange_data': {},
            'comparison': {},
            'conclusion': None
        }
        
        try:
            # 1. 获取本地数据
            local_df = pd.DataFrame()  # 初始化为空DataFrame
            try:
                local_df = get_local_kline_data(symbol, interval=interval)
            except Exception as e:
                # 表可能不存在或其他错误
                if verbose:
                    print(f"  警告: 获取本地数据失败: {str(e)}")
                symbol_detail['local_data'] = {
                    'record_count': 0,
                    'error': f'获取本地数据失败: {str(e)}'
                }
            
            if not local_df.empty:
                # 确保有 trade_date_dt 列（用于日期过滤）
                if 'trade_date_dt' not in local_df.columns:
                    if 'trade_date' in local_df.columns:
                        local_df['trade_date_dt'] = pd.to_datetime(local_df['trade_date'])
                    else:
                        # 如果没有 trade_date 列，使用所有数据
                        local_df['trade_date_dt'] = pd.NaT
                
                # 确定要检查的日期范围
                if start_dt and end_dt:
                    mask = (local_df['trade_date_dt'] >= start_dt) & (local_df['trade_date_dt'] <= end_dt)
                    local_df_filtered = local_df[mask].copy()
                elif start_dt:
                    mask = local_df['trade_date_dt'] >= start_dt
                    local_df_filtered = local_df[mask].copy()
                elif end_dt:
                    mask = local_df['trade_date_dt'] <= end_dt
                    local_df_filtered = local_df[mask].copy()
                else:
                    local_df_filtered = local_df.copy()
                
                # 显示实际总记录数（不过滤）
                total_record_count = len(local_df)
                
                # 计算日期范围
                date_start = None
                date_end = None
                if 'trade_date_dt' in local_df.columns and not local_df['trade_date_dt'].isna().all():
                    valid_dates = local_df['trade_date_dt'].dropna()
                    if not valid_dates.empty:
                        date_start = valid_dates.min().strftime('%Y-%m-%d')
                        date_end = valid_dates.max().strftime('%Y-%m-%d')
                
                symbol_detail['local_data'] = {
                    'record_count': total_record_count,  # 显示总记录数
                    'date_range': {
                        'start': date_start,
                        'end': date_end
                    },
                    'duplicates': local_df.duplicated(subset=['trade_date']).sum() if 'trade_date' in local_df.columns else 0,
                    'null_counts': {
                        'open': local_df['open'].isna().sum() if 'open' in local_df.columns else 0,
                        'high': local_df['high'].isna().sum() if 'high' in local_df.columns else 0,
                        'low': local_df['low'].isna().sum() if 'low' in local_df.columns else 0,
                        'close': local_df['close'].isna().sum() if 'close' in local_df.columns else 0,
                        'volume': local_df['volume'].isna().sum() if 'volume' in local_df.columns else 0
                    },
                    'invalid_prices': (((local_df['open'] <= 0) if 'open' in local_df.columns else pd.Series([False])) | 
                                      ((local_df['high'] <= 0) if 'high' in local_df.columns else pd.Series([False])) | 
                                      ((local_df['low'] <= 0) if 'low' in local_df.columns else pd.Series([False])) | 
                                      ((local_df['close'] <= 0) if 'close' in local_df.columns else pd.Series([False]))).sum(),
                    'invalid_volumes': (local_df['volume'] < 0).sum() if 'volume' in local_df.columns else 0
                }
            else:
                symbol_detail['local_data'] = {
                    'record_count': 0,
                    'error': '本地数据为空'
                }
            
            # 2. 从交易所API获取数据
            time.sleep(0.2)  # 避免API限流
            
            # 计算时间戳和日期范围
            # 如果没有指定日期范围，使用合理的默认值
            if start_dt:
                actual_start_dt = start_dt
            elif not local_df.empty and 'trade_date_dt' in local_df.columns:
                # 使用本地数据的最早日期
                valid_dates = local_df['trade_date_dt'].dropna()
                if not valid_dates.empty:
                    actual_start_dt = valid_dates.min().to_pydatetime()
                else:
                    # 默认从2020年开始
                    actual_start_dt = dt(2020, 1, 1)
            else:
                # 默认从2020年开始
                actual_start_dt = dt(2020, 1, 1)
            
            if end_dt:
                actual_end_dt = end_dt
            elif not local_df.empty and 'trade_date_dt' in local_df.columns:
                # 使用本地数据的最晚日期，或者当前时间
                valid_dates = local_df['trade_date_dt'].dropna()
                if not valid_dates.empty:
                    # 使用本地数据最晚日期加1天，确保能获取到最新数据
                    actual_end_dt = valid_dates.max().to_pydatetime() + timedelta(days=1)
                else:
                    actual_end_dt = dt.now()
            else:
                # 如果没有指定结束日期，使用当前时间
                actual_end_dt = dt.now()
            
            # 计算时间戳
            start_timestamp = int(actual_start_dt.timestamp() * 1000)
            end_timestamp = int(actual_end_dt.timestamp() * 1000)
            
            # 计算数据条数，判断是否需要分段下载
            def calculate_interval_seconds(interval: str) -> int:
                """计算K线间隔对应的秒数"""
                interval_map = {
                    '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
                    '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
                    '12h': 43200, '1d': 86400, '3d': 259200, '1w': 604800, '1M': 2592000
                }
                return interval_map.get(interval, 86400)
            
            def calculate_data_count(start_time: datetime, end_time: datetime, interval: str) -> int:
                """计算指定时间范围内的数据条数"""
                if not start_time or not end_time:
                    return 0
                interval_seconds = calculate_interval_seconds(interval)
                total_seconds = int((end_time - start_time).total_seconds())
                count = total_seconds // interval_seconds + 1
                return count
            
            def split_time_range(start_time: datetime, end_time: datetime, interval: str, max_count: int = 1500) -> List[tuple]:
                """将时间范围分割成多个段，每段不超过max_count条数据"""
                if not start_time or not end_time:
                    return []
                interval_seconds = calculate_interval_seconds(interval)
                max_seconds = (max_count - 1) * interval_seconds
                ranges = []
                current_start = start_time
                while current_start < end_time:
                    current_end = min(current_start + timedelta(seconds=max_seconds), end_time)
                    ranges.append((current_start, current_end))
                    current_start = current_end + timedelta(seconds=interval_seconds)
                return ranges
            
            # 计算数据条数
            data_count = calculate_data_count(actual_start_dt, actual_end_dt, interval)
            
            # 获取交易所数据（分段获取）
            try:
                exchange_df = pd.DataFrame()
                
                if data_count > 1500:
                    # 需要分段下载
                    if verbose:
                        print(f"  数据条数 {data_count} 超过1500条，将分段下载...")
                    
                    time_ranges = split_time_range(actual_start_dt, actual_end_dt, interval, max_count=1500)
                    
                    for idx, (seg_start, seg_end) in enumerate(time_ranges, 1):
                        seg_start_ts = int(seg_start.timestamp() * 1000)
                        seg_end_ts = int(seg_end.timestamp() * 1000)
                        
                        if verbose:
                            print(f"  下载第 {idx}/{len(time_ranges)} 段: {seg_start.strftime('%Y-%m-%d')} 至 {seg_end.strftime('%Y-%m-%d')}")
                        
                        time.sleep(0.2)  # 避免API限流
                        
                        api_data = kline_candlestick_data(
                            symbol=symbol,
                            interval=interval,
                            starttime=seg_start_ts,
                            endtime=seg_end_ts,
                            limit=1500
                        )
                        
                        if api_data:
                            seg_df = kline2df(api_data)
                            if not seg_df.empty:
                                if exchange_df.empty:
                                    exchange_df = seg_df
                                else:
                                    # 合并数据，去重
                                    exchange_df = pd.concat([exchange_df, seg_df], ignore_index=True)
                                    exchange_df = exchange_df.drop_duplicates(subset=['trade_date'], keep='first')
                                    exchange_df = exchange_df.sort_values('trade_date').reset_index(drop=True)
                else:
                    # 单次下载即可
                    api_data = kline_candlestick_data(
                        symbol=symbol,
                        interval=interval,
                        starttime=start_timestamp,
                        endtime=end_timestamp,
                        limit=1500
                    )
                    
                    if api_data:
                        exchange_df = kline2df(api_data)
                
                if not exchange_df.empty:
                    # 转换trade_date为datetime
                    if 'trade_date' in exchange_df.columns:
                        exchange_df['trade_date_dt'] = pd.to_datetime(exchange_df['trade_date'])
                    else:
                        exchange_df['trade_date_dt'] = pd.NaT
                    
                    # 显示实际总记录数（不过滤）
                    total_record_count = len(exchange_df)
                    
                    # 计算日期范围
                    date_start = None
                    date_end = None
                    if 'trade_date_dt' in exchange_df.columns and not exchange_df['trade_date_dt'].isna().all():
                        valid_dates = exchange_df['trade_date_dt'].dropna()
                        if not valid_dates.empty:
                            date_start = valid_dates.min().strftime('%Y-%m-%d')
                            date_end = valid_dates.max().strftime('%Y-%m-%d')
                    
                    symbol_detail['exchange_data'] = {
                        'record_count': total_record_count,  # 显示总记录数
                        'date_range': {
                            'start': date_start,
                            'end': date_end
                        },
                        'duplicates': exchange_df.duplicated(subset=['trade_date']).sum() if 'trade_date' in exchange_df.columns else 0,
                        'null_counts': {
                            'open': exchange_df['open'].isna().sum() if 'open' in exchange_df.columns else 0,
                            'high': exchange_df['high'].isna().sum() if 'high' in exchange_df.columns else 0,
                            'low': exchange_df['low'].isna().sum() if 'low' in exchange_df.columns else 0,
                            'close': exchange_df['close'].isna().sum() if 'close' in exchange_df.columns else 0,
                            'volume': exchange_df['volume'].isna().sum() if 'volume' in exchange_df.columns else 0
                        },
                        'invalid_prices': (((exchange_df['open'] <= 0) if 'open' in exchange_df.columns else pd.Series([False])) | 
                                          ((exchange_df['high'] <= 0) if 'high' in exchange_df.columns else pd.Series([False])) | 
                                          ((exchange_df['low'] <= 0) if 'low' in exchange_df.columns else pd.Series([False])) | 
                                          ((exchange_df['close'] <= 0) if 'close' in exchange_df.columns else pd.Series([False]))).sum(),
                        'invalid_volumes': (exchange_df['volume'] < 0).sum() if 'volume' in exchange_df.columns else 0
                    }
                    
                    # 3. 对比分析
                    comparison = {}
                    
                    # 对比记录数
                    local_count = symbol_detail['local_data'].get('record_count', 0)
                    exchange_count = symbol_detail['exchange_data'].get('record_count', 0)
                    comparison['record_count_diff'] = local_count - exchange_count
                    
                    # 对比重复数据
                    local_duplicates = symbol_detail['local_data'].get('duplicates', 0)
                    exchange_duplicates = symbol_detail['exchange_data'].get('duplicates', 0)
                    comparison['duplicates_diff'] = local_duplicates - exchange_duplicates
                    
                    # 对比空值
                    local_nulls = sum(symbol_detail['local_data'].get('null_counts', {}).values())
                    exchange_nulls = sum(symbol_detail['exchange_data'].get('null_counts', {}).values())
                    comparison['nulls_diff'] = local_nulls - exchange_nulls
                    
                    # 对比无效价格
                    local_invalid_prices = symbol_detail['local_data'].get('invalid_prices', 0)
                    exchange_invalid_prices = symbol_detail['exchange_data'].get('invalid_prices', 0)
                    comparison['invalid_prices_diff'] = local_invalid_prices - exchange_invalid_prices
                    
                    # 对比无效成交量
                    local_invalid_volumes = symbol_detail['local_data'].get('invalid_volumes', 0)
                    exchange_invalid_volumes = symbol_detail['exchange_data'].get('invalid_volumes', 0)
                    comparison['invalid_volumes_diff'] = local_invalid_volumes - exchange_invalid_volumes
                    
                    symbol_detail['comparison'] = comparison
                    
                    # 4. 得出结论
                    conclusion_parts = []
                    
                    # 如果交易所数据也有问题
                    if exchange_duplicates > 0 or exchange_nulls > 0 or exchange_invalid_prices > 0 or exchange_invalid_volumes > 0:
                        conclusion_parts.append("交易所API数据存在问题")
                        recheck_results['exchange_api_issues'].append(symbol)
                    
                    # 如果本地数据问题更严重
                    if (local_duplicates > exchange_duplicates or 
                        local_nulls > exchange_nulls or 
                        local_invalid_prices > exchange_invalid_prices or 
                        local_invalid_volumes > exchange_invalid_volumes):
                        conclusion_parts.append("本地数据问题更严重")
                        recheck_results['local_data_issues'].append(symbol)
                    
                    # 如果两边都有问题
                    if (exchange_duplicates > 0 or exchange_nulls > 0 or exchange_invalid_prices > 0 or exchange_invalid_volumes > 0) and \
                       (local_duplicates > 0 or local_nulls > 0 or local_invalid_prices > 0 or local_invalid_volumes > 0):
                        recheck_results['both_issues'].append(symbol)
                    
                    # 如果本地数据问题可以通过重新下载修复
                    if (local_duplicates > exchange_duplicates or 
                        local_nulls > exchange_nulls or 
                        local_invalid_prices > exchange_invalid_prices or 
                        local_invalid_volumes > exchange_invalid_volumes) and \
                       (exchange_duplicates == 0 and exchange_nulls == 0 and exchange_invalid_prices == 0 and exchange_invalid_volumes == 0):
                        conclusion_parts.append("建议重新下载修复")
                        recheck_results['fixed_by_redownload'].append(symbol)
                    
                    if conclusion_parts:
                        symbol_detail['conclusion'] = " | ".join(conclusion_parts)
                    else:
                        symbol_detail['conclusion'] = "数据正常"
                    
                    if verbose:
                        print(f"  本地记录数: {local_count}, 交易所记录数: {exchange_count}")
                        print(f"  本地重复: {local_duplicates}, 交易所重复: {exchange_duplicates}")
                        print(f"  本地空值: {local_nulls}, 交易所空值: {exchange_nulls}")
                        print(f"  结论: {symbol_detail['conclusion']}")
                    else:
                        symbol_detail['exchange_data'] = {
                            'record_count': 0,
                            'error': '交易所API返回空数据'
                        }
                        symbol_detail['conclusion'] = "交易所API返回空数据"
                        recheck_results['exchange_api_issues'].append(symbol)
                        
                        if verbose:
                            print(f"  警告: 交易所API返回空数据")
                else:
                    symbol_detail['exchange_data'] = {
                        'record_count': 0,
                        'error': '交易所API返回None'
                    }
                    symbol_detail['conclusion'] = "交易所API返回None"
                    recheck_results['exchange_api_issues'].append(symbol)
                    
                    if verbose:
                        print(f"  警告: 交易所API返回None")
                        
            except Exception as e:
                symbol_detail['exchange_data'] = {
                    'error': f'获取交易所数据失败: {str(e)}'
                }
                symbol_detail['conclusion'] = f"获取交易所数据失败: {str(e)}"
                recheck_results['exchange_api_issues'].append(symbol)
                
                if verbose:
                    print(f"  错误: 获取交易所数据失败: {str(e)}")
                    
        except Exception as e:
            symbol_detail['error'] = f'复检过程出错: {str(e)}'
            if verbose:
                print(f"  错误: {str(e)}")
        
        recheck_results['details'][symbol] = symbol_detail
    
    if verbose:
        print("\n" + "=" * 80)
        print("复检总结:")
        print(f"  总复检数: {recheck_results['total_rechecked']}")
        print(f"  交易所API问题: {len(recheck_results['exchange_api_issues'])}")
        print(f"  本地数据问题: {len(recheck_results['local_data_issues'])}")
        print(f"  两边都有问题: {len(recheck_results['both_issues'])}")
        print(f"  可通过重新下载修复: {len(recheck_results['fixed_by_redownload'])}")
    
    # 生成TXT报告文件
    if output_file:
        try:
            report_lines = []
            report_lines.append("=" * 80)
            report_lines.append("数据复检报告")
            report_lines.append("=" * 80)
            report_lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"K线间隔: {interval}")
            if start_date:
                report_lines.append(f"开始日期: {start_date}")
            if end_date:
                report_lines.append(f"结束日期: {end_date}")
            report_lines.append("")
            
            # 总结统计
            report_lines.append("-" * 80)
            report_lines.append("复检总结")
            report_lines.append("-" * 80)
            report_lines.append(f"总复检数: {recheck_results['total_rechecked']}")
            report_lines.append(f"交易所API问题: {len(recheck_results['exchange_api_issues'])}")
            report_lines.append(f"本地数据问题: {len(recheck_results['local_data_issues'])}")
            report_lines.append(f"两边都有问题: {len(recheck_results['both_issues'])}")
            report_lines.append(f"可通过重新下载修复: {len(recheck_results['fixed_by_redownload'])}")
            report_lines.append("")
            
            # 问题分类
            if recheck_results['exchange_api_issues']:
                report_lines.append("-" * 80)
                report_lines.append("交易所API问题交易对")
                report_lines.append("-" * 80)
                for symbol in recheck_results['exchange_api_issues']:
                    report_lines.append(f"  - {symbol}")
                report_lines.append("")
            
            if recheck_results['local_data_issues']:
                report_lines.append("-" * 80)
                report_lines.append("本地数据问题交易对")
                report_lines.append("-" * 80)
                for symbol in recheck_results['local_data_issues']:
                    report_lines.append(f"  - {symbol}")
                report_lines.append("")
            
            if recheck_results['both_issues']:
                report_lines.append("-" * 80)
                report_lines.append("两边都有问题的交易对")
                report_lines.append("-" * 80)
                for symbol in recheck_results['both_issues']:
                    report_lines.append(f"  - {symbol}")
                report_lines.append("")
            
            if recheck_results['fixed_by_redownload']:
                report_lines.append("-" * 80)
                report_lines.append("可通过重新下载修复的交易对")
                report_lines.append("-" * 80)
                for symbol in recheck_results['fixed_by_redownload']:
                    report_lines.append(f"  - {symbol}")
                report_lines.append("")
            
            # 详细对比信息
            report_lines.append("=" * 80)
            report_lines.append("详细对比信息")
            report_lines.append("=" * 80)
            
            for symbol, detail in recheck_results['details'].items():
                report_lines.append("")
                report_lines.append(f"交易对: {symbol}")
                report_lines.append("-" * 80)
                
                if detail.get('issues'):
                    report_lines.append(f"原始问题: {', '.join(detail['issues'])}")
                
                # 本地数据信息
                local_data = detail.get('local_data', {})
                report_lines.append("\n本地数据:")
                if 'error' in local_data:
                    report_lines.append(f"  错误: {local_data['error']}")
                else:
                    report_lines.append(f"  记录数: {local_data.get('record_count', 0)}")
                    if local_data.get('date_range', {}).get('start'):
                        report_lines.append(f"  日期范围: {local_data['date_range']['start']} 至 {local_data['date_range']['end']}")
                    report_lines.append(f"  重复数据: {local_data.get('duplicates', 0)}")
                    null_counts = local_data.get('null_counts', {})
                    total_nulls = sum(null_counts.values())
                    report_lines.append(f"  空值总数: {total_nulls}")
                    if total_nulls > 0:
                        report_lines.append(f"    - open: {null_counts.get('open', 0)}")
                        report_lines.append(f"    - high: {null_counts.get('high', 0)}")
                        report_lines.append(f"    - low: {null_counts.get('low', 0)}")
                        report_lines.append(f"    - close: {null_counts.get('close', 0)}")
                        report_lines.append(f"    - volume: {null_counts.get('volume', 0)}")
                    report_lines.append(f"  无效价格: {local_data.get('invalid_prices', 0)}")
                    report_lines.append(f"  无效成交量: {local_data.get('invalid_volumes', 0)}")
                
                # 交易所数据信息
                exchange_data = detail.get('exchange_data', {})
                report_lines.append("\n交易所数据:")
                if 'error' in exchange_data:
                    report_lines.append(f"  错误: {exchange_data['error']}")
                else:
                    report_lines.append(f"  记录数: {exchange_data.get('record_count', 0)}")
                    if exchange_data.get('date_range', {}).get('start'):
                        report_lines.append(f"  日期范围: {exchange_data['date_range']['start']} 至 {exchange_data['date_range']['end']}")
                    report_lines.append(f"  重复数据: {exchange_data.get('duplicates', 0)}")
                    null_counts = exchange_data.get('null_counts', {})
                    total_nulls = sum(null_counts.values())
                    report_lines.append(f"  空值总数: {total_nulls}")
                    if total_nulls > 0:
                        report_lines.append(f"    - open: {null_counts.get('open', 0)}")
                        report_lines.append(f"    - high: {null_counts.get('high', 0)}")
                        report_lines.append(f"    - low: {null_counts.get('low', 0)}")
                        report_lines.append(f"    - close: {null_counts.get('close', 0)}")
                        report_lines.append(f"    - volume: {null_counts.get('volume', 0)}")
                    report_lines.append(f"  无效价格: {exchange_data.get('invalid_prices', 0)}")
                    report_lines.append(f"  无效成交量: {exchange_data.get('invalid_volumes', 0)}")
                
                # 对比信息
                comparison = detail.get('comparison', {})
                if comparison:
                    report_lines.append("\n对比分析:")
                    report_lines.append(f"  记录数差异: {comparison.get('record_count_diff', 0)} (本地 - 交易所)")
                    report_lines.append(f"  重复数据差异: {comparison.get('duplicates_diff', 0)}")
                    report_lines.append(f"  空值差异: {comparison.get('nulls_diff', 0)}")
                    report_lines.append(f"  无效价格差异: {comparison.get('invalid_prices_diff', 0)}")
                    report_lines.append(f"  无效成交量差异: {comparison.get('invalid_volumes_diff', 0)}")
                
                # 结论
                if detail.get('conclusion'):
                    report_lines.append(f"\n结论: {detail['conclusion']}")
            
            # 写入文件
            report_content = "\n".join(report_lines)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            if verbose:
                print(f"\n复检报告已保存到: {output_file}")
        except Exception as e:
            if verbose:
                print(f"\n警告: 生成报告文件失败: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # 转换所有 numpy/pandas 类型为 Python 原生类型，以便 JSON 序列化
    def convert_to_python_types(obj):
        """递归转换 numpy/pandas 类型为 Python 原生类型"""
        try:
            import numpy as np
        except ImportError:
            np = None
        
        if isinstance(obj, dict):
            return {k: convert_to_python_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_python_types(item) for item in obj]
        elif np is not None:
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
        elif pd.isna(obj):
            return None
        elif hasattr(obj, 'item'):  # numpy scalar
            return obj.item()
        else:
            return obj
    
    return convert_to_python_types(recheck_results)


if __name__ == "__main__":
    """
    命令行使用示例:
    python data.py --symbol BTCUSDT --interval 1d
    python data.py --interval 1d --start-date 2021-01-01 --end-date 2025-12-31
    python data.py --interval 1h --check-duplicates --check-missing-dates --check-data-quality
    python data.py --interval 1h --auto-download  # 检查并自动下载缺失数据
    python data.py --interval 1d --generate-report report.html --report-format html  # 生成HTML报告
    python data.py --interval 1h --generate-script download.sh  # 生成下载脚本
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='检查K线数据完整性')
    parser.add_argument(
        '--symbol',
        type=str,
        default=None,
        help='交易对符号（如BTCUSDT），如果不指定则检查所有交易对'
    )
    parser.add_argument(
        '--interval',
        type=str,
        default='1d',
        help='K线间隔（默认: 1d）'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='开始日期（格式: YYYY-MM-DD）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='结束日期（格式: YYYY-MM-DD）'
    )
    parser.add_argument(
        '--check-duplicates',
        action='store_true',
        help='检查重复数据'
    )
    parser.add_argument(
        '--check-missing-dates',
        action='store_true',
        help='检查缺失日期'
    )
    parser.add_argument(
        '--check-data-quality',
        action='store_true',
        help='检查数据质量'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='静默模式，只输出总结'
    )
    parser.add_argument(
        '--auto-download',
        action='store_true',
        dest='auto_download',
        help='自动下载缺失的数据'
    )
    parser.add_argument(
        '--generate-script',
        type=str,
        default=None,
        dest='generate_script',
        help='生成下载脚本并保存到指定文件'
    )
    parser.add_argument(
        '--generate-report',
        type=str,
        default=None,
        dest='generate_report',
        help='生成完整性报告并保存到指定文件'
    )
    parser.add_argument(
        '--report-format',
        type=str,
        default='text',
        choices=['text', 'json', 'html', 'markdown'],
        dest='report_format',
        help='报告格式（默认: text）'
    )
    
    args = parser.parse_args()
    
    # 如果没有指定任何检查项，默认全部检查
    check_duplicates = args.check_duplicates if (args.check_duplicates or args.check_missing_dates or args.check_data_quality) else True
    check_missing_dates = args.check_missing_dates if (args.check_missing_dates or args.check_data_quality) else True
    check_data_quality = args.check_data_quality if args.check_data_quality else True
    
    results = check_data_integrity(
        symbol=args.symbol,
        interval=args.interval,
        start_date=args.start_date,
        end_date=args.end_date,
        check_duplicates=check_duplicates,
        check_missing_dates=check_missing_dates,
        check_data_quality=check_data_quality,
        verbose=not args.quiet
    )
    
    # 生成下载脚本
    if args.generate_script:
        script_content = generate_download_script_from_check(
            check_results=results,
            interval=args.interval,
            output_file=args.generate_script,
            auto_execute=False
        )
        if not args.quiet:
            print(f"\n下载脚本已生成: {args.generate_script}")
    
    # 自动下载缺失数据
    if args.auto_download:
        download_stats = download_missing_data_from_check(
            check_results=results,
            interval=args.interval,
            verbose=not args.quiet
        )
    
    # 生成报告
    if args.generate_report:
        # 从文件扩展名推断格式
        report_file = args.generate_report
        if report_file.endswith('.html'):
            report_format = 'html'
        elif report_file.endswith('.json'):
            report_format = 'json'
        elif report_file.endswith('.md'):
            report_format = 'markdown'
        else:
            report_format = args.report_format
        
        report_content = generate_integrity_report(
            check_results=results,
            interval=args.interval,
            start_date=args.start_date,
            end_date=args.end_date,
            check_duplicates=check_duplicates,
            check_missing_dates=check_missing_dates,
            check_data_quality=check_data_quality,
            output_format=report_format,
            output_file=report_file
        )
        if not args.quiet:
            print(f"\n报告已生成: {report_file}")
    
    # 如果有问题，返回非零退出码
    if results['symbols_with_issues'] or results['summary']['empty_tables'] > 0:
        exit(1)
    else:
        exit(0)