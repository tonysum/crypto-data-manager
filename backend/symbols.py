"""
交易对管理模块

功能：
1. 创建和维护交易对表
2. 同步交易所交易对列表
3. 标记下架的交易对
4. 提供交易对查询接口
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set
from sqlalchemy import text
from db import engine

# 交易对表名
SYMBOLS_TABLE = 'symbols'


def create_symbols_table():
    """创建交易对表（如果不存在）"""
    with engine.connect() as conn:
        # 检查表是否存在
        result = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                );
            """),
            {"table_name": SYMBOLS_TABLE}
        )
        table_exists = result.fetchone()[0]
        
        if not table_exists:
            # 创建交易对表
            text_create = f"""
            CREATE TABLE "{SYMBOLS_TABLE}" (
                symbol VARCHAR(50) PRIMARY KEY,
                status VARCHAR(20) NOT NULL DEFAULT 'TRADING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_sync_at TIMESTAMP
            );
            CREATE INDEX idx_symbols_status ON "{SYMBOLS_TABLE}"(status);
            CREATE INDEX idx_symbols_last_sync ON "{SYMBOLS_TABLE}"(last_sync_at);
            """
            conn.execute(text(text_create))
            conn.commit()
            logging.info(f"交易对表 '{SYMBOLS_TABLE}' 创建成功")
        else:
            logging.info(f"交易对表 '{SYMBOLS_TABLE}' 已存在")
        
        return table_exists


def get_all_symbols() -> List[str]:
    """获取所有交易对列表（从交易对表）"""
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT symbol FROM "{SYMBOLS_TABLE}" ORDER BY symbol')
        )
        symbols = [row[0] for row in result.fetchall()]
    return symbols


def get_trading_symbols() -> List[str]:
    """获取状态为TRADING的交易对列表"""
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT symbol FROM "{SYMBOLS_TABLE}" WHERE status = \'TRADING\' ORDER BY symbol')
        )
        symbols = [row[0] for row in result.fetchall()]
    return symbols


def get_symbol_info(symbol: str) -> Optional[Dict]:
    """获取交易对的详细信息"""
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT symbol, status, created_at, updated_at, last_sync_at FROM "{SYMBOLS_TABLE}" WHERE symbol = :symbol'),
            {"symbol": symbol}
        )
        row = result.fetchone()
        if row:
            return {
                "symbol": row[0],
                "status": row[1],
                "created_at": row[2].isoformat() if row[2] else None,
                "updated_at": row[3].isoformat() if row[3] else None,
                "last_sync_at": row[4].isoformat() if row[4] else None,
            }
    return None


def sync_symbols_from_exchange(exchange_symbols: List[str], dry_run: bool = False) -> Dict:
    """
    同步交易所交易对列表
    
    Args:
        exchange_symbols: 交易所返回的交易对列表
        dry_run: 是否为试运行（不实际更新数据库）
    
    Returns:
        同步结果统计
    """
    if not exchange_symbols:
        logging.warning("交易所交易对列表为空")
        return {
            "added": 0,
            "updated": 0,
            "delisted": 0,
            "total_exchange": 0,
            "total_local": 0
        }
    
    exchange_symbols_set = set(exchange_symbols)
    now = datetime.now(timezone.utc)
    
    with engine.connect() as conn:
        # 获取本地所有交易对
        result = conn.execute(
            text(f'SELECT symbol, status FROM "{SYMBOLS_TABLE}"')
        )
        local_symbols = {row[0]: row[1] for row in result.fetchall()}
        local_symbols_set = set(local_symbols.keys())
        
        # 统计
        added = 0
        updated = 0
        delisted = 0
        
        # 新增的交易对
        new_symbols = exchange_symbols_set - local_symbols_set
        for symbol in new_symbols:
            if not dry_run:
                conn.execute(
                    text(f"""
                        INSERT INTO "{SYMBOLS_TABLE}" (symbol, status, created_at, updated_at, last_sync_at)
                        VALUES (:symbol, 'TRADING', :now, :now, :now)
                        ON CONFLICT (symbol) DO UPDATE SET
                            status = 'TRADING',
                            updated_at = :now,
                            last_sync_at = :now
                    """),
                    {"symbol": symbol, "now": now}
                )
            added += 1
            logging.info(f"{'[试运行] ' if dry_run else ''}新增交易对: {symbol}")
        
        # 更新已存在的交易对状态为TRADING（如果之前是DELISTED）
        existing_trading = exchange_symbols_set & local_symbols_set
        for symbol in existing_trading:
            if local_symbols[symbol] != 'TRADING':
                if not dry_run:
                    conn.execute(
                        text(f"""
                            UPDATE "{SYMBOLS_TABLE}"
                            SET status = 'TRADING',
                                updated_at = :now,
                                last_sync_at = :now
                            WHERE symbol = :symbol
                        """),
                        {"symbol": symbol, "now": now}
                    )
                updated += 1
                logging.info(f"{'[试运行] ' if dry_run else ''}更新交易对状态为TRADING: {symbol}")
            elif not dry_run:
                # 更新同步时间
                conn.execute(
                    text(f"""
                        UPDATE "{SYMBOLS_TABLE}"
                        SET last_sync_at = :now
                        WHERE symbol = :symbol
                    """),
                    {"symbol": symbol, "now": now}
                )
        
        # 标记下架的交易对
        delisted_symbols = local_symbols_set - exchange_symbols_set
        for symbol in delisted_symbols:
            if local_symbols[symbol] == 'TRADING':
                if not dry_run:
                    conn.execute(
                        text(f"""
                            UPDATE "{SYMBOLS_TABLE}"
                            SET status = 'DELISTED',
                                updated_at = :now,
                                last_sync_at = :now
                            WHERE symbol = :symbol
                        """),
                        {"symbol": symbol, "now": now}
                    )
                delisted += 1
                logging.info(f"{'[试运行] ' if dry_run else ''}标记交易对为下架: {symbol}")
        
        if not dry_run:
            conn.commit()
        
        return {
            "added": added,
            "updated": updated,
            "delisted": delisted,
            "total_exchange": len(exchange_symbols_set),
            "total_local": len(local_symbols_set)
        }


def update_symbol_status(symbol: str, status: str) -> bool:
    """
    更新交易对状态
    
    Args:
        symbol: 交易对符号
        status: 状态（TRADING, DELISTED等）
    
    Returns:
        是否更新成功
    """
    valid_statuses = ['TRADING', 'DELISTED', 'BREAK', 'PRE_TRADING', 'POST_TRADING', 'PENDING_TRADING']
    if status not in valid_statuses:
        logging.error(f"无效的状态: {status}, 有效状态: {valid_statuses}")
        return False
    
    with engine.connect() as conn:
        result = conn.execute(
            text(f"""
                UPDATE "{SYMBOLS_TABLE}"
                SET status = :status,
                    updated_at = CURRENT_TIMESTAMP
                WHERE symbol = :symbol
            """),
            {"symbol": symbol, "status": status}
        )
        conn.commit()
        
        if result.rowcount > 0:
            logging.info(f"更新交易对 {symbol} 状态为 {status}")
            return True
        else:
            logging.warning(f"交易对 {symbol} 不存在")
            return False


def add_symbol(symbol: str, status: str = 'TRADING') -> bool:
    """
    添加交易对
    
    Args:
        symbol: 交易对符号
        status: 状态（默认TRADING）
    
    Returns:
        是否添加成功
    """
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        try:
            conn.execute(
                text(f"""
                    INSERT INTO "{SYMBOLS_TABLE}" (symbol, status, created_at, updated_at, last_sync_at)
                    VALUES (:symbol, :status, :now, :now, :now)
                    ON CONFLICT (symbol) DO UPDATE SET
                        status = :status,
                        updated_at = :now
                """),
                {"symbol": symbol, "status": status, "now": now}
            )
            conn.commit()
            logging.info(f"添加交易对: {symbol} (状态: {status})")
            return True
        except Exception as e:
            logging.error(f"添加交易对失败: {symbol}, 错误: {e}")
            conn.rollback()
            return False


def delete_symbol(symbol: str) -> bool:
    """
    删除交易对
    
    Args:
        symbol: 交易对符号
    
    Returns:
        是否删除成功
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(f'DELETE FROM "{SYMBOLS_TABLE}" WHERE symbol = :symbol'),
            {"symbol": symbol}
        )
        conn.commit()
        
        if result.rowcount > 0:
            logging.info(f"删除交易对: {symbol}")
            return True
        else:
            logging.warning(f"交易对 {symbol} 不存在")
            return False


def get_symbols_by_status(status: str) -> List[str]:
    """
    根据状态获取交易对列表
    
    Args:
        status: 状态（TRADING, DELISTED等）
    
    Returns:
        交易对列表
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT symbol FROM "{SYMBOLS_TABLE}" WHERE status = :status ORDER BY symbol'),
            {"status": status}
        )
        symbols = [row[0] for row in result.fetchall()]
    return symbols


def get_symbols_statistics() -> Dict:
    """获取交易对统计信息"""
    with engine.connect() as conn:
        # 总数
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{SYMBOLS_TABLE}"'))
        total = result.fetchone()[0]
        
        # 按状态统计
        result = conn.execute(
            text(f'SELECT status, COUNT(*) FROM "{SYMBOLS_TABLE}" GROUP BY status')
        )
        status_counts = {row[0]: row[1] for row in result.fetchall()}
        
        # 最后同步时间
        result = conn.execute(
            text(f'SELECT MAX(last_sync_at) FROM "{SYMBOLS_TABLE}"')
        )
        last_sync = result.fetchone()[0]
        
    return {
        "total": total,
        "by_status": status_counts,
        "last_sync_at": last_sync.isoformat() if last_sync else None
    }
