#!/usr/bin/env python3
"""
其他数据下载程序 - 自动守护进程版本
持续运行，定时下载其他类型的数据

功能：
1. 下载所有交易对的顶级交易者数据（每小时更新）
2. 下载所有交易对的资金费率数据（每小时更新）
3. 下载所有交易对的基差数据（每小时更新）
4. 下载所有交易对的Premium Index K线数据（每小时更新）
4. 智能调度，避免API速率限制
5. 持续运行，支持开机自启动
6. 错误重试机制

使用方法：
  python download_other.py                    # 持续运行，下载所有数据
  python download_other.py --daemon           # 后台守护进程模式
  python download_other.py --once             # 只运行一次
  python download_other.py --trader-only      # 只下载交易者数据
  python download_other.py --funding-only      # 只下载资金费率
  python download_other.py --basis-only       # 只下载基差数据
  python download_other.py --premium-only     # 只下载Premium Index

作者：量化交易助手
更新时间：2026-01-19
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List
import argparse
from sqlalchemy import text

# 导入数据库引擎和配置
from db import engine
from config import (
    BINANCE_FUTURES_BASE_URL,
    API_REQUEST_INTERVAL,
    UPDATE_INTERVAL_1H,
    BINANCE_PROXY
)

import requests

# 配置日志
if not logging.getLogger().hasHandlers():
    log_file = f'download_other_{datetime.now().strftime("%Y%m%d")}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


class AutoDataDownloader:
    """自动数据下载器 - 持续运行版本（仅其他数据）"""
    
    def __init__(self, download_trader=True, download_funding=True, download_basis=True, download_premium=True):
        self.download_trader = download_trader
        self.download_funding = download_funding
        self.download_basis = download_basis
        self.download_premium = download_premium
        
        self.last_trader_update = None
        self.last_funding_update = None
        self.last_basis_update = None
        self.last_premium_update = None
        
        # 🔧 配置代理
        self.proxies = None
        if BINANCE_PROXY:
            self.proxies = {
                'http': BINANCE_PROXY,
                'https': BINANCE_PROXY
            }
        
        logging.info("="*80)
        logging.info("🚀 其他数据下载守护进程启动")
        logging.info("="*80)
        logging.info(f"交易者数据: {'✅' if download_trader else '❌'}")
        logging.info(f"资金费率: {'✅' if download_funding else '❌'}")
        logging.info(f"基差数据: {'✅' if download_basis else '❌'}")
        logging.info(f"Premium Index: {'✅' if download_premium else '❌'}")
        logging.info("="*80)
    
    def get_all_symbols(self) -> List[str]:
        """获取所有USDT交易对"""
        try:
            url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=10, proxies=self.proxies)
            data = response.json()
            
            symbols = []
            for symbol_info in data['symbols']:
                symbol = symbol_info['symbol']
                if symbol.endswith('USDT') and symbol_info['status'] == 'TRADING':
                    if not symbol.endswith(('UPUSDT', 'DOWNUSDT', 'USDTM')):
                        symbols.append(symbol)
            
            logging.info(f"获取到 {len(symbols)} 个USDT交易对")
            return sorted(symbols)
        except Exception as e:
            logging.error(f"获取交易对列表失败: {e}")
            return []
    
    def get_latest_trader_timestamp(self, symbol: str, table_name: str) -> int:
        """获取交易者数据的最新 timestamp
        
        Args:
            symbol: 交易对
            table_name: 表名 ('top_account_ratio' 或 'top_position_ratio')
        
        Returns:
            最新的 timestamp，如果没有数据则返回 0
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT MAX(timestamp) FROM {table_name} WHERE symbol = :symbol
                """), {'symbol': symbol})
                row = result.fetchone()
                if row and row[0]:
                    return int(row[0])
        except Exception as e:
            # 表不存在或其他错误，返回 0
            logging.debug(f"查询 {table_name} 最新 timestamp 失败: {e}")
        return 0
    
    def get_latest_funding_time(self, symbol: str) -> datetime:
        """获取资金费率的最新 funding_time
        
        Args:
            symbol: 交易对
        
        Returns:
            最新的 funding_time，如果没有数据则返回 None
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(funding_time) FROM funding_rates WHERE symbol = :symbol
                """), {'symbol': symbol})
                row = result.fetchone()
                if row and row[0]:
                    return row[0]
        except Exception as e:
            logging.debug(f"查询资金费率最新 funding_time 失败: {e}")
        return None
    
    def get_latest_basis_timestamp(self, symbol: str) -> datetime:
        """获取基差数据的最新 timestamp
        
        Args:
            symbol: 交易对
        
        Returns:
            最新的 timestamp，如果没有数据则返回 None
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(timestamp) FROM basis_data WHERE symbol = :symbol
                """), {'symbol': symbol})
                row = result.fetchone()
                if row and row[0]:
                    return row[0]
        except Exception as e:
            logging.debug(f"查询基差数据最新 timestamp 失败: {e}")
        return None
    
    def get_latest_premium_open_time(self, symbol: str) -> int:
        """获取 Premium Index 的最新 open_time
        
        Args:
            symbol: 交易对
        
        Returns:
            最新的 open_time，如果没有数据则返回 0
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(open_time) FROM premium_index_history 
                    WHERE symbol = :symbol AND interval = '1h'
                """), {'symbol': symbol})
                row = result.fetchone()
                if row and row[0]:
                    return int(row[0])
        except Exception as e:
            logging.debug(f"查询 Premium Index 最新 open_time 失败: {e}")
        return 0
    
    def download_trader_data(self, symbol: str) -> bool:
        """下载单个交易对的顶级交易者数据（增量更新）"""
        try:
            with engine.connect() as conn:
                # 创建表（如果不存在）
                # top_account_ratio 表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS top_account_ratio (
                        symbol VARCHAR(50),
                        timestamp BIGINT,
                        long_short_ratio DOUBLE PRECISION,
                        long_account DOUBLE PRECISION,
                        short_account DOUBLE PRECISION,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """))
                
                # top_position_ratio 表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS top_position_ratio (
                        symbol VARCHAR(50),
                        timestamp BIGINT,
                        long_short_ratio DOUBLE PRECISION,
                        long_position DOUBLE PRECISION,
                        short_position DOUBLE PRECISION,
                        PRIMARY KEY (symbol, timestamp)
                    )
                """))
                
                conn.commit()
            
            # 下载数据
            endpoints = {
                'top_account_ratio': '/futures/data/topLongShortAccountRatio',
                'top_position_ratio': '/futures/data/topLongShortPositionRatio',
            }
            
            total_records = 0
            new_records = 0
            
            for table_name, endpoint in endpoints.items():
                try:
                    # 查询本地最新 timestamp
                    latest_timestamp = self.get_latest_trader_timestamp(symbol, table_name)
                    
                    url = f"{BINANCE_FUTURES_BASE_URL}{endpoint}"
                    params = {'symbol': symbol, 'period': '1h', 'limit': 168}
                    
                    # 如果本地有数据，只获取最新记录之后的数据
                    if latest_timestamp > 0:
                        # 币安 API 的 startTime 参数（毫秒时间戳）
                        params['startTime'] = latest_timestamp + 1
                        logging.debug(f"    {symbol} {table_name}: 本地最新 timestamp={latest_timestamp}, 从 {params['startTime']} 开始获取")
                    
                    response = requests.get(url, params=params, timeout=10, proxies=self.proxies)
                    data = response.json()
                    
                    if not data or not isinstance(data, list):
                        if latest_timestamp > 0:
                            logging.debug(f"    {symbol} {table_name}: 本地已是最新，无需更新")
                        continue
                    
                    with engine.connect() as conn:
                        for item in data:
                            timestamp = item['timestamp']
                            
                            # 跳过已存在的数据（虽然 ON CONFLICT 会处理，但可以提前过滤减少数据库操作）
                            if timestamp <= latest_timestamp:
                                continue
                            
                            long_short_ratio = float(item['longShortRatio'])
                            
                            if table_name == 'top_account_ratio':
                                conn.execute(text("""
                                    INSERT INTO top_account_ratio
                                    (symbol, timestamp, long_short_ratio, long_account, short_account)
                                    VALUES (:symbol, :timestamp, :long_short_ratio, :long_account, :short_account)
                                    ON CONFLICT (symbol, timestamp) 
                                    DO UPDATE SET 
                                        long_short_ratio = EXCLUDED.long_short_ratio,
                                        long_account = EXCLUDED.long_account,
                                        short_account = EXCLUDED.short_account
                                """), {
                                    'symbol': symbol,
                                    'timestamp': timestamp,
                                    'long_short_ratio': long_short_ratio,
                                    'long_account': float(item['longAccount']),
                                    'short_account': float(item['shortAccount'])
                                })
                            
                            elif table_name == 'top_position_ratio':
                                conn.execute(text("""
                                    INSERT INTO top_position_ratio
                                    (symbol, timestamp, long_short_ratio, long_position, short_position)
                                    VALUES (:symbol, :timestamp, :long_short_ratio, :long_position, :short_position)
                                    ON CONFLICT (symbol, timestamp) 
                                    DO UPDATE SET 
                                        long_short_ratio = EXCLUDED.long_short_ratio,
                                        long_position = EXCLUDED.long_position,
                                        short_position = EXCLUDED.short_position
                                """), {
                                    'symbol': symbol,
                                    'timestamp': timestamp,
                                    'long_short_ratio': long_short_ratio,
                                    'long_position': float(item['longPosition']),
                                    'short_position': float(item['shortPosition'])
                                })
                            
                            total_records += 1
                            new_records += 1
                        
                        conn.commit()
                    
                    time.sleep(0.05)
                
                except Exception as e:
                    logging.debug(f"    {table_name} 失败: {e}")
                    continue
            
            if new_records > 0:
                logging.info(f"  ✅ {symbol} 交易者: +{new_records} 条新数据（共 {total_records} 条）")
                return True
            elif total_records > 0:
                logging.debug(f"  {symbol} 交易者: 已是最新，无需更新")
                return True
            return False
                
        except Exception as e:
            logging.error(f"  ❌ {symbol} 交易者失败: {e}")
            return False
    
    def download_funding_rate(self, symbol: str) -> bool:
        """下载单个交易对的资金费率数据（增量更新）"""
        try:
            with engine.connect() as conn:
                # 创建资金费率表（添加唯一约束避免重复）
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS funding_rates (
                        id BIGSERIAL PRIMARY KEY,
                        symbol VARCHAR(50) NOT NULL,
                        funding_rate DOUBLE PRECISION,
                        funding_time TIMESTAMP NOT NULL,
                        mark_price DOUBLE PRECISION,
                        index_price DOUBLE PRECISION,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, funding_time)
                    )
                """))
                
                # 创建索引（如果不存在）
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_funding_symbol_time 
                    ON funding_rates(symbol, timestamp)
                """))
                
                conn.commit()
            
            # 查询本地最新 funding_time
            latest_funding_time = self.get_latest_funding_time(symbol)
            
            # 获取资金费率
            try:
                url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/premiumIndex"
                params = {'symbol': symbol}
                response = requests.get(url, params=params, timeout=10, 
                                      proxies=self.proxies)
                response.raise_for_status()
                data = response.json()
                
                funding_time = datetime.fromtimestamp(int(data['nextFundingTime'])/1000)
                
                # 如果本地已有这个 funding_time 的记录，跳过
                if latest_funding_time and funding_time <= latest_funding_time:
                    logging.debug(f"  {symbol} 资金费率: 本地已是最新（funding_time={funding_time}）")
                    return True
                
                with engine.connect() as conn:
                    result = conn.execute(text("""
                        INSERT INTO funding_rates 
                        (symbol, funding_rate, funding_time, mark_price, index_price, timestamp)
                        VALUES (:symbol, :funding_rate, :funding_time, :mark_price, :index_price, :timestamp)
                        ON CONFLICT (symbol, funding_time) 
                        DO UPDATE SET 
                            funding_rate = EXCLUDED.funding_rate,
                            mark_price = EXCLUDED.mark_price,
                            index_price = EXCLUDED.index_price,
                            timestamp = EXCLUDED.timestamp
                    """), {
                        'symbol': symbol,
                        'funding_rate': float(data['lastFundingRate']),
                        'funding_time': funding_time,
                        'mark_price': float(data['markPrice']),
                        'index_price': float(data['indexPrice']),
                        'timestamp': datetime.now()
                    })
                    conn.commit()
                    
                    # 检查是否有新数据插入
                    if result.rowcount > 0:
                        logging.info(f"  ✅ {symbol} 资金费率: +1 条新数据")
                        time.sleep(0.05)
                        return True
                    else:
                        logging.debug(f"  {symbol} 资金费率: 数据已存在，已更新")
                        return True
                
            except Exception as e:
                logging.debug(f"    资金费率失败: {e}")
                return False
                
        except Exception as e:
            logging.error(f"  ❌ {symbol} 资金费率失败: {e}")
            return False
    
    def download_basis_data(self, symbol: str) -> bool:
        """下载单个交易对的基差数据（增量更新）"""
        try:
            with engine.connect() as conn:
                # 创建基差表（添加唯一约束避免重复，基于 symbol 和 timestamp）
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS basis_data (
                        id BIGSERIAL PRIMARY KEY,
                        symbol VARCHAR(50) NOT NULL,
                        futures_price DOUBLE PRECISION,
                        spot_price DOUBLE PRECISION,
                        basis DOUBLE PRECISION,
                        basis_rate DOUBLE PRECISION,
                        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, timestamp)
                    )
                """))
                
                # 创建索引（如果不存在）
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_basis_symbol_time 
                    ON basis_data(symbol, timestamp)
                """))
                
                conn.commit()
            
            # 查询本地最新 timestamp
            latest_timestamp = self.get_latest_basis_timestamp(symbol)
            current_timestamp = datetime.now()
            
            # 如果本地有最近1小时内的数据，跳过（避免频繁更新）
            if latest_timestamp:
                time_diff = (current_timestamp - latest_timestamp).total_seconds()
                if time_diff < 3600:  # 1小时内
                    logging.debug(f"  {symbol} 基差数据: 本地数据较新（{int(time_diff)}秒前），跳过更新")
                    return True
            
            # 获取基差
            try:
                # 获取期货价格
                futures_url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/ticker/price"
                futures_params = {'symbol': symbol}
                futures_resp = requests.get(futures_url, params=futures_params, 
                                          timeout=10, proxies=self.proxies)
                futures_resp.raise_for_status()
                futures_price = float(futures_resp.json()['price'])
                
                # 获取现货价格
                spot_url = "https://api.binance.com/api/v3/ticker/price"
                spot_params = {'symbol': symbol}
                spot_resp = requests.get(spot_url, params=spot_params, 
                                       timeout=10, proxies=self.proxies)
                spot_resp.raise_for_status()
                spot_price = float(spot_resp.json()['price'])
                
                # 计算基差
                basis = futures_price - spot_price
                basis_rate = (basis / spot_price) * 100 if spot_price > 0 else 0
                
                with engine.connect() as conn:
                    result = conn.execute(text("""
                        INSERT INTO basis_data 
                        (symbol, futures_price, spot_price, basis, basis_rate, timestamp)
                        VALUES (:symbol, :futures_price, :spot_price, :basis, :basis_rate, :timestamp)
                        ON CONFLICT (symbol, timestamp) 
                        DO UPDATE SET 
                            futures_price = EXCLUDED.futures_price,
                            spot_price = EXCLUDED.spot_price,
                            basis = EXCLUDED.basis,
                            basis_rate = EXCLUDED.basis_rate
                    """), {
                        'symbol': symbol,
                        'futures_price': futures_price,
                        'spot_price': spot_price,
                        'basis': basis,
                        'basis_rate': basis_rate,
                        'timestamp': current_timestamp
                    })
                    conn.commit()
                    
                    # 检查是否有新数据插入
                    if result.rowcount > 0:
                        logging.info(f"  ✅ {symbol} 基差数据: +1 条新数据")
                        return True
                    else:
                        logging.debug(f"  {symbol} 基差数据: 数据已存在，已更新")
                        return True
                
            except Exception as e:
                logging.debug(f"    基差失败: {e}")
                return False
                
        except Exception as e:
            logging.error(f"  ❌ {symbol} 基差数据失败: {e}")
            return False
    
    def download_premium_index_klines(self, symbol: str, limit: int = 24) -> bool:
        """下载单个交易对的Premium Index K线数据（增量更新）
        
        Args:
            symbol: 交易对
            limit: 下载最近N小时的数据（默认24小时），如果本地有数据则从最新记录之后开始
        
        Returns:
            是否成功
        """
        try:
            with engine.connect() as conn:
                # 创建 premium_index_history 表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS premium_index_history (
                        id BIGSERIAL PRIMARY KEY,
                        symbol VARCHAR(50) NOT NULL,
                        open_time BIGINT NOT NULL,
                        open DOUBLE PRECISION NOT NULL,
                        high DOUBLE PRECISION NOT NULL,
                        low DOUBLE PRECISION NOT NULL,
                        close DOUBLE PRECISION NOT NULL,
                        interval VARCHAR(10) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol, open_time, interval)
                    )
                """))
                
                # 创建索引（如果不存在）
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_premium_symbol_time 
                    ON premium_index_history(symbol, open_time)
                """))
                
                conn.commit()
            
            # 查询本地最新 open_time
            latest_open_time = self.get_latest_premium_open_time(symbol)
            
            # 计算时间范围
            end_time = datetime.now()
            end_ts = int(end_time.timestamp() * 1000)
            
            # 如果本地有数据，从最新记录之后开始；否则获取最近N小时的数据
            if latest_open_time > 0:
                start_ts = latest_open_time + 1  # 从最新记录之后开始
                # 计算需要获取的小时数（最多不超过 limit）
                hours_needed = min(limit, int((end_ts - start_ts) / (1000 * 3600)) + 1)
                logging.debug(f"    {symbol} Premium Index: 本地最新 open_time={latest_open_time}, 从 {start_ts} 开始获取")
            else:
                start_time = end_time - timedelta(hours=limit)
                start_ts = int(start_time.timestamp() * 1000)
                hours_needed = limit
            
            # 如果本地已是最新，跳过
            if latest_open_time > 0 and start_ts >= end_ts:
                logging.debug(f"  {symbol} Premium Index: 本地已是最新，无需更新")
                return True
            
            # 请求API
            url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/premiumIndexKlines"
            params = {
                'symbol': symbol,
                'interval': '1h',
                'startTime': start_ts,
                'endTime': end_ts,
                'limit': hours_needed
            }
            
            response = requests.get(url, params=params, timeout=10,
                                  proxies=self.proxies)
            
            if response.status_code != 200:
                logging.debug(f"    {symbol} Premium Index 请求失败: HTTP {response.status_code}")
                return False
            
            klines = response.json()
            
            if not klines:
                if latest_open_time > 0:
                    logging.debug(f"  {symbol} Premium Index: 本地已是最新，无需更新")
                return False
            
            # 保存数据
            saved_count = 0
            with engine.connect() as conn:
                for kline in klines:
                    try:
                        open_time = int(kline[0])
                        
                        # 跳过已存在的数据（虽然 ON CONFLICT 会处理，但可以提前过滤）
                        if open_time <= latest_open_time:
                            continue
                        
                        result = conn.execute(text("""
                            INSERT INTO premium_index_history
                            (symbol, open_time, open, high, low, close, interval)
                            VALUES (:symbol, :open_time, :open, :high, :low, :close, :interval)
                            ON CONFLICT (symbol, open_time, interval) DO NOTHING
                        """), {
                            'symbol': symbol,
                            'open_time': open_time,
                            'open': float(kline[1]),          # open
                            'high': float(kline[2]),          # high
                            'low': float(kline[3]),            # low
                            'close': float(kline[4]),          # close
                            'interval': '1h'
                        })
                        
                        # 检查是否有行被插入（PostgreSQL 的 rowcount 在 ON CONFLICT DO NOTHING 时仍然有效）
                        if result.rowcount > 0:
                            saved_count += 1
                            
                    except Exception as e:
                        logging.debug(f"    {symbol} Premium Index 保存单条失败: {e}")
                        continue
                
                conn.commit()
            
            if saved_count > 0:
                logging.info(f"  ✅ {symbol} Premium Index: +{saved_count} 条新数据")
                return True
            elif latest_open_time > 0:
                logging.debug(f"  {symbol} Premium Index: 本地已是最新，无需更新")
                return True
            return False
                
        except Exception as e:
            logging.debug(f"  {symbol} Premium Index 失败: {e}")
            return False
    
    def run_once(self):
        """执行一次完整的数据下载"""
        logging.info("\n" + "="*80)
        logging.info(f"🔄 开始更新数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*80)
        
        # 获取交易对列表
        symbols = self.get_all_symbols()
        if not symbols:
            logging.error("无法获取交易对列表")
            return
        
        task_num = 1
        total_tasks = sum([self.download_trader, self.download_funding, self.download_basis, self.download_premium])
        
        # 下载交易者数据
        if self.download_trader:
            logging.info(f"\n📊 [{task_num}/{total_tasks}] 更新交易者数据 ({len(symbols)} 个交易对)")
            success_count = 0
            for i, symbol in enumerate(symbols, 1):
                if self.download_trader_data(symbol):
                    success_count += 1
                if i % 50 == 0:
                    logging.info(f"  进度: {i}/{len(symbols)}")
                time.sleep(API_REQUEST_INTERVAL)
            
            logging.info(f"✅ 交易者数据完成: {success_count}/{len(symbols)}")
            self.last_trader_update = datetime.now()
            task_num += 1
        
        # 下载资金费率
        if self.download_funding:
            logging.info(f"\n💸 [{task_num}/{total_tasks}] 更新资金费率 ({len(symbols)} 个交易对)")
            success_count = 0
            for i, symbol in enumerate(symbols, 1):
                if self.download_funding_rate(symbol):
                    success_count += 1
                if i % 50 == 0:
                    logging.info(f"  进度: {i}/{len(symbols)}")
                time.sleep(API_REQUEST_INTERVAL)
            
            logging.info(f"✅ 资金费率完成: {success_count}/{len(symbols)}")
            self.last_funding_update = datetime.now()
            task_num += 1
        
        # 下载基差数据
        if self.download_basis:
            logging.info(f"\n📈 [{task_num}/{total_tasks}] 更新基差数据 ({len(symbols)} 个交易对)")
            success_count = 0
            for i, symbol in enumerate(symbols, 1):
                if self.download_basis_data(symbol):
                    success_count += 1
                if i % 50 == 0:
                    logging.info(f"  进度: {i}/{len(symbols)}")
                time.sleep(API_REQUEST_INTERVAL)
            
            logging.info(f"✅ 基差数据完成: {success_count}/{len(symbols)}")
            self.last_basis_update = datetime.now()
            task_num += 1
        
        # 下载Premium Index
        if self.download_premium:
            logging.info(f"\n📊 [{task_num}/{total_tasks}] 更新Premium Index ({len(symbols)} 个交易对)")
            success_count = 0
            for i, symbol in enumerate(symbols, 1):
                if self.download_premium_index_klines(symbol, limit=24):
                    success_count += 1
                if i % 50 == 0:
                    logging.info(f"  进度: {i}/{len(symbols)}")
                time.sleep(API_REQUEST_INTERVAL)
            
            logging.info(f"✅ Premium Index完成: {success_count}/{len(symbols)}")
            self.last_premium_update = datetime.now()
        
        logging.info("\n" + "="*80)
        logging.info(f"✅ 本轮更新完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*80)
    
    def run_daemon(self):
        """持续运行模式"""
        logging.info("🔄 进入持续运行模式...")
        logging.info(f"更新策略:")
        if self.download_trader:
            logging.info(f"  • 交易者数据: 每{UPDATE_INTERVAL_1H//3600}小时更新")
        if self.download_funding:
            logging.info(f"  • 资金费率: 每{UPDATE_INTERVAL_1H//3600}小时更新")
        if self.download_basis:
            logging.info(f"  • 基差数据: 每{UPDATE_INTERVAL_1H//3600}小时更新")
        if self.download_premium:
            logging.info(f"  • Premium Index: 每{UPDATE_INTERVAL_1H//3600}小时更新")
        logging.info("="*80)
        
        # 首次运行
        self.run_once()
        
        # 持续运行
        while True:
            try:
                now = datetime.now()
                
                # 检查是否需要更新小时数据
                need_trader = self.download_trader and \
                              (self.last_trader_update is None or \
                               (now - self.last_trader_update).total_seconds() >= UPDATE_INTERVAL_1H)
                
                need_funding = self.download_funding and \
                               (self.last_funding_update is None or \
                                (now - self.last_funding_update).total_seconds() >= UPDATE_INTERVAL_1H)
                
                need_basis = self.download_basis and \
                             (self.last_basis_update is None or \
                              (now - self.last_basis_update).total_seconds() >= UPDATE_INTERVAL_1H)
                
                need_premium = self.download_premium and \
                               (self.last_premium_update is None or \
                                (now - self.last_premium_update).total_seconds() >= UPDATE_INTERVAL_1H)
                
                if need_trader or need_funding or need_basis or need_premium:
                    logging.info(f"\n⏰ 触发小时数据更新...")
                    symbols = self.get_all_symbols()
                    
                    if need_trader:
                        success = 0
                        for symbol in symbols:
                            if self.download_trader_data(symbol):
                                success += 1
                            time.sleep(API_REQUEST_INTERVAL)
                        logging.info(f"✅ 交易者数据更新完成: {success}/{len(symbols)}")
                        self.last_trader_update = now
                    
                    if need_funding:
                        success = 0
                        for symbol in symbols:
                            if self.download_funding_rate(symbol):
                                success += 1
                            time.sleep(API_REQUEST_INTERVAL)
                        logging.info(f"✅ 资金费率更新完成: {success}/{len(symbols)}")
                        self.last_funding_update = now
                    
                    if need_basis:
                        success = 0
                        for symbol in symbols:
                            if self.download_basis_data(symbol):
                                success += 1
                            time.sleep(API_REQUEST_INTERVAL)
                        logging.info(f"✅ 基差数据更新完成: {success}/{len(symbols)}")
                        self.last_basis_update = now
                    
                    if need_premium:
                        success = 0
                        for symbol in symbols:
                            if self.download_premium_index_klines(symbol, limit=24):
                                success += 1
                            time.sleep(API_REQUEST_INTERVAL)
                        logging.info(f"✅ Premium Index更新完成: {success}/{len(symbols)}")
                        self.last_premium_update = now
                
                # 等待一段时间再检查
                time.sleep(60)  # 每分钟检查一次
                
            except KeyboardInterrupt:
                logging.info("\n用户中断，退出程序")
                break
            except Exception as e:
                logging.error(f"运行出错: {e}")
                import traceback
                traceback.print_exc()
                logging.info("等待5分钟后重试...")
                time.sleep(300)


def main():
    parser = argparse.ArgumentParser(description='其他数据下载守护进程')
    parser.add_argument('--once', action='store_true', help='只运行一次，不持续运行')
    parser.add_argument('--daemon', action='store_true', help='后台守护进程模式（与--once相反）')
    parser.add_argument('--trader-only', dest='only_trader', action='store_true', help='只下载交易者数据')
    parser.add_argument('--funding-only', dest='only_funding', action='store_true', help='只下载资金费率')
    parser.add_argument('--basis-only', dest='only_basis', action='store_true', help='只下载基差数据')
    parser.add_argument('--premium-only', dest='only_premium', action='store_true', help='只下载Premium Index')
    
    args = parser.parse_args()
    
    # 确定下载内容
    if args.only_trader:
        download_trader, download_funding, download_basis, download_premium = True, False, False, False
    elif args.only_funding:
        download_trader, download_funding, download_basis, download_premium = False, True, False, False
    elif args.only_basis:
        download_trader, download_funding, download_basis, download_premium = False, False, True, False
    elif args.only_premium:
        download_trader, download_funding, download_basis, download_premium = False, False, False, True
    else:
        download_trader, download_funding, download_basis, download_premium = True, True, True, True
    
    downloader = AutoDataDownloader(
        download_trader=download_trader,
        download_funding=download_funding,
        download_basis=download_basis,
        download_premium=download_premium
    )
    
    try:
        if args.once:
            # 只运行一次
            downloader.run_once()
        else:
            # 持续运行
            downloader.run_daemon()
    except KeyboardInterrupt:
        logging.info("\n用户中断")
    except Exception as e:
        logging.error(f"程序异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
