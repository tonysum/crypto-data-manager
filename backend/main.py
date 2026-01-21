"""
数据管理服务 (Data Service)
端口: 8001

职责:
- K线数据下载和管理
- 数据查询和检索
- 数据完整性检查
- 数据修复和重检
"""

import sys
from pathlib import Path
import asyncio
from contextlib import asynccontextmanager

# 添加项目根目录到Python路径（独立项目：main.py在backend目录下，上一级就是项目根目录）
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime, timezone
import logging
from enum import Enum
import pandas as pd
import numpy as np
from pathlib import Path
import httpx

from download_klines import (
    download_kline_data,
    download_all_symbols,
    download_missing_symbols,
    auto_update_all_symbols,
    get_local_symbols,
    get_existing_dates,
    calculate_data_count,
    split_time_range,
    calculate_interval_seconds
)
from data import (
    get_local_kline_data,
    delete_all_tables,
    delete_kline_data,
    check_data_integrity,
    generate_download_script_from_check,
    download_missing_data_from_check,
    generate_integrity_report,
    recheck_problematic_symbols,
    get_local_symbols
)
from symbols import (
    create_symbols_table,
    get_all_symbols,
    get_trading_symbols,
    get_symbol_info,
    sync_symbols_from_exchange,
    update_symbol_status,
    add_symbol,
    delete_symbol,
    get_symbols_by_status,
    get_symbols_statistics
)
try:
    from binance_client import kline_candlestick_data, kline2df, in_exchange_trading_symbols
    from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (
        KlineCandlestickDataIntervalEnum
    )
    BINANCE_API_AVAILABLE = True
except ImportError:
    BINANCE_API_AVAILABLE = False
    logging.warning("Binance API module not available, real-time K-line feature disabled")
try:
    from binance_client import get_top3_gainers
    BINANCE_API_TOP3_AVAILABLE = True
except ImportError:
    BINANCE_API_TOP3_AVAILABLE = False
    logging.warning("binance_client module not available, 24h top gainers feature disabled")
from config import SERVICE_PORT as DATA_SERVICE_PORT, ALLOWED_ORIGINS

# 配置日志
# 确保日志输出到标准输出（终端），这样后台任务的日志也能显示
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # 明确指定输出到标准输出
    ],
    force=True  # 强制重新配置，避免被其他模块的配置覆盖
)

# 确保日志立即刷新到终端（无缓冲）
for handler in logging.root.handlers:
    handler.flush()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理（启动和关闭事件）"""
    # 启动时执行
    try:
        logging.info("初始化交易对表...")
        create_symbols_table()
        logging.info("交易对表初始化完成")
    except Exception as e:
        logging.error(f"初始化交易对表失败: {e}")
    
    yield  # 应用运行期间
    
    # 关闭时执行（如果需要清理资源）
    # 目前不需要清理操作


app = FastAPI(
    title="数据管理服务",
    description="提供币安U本位合约K线数据的下载和管理API",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc",  # ReDoc 文档
    openapi_url="/openapi.json",
    # 确保 OpenAPI schema 正确生成
    openapi_tags=[
        {
            "name": "数据下载",
            "description": "K线数据下载相关接口",
        },
        {
            "name": "数据查询",
            "description": "数据查询和检索接口",
        },
        {
            "name": "数据管理",
            "description": "数据完整性检查和修复接口",
        },
        {
            "name": "系统信息",
            "description": "系统信息和健康检查接口",
        },
        {
            "name": "交易对管理",
            "description": "交易对管理和同步接口",
        },
    ],
    lifespan=lifespan,  # 使用新的 lifespan 事件处理器
)

# 配置CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IntervalEnum(str, Enum):
    """K线间隔枚举"""
    m1 = "1m"
    m3 = "3m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    h1 = "1h"
    h2 = "2h"
    h4 = "4h"
    h6 = "6h"
    h8 = "8h"
    h12 = "12h"
    d1 = "1d"
    d3 = "3d"
    w1 = "1w"
    M1 = "1M"


class DownloadRequest(BaseModel):
    """下载请求模型"""
    interval: IntervalEnum = Field(default=IntervalEnum.d1, description="K线间隔")
    symbol: Optional[str] = Field(default=None, description="交易对符号（如BTCUSDT），如果不指定则下载所有交易对")
    start_time: Optional[str] = Field(default=None, description="开始时间（格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）")
    end_time: Optional[str] = Field(default=None, description="结束时间（格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）")
    limit: Optional[int] = Field(default=1500, description="每次请求的最大数据条数")
    update_existing: bool = Field(default=False, description="是否更新已存在的数据")
    missing_only: bool = Field(default=False, description="是否只下载缺失的交易对")
    days_back: Optional[int] = Field(default=None, description="从当前时间往前推的天数（如果提供了start_time和end_time则忽略此参数）")
    auto_split: bool = Field(default=True, description="是否自动分段下载（当数据条数超过limit时）")
    request_delay: float = Field(default=0.1, description="请求之间的延迟时间（秒）")
    batch_size: Optional[int] = Field(default=None, description="批量下载时的批次大小")
    batch_delay: Optional[float] = Field(default=None, description="批次之间的延迟时间（秒）")


class DownloadResponse(BaseModel):
    """下载响应模型"""
    status: str = Field(description="任务状态，通常为 'started'")
    message: str = Field(description="响应消息")
    interval: str = Field(description="K线间隔")
    symbol: Optional[str] = Field(default=None, description="交易对符号（如果指定了单个交易对）")
    mode: Optional[str] = Field(default=None, description="下载模式：'missing_only' 表示只下载缺失的交易对")


class AutoUpdateRequest(BaseModel):
    """自动补全请求模型"""
    interval: IntervalEnum = Field(default=IntervalEnum.d1, description="K线间隔")
    limit: Optional[int] = Field(default=1500, description="每次请求的最大数据条数")
    auto_split: bool = Field(default=True, description="是否自动分段下载")
    request_delay: float = Field(default=0.1, description="请求之间的延迟时间（秒）")
    batch_size: int = Field(default=30, description="批量下载时的批次大小")
    batch_delay: float = Field(default=3.0, description="批次之间的延迟时间（秒）")


class DeleteTablesRequest(BaseModel):
    """删除表请求模型"""
    confirm: bool = Field(description="确认删除，必须为True才能执行删除操作")


class DataIntegrityRequest(BaseModel):
    """数据完整性检查请求模型"""
    symbol: Optional[str] = Field(default=None, description="交易对符号（如BTCUSDT），如果不指定则检查所有交易对")
    interval: str = Field(default="1d", description="K线间隔")
    start_date: Optional[str] = Field(default=None, description="开始日期（格式: YYYY-MM-DD）")
    end_date: Optional[str] = Field(default=None, description="结束日期（格式: YYYY-MM-DD）")
    check_duplicates: bool = Field(default=True, description="是否检查重复数据")
    check_missing_dates: bool = Field(default=True, description="是否检查缺失日期")
    check_data_quality: bool = Field(default=True, description="是否检查数据质量")


class GenerateReportRequest(BaseModel):
    """生成报告请求模型"""
    check_results: Dict = Field(description="数据完整性检查结果")
    interval: str = Field(description="K线间隔")
    start_date: Optional[str] = Field(default=None, description="开始日期")
    end_date: Optional[str] = Field(default=None, description="结束日期")
    format: str = Field(default="txt", description="报告格式：txt, json, html, md")


class GenerateDownloadScriptRequest(BaseModel):
    """生成下载脚本请求模型"""
    check_results: Dict = Field(description="数据完整性检查结果")
    interval: str = Field(description="K线间隔")


class RecheckRequest(BaseModel):
    """复检请求模型"""
    check_results: Dict = Field(description="数据完整性检查结果")
    interval: str = Field(description="K线间隔")
    start_date: Optional[str] = Field(default=None, description="开始日期")
    end_date: Optional[str] = Field(default=None, description="结束日期")


class DeleteKlineDataRequest(BaseModel):
    """删除K线数据请求模型"""
    symbol: str = Field(..., description="交易对符号，例如 'BTCUSDT'")
    interval: str = Field(..., description="K线间隔，例如 '1d', '1h', '4h'")
    start_time: Optional[str] = Field(None, description="开始时间（格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'），如果为None则删除全部")
    end_time: Optional[str] = Field(None, description="结束时间（格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'），如果为None则删除全部")


class UpdateKlineDataRequest(BaseModel):
    """更新K线数据请求模型"""
    symbol: str = Field(..., description="交易对符号，例如 'BTCUSDT'")
    interval: str = Field(..., description="K线间隔，例如 '1d', '1h', '4h'")
    trade_date: str = Field(..., description="交易日期（格式: 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'）")
    open: Optional[float] = Field(None, description="开盘价")
    high: Optional[float] = Field(None, description="最高价")
    low: Optional[float] = Field(None, description="最低价")
    close: Optional[float] = Field(None, description="收盘价")
    volume: Optional[float] = Field(None, description="成交量")
    quote_volume: Optional[float] = Field(None, description="成交额")
    trade_count: Optional[int] = Field(None, description="成交笔数")
    active_buy_volume: Optional[float] = Field(None, description="主动买入成交量")
    active_buy_quote_volume: Optional[float] = Field(None, description="主动买入成交额")


def parse_datetime(time_str: str) -> Optional[datetime]:
    """解析时间字符串，支持多种格式"""
    if not time_str:
        return None
    
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"无法解析时间格式: {time_str}")


@app.get("/", tags=["系统信息"])
async def root():
    """根路径，返回API信息"""
    return {
        "service": "数据管理服务",
        "version": "1.0.0",
        "port": DATA_SERVICE_PORT,
        "docs": "/docs (Swagger UI)",
        "redoc": "/redoc (ReDoc)",
        "openapi": "/openapi.json",
        "endpoints": {
            "下载K线数据": "/api/download",
            "查询本地交易对": "/api/symbols",
            "查询交易对数据日期": "/api/dates/{interval}/{symbol}",
            "获取K线数据": "/api/kline/{interval}/{symbol}",
            "删除所有表": "/api/tables/delete",
            "数据完整性检查": "/api/data-integrity",
            "生成完整性报告": "/api/generate-integrity-report",
            "生成下载脚本": "/api/generate-download-script",
            "下载缺失数据": "/api/download-missing-data",
            "复检问题数据": "/api/recheck-problematic-symbols",
            "删除K线数据": "/api/kline-data (DELETE)",
            "更新K线数据": "/api/kline-data (PUT)",
            "交易对管理": "/api/symbols/manage/*",
            "同步交易对": "/api/symbols/manage/sync"
        }
    }


@app.post("/api/download", tags=["数据下载"], response_model=DownloadResponse)
async def download_klines(request: DownloadRequest, background_tasks: BackgroundTasks):
    """
    下载K线数据
    
    此接口用于从币安交易所下载K线数据到本地数据库。支持三种下载模式：
    
    1. **下载单个交易对**：指定 `symbol` 参数，下载指定交易对的K线数据
    2. **下载所有交易对**：不指定 `symbol` 且 `missing_only=False`，下载所有交易对的K线数据
    3. **只下载缺失的交易对**：设置 `missing_only=True`，只下载本地数据库中不存在的交易对数据
    
    **参数说明：**
    - `interval`: K线间隔（1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M）
    - `symbol`: 交易对符号（如 BTCUSDT），可选
    - `start_time`: 开始时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
    - `end_time`: 结束时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
    - `days_back`: 从当前时间往前推的天数（如果提供了start_time和end_time则忽略此参数）
    - `limit`: 每次API请求的最大数据条数（默认1500）
    - `update_existing`: 是否更新已存在的数据（默认False）
    - `missing_only`: 是否只下载缺失的交易对（默认False）
    - `auto_split`: 是否自动分段下载（当数据条数超过limit时，默认True）
    - `request_delay`: 请求之间的延迟时间（秒，默认0.1）
    - `batch_size`: 批量下载时的批次大小（可选）
    - `batch_delay`: 批次之间的延迟时间（秒，可选）
    
    **响应说明：**
    - 接口会立即返回，实际下载任务在后台执行
    - 下载进度和结果会输出到服务端控制台的日志中
    - 可以通过查看服务端日志来监控下载进度
    
    **使用示例：**
    
    1. 下载单个交易对（BTCUSDT）的1日K线数据：
    ```json
    {
      "interval": "1d",
      "symbol": "BTCUSDT",
      "start_time": "2024-01-01",
      "end_time": "2024-12-31"
    }
    ```
    
    2. 下载所有交易对的1小时K线数据（最近30天）：
    ```json
    {
      "interval": "1h",
      "days_back": 30
    }
    ```
    
    3. 只下载缺失的交易对数据：
    ```json
    {
      "interval": "1d",
      "missing_only": true
    }
    ```
    """
    try:
        start_time = parse_datetime(request.start_time) if request.start_time else None
        end_time = parse_datetime(request.end_time) if request.end_time else None
        
        if request.start_time and not start_time:
            raise HTTPException(status_code=400, detail="开始时间格式错误")
        if request.end_time and not end_time:
            raise HTTPException(status_code=400, detail="结束时间格式错误")
        
        if request.missing_only:
            logging.info("=" * 80)
            logging.info(f"收到下载请求：下载缺失的交易对数据，间隔: {request.interval.value}")
            logging.info("=" * 80)
            background_tasks.add_task(
                download_missing_symbols,
                interval=request.interval.value,
                days_back=request.days_back,
                start_time=start_time,
                end_time=end_time,
                limit=request.limit,
                auto_split=request.auto_split,
                request_delay=request.request_delay,
                batch_size=request.batch_size,
                batch_delay=request.batch_delay
            )
            logging.info("后台任务已启动：下载缺失的交易对数据")
            return {
                "status": "started",
                "message": "已开始下载缺失的交易对数据",
                "interval": request.interval.value,
                "mode": "missing_only"
            }
        elif request.symbol:
            logging.info("=" * 80)
            logging.info(f"收到下载请求：下载 {request.symbol} 的K线数据，间隔: {request.interval.value}")
            if start_time:
                logging.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if end_time:
                logging.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info("=" * 80)
            background_tasks.add_task(
                download_kline_data,
                symbol=request.symbol,
                interval=request.interval.value,
                start_time=start_time,
                end_time=end_time,
                limit=request.limit,
                update_existing=request.update_existing,
                auto_split=request.auto_split,
                request_delay=request.request_delay
            )
            logging.info(f"后台任务已启动：下载 {request.symbol} 的K线数据")
            return {
                "status": "started",
                "message": f"已开始下载 {request.symbol} 的K线数据",
                "symbol": request.symbol,
                "interval": request.interval.value
            }
        else:
            logging.info("=" * 80)
            logging.info(f"收到下载请求：下载所有交易对的K线数据，间隔: {request.interval.value}")
            if start_time:
                logging.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if end_time:
                logging.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if request.days_back:
                logging.info(f"回溯天数: {request.days_back}")
            logging.info("=" * 80)
            background_tasks.add_task(
                download_all_symbols,
                interval=request.interval.value,
                days_back=request.days_back,
                start_time=start_time,
                end_time=end_time,
                limit=request.limit,
                update_existing=request.update_existing,
                symbols=None,
                auto_split=request.auto_split,
                request_delay=request.request_delay,
                batch_size=request.batch_size,
                batch_delay=request.batch_delay
            )
            logging.info("后台任务已启动：下载所有交易对的K线数据")
            return {
                "status": "started",
                "message": "已开始下载所有交易对的K线数据",
                "interval": request.interval.value
            }
    except Exception as e:
        logging.error(f"下载K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")


@app.post("/api/auto-update", tags=["数据下载"])
async def auto_update_data(request: AutoUpdateRequest, background_tasks: BackgroundTasks):
    """
    自动补全所有交易对的数据：从最后更新日期到现在
    
    功能：
    1. 获取指定interval的所有交易对（本地+交易所）
    2. 对于每个交易对，获取最后更新日期
    3. 从最后更新日期的下一个K线开始，补全到当前时间
    4. 对于没有数据的交易对，从默认开始时间下载
    
    注意：此接口会立即返回，实际补全任务在后台执行。
    对于5分钟等短间隔，可能需要较长时间来获取交易对列表和计算补全范围。
    """
    interval_value = request.interval.value
    
    # 立即记录日志并返回，不等待任务启动
    logging.info(f"收到自动补全请求：interval={interval_value}")
    
    # 使用 BackgroundTasks 确保任务在响应返回后才执行
    background_tasks.add_task(
        auto_update_all_symbols,
        interval=interval_value,
        limit=request.limit,
        auto_split=request.auto_split,
        request_delay=request.request_delay,
        batch_size=request.batch_size,
        batch_delay=request.batch_delay
    )
    
    # 立即返回响应，不等待后台任务
    return {
        "status": "started",
        "message": f"已开始自动补全 {interval_value} 数据，将从每个交易对的最后更新日期补全到现在",
        "interval": interval_value,
        "mode": "auto_update"
    }


@app.get("/api/symbols", tags=["数据查询"])
async def get_symbols(interval: str = "1d"):
    """获取本地数据库中指定时间间隔的交易对列表"""
    try:
        symbols = get_local_symbols(interval)
        return {
            "interval": interval,
            "count": len(symbols),
            "symbols": symbols
        }
    except Exception as e:
        logging.error(f"获取交易对列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


# ========== 交易对管理 API ==========

class SyncSymbolsRequest(BaseModel):
    """同步交易对请求模型"""
    dry_run: bool = Field(False, description="是否为试运行（不实际更新数据库）")


class UpdateSymbolStatusRequest(BaseModel):
    """更新交易对状态请求模型"""
    symbol: str = Field(..., description="交易对符号")
    status: str = Field(..., description="状态（TRADING, DELISTED等）")


class AddSymbolRequest(BaseModel):
    """添加交易对请求模型"""
    symbol: str = Field(..., description="交易对符号")
    status: str = Field("TRADING", description="状态（默认TRADING）")


@app.get("/api/symbols/manage/all", tags=["交易对管理"])
async def get_all_symbols_manage():
    """获取所有交易对列表（包含状态信息）"""
    try:
        symbols = get_all_symbols()
        symbols_info = []
        for symbol in symbols:
            info = get_symbol_info(symbol)
            if info:
                symbols_info.append(info)
        return {
            "count": len(symbols_info),
            "symbols": symbols_info
        }
    except Exception as e:
        logging.error(f"获取交易对列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/symbols/manage/trading", tags=["交易对管理"])
async def get_trading_symbols_manage():
    """获取状态为TRADING的交易对列表"""
    try:
        symbols = get_trading_symbols()
        return {
            "count": len(symbols),
            "symbols": symbols
        }
    except Exception as e:
        logging.error(f"获取交易对列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/symbols/manage/statistics", tags=["交易对管理"])
async def get_symbols_statistics_api():
    """获取交易对统计信息"""
    try:
        stats = get_symbols_statistics()
        return stats
    except Exception as e:
        logging.error(f"获取交易对统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/symbols/manage/{symbol}", tags=["交易对管理"])
async def get_symbol_info_api(symbol: str):
    """获取交易对的详细信息"""
    try:
        info = get_symbol_info(symbol)
        if info:
            return info
        else:
            raise HTTPException(status_code=404, detail=f"交易对 {symbol} 不存在")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"获取交易对信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/symbols/manage/status/{status}", tags=["交易对管理"])
async def get_symbols_by_status_api(status: str):
    """根据状态获取交易对列表"""
    try:
        symbols = get_symbols_by_status(status)
        return {
            "status": status,
            "count": len(symbols),
            "symbols": symbols
        }
    except Exception as e:
        logging.error(f"获取交易对列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.post("/api/symbols/manage/sync", tags=["交易对管理"])
async def sync_symbols(request: SyncSymbolsRequest, background_tasks: BackgroundTasks):
    """同步交易所交易对列表"""
    if not BINANCE_API_AVAILABLE:
        raise HTTPException(status_code=503, detail="Binance API不可用")
    
    try:
        # 获取交易所交易对列表
        exchange_symbols = in_exchange_trading_symbols()
        
        if not exchange_symbols:
            raise HTTPException(status_code=500, detail="无法获取交易所交易对列表")
        
        # 同步交易对
        if request.dry_run:
            # 试运行，立即返回结果
            result = sync_symbols_from_exchange(exchange_symbols, dry_run=True)
            return {
                "status": "dry_run",
                "message": "试运行完成",
                "result": result
            }
        else:
            # 实际同步，使用后台任务
            def sync_task():
                sync_symbols_from_exchange(exchange_symbols, dry_run=False)
            
            background_tasks.add_task(sync_task)
            return {
                "status": "started",
                "message": "已开始同步交易对列表",
                "exchange_symbols_count": len(exchange_symbols)
            }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"同步交易对失败: {e}")
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@app.put("/api/symbols/manage/status", tags=["交易对管理"])
async def update_symbol_status_api(request: UpdateSymbolStatusRequest):
    """更新交易对状态"""
    try:
        success = update_symbol_status(request.symbol, request.status)
        if success:
            return {
                "status": "success",
                "message": f"交易对 {request.symbol} 状态已更新为 {request.status}"
            }
        else:
            raise HTTPException(status_code=404, detail=f"交易对 {request.symbol} 不存在或状态无效")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"更新交易对状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@app.post("/api/symbols/manage/add", tags=["交易对管理"])
async def add_symbol_api(request: AddSymbolRequest):
    """添加交易对"""
    try:
        success = add_symbol(request.symbol, request.status)
        if success:
            return {
                "status": "success",
                "message": f"交易对 {request.symbol} 已添加（状态: {request.status}）"
            }
        else:
            raise HTTPException(status_code=400, detail=f"添加交易对失败")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"添加交易对失败: {e}")
        raise HTTPException(status_code=500, detail=f"添加失败: {str(e)}")


@app.delete("/api/symbols/manage/{symbol}", tags=["交易对管理"])
async def delete_symbol_api(symbol: str):
    """删除交易对"""
    try:
        success = delete_symbol(symbol)
        if success:
            return {
                "status": "success",
                "message": f"交易对 {symbol} 已删除"
            }
        else:
            raise HTTPException(status_code=404, detail=f"交易对 {symbol} 不存在")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"删除交易对失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@app.get("/api/dates/{interval}/{symbol}", tags=["数据查询"])
async def get_dates(interval: str, symbol: str):
    """获取指定交易对的数据日期列表"""
    try:
        dates = get_existing_dates(symbol, interval)
        return {
            "symbol": symbol,
            "interval": interval,
            "count": len(dates),
            "dates": sorted(list(dates))
        }
    except Exception as e:
        logging.error(f"获取日期列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/kline/{interval}/{symbol}", tags=["数据查询"])
async def get_kline_data(
    interval: str,
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
):
    """获取K线数据"""
    try:
        df = get_local_kline_data(symbol, interval)
        if df.empty:
            # 检查表是否存在
            from db import engine
            from sqlalchemy import text
            table_name = f'K{interval}{symbol}'
            try:
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
                    # 表不存在，返回友好的错误信息
                    return {
                        "symbol": symbol,
                        "interval": interval,
                        "count": 0,
                        "data": [],
                        "message": f"交易对 {symbol} 在 {interval} 间隔下暂无数据，请先下载数据"
                    }
            except Exception:
                pass  # 如果检查表存在性失败，继续返回空数据
            
            return {
                "symbol": symbol,
                "interval": interval,
                "count": 0,
                "data": []
            }
        
        # 处理trade_date格式，创建用于筛选的日期字符串列
        if df['trade_date'].dtype == 'object':
            # 字符串格式，提取日期部分用于筛选
            df['trade_date_str'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            # 转换为datetime用于排序和精确筛选
            df['trade_date_dt'] = pd.to_datetime(df['trade_date'])
        else:
            # 已经是datetime格式
            df['trade_date_str'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            df['trade_date_dt'] = pd.to_datetime(df['trade_date'])
        
        # 统一时间范围筛选逻辑：使用完整的datetime进行比较
        if start_date:
            try:
                if len(start_date) == 10:  # YYYY-MM-DD
                    start_dt = pd.to_datetime(start_date)
                else:  # YYYY-MM-DD HH:MM:SS
                    start_dt = pd.to_datetime(start_date)
                df = df[df['trade_date_dt'] >= start_dt]
            except ValueError:
                # 如果解析失败，回退到字符串比较
                df = df[df['trade_date_str'] >= start_date]
        
        if end_date:
            try:
                if len(end_date) == 10:  # YYYY-MM-DD
                    # 对于日期格式，如果结束日期是今天，限制为当前时间（UTC），确保与币安API的时间范围一致
                    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    now_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
                    if end_dt > now_utc:
                        end_dt = now_utc
                else:  # YYYY-MM-DD HH:MM:SS
                    end_dt = pd.to_datetime(end_date)
                    # 如果指定的结束时间超过当前时间，限制为当前时间
                    now_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
                    if end_dt > now_utc:
                        end_dt = now_utc
                df = df[df['trade_date_dt'] <= end_dt]
            except ValueError:
                # 如果解析失败，回退到字符串比较
                df = df[df['trade_date_str'] <= end_date]
        
        # 按完整时间排序（确保顺序正确，升序）- 使用trade_date_dt而不是trade_date_str
        df = df.sort_values('trade_date_dt', ascending=True)
        
        # 获取总数（在限制之前）
        total_count = len(df)
        
        # 如果指定了offset和limit，进行分页
        if offset is not None and limit is not None:
            # 从后往前取（最新的数据），但保持升序
            # 先取最后 offset+limit 条，然后取前 limit 条，最后重新排序确保升序
            df_selected = df.tail(offset + limit).head(limit)
            df = df_selected.sort_values('trade_date_dt', ascending=True)
        elif limit is not None:
            # 只指定limit，取最新的limit条，但保持升序
            df_selected = df.tail(limit)
            df = df_selected.sort_values('trade_date_dt', ascending=True)
        elif offset is not None:
            # 只指定offset，从后往前跳过offset条
            df_selected = df.head(-offset) if offset > 0 else df
            df = df_selected.sort_values('trade_date_dt', ascending=True)
        
        # 删除临时列
        if 'trade_date_dt' in df.columns:
            df = df.drop(columns=['trade_date_dt'])
        if 'trade_date_str' in df.columns:
            df = df.drop(columns=['trade_date_str'])
        
        # 清理无效的浮点值（inf, -inf, NaN）以确保JSON兼容性
        df = df.replace([np.inf, -np.inf], np.nan)  # 将inf替换为NaN
        
        # 转换为字典列表
        data = df.to_dict('records')
        
        # 清理函数：确保所有数值都是JSON兼容的
        def clean_value(value):
            # 处理 None
            if value is None:
                return None
            
            # 处理 pandas NaT (Not a Time) 和 NaN
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            
            # 处理 NumPy 类型
            if isinstance(value, (np.integer, np.floating)):
                # 检查是否为 inf 或 nan
                try:
                    if np.isinf(value) or np.isnan(value):
                        return None
                except (TypeError, ValueError):
                    return None
                # 转换为 Python 原生类型
                if isinstance(value, np.integer):
                    return int(value)
                else:
                    return float(value)
            
            # 处理 Python 原生数值类型
            if isinstance(value, (int, float)):
                # 检查是否为 inf 或 nan
                try:
                    if np.isinf(value) or np.isnan(value):
                        return None
                except (TypeError, ValueError):
                    pass
                return value
            
            # 处理 datetime 类型
            if isinstance(value, (pd.Timestamp, datetime)):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            
            # 处理字符串
            if isinstance(value, str):
                return value
            
            # 其他类型直接返回
            return value
        
        # 清理数据中的每个值
        cleaned_data = []
        for record in data:
            cleaned_record = {}
            for key, value in record.items():
                if isinstance(value, dict):
                    cleaned_record[key] = {k: clean_value(v) for k, v in value.items()}
                elif isinstance(value, (list, tuple)):
                    cleaned_record[key] = [clean_value(v) for v in value]
                else:
                    cleaned_record[key] = clean_value(value)
            cleaned_data.append(cleaned_record)
        
        return {
            "symbol": symbol,
            "interval": interval,
            "count": len(cleaned_data),
            "total_count": total_count,  # 返回总数据量
            "data": cleaned_data
        }
    except Exception as e:
        logging.error(f"获取K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.post("/api/tables/delete", tags=["数据管理"])
async def delete_tables(request: DeleteTablesRequest):
    """删除所有表"""
    if not request.confirm:
        raise HTTPException(status_code=400, detail="必须设置 confirm=true 才能执行删除操作")
    
    try:
        count = delete_all_tables(confirm=True)
        return {
            "status": "success",
            "message": f"已删除 {count} 个表"
        }
    except Exception as e:
        logging.error(f"删除表失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@app.post("/api/data-integrity", tags=["数据管理"])
async def check_data_integrity_api(request: DataIntegrityRequest):
    """检查K线数据完整性"""
    try:
        result = check_data_integrity(
            symbol=request.symbol,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            check_duplicates=request.check_duplicates,
            check_missing_dates=request.check_missing_dates,
            check_data_quality=request.check_data_quality,
            verbose=True
        )
        return result
    except Exception as e:
        logging.error(f"数据完整性检查失败: {e}")
        raise HTTPException(status_code=500, detail=f"检查失败: {str(e)}")


@app.post("/api/generate-integrity-report", tags=["数据管理"])
async def generate_integrity_report_api(request: GenerateReportRequest):
    """生成数据完整性报告"""
    try:
        # 从check_results中提取检查配置
        check_duplicates = request.check_results.get('check_duplicates', True)
        check_missing_dates = request.check_results.get('check_missing_dates', True)
        check_data_quality = request.check_results.get('check_data_quality', True)
        
        # 将format映射到output_format，并处理格式转换
        format_mapping = {
            'txt': 'text',
            'text': 'text',
            'json': 'json',
            'html': 'html',
            'md': 'markdown',
            'markdown': 'markdown'
        }
        output_format = format_mapping.get(request.format.lower(), 'text')
        
        report_content = generate_integrity_report(
            check_results=request.check_results,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            check_duplicates=check_duplicates,
            check_missing_dates=check_missing_dates,
            check_data_quality=check_data_quality,
            output_format=output_format
        )
        return {
            "status": "success",
            "format": request.format,
            "report": report_content
        }
    except Exception as e:
        logging.error(f"生成报告失败: {e}")
        raise HTTPException(status_code=500, detail=f"生成失败: {str(e)}")


@app.post("/api/generate-download-script", tags=["数据管理"])
async def generate_download_script_api(request: GenerateDownloadScriptRequest):
    """生成下载脚本"""
    try:
        script_content = generate_download_script_from_check(
            check_results=request.check_results,
            interval=request.interval
        )
        return {
            "status": "success",
            "script": script_content
        }
    except Exception as e:
        logging.error(f"生成脚本失败: {e}")
        raise HTTPException(status_code=500, detail=f"生成失败: {str(e)}")


@app.post("/api/download-missing-data", tags=["数据管理"])
async def download_missing_data_api(request: DataIntegrityRequest):
    """下载缺失数据"""
    try:
        # 先检查数据完整性
        check_results = check_data_integrity(
            symbol=request.symbol,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            check_duplicates=request.check_duplicates,
            check_missing_dates=request.check_missing_dates,
            check_data_quality=request.check_data_quality,
            verbose=True
        )
        
        # 下载缺失数据
        download_stats = download_missing_data_from_check(
            check_results=check_results,
            interval=request.interval,
            verbose=True
        )
        
        # 重新检查数据完整性
        check_results_after = check_data_integrity(
            symbol=request.symbol,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            check_duplicates=request.check_duplicates,
            check_missing_dates=request.check_missing_dates,
            check_data_quality=request.check_data_quality,
            verbose=True
        )
        
        return {
            "status": "success",
            "check_results_before": check_results,
            "download_stats": download_stats,
            "check_results_after": check_results_after
        }
    except Exception as e:
        logging.error(f"下载缺失数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")


@app.post("/api/recheck-problematic-symbols", tags=["数据管理"])
async def recheck_problematic_symbols_api(request: RecheckRequest):
    """复检问题数据"""
    try:
        import time
        recheck_results = recheck_problematic_symbols(
            check_results=request.check_results,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            verbose=True
        )
        
        # 生成报告文件
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"recheck_report_{request.interval}_{timestamp}.txt"
        
        recheck_results_with_report = recheck_problematic_symbols(
            check_results=request.check_results,
            interval=request.interval,
            start_date=request.start_date,
            end_date=request.end_date,
            verbose=True,
            output_file=output_file
        )
        
        # 读取报告文件内容
        report_content = None
        report_file = None
        if output_file and Path(output_file).exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                report_content = f.read()
            report_file = output_file
        
        return {
            "status": "success",
            "recheck_results": recheck_results_with_report,
            "report_file": report_file,
            "report_content": report_content
        }
    except Exception as e:
        logging.error(f"复检失败: {e}")
        raise HTTPException(status_code=500, detail=f"复检失败: {str(e)}")


@app.delete("/api/kline-data", tags=["数据管理"])
async def delete_kline_data_api(request: DeleteKlineDataRequest):
    """删除K线数据"""
    try:
        result = delete_kline_data(
            symbol=request.symbol,
            interval=request.interval,
            start_time=request.start_time,
            end_time=request.end_time,
            verbose=True
        )
        
        if not result['success']:
            raise HTTPException(status_code=400, detail=result['message'])
        
        return {
            "status": "success",
            "message": result['message'],
            "deleted_count": result['deleted_count']
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"删除K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@app.put("/api/kline-data", tags=["数据管理"])
async def update_kline_data_api(request: UpdateKlineDataRequest):
    """更新单条K线数据"""
    try:
        from db import engine
        from sqlalchemy import text
        
        table_name = f'K{request.interval}{request.symbol}'
        
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
                raise HTTPException(status_code=404, detail=f'表 {table_name} 不存在')
            
            # 检查记录是否存在并获取当前值
            check_stmt = f"SELECT * FROM {table_name} WHERE trade_date = :trade_date"
            check_result = conn.execute(text(check_stmt), {"trade_date": request.trade_date})
            existing_record = check_result.fetchone()
            
            if not existing_record:
                raise HTTPException(status_code=404, detail=f'未找到日期为 {request.trade_date} 的数据')
            
            # 获取列名
            columns = check_result.keys()
            column_dict = {col: idx for idx, col in enumerate(columns)}
            
            # 构建UPDATE语句，只更新提供的字段
            update_fields = []
            update_values = {"trade_date": request.trade_date}
            
            # 获取当前值用于计算diff和pct_chg
            current_open = existing_record[column_dict.get('open', 2)] if 'open' in column_dict else None
            current_close = existing_record[column_dict.get('close', 5)] if 'close' in column_dict else None
            
            if request.open is not None:
                update_fields.append("open = :open")
                update_values["open"] = request.open
                current_open = request.open
            if request.high is not None:
                update_fields.append("high = :high")
                update_values["high"] = request.high
            if request.low is not None:
                update_fields.append("low = :low")
                update_values["low"] = request.low
            if request.close is not None:
                update_fields.append("close = :close")
                update_values["close"] = request.close
                current_close = request.close
            if request.volume is not None:
                update_fields.append("volume = :volume")
                update_values["volume"] = request.volume
            if request.quote_volume is not None:
                update_fields.append("quote_volume = :quote_volume")
                update_values["quote_volume"] = request.quote_volume
            if request.trade_count is not None:
                update_fields.append("trade_count = :trade_count")
                update_values["trade_count"] = request.trade_count
            if request.active_buy_volume is not None:
                update_fields.append("active_buy_volume = :active_buy_volume")
                update_values["active_buy_volume"] = request.active_buy_volume
            if request.active_buy_quote_volume is not None:
                update_fields.append("active_buy_quote_volume = :active_buy_quote_volume")
                update_values["active_buy_quote_volume"] = request.active_buy_quote_volume
            
            if not update_fields:
                raise HTTPException(status_code=400, detail="至少需要提供一个要更新的字段")
            
            # 重新计算diff和pct_chg（如果提供了open或close）
            if (request.open is not None or request.close is not None) and current_open and current_close:
                diff = current_close - current_open
                pct_chg = diff / current_open if current_open != 0 else 0
                update_fields.append("diff = :diff")
                update_fields.append("pct_chg = :pct_chg")
                update_values["diff"] = diff
                update_values["pct_chg"] = pct_chg
            
            update_stmt = f"UPDATE {table_name} SET {', '.join(update_fields)} WHERE trade_date = :trade_date"
            conn.execute(text(update_stmt), update_values)
            conn.commit()
            
            # 获取更新后的数据
            updated_result = conn.execute(text(check_stmt), {"trade_date": request.trade_date})
            updated_record = updated_result.fetchone()
            updated_columns = updated_result.keys()
            
            updated_data = dict(zip(updated_columns, updated_record))
            
            return {
                "status": "success",
                "message": f"成功更新 {request.symbol} {request.interval} 在 {request.trade_date} 的数据",
                "data": updated_data
            }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"更新K线数据失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@app.get("/api/top-gainers", tags=["数据查询"])
async def get_top_gainers(date: Optional[str] = None, top_n: int = 3):
    """
    获取指定日期涨幅前三的交易对
    
    Args:
        date: 日期（格式: YYYY-MM-DD），如果不指定则使用前一天
        top_n: 返回前N名，默认3
    
    Returns:
        涨幅排名列表
    """
    try:
        from datetime import datetime, timedelta
        
        # 如果没有指定日期，使用前一天
        if not date:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime('%Y-%m-%d')
        
        # 获取所有交易对
        symbols = get_local_symbols(interval="1d")
        if not symbols:
            return {
                "date": date,
                "top_gainers": [],
                "message": "未找到交易对数据"
            }
        
        all_data = []
        
        # 读取所有交易对的数据
        for symbol in symbols:
            try:
                df = get_local_kline_data(symbol, interval="1d")
                if df.empty:
                    continue
                
                # 标准化trade_date格式
                if df['trade_date'].dtype == 'object':
                    df['trade_date_str'] = df['trade_date'].str[:10]
                else:
                    df['trade_date_str'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                
                # 筛选指定日期
                df_filtered = df[df['trade_date_str'] == date].copy()
                
                if df_filtered.empty:
                    continue
                
                # 添加symbol列
                df_filtered['symbol'] = symbol
                
                # 处理pct_chg
                row = df_filtered.iloc[0]
                pct_chg = row.get('pct_chg')
                
                # 如果pct_chg为NaN，尝试计算
                if pd.isna(pct_chg):
                    date_dt = datetime.strptime(date, '%Y-%m-%d')
                    prev_date = (date_dt - timedelta(days=1)).strftime('%Y-%m-%d')
                    prev_data = df[df['trade_date_str'] == prev_date]
                    
                    if not prev_data.empty and not pd.isna(prev_data.iloc[0]['close']):
                        prev_close = prev_data.iloc[0]['close']
                        current_close = row['close']
                        if not pd.isna(current_close) and prev_close > 0:
                            pct_chg = (current_close - prev_close) / prev_close * 100
                
                if not pd.isna(pct_chg):
                    all_data.append({
                        'symbol': symbol,
                        'pct_chg': float(pct_chg),
                        'close': float(row['close']) if not pd.isna(row['close']) else None,
                        'open': float(row['open']) if not pd.isna(row['open']) else None,
                        'high': float(row['high']) if not pd.isna(row['high']) else None,
                        'low': float(row['low']) if not pd.isna(row['low']) else None,
                        'volume': float(row['volume']) if not pd.isna(row['volume']) else None,
                    })
            except Exception as e:
                logging.debug(f"读取 {symbol} 数据失败: {e}")
                continue
        
        if not all_data:
            return {
                "date": date,
                "top_gainers": [],
                "message": f"未找到 {date} 的数据"
            }
        
        # 按涨幅排序，取前N名
        sorted_data = sorted(all_data, key=lambda x: x['pct_chg'], reverse=True)
        top_gainers = sorted_data[:top_n]
        
        return {
            "date": date,
            "top_gainers": top_gainers,
            "total_count": len(all_data)
        }
    except Exception as e:
        logging.error(f"获取涨幅排名失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/top-gainers-24h", tags=["数据查询"])
async def get_top_gainers_24h(top_n: int = 3):
    """
    获取过去24小时涨幅前三的交易对（使用币安API）
    
    Args:
        top_n: 返回前N名，默认3
    
    Returns:
        涨幅排名列表
    """
    if not BINANCE_API_TOP3_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="24小时涨幅数据服务不可用，请检查binance_client模块"
        )
    
    try:
        logging.info("开始获取24小时涨幅排名...")
        
        # 检查函数是否可用
        if not callable(get_top3_gainers):
            raise ValueError("get_top3_gainers 不是一个可调用函数")
        
        df = get_top3_gainers(top_n=top_n)
        
        # 检查返回值
        if df is None:
            logging.warning("get_top3_gainers() 返回 None")
            return {
                "top_gainers": [],
                "message": "未找到24小时涨幅数据（API返回None）"
            }
        
        if not isinstance(df, pd.DataFrame):
            logging.warning(f"get_top3_gainers() 返回了非DataFrame类型: {type(df)}")
            return {
                "top_gainers": [],
                "message": f"数据格式错误: {type(df)}"
            }
        
        logging.info(f"获取到数据: {len(df) if not df.empty else 0} 条")
        
        if df.empty:
            logging.warning("get_top3_gainers() 返回空DataFrame")
            return {
                "top_gainers": [],
                "message": "未找到24小时涨幅数据"
            }
        
        # 转换为字典列表
        top_gainers = []
        for idx, row in df.head(top_n).iterrows():
            try:
                symbol = str(row.get('symbol', '')) if pd.notna(row.get('symbol')) else ''
                price_change_percent = float(row.get('price_change_percent', 0)) if pd.notna(row.get('price_change_percent')) else 0
                
                gainer = {
                    'symbol': symbol,
                    'price_change_percent': price_change_percent,
                    'last_price': float(row.get('last_price', 0)) if pd.notna(row.get('last_price')) else None,
                    'open_price': float(row.get('open_price', 0)) if pd.notna(row.get('open_price')) else None,
                    'high_price': float(row.get('high_price', 0)) if pd.notna(row.get('high_price')) else None,
                    'low_price': float(row.get('low_price', 0)) if pd.notna(row.get('low_price')) else None,
                    'volume': float(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                }
                top_gainers.append(gainer)
                logging.debug(f"添加交易对: {symbol}, 涨幅: {price_change_percent}%")
            except Exception as e:
                logging.error(f"处理第 {idx} 行数据失败: {e}")
                continue
        
        logging.info(f"成功处理 {len(top_gainers)} 个交易对")
        return {
            "top_gainers": top_gainers,
            "total_count": len(df)
        }
    except Exception as e:
        logging.error(f"获取24小时涨幅排名失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/top-gainers-prev-day-binance", tags=["数据查询"])
async def get_top_gainers_prev_day_binance(top_n: int = 3):
    """
    获取前一天涨幅前N的交易对（从交易所API数据计算）
    
    该接口通过币安API获取所有USDT交易对前一天的日K线数据，
    计算前一天（24:00-24:00）的涨幅，返回涨幅排名前N的交易对
    
    Args:
        top_n: 返回前N名，默认3
    
    Returns:
        dict: {
            'date': '前一天的日期',
            'data_source': '交易所API计算',
            'top_gainers': [
                {
                    'symbol': '交易对',
                    'pct_chg': 涨幅百分比,
                    'open': 开盘价,
                    'close': 收盘价,
                    'high': 最高价,
                    'low': 最低价,
                    'volume': 成交量
                }
            ],
            'total_count': 总交易对数
        }
    """
    if not BINANCE_API_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="币安API模块不可用，无法获取交易所数据"
        )
    
    try:
        from datetime import datetime, timedelta, timezone
        
        logging.info("开始获取前一天涨幅排名（交易所数据）...")
        
        # 计算前一天的日期
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        
        logging.info(f"计算日期: {date_str}")
        
        # 获取所有交易对列表
        symbols = in_exchange_trading_symbols()
        if not symbols:
            logging.warning("未找到任何交易对")
            return {
                "date": date_str,
                "data_source": "交易所API计算",
                "top_gainers": [],
                "total_count": 0,
                "message": "未找到交易对数据"
            }
        
        logging.info(f"获取到 {len(symbols)} 个交易对，开始获取API数据...")
        
        all_gainers = []
        
        async def fetch_gainer(symbol):
            try:
                # 转换时间参数（UTC时间）
                start_dt = datetime.combine(yesterday, datetime.min.time(), tzinfo=timezone.utc)
                end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)
                
                start_timestamp = int(start_dt.timestamp() * 1000)
                end_timestamp = int(end_dt.timestamp() * 1000)
                
                # 从币安API获取前一天的日K线数据
                klines = kline_candlestick_data(
                    symbol=symbol,
                    interval=KlineCandlestickDataIntervalEnum.INTERVAL_1d.value,
                    starttime=start_timestamp,
                    endtime=end_timestamp,
                    limit=1
                )
                
                if klines and len(klines) > 0:
                    kline = klines[0]
                    
                    # 提取OHLCV数据 (索引: 1=开, 2=高, 3=低, 4=收, 5=量)
                    open_price = float(kline[1])
                    high_price = float(kline[2])
                    low_price = float(kline[3])
                    close_price = float(kline[4])
                    volume = float(kline[5])
                    
                    # 计算涨幅
                    if open_price > 0:
                        pct_chg = (close_price - open_price) / open_price * 100
                    else:
                        pct_chg = 0
                    
                    gainer = {
                        'symbol': symbol,
                        'pct_chg': round(pct_chg, 2),
                        'open': open_price,
                        'close': close_price,
                        'high': high_price,
                        'low': low_price,
                        'volume': volume
                    }
                    logging.debug(f"{symbol}: 涨幅 {pct_chg:.2f}%")
                    return gainer
                
            except Exception as e:
                logging.warning(f"获取 {symbol} 数据失败: {e}")
            
            return None

        tasks = [fetch_gainer(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        
        all_gainers = [g for g in results if g is not None]
        
        logging.info(f"处理完成: {len(symbols)} 个交易对，成功获取 {len(all_gainers)} 个")
        
        if not all_gainers:
            logging.warning("未能获取任何交易对的数据")
            return {
                "date": date_str,
                "data_source": "交易所API计算",
                "top_gainers": [],
                "total_count": 0,
                "message": "未能获取交易所数据"
            }
        
        # 按涨幅降序排序，取前N名
        sorted_gainers = sorted(all_gainers, key=lambda x: x['pct_chg'], reverse=True)
        top_gainers = sorted_gainers[:top_n]
        
        logging.info(f"成功获取 {len(all_gainers)} 个交易对的数据，前 {len(top_gainers)} 名涨幅:")
        for idx, gainer in enumerate(top_gainers, 1):
            logging.info(f"  {idx}. {gainer['symbol']}: {gainer['pct_chg']:.2f}%")
        
        return {
            "date": date_str,
            "data_source": "交易所API计算",
            "top_gainers": top_gainers,
            "total_count": len(all_gainers)
        }
    
    except Exception as e:
        logging.error(f"获取前一天涨幅排名失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/kline-binance/{interval}/{symbol}", tags=["数据下载"])
async def get_kline_data_from_binance(
    interval: str,
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None
):
    """从币安API获取实时K线数据"""
    if not BINANCE_API_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="币安API模块不可用，无法获取实时数据"
        )
    
    try:
        # 转换时间间隔
        try:
            interval_enum = KlineCandlestickDataIntervalEnum[f"INTERVAL_{interval}"].value
        except KeyError:
            raise HTTPException(status_code=400, detail=f"不支持的K线间隔: {interval}")
        
        # 转换时间参数
        start_timestamp = None
        end_timestamp = None
        
        # 解析日期范围（用于后续过滤）
        start_dt_filter = None
        end_dt_filter = None
        
        if start_date:
            try:
                if len(start_date) == 10:  # YYYY-MM-DD
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    start_dt_filter = start_dt.replace(tzinfo=timezone.utc)
                else:  # YYYY-MM-DD HH:MM:SS
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
                    start_dt_filter = start_dt.replace(tzinfo=timezone.utc)
                start_timestamp = int(start_dt_filter.timestamp() * 1000)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"开始日期格式错误: {start_date}")
        
        if end_date:
            try:
                if len(end_date) == 10:  # YYYY-MM-DD
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    end_dt = end_dt.replace(hour=23, minute=59, second=59)
                    end_dt_filter = end_dt.replace(tzinfo=timezone.utc)
                else:  # YYYY-MM-DD HH:MM:SS
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
                    end_dt_filter = end_dt.replace(tzinfo=timezone.utc)
                
                # 限制结束时间不超过当前时间（UTC），避免请求未来的数据
                now_utc = datetime.now(timezone.utc)
                if end_dt_filter > now_utc:
                    end_dt_filter = now_utc
                
                end_timestamp = int(end_dt_filter.timestamp() * 1000)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"结束日期格式错误: {end_date}")
        
        # 计算需要的数据条数，决定是否需要分段请求
        max_limit = 1500  # 币安API单次请求最大限制
        need_split = False
        
        if start_dt_filter and end_dt_filter:
            # 计算预计数据条数
            data_count = calculate_data_count(start_dt_filter, end_dt_filter, interval)
            if data_count > max_limit:
                need_split = True
                logging.info(f"数据条数({data_count})超过限制({max_limit})，将分段请求")
        
        # 调用币安API（可能需要分段）
        all_klines = []
        
        if need_split and start_dt_filter and end_dt_filter:
            # 分段请求
            time_ranges = split_time_range(start_dt_filter, end_dt_filter, interval, max_limit)
            logging.info(f"将分为 {len(time_ranges)} 段请求")
            
            for idx, (seg_start, seg_end) in enumerate(time_ranges, 1):
                seg_start_ts = int(seg_start.timestamp() * 1000)
                seg_end_ts = int(seg_end.timestamp() * 1000)
                
                logging.info(f"请求第 {idx}/{len(time_ranges)} 段: {seg_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {seg_end.strftime('%Y-%m-%d %H:%M:%S')}")
                
                seg_klines = kline_candlestick_data(
                    symbol=symbol,
                    interval=interval_enum,
                    starttime=seg_start_ts,
                    endtime=seg_end_ts,
                    limit=max_limit
                )
                
                if seg_klines:
                    all_klines.extend(seg_klines)
                elif seg_klines is None:
                    # 如果某一段请求失败，记录警告但继续处理其他段
                    logging.warning(f"第 {idx}/{len(time_ranges)} 段请求失败，跳过该段")
                
                # 避免API频率限制
                import time
                time.sleep(0.1)
        else:
            # 单次请求
            # 如果没有指定limit，或者limit小于max_limit，使用max_limit确保获取足够的数据
            request_limit = max_limit  # 默认使用1500，确保获取足够的数据
            if limit and limit < max_limit:
                request_limit = limit  # 如果用户明确指定了更小的limit，使用用户的设置
            elif limit and limit > max_limit:
                request_limit = max_limit  # 不能超过币安API的限制
            
            logging.info(f"单次请求，使用limit={request_limit}")
            
            klines = kline_candlestick_data(
                symbol=symbol,
                interval=interval_enum,
                starttime=start_timestamp,
                endtime=end_timestamp,
                limit=request_limit
            )
            
            if klines:
                all_klines = klines
            elif klines is None:
                # kline_candlestick_data 返回 None 表示请求失败（通常是网络错误）
                raise HTTPException(
                    status_code=503,
                    detail=f"无法从币安API获取 {symbol} 的K线数据。可能原因：网络连接问题、交易对不存在或币安API服务暂时不可用。请检查后端日志获取详细信息。"
                )
        
        if not all_klines:
            return {
                "symbol": symbol,
                "interval": interval,
                "count": 0,
                "data": [],
                "message": f"未获取到 {symbol} 的K线数据。可能原因：交易对不存在、已下架或指定时间范围内无数据。"
            }
        
        # 转换为DataFrame并格式化
        df = kline2df(all_klines)
        
        # 确保trade_date是datetime类型（带时区）
        if df['trade_date'].dtype == 'object':
            df['trade_date'] = pd.to_datetime(df['trade_date'], utc=True)
        elif not hasattr(df['trade_date'].dtype, 'tz') or df['trade_date'].dtype.tz is None:
            # 如果没有时区信息，假设是UTC
            df['trade_date'] = pd.to_datetime(df['trade_date'], utc=True)
        
        # 根据日期范围过滤数据（币安API可能返回范围外的数据）
        if start_dt_filter is not None:
            df = df[df['trade_date'] >= start_dt_filter]
        if end_dt_filter is not None:
            df = df[df['trade_date'] <= end_dt_filter]
        
        # 将trade_date转换为字符串格式
        if interval in ['1d', '3d', '1w', '1M']:
            df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
        else:
            df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 按时间排序（降序，最新的在前）
        df = df.sort_values('trade_date', ascending=False)
        
        # 清理无效值
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # 转换为字典列表
        data = df.to_dict('records')
        
        # 清理JSON不兼容的值
        def clean_value(value):
            if value is None:
                return None
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                pass
            if isinstance(value, (np.integer, np.int64, np.int32)):
                return int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                if np.isinf(value) or np.isnan(value):
                    return None
                return float(value)
            elif isinstance(value, (pd.Timestamp, datetime)):
                return value.strftime('%Y-%m-%d %H:%M:%S')
            return value
        
        cleaned_data = []
        for record in data:
            cleaned_record = {}
            for key, value in record.items():
                cleaned_record[key] = clean_value(value)
            cleaned_data.append(cleaned_record)
        
        return {
            "symbol": symbol,
            "interval": interval,
            "count": len(cleaned_data),
            "data": cleaned_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"从币安API获取K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/api/health", tags=["系统信息"])
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "service": "数据管理服务",
        "port": DATA_SERVICE_PORT,
        "docs": "/docs",
        "openapi": "/openapi.json"
    }


# @app.get("/redoc", include_in_schema=False)
# async def redoc_redirect():
#     """ReDoc 重定向到 Swagger UI（ReDoc CDN 资源不可用）"""
#     from fastapi.responses import RedirectResponse
#     return RedirectResponse(url="/docs", status_code=302)


# 注意：以下数据库文件下载/上传功能已废弃，因为现在使用 PostgreSQL 数据库
# PostgreSQL 是服务器数据库，不支持文件下载/上传
# 如需备份数据，请使用 PostgreSQL 的 pg_dump 工具

# @app.get("/api/download-database", response_class=FileResponse)
# async def download_database():
#     """已废弃：PostgreSQL 不支持文件下载"""
#     raise HTTPException(
#         status_code=410,
#         detail="此功能已废弃。现在使用 PostgreSQL 数据库，请使用 pg_dump 进行备份。"
#     )


# @app.post("/api/upload-database")
# async def upload_database(file: UploadFile = File(...)):
#     """已废弃：PostgreSQL 不支持文件上传"""
#     raise HTTPException(
#         status_code=410,
#         detail="此功能已废弃。现在使用 PostgreSQL 数据库，请使用 pg_restore 进行恢复。"
#     )


@app.get("/api/ip-info", tags=["系统信息"])
async def get_ip_info(request: Request):
    """
    获取IP地址信息
    
    返回：
    - client_ip: 客户端IP（从请求头获取，可能是VPN IP）
    - real_ip: 真实IP（通过外部API获取）
    """
    try:
        # 获取客户端IP（从请求头中获取，可能是VPN IP）
        client_ip = None
        
        # 检查常见的代理头
        forwarded_for = request.headers.get("X-Forwarded-For")
        real_ip = request.headers.get("X-Real-IP")
        cf_connecting_ip = request.headers.get("CF-Connecting-IP")  # Cloudflare
        
        if cf_connecting_ip:
            client_ip = cf_connecting_ip.split(",")[0].strip()
        elif forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        elif real_ip:
            client_ip = real_ip
        else:
            # 如果没有代理头，使用直接连接的IP
            client_ip = request.client.host if request.client else None
        
        # 通过外部API获取真实IP地址
        real_ip_address = None
        ip_service_url = None
        
        try:
            # 尝试使用多个IP查询服务
            ip_services = [
                "https://api.ipify.org?format=json",
                "https://api64.ipify.org?format=json",
                "https://ipapi.co/json/",
            ]
            
            async with httpx.AsyncClient(timeout=5.0) as client:
                for service_url in ip_services:
                    try:
                        response = await client.get(service_url)
                        if response.status_code == 200:
                            data = response.json()
                            if "ip" in data:
                                real_ip_address = data["ip"]
                            elif "ip" in data.get("query", ""):
                                real_ip_address = data.get("query")
                            else:
                                real_ip_address = data.get("ip", data.get("origin", None))
                            
                            if real_ip_address:
                                ip_service_url = service_url
                                break
                    except Exception as e:
                        logging.debug(f"IP服务 {service_url} 查询失败: {e}")
                        continue
        except Exception as e:
            logging.warning(f"获取真实IP地址失败: {e}")
        
        return {
            "client_ip": client_ip,
            "real_ip": real_ip_address,
            "ip_service": ip_service_url,
            "headers": {
                "X-Forwarded-For": forwarded_for,
                "X-Real-IP": real_ip,
                "CF-Connecting-IP": cf_connecting_ip,
            }
        }
    except Exception as e:
        logging.error(f"获取IP信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取IP信息失败: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=DATA_SERVICE_PORT)

