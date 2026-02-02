import os
import logging
import re
import time
from pathlib import Path
from typing import Optional, List
from functools import wraps

import pandas as pd  # pyright: ignore[reportMissingImports]
from dotenv import load_dotenv  # pyright: ignore[reportMissingImports]

from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (  # pyright: ignore[reportMissingImports]
    DerivativesTradingUsdsFutures,
    ConfigurationRestAPI,
    DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models import (  # pyright: ignore[reportMissingImports]
    KlineCandlestickDataIntervalEnum,
    TopTraderLongShortRatioPositionsPeriodEnum
)

# 🔧 加载 .env 文件
# 从当前文件所在目录向上查找 .env 文件（支持 backend/ 目录和项目根目录）
backend_dir = Path(__file__).parent
project_root = backend_dir.parent
env_path = project_root / '.env'
if not env_path.exists():
    # 如果项目根目录没有 .env，尝试 backend 目录
    env_path = backend_dir / '.env'

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.info(f"已加载环境变量文件: {env_path}")
else:
    logging.warning(f"未找到 .env 文件，将使用环境变量或默认值。查找路径: {env_path}")

# Configure logging
logging.basicConfig(level=logging.INFO)

# 从配置获取网络设置
try:
    from config import (
        BINANCE_TIMEOUT,
        BINANCE_MAX_RETRIES,
        BINANCE_RETRY_DELAY,
        BINANCE_PROXY
    )
except ImportError:
    # 如果config模块不可用，使用环境变量或默认值
    BINANCE_TIMEOUT = int(os.getenv("BINANCE_TIMEOUT", "30"))
    BINANCE_MAX_RETRIES = int(os.getenv("BINANCE_MAX_RETRIES", "3"))
    BINANCE_RETRY_DELAY = float(os.getenv("BINANCE_RETRY_DELAY", "2.0"))
    BINANCE_PROXY = os.getenv("BINANCE_PROXY", "")


def retry_on_network_error(max_retries: int = BINANCE_MAX_RETRIES, delay: float = BINANCE_RETRY_DELAY):
    """
    网络错误重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始重试延迟（秒），使用指数退避
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    error_type = type(e).__name__
                    
                    # 判断是否是网络相关错误
                    is_network_error = (
                        'network' in error_msg or
                        'timeout' in error_msg or
                        'connection' in error_msg or
                        'connect' in error_msg or
                        'ConnectTimeoutError' in error_type or
                        'ConnectionError' in error_type or
                        'TimeoutError' in error_type or
                        'Max retries exceeded' in error_msg
                    )
                    
                    if not is_network_error or attempt == max_retries:
                        # 不是网络错误或已达到最大重试次数，直接抛出异常
                        raise
                    
                    last_exception = e
                    # 指数退避：延迟时间 = delay * (2 ^ attempt)
                    wait_time = delay * (2 ** attempt)
                    logging.warning(
                        f"网络错误（尝试 {attempt + 1}/{max_retries + 1}）: {error_type}: {str(e)[:100]}"
                    )
                    logging.info(f"等待 {wait_time:.1f} 秒后重试...")
                    time.sleep(wait_time)
            
            # 所有重试都失败
            logging.error(f"所有重试均失败，最后错误: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


class BinanceClient:
    """币安API客户端封装类"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_path: Optional[str] = None
    ):
        """
        初始化币安API客户端
        
        Args:
            api_key: API密钥（优先级：参数 > .env文件 > 环境变量 > 默认值）
            api_secret: API密钥（优先级：参数 > .env文件 > 环境变量 > 默认值）
            base_path: API基础路径（优先级：参数 > .env文件 > 环境变量 > 默认值）
        """
        # 🔧 从 .env 文件或环境变量获取配置（已通过 load_dotenv() 加载）
        # 优先级：函数参数 > .env文件/环境变量 > 默认值
        self.api_key = api_key or os.getenv("BINANCE_API_KEY")
        self.api_secret = api_secret or os.getenv("BINANCE_API_SECRET")
        self.base_path = base_path or os.getenv("BASE_PATH", DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL)
        
        # 🔧 验证必需的配置
        if not self.api_key:
            raise ValueError(
                "BINANCE_API_KEY 未设置。请创建 .env 文件并设置 BINANCE_API_KEY，"
                "或在环境变量中设置 BINANCE_API_KEY。"
            )
        if not self.api_secret:
            raise ValueError(
                "BINANCE_API_SECRET 未设置。请创建 .env 文件并设置 BINANCE_API_SECRET，"
                "或在环境变量中设置 BINANCE_API_SECRET。"
            )
        
        # 创建配置和客户端
        # 注意：binance_sdk 可能不支持直接设置超时和代理
        # 如果需要这些功能，可能需要修改 SDK 或使用其他方式
        configuration_rest_api = ConfigurationRestAPI(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_path=self.base_path
        )
        self.client = DerivativesTradingUsdsFutures(config_rest_api=configuration_rest_api)
        
        # 存储网络配置（用于日志和错误提示）
        self.timeout = BINANCE_TIMEOUT
        self.max_retries = BINANCE_MAX_RETRIES
        self.retry_delay = BINANCE_RETRY_DELAY
        self.proxy = BINANCE_PROXY
        
        if self.proxy:
            logging.info(f"已配置代理: {self.proxy}")
        logging.info(f"网络配置: 超时={self.timeout}秒, 最大重试={self.max_retries}次")
    
    @retry_on_network_error(max_retries=BINANCE_MAX_RETRIES, delay=BINANCE_RETRY_DELAY)
    def in_exchange_trading_symbols(
        self,
        symbol_pattern: str = r"usdt$",
        status: str = "TRADING"
    ) -> List[str]:
        """
        获取币安交易所所有合约交易对
        
        Args:
            symbol_pattern: 交易对符号匹配模式（默认匹配USDT结尾）
            status: 交易状态过滤（默认只返回TRADING状态的）
        
        Returns:
            符合条件的交易对符号列表
        """
        try:
            response = self.client.rest_api.exchange_information()
            rate_limits = response.rate_limits
            # logging.info(f"exchange_info() rate limits: {rate_limits}")

            data = response.data()
            usdt_symbols = [
                t.symbol for t in data.symbols
                if re.search(symbol_pattern, t.symbol, flags=re.IGNORECASE) and t.status == status
            ]
            return usdt_symbols
        except Exception as e:
            error_msg = str(e).lower()
            error_type = type(e).__name__
            
            # 判断是否是网络错误
            is_network_error = (
                'network' in error_msg or
                'timeout' in error_msg or
                'connection' in error_msg or
                'ConnectTimeoutError' in error_type or
                'ConnectionError' in error_type
            )
            
            if is_network_error:
                logging.error(
                    f"连接币安API失败: {error_type}\n"
                    f"  错误详情: {str(e)[:200]}\n"
                    f"  建议检查:\n"
                    f"  1. 网络连接是否正常\n"
                    f"  2. 防火墙设置是否阻止了连接\n"
                    f"  3. 是否需要配置代理 (BINANCE_PROXY)\n"
                    f"  4. 币安API服务器是否可访问 (fapi.binance.com)"
                )
            else:
                logging.error(f"exchange_info() error: {e}")
            
            return []
    
    @retry_on_network_error(max_retries=BINANCE_MAX_RETRIES, delay=BINANCE_RETRY_DELAY)
    def kline_candlestick_data(
        self,
        symbol: str,
        interval: str,
        starttime: Optional[int] = None,
        endtime: Optional[int] = None,
        limit: Optional[int] = None
    ):
        """
        获取K线数据
        
        Args:
            symbol: 交易对符号
            interval: K线间隔
            starttime: 开始时间（时间戳，毫秒）
            endtime: 结束时间（时间戳，毫秒）
            limit: 返回数据条数限制
        
        Returns:
            K线数据
        """
        try:
            response = self.client.rest_api.kline_candlestick_data(
                symbol=symbol,
                interval=interval,
                start_time=starttime,
                end_time=endtime,
                limit=limit,
            )

            rate_limits = response.rate_limits
            logging.info(f"kline_candlestick_data() rate limits: {rate_limits}")

            data = response.data()
            return data
        except Exception as e:
            error_msg = str(e).lower()
            error_type = type(e).__name__
            
            is_network_error = (
                'network' in error_msg or
                'timeout' in error_msg or
                'connection' in error_msg or
                'ConnectTimeoutError' in error_type or
                'ConnectionError' in error_type
            )
            
            if is_network_error:
                logging.error(
                    f"获取K线数据失败（{symbol}）: {error_type}\n"
                    f"  错误详情: {str(e)[:200]}"
                )
            else:
                logging.error(f"kline_candlestick_data() error: {e}")
            return None
    
    @retry_on_network_error(max_retries=BINANCE_MAX_RETRIES, delay=BINANCE_RETRY_DELAY)
    def ticker24hr_price_change_statistics(self):
        """
        获取24小时价格变动统计
        
        Returns:
            24小时价格变动统计数据
        """
        try:
            response = self.client.rest_api.ticker24hr_price_change_statistics()

            rate_limits = response.rate_limits
            logging.info(f"ticker24hr_price_change_statistics() rate limits: {rate_limits}")

            data = response.data()
            for t in data:
                if t[0] == "actual_instance":    
                    return t[1]
            return None
        except Exception as e:
            error_msg = str(e).lower()
            error_type = type(e).__name__
            
            is_network_error = (
                'network' in error_msg or
                'timeout' in error_msg or
                'connection' in error_msg or
                'ConnectTimeoutError' in error_type or
                'ConnectionError' in error_type
            )
            
            if is_network_error:
                logging.error(
                    f"获取24小时价格统计失败: {error_type}\n"
                    f"  错误详情: {str(e)[:200]}"
                )
            else:
                logging.error(f"ticker24hr_price_change_statistics() error: {e}", exc_info=True)
            return None
    
    def sort_tickers(
        self,
        symbol_pattern: str = r"usdt$",
        exclude_patterns: tuple = ("UP", "DOWN", "USDTM"),
        reverse: bool = True
    ) -> List:
        """
        按照涨幅降序排序交易对
        
        Args:
            symbol_pattern: 交易对符号匹配模式（默认匹配USDT结尾）
            exclude_patterns: 要排除的交易对后缀（默认排除杠杆/合约交易对）
            reverse: 是否降序排序（默认True，涨幅从高到低）
        
        Returns:
            排序后的交易对列表
        """
        tickers = self.ticker24hr_price_change_statistics()

        if not tickers or tickers is None:
            logging.warning("ticker24hr_price_change_statistics() 返回空或None")
            return []

        in_trading_symbols = self.in_exchange_trading_symbols(symbol_pattern=symbol_pattern)

        if not in_trading_symbols:
            return []

        usdt_tickers = [
            t for t in tickers
            if re.search(symbol_pattern, t.symbol, flags=re.IGNORECASE)
        ]

        in_trading_tickers = [
            t for t in usdt_tickers if t.symbol in in_trading_symbols
        ]
        
        valid_tickers = [
            t for t in in_trading_tickers
            if t.price_change_percent and not t.symbol.endswith(exclude_patterns)
        ]
        
        sorted_tickers = sorted(
            valid_tickers,
            key=lambda x: float(x.price_change_percent),
            reverse=reverse
        )
        
        return sorted_tickers
    
    def get_top_gainers(
        self,
        top_n: int = 3,
        symbol_pattern: str = r"usdt$",
        exclude_patterns: tuple = ("UP", "DOWN", "USDTM")
    ) -> pd.DataFrame:
        """
        获取涨幅前N的交易对
        
        Args:
            top_n: 返回前N个交易对（默认3）
            symbol_pattern: 交易对符号匹配模式（默认匹配USDT结尾）
            exclude_patterns: 要排除的交易对后缀（默认排除杠杆/合约交易对）
        
        Returns:
            包含前N个交易对信息的DataFrame
        """
        try:
            tickers = self.sort_tickers(
                symbol_pattern=symbol_pattern,
                exclude_patterns=exclude_patterns
            )
            
            if not tickers:
                logging.warning("sort_tickers() 返回空列表")
                return pd.DataFrame()
            
            tickers_list = [vars(ticker) for ticker in tickers[:top_n]]
            
            if not tickers_list:
                logging.warning("tickers_list 为空")
                return pd.DataFrame()

            df = pd.DataFrame(tickers_list)
            
            if df.empty:
                logging.warning("DataFrame 为空")
                return df
            
            # 处理时间列（如果存在）
            if 'open_time' in df.columns:
                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True).dt.tz_localize(None)
            if 'close_time' in df.columns:
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True).dt.tz_localize(None)
            
            # 数值列转换为浮点数
            numeric_columns = [
                'price_change', 'price_change_percent', 'last_price', 
                'open_price', 'volume', 'high_price', 'low_price'
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            return df
        except Exception as e:
            logging.error(f"get_top_gainers() 执行失败: {e}", exc_info=True)
            return pd.DataFrame()


# ============================================================================
# 全局默认实例（保持向后兼容）
# ============================================================================

# 创建默认的客户端实例
_default_client = BinanceClient()

# ============================================================================
# 便捷函数（保持向后兼容，内部使用默认实例）
# ============================================================================

def in_exchange_trading_symbols(symbol_pattern: str = r"usdt$", status: str = "TRADING") -> List[str]:
    """
    获取币安交易所所有合约交易对（便捷函数）
    
    Args:
        symbol_pattern: 交易对符号匹配模式（默认匹配USDT结尾）
        status: 交易状态过滤（默认只返回TRADING状态的）
    
    Returns:
        符合条件的交易对符号列表
    """
    return _default_client.in_exchange_trading_symbols(
        symbol_pattern=symbol_pattern,
        status=status
    )


def kline_candlestick_data(
    symbol: str,
    interval: str,
    starttime: Optional[int] = None,
    endtime: Optional[int] = None,
    limit: Optional[int] = None
):
    """
    获取K线数据（便捷函数）
    
    Args:
        symbol: 交易对符号
        interval: K线间隔
        starttime: 开始时间（时间戳，毫秒）
        endtime: 结束时间（时间戳，毫秒）
        limit: 返回数据条数限制
    
    Returns:
        K线数据
    """
    return _default_client.kline_candlestick_data(
        symbol=symbol,
        interval=interval,
        starttime=starttime,
        endtime=endtime,
        limit=limit
    )


def kline2df(data) -> pd.DataFrame:
    """
    K线数据转换为DataFrame
    
    Args:
        data: K线数据列表
    
    Returns:
        转换后的DataFrame
    """
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close",
        "volume", "close_time", "quote_volume", "trade_count",
        "active_buy_volume", "active_buy_quote_volume", "reserved_field"
    ])
   
    # 数据类型转换（字符串→数值/日期）
    df["open"] = pd.to_numeric(df["open"])
    df["high"] = pd.to_numeric(df["high"])
    df["low"] = pd.to_numeric(df["low"])
    df["close"] = pd.to_numeric(df["close"])
    df["volume"] = pd.to_numeric(df["volume"])
    df["quote_volume"] = pd.to_numeric(df["quote_volume"])
    df["trade_count"] = pd.to_numeric(df["trade_count"])
    df["active_buy_volume"] = pd.to_numeric(df["active_buy_volume"])
    df["active_buy_quote_volume"] = pd.to_numeric(df["active_buy_quote_volume"])
    
    # 计算涨跌幅
    df["diff"] = df["close"] - df["close"].shift(1)
    df["pct_chg"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1) * 100
    
    # 时间戳转换为可读日期（毫秒级→秒级→datetime）
    # 🔧 关键修复：显式指定 UTC，然后取消时区信息使其成为 naive datetime (本质仍是 UTC)
    # 这可以避免 pandas 在转换过程中受到本地系统时区的影响
    df["trade_date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_localize(None)
        
    return df


def ticker24hr_price_change_statistics():
    """
    获取24小时价格变动统计（便捷函数）
    
    Returns:
        24小时价格变动统计数据
    """
    return _default_client.ticker24hr_price_change_statistics()


def sort_tickers(
    symbol_pattern: str = r"usdt$",
    exclude_patterns: tuple = ("UP", "DOWN", "USDTM"),
    reverse: bool = True
) -> List:
    """
    按照涨幅降序排序交易对（便捷函数）
    
    Args:
        symbol_pattern: 交易对符号匹配模式（默认匹配USDT结尾）
        exclude_patterns: 要排除的交易对后缀（默认排除杠杆/合约交易对）
        reverse: 是否降序排序（默认True，涨幅从高到低）
    
    Returns:
        排序后的交易对列表
    """
    return _default_client.sort_tickers(
        symbol_pattern=symbol_pattern,
        exclude_patterns=exclude_patterns,
        reverse=reverse
    )


def get_top3_gainers(top_n: int = 3) -> pd.DataFrame:
    """
    获取涨幅前三的交易对（便捷函数，保持向后兼容）
    
    Args:
        top_n: 返回前N个交易对（默认3，保持向后兼容）
    
    Returns:
        包含前N个交易对信息的DataFrame
    """
    return _default_client.get_top_gainers(top_n=top_n)
