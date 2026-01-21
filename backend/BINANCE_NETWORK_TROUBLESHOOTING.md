# 币安API网络连接问题排查指南

## 常见错误

### 1. 连接超时错误

```
ConnectTimeoutError: Connection to fapi.binance.com timed out
```

### 2. 网络错误

```
Network error: HTTPSConnectionPool(...): Max retries exceeded
```

## 解决方案

### 方案 1: 配置代理（如果在中国大陆）

币安API在某些地区可能无法直接访问，需要配置代理：

在 `.env` 文件中添加：

```env
# HTTP/HTTPS 代理
BINANCE_PROXY=http://proxy_host:proxy_port

# 或 SOCKS5 代理
BINANCE_PROXY=socks5://proxy_host:proxy_port
```

### 方案 2: 增加超时时间

如果网络较慢，可以增加超时时间：

```env
# 连接超时时间（秒），默认30秒
BINANCE_TIMEOUT=60

# 最大重试次数，默认3次
BINANCE_MAX_RETRIES=5

# 重试延迟（秒），默认2秒（使用指数退避）
BINANCE_RETRY_DELAY=3.0
```

### 方案 3: 检查网络连接

```bash
# 测试币安API服务器是否可访问
curl -I https://fapi.binance.com/fapi/v1/ping

# 或使用 ping（如果支持）
ping fapi.binance.com
```

### 方案 4: 使用本地数据（降级方案）

如果无法连接币安API，系统会自动降级使用本地数据库中的数据：

- 查询本地交易对列表
- 使用本地K线数据
- 24小时涨幅功能会暂时不可用

## 环境变量配置

在 `.env` 文件中可以配置以下网络相关参数：

```env
# 币安API密钥（必需）
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret

# 网络配置（可选）
BINANCE_TIMEOUT=30              # 连接超时（秒）
BINANCE_MAX_RETRIES=3           # 最大重试次数
BINANCE_RETRY_DELAY=2.0         # 重试延迟（秒）
BINANCE_PROXY=http://proxy:port # 代理设置
```

## 错误处理机制

系统已实现以下错误处理：

1. **自动重试**: 网络错误会自动重试（默认3次）
2. **指数退避**: 重试延迟逐渐增加（2秒 → 4秒 → 8秒）
3. **错误分类**: 区分网络错误和其他错误
4. **友好提示**: 提供详细的错误信息和解决建议
5. **降级处理**: 网络失败时返回空结果，不影响其他功能

## 常见问题

### Q: 为什么连接超时？

A: 可能的原因：
- 网络不稳定
- 防火墙阻止连接
- 需要代理才能访问
- 币安API服务器暂时不可用

### Q: 如何知道是否需要代理？

A: 尝试直接访问：
```bash
curl https://fapi.binance.com/fapi/v1/ping
```

如果失败，可能需要配置代理。

### Q: 代理配置不生效？

A: 注意：
- 当前版本的 binance_sdk 可能不支持直接设置代理
- 可能需要通过系统代理或环境变量设置
- 或者使用支持代理的 HTTP 客户端包装

### Q: 重试机制如何工作？

A: 
- 默认最多重试3次
- 每次重试前等待时间递增（指数退避）
- 只有网络相关错误才会重试
- API错误（如401、403）不会重试

## 测试连接

可以使用以下Python代码测试连接：

```python
from binance_client import BinanceClient

try:
    client = BinanceClient()
    symbols = client.in_exchange_trading_symbols()
    print(f"成功连接！获取到 {len(symbols)} 个交易对")
except Exception as e:
    print(f"连接失败: {e}")
```

## 更多帮助

如果问题仍然存在，请检查：
1. 网络连接状态
2. 防火墙设置
3. DNS解析是否正常
4. 币安API服务状态页面
