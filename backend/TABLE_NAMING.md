# 数据库表命名格式说明

## 📋 表命名规则

### 1. K线数据表

**格式：** `K{interval}{symbol}`

**说明：**
- `K` - 固定前缀，表示K线数据
- `{interval}` - K线时间间隔
- `{symbol}` - 交易对符号（全大写，无分隔符）

**示例：**

| 表名 | 说明 | 时间间隔 | 交易对 |
|------|------|----------|--------|
| `K1dBTCUSDT` | 日线BTCUSDT | 1d (1天) | BTCUSDT |
| `K1hETHUSDT` | 1小时ETHUSDT | 1h (1小时) | ETHUSDT |
| `K5mENSUSDT` | 5分钟ENSUSDT | 5m (5分钟) | ENSUSDT |
| `K4hSOLUSDT` | 4小时SOLUSDT | 4h (4小时) | SOLUSDT |
| `K1wBNBUSDT` | 周线BNBUSDT | 1w (1周) | BNBUSDT |
| `K1MADAUSDT` | 月线ADAUSDT | 1M (1月) | ADAUSDT |

### 2. 支持的时间间隔

| 间隔代码 | 说明 | 示例表名 |
|---------|------|----------|
| `1m` | 1分钟 | `K1mBTCUSDT` |
| `3m` | 3分钟 | `K3mBTCUSDT` |
| `5m` | 5分钟 | `K5mBTCUSDT` |
| `15m` | 15分钟 | `K15mBTCUSDT` |
| `30m` | 30分钟 | `K30mBTCUSDT` |
| `1h` | 1小时 | `K1hBTCUSDT` |
| `2h` | 2小时 | `K2hBTCUSDT` |
| `4h` | 4小时 | `K4hBTCUSDT` |
| `6h` | 6小时 | `K6hBTCUSDT` |
| `8h` | 8小时 | `K8hBTCUSDT` |
| `12h` | 12小时 | `K12hBTCUSDT` |
| `1d` | 1天（日线） | `K1dBTCUSDT` |
| `3d` | 3天 | `K3dBTCUSDT` |
| `1w` | 1周 | `K1wBTCUSDT` |
| `1M` | 1月 | `K1MBTCUSDT` |

### 3. 其他系统表

#### 回测记录表

**表名：** `backtrade_records`

**说明：** 存储回测交易记录

**字段：**
- `id` - 主键（BIGSERIAL）
- `entry_date` - 入场日期
- `symbol` - 交易对
- `entry_price` - 入场价格
- `position_size` - 仓位大小
- `leverage` - 杠杆倍数
- `exit_date` - 出场日期
- `exit_price` - 出场价格
- `profit_loss` - 盈亏
- `created_at` - 创建时间
- 等等...

#### 交易对表（可选）

**表名：** `symbols`

**说明：** 存储交易对元数据信息

## 🔍 表名查询示例

### 查询所有日线表

```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'K1d%'
ORDER BY table_name;
```

### 查询所有5分钟表

```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'K5m%'
ORDER BY table_name;
```

### 查询特定交易对的所有时间间隔表

```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'K%BTCUSDT'
ORDER BY table_name;
```

### 在代码中构建表名

```python
# Python示例
interval = "1d"  # 或 "5m", "1h", "4h" 等
symbol = "BTCUSDT"
table_name = f"K{interval}{symbol}"
# 结果: "K1dBTCUSDT"
```

## 📝 注意事项

### 1. 大小写敏感性

- **PostgreSQL**: 表名如果使用引号创建（如 `"K1dBTCUSDT"`），会保持原始大小写
- **查询时**: 如果表名包含引号创建，查询时也需要使用引号：`SELECT * FROM "K1dBTCUSDT"`
- **建议**: 统一使用大写字母，避免大小写问题

### 2. 特殊字符

- 交易对符号中不应包含特殊字符
- 如果交易对包含连字符（如 `BTC-USDT`），应转换为无分隔符格式（`BTCUSDT`）

### 3. 表名长度限制

- PostgreSQL 表名最大长度为 63 个字符
- 当前格式 `K{interval}{symbol}` 通常不会超过限制
- 最长示例：`K1MBTCUSDT` (11字符) 或 `K15mBTCUSDT` (12字符)

### 4. 表名验证

在创建表前，建议验证表名格式：

```python
import re

def validate_table_name(table_name: str) -> bool:
    """验证表名格式是否正确"""
    pattern = r'^K\d+[mhdwM][A-Z0-9]+$'
    return bool(re.match(pattern, table_name))

# 示例
assert validate_table_name("K1dBTCUSDT") == True
assert validate_table_name("K5mETHUSDT") == True
assert validate_table_name("backtrade_records") == False  # 系统表
```

## 🔧 代码中的使用

### 获取表名

```python
# 在 download_klines.py 中
table_name = f'K{interval}{symbol}'

# 在 data.py 中
table_name = f'K{interval}{symbol}'

# 在 main.py (API) 中
table_name = f'K{request.interval}{request.symbol}'
```

### 查询表列表

```python
# 获取所有日线表
prefix = 'K1d'
stmt = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE :prefix
"""
result = conn.execute(text(stmt), {"prefix": f"{prefix}%"})
```

### 从表名提取信息

```python
def parse_table_name(table_name: str) -> dict:
    """从表名解析出时间间隔和交易对"""
    if not table_name.startswith('K'):
        return None
    
    # 找到第一个字母（时间间隔单位）
    import re
    match = re.match(r'^K(\d+)([mhdwM])(.+)$', table_name)
    if match:
        number = match.group(1)
        unit = match.group(2)
        symbol = match.group(3)
        return {
            'interval': f"{number}{unit}",
            'symbol': symbol
        }
    return None

# 示例
result = parse_table_name("K1dBTCUSDT")
# {'interval': '1d', 'symbol': 'BTCUSDT'}
```

## 📚 相关文档

- [数据库迁移指南](./MIGRATION.md)
- [API文档](../README.md)
- [数据下载说明](./download_klines.py) - 查看文件开头的注释
