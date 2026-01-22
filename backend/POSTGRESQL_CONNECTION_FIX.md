# PostgreSQL 连接问题修复指南

## 错误信息

```
FATAL: no pg_hba.conf entry for host "192.168.2.101", user "postgres", database "crypto_data", no encryption
```

## 问题原因

PostgreSQL 服务器的 `pg_hba.conf` 文件没有允许从你的 IP 地址（192.168.2.101）连接。

## 解决方案

### 方案 1：在 PostgreSQL 服务器上配置 pg_hba.conf（推荐）

1. **登录到 PostgreSQL 服务器**（192.168.2.200）

2. **编辑 pg_hba.conf 文件**：
   ```bash
   sudo nano /etc/postgresql/15/main/pg_hba.conf
   # 或者
   sudo nano /var/lib/pgsql/15/data/pg_hba.conf
   ```

3. **添加以下行**（允许从你的 IP 连接）：
   ```
   # 允许从 192.168.2.101 连接（使用密码认证）
   host    crypto_data    postgres    192.168.2.101/32    scram-sha-256
   
   # 或者允许整个子网
   host    crypto_data    postgres    192.168.2.0/24     scram-sha-256
   ```

4. **重新加载 PostgreSQL 配置**：
   ```bash
   sudo systemctl reload postgresql
   # 或者
   sudo systemctl reload postgresql@15-main
   ```

### 方案 2：使用 SSL 连接

如果服务器要求 SSL 连接，在 `.env` 文件中添加：

```env
PG_SSLMODE=require
```

或者：

```env
PG_SSLMODE=prefer
```

### 方案 3：修改连接方式

如果无法修改服务器配置，可以：

1. **使用 SSH 隧道**：
   ```bash
   ssh -L 5432:localhost:5432 user@192.168.2.200
   ```
   然后修改 `.env` 文件：
   ```env
   PG_HOST=localhost
   ```

2. **使用 VPN 或内网连接**：确保你的 IP 在允许的 IP 范围内

## 验证连接

修复后，测试连接：

```bash
psql -h 192.168.2.200 -U postgres -d crypto_data
```

或者在 Python 中：

```python
from db import engine
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("连接成功！")
```

## 当前代码改进

代码已经添加了：
1. ✅ SSL 支持（自动检测远程连接并使用 `prefer` 模式）
2. ✅ 密码 URL 编码（处理特殊字符）
3. ✅ 错误处理（避免启动时连接失败导致应用崩溃）

## 快速修复

如果急需启动服务，可以临时修改 `.env` 文件：

```env
# 如果服务器支持 SSL
PG_SSLMODE=prefer

# 或者使用本地数据库（如果数据已迁移）
PG_HOST=localhost
```
