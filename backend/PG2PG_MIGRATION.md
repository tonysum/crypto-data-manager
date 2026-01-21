# PostgreSQL 到 PostgreSQL 数据迁移指南

本指南介绍如何将本地 PostgreSQL 数据库的数据迁移到云服务器的 PostgreSQL 数据库。

## 前置要求

1. **安装 PostgreSQL 客户端工具**：
   - macOS: `brew install postgresql`
   - Ubuntu/Debian: `sudo apt-get install postgresql-client`
   - 或者确保 `pg_dump` 和 `pg_restore` 命令可用

2. **确保网络连接**：
   - 本地可以访问云服务器的 PostgreSQL（端口开放）
   - 或者使用 SSH 隧道

## 方法一：使用 pg_dump/pg_restore（推荐，速度快）

### 1. 基本用法

```bash
cd backend
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method dump
```

### 2. 指定源数据库（如果与默认不同）

```bash
python migrate_pg2pg.py \
  --source-host localhost \
  --source-port 5432 \
  --source-db crypto_data \
  --source-user postgres \
  --source-password local-password \
  --target-host your-cloud-server.com \
  --target-port 5432 \
  --target-db crypto_data \
  --target-user postgres \
  --target-password cloud-password \
  --method dump
```

### 3. 只迁移特定表

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method dump \
  --tables K1dBTCUSDT K1dETHUSDT symbols
```

### 4. 只迁移K线数据表（使用过滤）

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method dump \
  --table-filter K1d
```

## 方法二：使用 Python 脚本（更灵活，支持增量迁移）

### 1. 完整迁移

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method python
```

### 2. 增量迁移（跳过已存在的数据）

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method python \
  --skip-existing
```

### 3. 只迁移特定表

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method python \
  --tables K1dBTCUSDT K1dETHUSDT
```

### 4. 从文件读取表列表

```bash
# 创建表列表文件
echo "K1dBTCUSDT" > tables.txt
echo "K1dETHUSDT" >> tables.txt

# 迁移
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method python \
  --table-file tables.txt
```

## 使用环境变量（推荐，更安全）

创建 `.env` 文件或设置环境变量：

```bash
# 源数据库（本地）
export SOURCE_PG_HOST=localhost
export SOURCE_PG_PORT=5432
export SOURCE_PG_DB=crypto_data
export SOURCE_PG_USER=postgres
export SOURCE_PG_PASSWORD=your-local-password

# 目标数据库（云服务器）
export TARGET_PG_HOST=your-cloud-server.com
export TARGET_PG_PORT=5432
export TARGET_PG_DB=crypto_data
export TARGET_PG_USER=postgres
export TARGET_PG_PASSWORD=your-cloud-password
```

然后运行：

```bash
python migrate_pg2pg.py --method dump
```

## 对比数据库（不执行迁移）

在迁移前，可以先对比两个数据库的表数量：

```bash
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --compare-only
```

## 云服务器配置说明

`your-cloud-server.com` 是一个占位符，你需要替换为实际的云服务器信息。具体包括：

### 1. 服务器地址（`--target-host`）

可以是以下任一形式：
- **IP 地址**：`192.168.1.100` 或 `123.45.67.89`
- **域名**：`db.example.com` 或 `postgres.example.com`
- **本地主机**（如果使用 SSH 隧道）：`localhost`

**示例**：
```bash
# 使用 IP 地址
--target-host 123.45.67.89

# 使用域名
--target-host db.example.com

# 使用 localhost（SSH 隧道）
--target-host localhost
```

### 2. 完整配置参数

迁移脚本需要以下云服务器数据库信息：

| 参数 | 说明 | 默认值 | 是否必需 |
|------|------|--------|----------|
| `--target-host` | 云服务器地址（IP 或域名） | - | ✅ **必需** |
| `--target-port` | PostgreSQL 端口 | `5432` | 可选 |
| `--target-db` | 数据库名 | `crypto_data` | 可选 |
| `--target-user` | 数据库用户名 | `postgres` | 可选 |
| `--target-password` | 数据库密码 | - | ✅ **必需** |

### 3. 完整示例

假设你的云服务器信息如下：
- **服务器地址**：`123.45.67.89`（或域名 `db.example.com`）
- **端口**：`5432`
- **数据库名**：`crypto_data`
- **用户名**：`postgres`
- **密码**：`your-cloud-password`

**迁移命令**：
```bash
python migrate_pg2pg.py \
  --target-host 123.45.67.89 \
  --target-port 5432 \
  --target-db crypto_data \
  --target-user postgres \
  --target-password your-cloud-password \
  --method dump
```

或者使用域名：
```bash
python migrate_pg2pg.py \
  --target-host db.example.com \
  --target-password your-cloud-password \
  --method dump
```

### 4. 如何获取云服务器信息

通常从以下地方获取：
- **云服务商控制台**：阿里云、腾讯云、AWS 等
- **服务器管理员**：如果数据库由他人管理
- **服务器配置文件**：检查服务器上的 PostgreSQL 配置

## 通过 SSH 隧道迁移

如果云服务器的 PostgreSQL 不对外开放端口，可以使用 SSH 隧道：

```bash
# 1. 建立 SSH 隧道（在另一个终端）
ssh -L 5433:localhost:5432 user@your-cloud-server.com

# 2. 使用本地端口连接
python migrate_pg2pg.py \
  --target-host localhost \
  --target-port 5433 \
  --target-password your-password \
  --method dump
```

**注意**：使用 SSH 隧道时，`--target-host` 填写 `localhost`，端口填写隧道本地端口（如 `5433`）。

## 注意事项

1. **备份数据**：迁移前建议先备份目标数据库
2. **网络稳定性**：确保网络连接稳定，大数据量迁移可能需要较长时间
3. **磁盘空间**：使用 `dump` 方法时会在本地创建临时文件，确保有足够空间
4. **权限**：确保数据库用户有足够的权限（CREATE, INSERT, SELECT 等）
5. **表名大小写**：PostgreSQL 中，如果表名用引号创建会保持大小写，迁移脚本会自动处理

## 故障排查

### 1. 连接失败

```
错误: 无法连接到 PostgreSQL
```

**解决方案**：
- 检查云服务器防火墙是否开放 PostgreSQL 端口（默认 5432）
- 检查 PostgreSQL 配置（`postgresql.conf` 和 `pg_hba.conf`）
- 使用 `--compare-only` 测试连接

### 2. 权限错误

```
错误: permission denied
```

**解决方案**：
- 确保数据库用户有足够权限
- 检查目标数据库是否存在
- 检查用户是否有 CREATE 权限

### 3. pg_dump 命令不存在

```
错误: pg_dump: command not found
```

**解决方案**：
- 安装 PostgreSQL 客户端工具
- 或使用 `--method python` 方法

### 4. 表名大小写问题

如果遇到表名大小写不匹配的错误，脚本会自动处理。如果仍有问题，可以：

```bash
# 使用 Python 方法，它会自动处理大小写问题
python migrate_pg2pg.py --method python --target-host ... --target-password ...
```

## 迁移后验证

迁移完成后，建议验证数据：

```bash
# 1. 对比表数量
python migrate_pg2pg.py --compare-only --target-host ... --target-password ...

# 2. 检查数据完整性（使用数据完整性检查功能）
# 在前端或通过 API 调用数据完整性检查接口
```

## 性能优化

1. **使用 dump 方法**：对于大数据量，`dump` 方法比 Python 脚本快得多
2. **批量迁移**：可以分批迁移表，避免一次性迁移所有数据
3. **网络优化**：如果可能，在云服务器本地执行迁移（从本地导出，在服务器导入）

## 示例：完整迁移流程

```bash
# 1. 对比数据库
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --compare-only

# 2. 迁移所有数据（使用 dump 方法，最快）
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --method dump

# 3. 再次对比，确认迁移成功
python migrate_pg2pg.py \
  --target-host your-cloud-server.com \
  --target-password your-password \
  --compare-only
```

## 更新后端配置

迁移完成后，更新 `.env` 文件，将数据库配置指向云服务器：

```env
PG_HOST=your-cloud-server.com
PG_PORT=5432
PG_DB=crypto_data
PG_USER=postgres
PG_PASSWORD=your-cloud-password
```

然后重启后端服务即可。
