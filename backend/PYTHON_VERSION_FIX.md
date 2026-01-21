# Python 3.13 兼容性问题修复

## 问题

`psycopg2-binary==2.9.9` 在 Python 3.13 上可能无法正常构建。

## 解决方案

### 方案 1: 使用更新的 psycopg2-binary（推荐）

```bash
cd backend
uv pip install --upgrade psycopg2-binary
```

或者使用最新版本：
```bash
uv pip install "psycopg2-binary>=2.9.9"
```

### 方案 2: 使用 psycopg（新的纯 Python 实现，推荐用于 Python 3.13+）

`psycopg` 是 `psycopg2` 的现代替代品，完全支持 Python 3.13：

```bash
cd backend
uv pip uninstall psycopg2-binary
uv pip install psycopg[binary]
```

然后需要修改 `backend/db.py` 中的导入：
```python
# 将
import psycopg2

# 改为
import psycopg
```

但 SQLAlchemy 会自动处理，通常不需要修改代码。

### 方案 3: 使用 Python 3.11 或 3.12

如果上述方案都不行，可以使用 Python 3.11 或 3.12：

```bash
# 使用 uv 指定 Python 版本
uv python install 3.12
uv run --python 3.12 python main.py

# 或创建虚拟环境时指定
uv venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

### 方案 4: 从源码编译 psycopg2（需要 PostgreSQL 开发库）

```bash
# macOS
brew install postgresql

# 然后安装 psycopg2（不是 binary 版本）
uv pip install psycopg2
```

## 推荐方案

对于 Python 3.13，推荐使用 **方案 2（psycopg）**，因为：
- 完全支持 Python 3.13
- 性能更好
- 是 psycopg2 的官方继任者
- 与 SQLAlchemy 完全兼容

## 快速修复命令

```bash
cd backend

# 卸载旧版本
uv pip uninstall psycopg2-binary

# 安装新版本（尝试最新版本）
uv pip install "psycopg2-binary>=2.9.9"

# 如果还是失败，使用 psycopg
uv pip install psycopg[binary]
```
