# 使用 uv 运行后端服务

`uv` 是一个快速的 Python 包管理器和项目管理工具。本指南说明如何使用 `uv` 来管理依赖和运行后端服务。

## 📦 安装 uv

```bash
# macOS 和 Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# 或使用 pip
pip install uv
```

## 🚀 快速开始

### 方法 1: 使用 requirements.txt（推荐）

这是最简单直接的方式：

1. **安装依赖**
   ```bash
   cd backend
   uv pip install -r requirements.txt
   ```
   这会创建虚拟环境并安装所有依赖。

2. **运行后端服务**
   ```bash
   uv run python main.py
   ```
   或者直接使用 uvicorn：
   ```bash
   uv run uvicorn main:app --host 0.0.0.0 --port 8001
   ```

### 方法 2: 使用 pyproject.toml

如果你使用 `pyproject.toml`：

1. **安装依赖**
   ```bash
   cd backend
   uv sync
   ```
   这会创建虚拟环境并安装所有依赖。

2. **运行后端服务**
   ```bash
   uv run python main.py
   ```

### 方法 3: 使用 uv 的虚拟环境

1. **创建并激活虚拟环境**
   ```bash
   cd backend
   uv venv
   source .venv/bin/activate  # Linux/macOS
   # 或 Windows: .venv\Scripts\activate
   ```

2. **安装依赖**
   ```bash
   uv pip install -r requirements.txt
   # 或
   uv pip sync requirements.txt
   ```

3. **运行服务**
   ```bash
   python main.py
   ```

## 📝 常用命令

### 添加新依赖

```bash
# 使用 pyproject.toml（推荐）
uv add package-name

# 或使用 requirements.txt
uv pip install package-name
# 然后手动更新 requirements.txt
```

### 更新依赖

```bash
# 使用 pyproject.toml
uv sync --upgrade

# 或使用 requirements.txt
uv pip install -r requirements.txt --upgrade
```

### 移除依赖

```bash
# 使用 pyproject.toml
uv remove package-name

# 或使用 requirements.txt
uv pip uninstall package-name
```

### 查看已安装的包

```bash
uv pip list
```

### 导出依赖

```bash
# 从虚拟环境导出到 requirements.txt
uv pip freeze > requirements.txt
```

## 🔧 开发环境

### 安装开发依赖

```bash
# 使用 pyproject.toml
uv sync --dev

# 或使用 requirements.txt
uv pip install -r requirements-dev.txt  # 如果有的话
```

## 🐳 Docker 中使用 uv

如果你想在 Docker 中使用 uv，可以修改 `Dockerfile`：

```dockerfile
FROM python:3.11-slim

# 安装 uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# 复制依赖文件
COPY pyproject.toml ./
COPY requirements.txt ./

# 使用 uv 安装依赖（更快）
RUN uv pip install --system -r requirements.txt

# 或使用 pyproject.toml
# RUN uv sync --frozen

COPY . .

EXPOSE 8001

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
```

## ⚡ uv 的优势

1. **速度快**: uv 比 pip 快 10-100 倍
2. **兼容性好**: 完全兼容 pip 和 requirements.txt
3. **现代化**: 支持 pyproject.toml 和 PEP 标准
4. **统一工具**: 可以替代 pip、pip-tools、virtualenv 等多个工具

## 📚 更多信息

- [uv 官方文档](https://docs.astral.sh/uv/)
- [uv GitHub](https://github.com/astral-sh/uv)
