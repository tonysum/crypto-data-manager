# 数据管理前端

币安K线数据管理前端应用，用于管理和维护币安U本位合约K线数据。

## 功能特性

- 📥 **下载K线数据** - 支持下载指定交易对或所有交易对的K线数据
- 🗑️ **删除K线数据** - 删除本地数据库中的K线数据
- ✏️ **修改K线数据** - 编辑本地数据库中的K线数据
- 📊 **查看K线数据** - 查询和查看指定交易对的K线数据，对比本地和币安API数据
- 📈 **列表与图表** - 查看交易对列表和K线图表
- ✅ **完整性检查** - 数据完整性校验和报告生成
- 💾 **数据库文件管理** - 上传和下载数据库文件

## 安装和运行

### 1. 安装依赖

```bash
npm install
```

### 2. 配置环境变量

创建 `.env.local` 文件：

```bash
NEXT_PUBLIC_DATA_SERVICE_URL=http://localhost:8001
NEXT_PUBLIC_BACKTEST_SERVICE_URL=http://localhost:8002
NEXT_PUBLIC_ORDER_SERVICE_URL=http://localhost:8003
```

### 3. 启动开发服务器

```bash
npm run dev
```

访问 http://localhost:3001 查看应用。

### 4. 构建生产版本

```bash
npm run build
npm start
```

## 技术栈

- **Next.js 16** - React框架
- **TypeScript** - 类型安全
- **Tailwind CSS** - 样式框架
- **React Hooks** - 状态管理

## 端口

- **开发环境**: 3001
- **生产环境**: 3001

## 后端API要求

前端需要连接到运行在 `http://localhost:8001` 的数据服务。

确保后端服务已启动：
```bash
cd ../backend
python services/data_service/main.py
```

## Docker 部署

```bash
# 构建镜像
docker build -t frontend-data .

# 运行容器
docker run -p 3001:3001 frontend-data
```

## 项目结构

```
frontend-data/
├── app/
│   ├── page.tsx          # 主页面
│   ├── layout.tsx        # 布局组件
│   └── globals.css       # 全局样式
├── components/           # React 组件
├── lib/                  # 工具函数和配置
├── contexts/             # React Context
└── public/               # 静态资源
```
