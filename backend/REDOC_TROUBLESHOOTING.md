# ReDoc 页面空白问题排查

## 问题现象

访问 `http://localhost:8001/redoc` 时页面显示空白。

## 可能原因

1. **JavaScript 资源加载失败**（最常见）
   - ReDoc 使用 CDN 加载 JavaScript 资源
   - 如果网络无法访问 `cdn.jsdelivr.net`，页面会空白

2. **OpenAPI Schema 格式问题**
   - Schema 格式不正确会导致 ReDoc 无法渲染

3. **浏览器兼容性问题**
   - 某些浏览器可能不支持 ReDoc

## 解决方案

### 方案 1: 检查浏览器控制台

1. 打开浏览器开发者工具（F12）
2. 查看 Console 标签页是否有错误
3. 查看 Network 标签页，检查资源是否加载成功

### 方案 2: 使用 Swagger UI（推荐）

如果 ReDoc 无法使用，可以使用 Swagger UI：

```
http://localhost:8001/docs
```

Swagger UI 功能更强大，支持：
- 交互式 API 测试
- 请求/响应示例
- 参数验证

### 方案 3: 检查网络连接

```bash
# 测试 CDN 是否可访问
curl -I https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js

# 如果无法访问，可能需要配置代理
```

### 方案 4: 验证 OpenAPI Schema

```bash
# 检查 OpenAPI schema 是否有效
curl http://localhost:8001/openapi.json | python3 -m json.tool > /dev/null

# 如果命令成功（退出码0），说明 schema 格式正确
```

### 方案 5: 清除浏览器缓存

1. 清除浏览器缓存
2. 强制刷新页面（Ctrl+Shift+R 或 Cmd+Shift+R）
3. 尝试使用无痕模式

## 临时解决方案

如果 ReDoc 无法使用，可以：

1. **使用 Swagger UI** (`/docs`)
2. **直接查看 OpenAPI JSON** (`/openapi.json`)
3. **使用 API 客户端**（如 Postman）导入 OpenAPI schema

## 验证步骤

1. ✅ 检查服务是否运行：`curl http://localhost:8001/api/health`
2. ✅ 检查 OpenAPI schema：`curl http://localhost:8001/openapi.json`
3. ✅ 尝试 Swagger UI：访问 `http://localhost:8001/docs`
4. ✅ 检查浏览器控制台错误

## 推荐做法

**优先使用 Swagger UI** (`/docs`)，因为：
- 功能更完整
- 支持交互式测试
- 资源加载更稳定
- 更好的错误提示
