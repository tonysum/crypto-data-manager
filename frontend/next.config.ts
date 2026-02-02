import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  /* config options here */
  output: 'standalone',
  
  // 开发模式配置
  ...(process.env.NODE_ENV === 'development' && {
    // 允许开发模式下的跨域请求（用于从外部 IP 访问）
    allowedDevOrigins: [
      '8.216.33.6',  // 服务器 IP
      '192.168.2.250',  // 内网 IP
      'localhost',
      '127.0.0.1',
    ],
    // 减少热重载的频率，避免频繁刷新
    // 默认情况下，Next.js 会在文件变化时自动刷新
    // 可以通过设置环境变量 NEXT_DISABLE_HMR=1 来禁用（但会失去热重载功能）
  }),
  
  // 生产环境配置
  reactStrictMode: true,  // React 严格模式（开发时会触发双重渲染，这是正常的）
} as NextConfig;

export default nextConfig;
