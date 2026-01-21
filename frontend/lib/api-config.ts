/**
 * API配置
 * 管理不同服务的API地址
 */

const getDefaultPort = (service: 'data' | 'backtest' | 'order'): string => {
  switch (service) {
    case 'data':
      return '8001'
    case 'backtest':
      return '8002'
    case 'order':
      return '8003'
    default:
      return '8000'
  }
}

const getApiUrl = (service: 'data' | 'backtest' | 'order'): string => {
  // 从环境变量获取服务地址，如果没有则使用默认值
  const envKey = `NEXT_PUBLIC_${service.toUpperCase()}_SERVICE_URL`
  const defaultUrl = `http://localhost:${getDefaultPort(service)}`
  return (typeof window !== 'undefined' ? (window as any).__ENV__?.[envKey] : undefined) 
    || (typeof process !== 'undefined' ? process.env[envKey] : undefined)
    || defaultUrl
}

// 兼容旧的API_URL配置（如果设置了，所有服务都使用它）
const legacyApiUrl = typeof window !== 'undefined' 
  ? (window as any).__ENV__?.NEXT_PUBLIC_API_URL
  : (typeof process !== 'undefined' ? process.env.NEXT_PUBLIC_API_URL : undefined)

export const API_URLS = {
  data: legacyApiUrl || getApiUrl('data'),
  backtest: legacyApiUrl || getApiUrl('backtest'),
  order: legacyApiUrl || getApiUrl('order'),
}

// 导出默认API URL（用于向后兼容）
export const API_BASE_URL = legacyApiUrl || API_URLS.data

