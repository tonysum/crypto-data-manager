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

// 缓存 API URL，避免每次渲染时重新计算
let cachedApiUrls: { [key: string]: string } | null = null
let cachedHostname: string | null = null

const getApiUrl = (service: 'data' | 'backtest' | 'order'): string => {
  // 从环境变量获取服务地址，如果没有则使用默认值
  const envKey = `NEXT_PUBLIC_${service.toUpperCase()}_SERVICE_URL`
  
  // 如果在浏览器环境中，尝试根据当前访问的域名自动推断后端地址
  if (typeof window !== 'undefined') {
    const currentHostname = window.location.hostname
    
    // 如果 hostname 改变，清除缓存
    if (cachedHostname !== currentHostname) {
      cachedApiUrls = null
      cachedHostname = currentHostname
    }
    
    // 如果已有缓存，直接返回
    if (cachedApiUrls && cachedApiUrls[service]) {
      return cachedApiUrls[service]
    }
    
    // 优先使用环境变量
    const envUrl = (window as any).__ENV__?.[envKey] || process.env[envKey]
    if (envUrl) {
      if (!cachedApiUrls) cachedApiUrls = {}
      cachedApiUrls[service] = envUrl
      return envUrl
    }
    
    // 如果当前访问的不是 localhost，使用相同的 hostname
    if (currentHostname !== 'localhost' && currentHostname !== '127.0.0.1') {
      const url = `http://${currentHostname}:${getDefaultPort(service)}`
      if (!cachedApiUrls) cachedApiUrls = {}
      cachedApiUrls[service] = url
      return url
    }
  }
  
  // 默认使用 localhost（开发环境）
  const defaultUrl = `http://localhost:${getDefaultPort(service)}`
  const finalUrl = (typeof process !== 'undefined' ? process.env[envKey] : undefined) || defaultUrl
  
  // 缓存结果
  if (typeof window !== 'undefined') {
    if (!cachedApiUrls) cachedApiUrls = {}
    cachedApiUrls[service] = finalUrl
  }
  
  return finalUrl
}

// 兼容旧的API_URL配置（如果设置了，所有服务都使用它）
const legacyApiUrl = typeof window !== 'undefined' 
  ? (window as any).__ENV__?.NEXT_PUBLIC_API_URL
  : (typeof process !== 'undefined' ? process.env.NEXT_PUBLIC_API_URL : undefined)

// 使用立即执行函数初始化，避免每次导入时重新计算
const initApiUrls = () => {
  if (legacyApiUrl) {
    return {
      data: legacyApiUrl,
      backtest: legacyApiUrl,
      order: legacyApiUrl,
    }
  }
  return {
    data: getApiUrl('data'),
    backtest: getApiUrl('backtest'),
    order: getApiUrl('order'),
  }
}

export const API_URLS = initApiUrls()

// 导出默认API URL（用于向后兼容）
export const API_BASE_URL = legacyApiUrl || API_URLS.data

