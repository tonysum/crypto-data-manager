/**
 * 网络错误处理工具
 * 统一处理各种网络错误，提供友好的错误提示
 */

export interface NetworkError {
  message: string
  type: 'network' | 'server' | 'timeout' | 'unknown'
  details?: string
}

/**
 * 处理 fetch 错误，返回友好的错误信息
 */
export function handleNetworkError(error: any, apiUrl?: string): NetworkError {
  // 网络连接错误（无法连接到服务器）
  if (
    error instanceof TypeError &&
    (error.message === 'Failed to fetch' || 
     error.message.includes('fetch') ||
     error.message.includes('NetworkError') ||
     error.message.includes('Network request failed'))
  ) {
    return {
      message: '网络连接失败',
      type: 'network',
      details: apiUrl 
        ? `无法连接到服务器 ${apiUrl}。请检查：\n1. 后端服务是否已启动\n2. 网络连接是否正常\n3. 服务器地址是否正确`
        : '无法连接到服务器。请检查网络连接和后端服务状态。'
    }
  }

  // 超时错误
  if (
    error.name === 'TimeoutError' ||
    error.message?.includes('timeout') ||
    error.message?.includes('超时')
  ) {
    return {
      message: '请求超时',
      type: 'timeout',
      details: '服务器响应时间过长。请稍后重试，或检查网络连接。'
    }
  }

  // 服务器错误（HTTP 5xx）
  if (error.status >= 500) {
    return {
      message: '服务器错误',
      type: 'server',
      details: `服务器内部错误 (${error.status})。请稍后重试，或联系管理员。`
    }
  }

  // HTTP 4xx 错误
  if (error.status >= 400 && error.status < 500) {
    return {
      message: error.message || '请求错误',
      type: 'server',
      details: `请求失败 (${error.status})。${error.message || '请检查请求参数。'}`
    }
  }

  // 其他错误
  return {
    message: error.message || '未知错误',
    type: 'unknown',
    details: error.toString()
  }
}

/**
 * 包装 fetch 请求，自动处理网络错误
 */
export async function safeFetch(
  url: string,
  options?: RequestInit,
  timeoutMs: number = 60000 // 默认60秒超时
): Promise<Response> {
  try {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs)

    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    })

    clearTimeout(timeoutId)

    if (!response.ok) {
      let errorDetail = `HTTP ${response.status}`
      try {
        const errorData = await response.json()
        errorDetail = errorData.detail || errorData.message || errorDetail
      } catch {
        errorDetail = `${errorDetail}: ${response.statusText}`
      }
      
      const error: any = new Error(errorDetail)
      error.status = response.status
      throw error
    }

    return response
  } catch (error: any) {
    // 处理超时
    if (error.name === 'AbortError') {
      const timeoutError: any = new Error('请求超时')
      timeoutError.name = 'TimeoutError'
      throw timeoutError
    }

    // 重新抛出其他错误
    throw error
  }
}

/**
 * 获取友好的错误消息（用于显示给用户）
 */
export function getErrorMessage(error: any, apiUrl?: string): string {
  const networkError = handleNetworkError(error, apiUrl)
  
  if (networkError.details) {
    return `${networkError.message}\n\n${networkError.details}`
  }
  
  return networkError.message
}
