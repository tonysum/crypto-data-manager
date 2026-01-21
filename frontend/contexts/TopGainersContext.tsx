'use client'

import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { API_URLS } from '../lib/api-config'
import { safeFetch, getErrorMessage } from '../lib/error-handler'

interface TopGainer {
  symbol: string
  pct_chg: number
  close: number | null
  open: number | null
  high: number | null
  low: number | null
  volume: number | null
}

interface TopGainersContextType {
  topGainers: TopGainer[]
  topGainersDate: string
  loading: boolean
  error: string | null
  refresh: () => Promise<void>
}

const TopGainersContext = createContext<TopGainersContextType | undefined>(undefined)

export function TopGainersProvider({ children }: { children: ReactNode }) {
  const [topGainers, setTopGainers] = useState<TopGainer[]>([])
  const [topGainersDate, setTopGainersDate] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchTopGainers = async () => {
    try {
      setLoading(true)
      setError(null)
      
      const apiUrl = `${API_URLS.data}/api/top-gainers?top_n=3`
      console.log('正在获取涨幅排名:', apiUrl)
      
      // 使用统一的 safeFetch，自动处理超时和错误
      const response = await safeFetch(apiUrl, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      })
      
      const data = await response.json()
      console.log('前一天涨幅排名数据:', data)
      
      // 如果返回了 message，说明有特殊提示（比如未找到数据）
      if (data.message) {
        console.warn('API 返回提示:', data.message)
      }
      
      setTopGainers(data.top_gainers || [])
      setTopGainersDate(data.date || '')
      
      // 如果没有数据，记录详细信息用于调试
      if (!data.top_gainers || data.top_gainers.length === 0) {
        console.warn('未获取到前一天涨幅数据:', {
          date: data.date,
          message: data.message,
          total_count: data.total_count
        })
      }
    } catch (err) {
      console.error('获取涨幅排名失败:', err)
      
      // 使用统一的错误处理工具
      const errorMessage = getErrorMessage(err, API_URLS.data)
      setError(errorMessage)
      setTopGainers([])
    } finally {
      setLoading(false)
    }
  }

  // 应用启动时加载数据
  useEffect(() => {
    fetchTopGainers()
  }, [])

  return (
    <TopGainersContext.Provider
      value={{
        topGainers,
        topGainersDate,
        loading,
        error,
        refresh: fetchTopGainers,
      }}
    >
      {children}
    </TopGainersContext.Provider>
  )
}

export function useTopGainers() {
  const context = useContext(TopGainersContext)
  if (context === undefined) {
    throw new Error('useTopGainers must be used within a TopGainersProvider')
  }
  return context
}

