'use client'

import { useState, useEffect } from 'react'

import { API_URLS } from '../lib/api-config'
const API_BASE_URL = API_URLS.data

export default function SymbolList() {
  const [interval, setInterval] = useState('1d')
  const [symbols, setSymbols] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchSymbols = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE_URL}/api/symbols?interval=${interval}`)
      
      if (!response.ok) {
        let errorDetail = '获取失败'
        try {
          const errorData = await response.json()
          errorDetail = errorData.detail || errorData.message || `HTTP ${response.status}`
        } catch {
          errorDetail = `HTTP ${response.status}: ${response.statusText}`
        }
        throw new Error(errorDetail)
      }
      
      const data = await response.json()
      setSymbols(data.symbols || [])
    } catch (err: any) {
      console.error('获取交易对列表错误:', err)
      let errorMessage = '请求失败'
      
      if (err.message) {
        errorMessage = err.message
      } else if (err.name === 'TypeError' && err.message.includes('fetch')) {
        errorMessage = `无法连接到后端服务器 (${API_BASE_URL})。请确保后端服务已启动。`
      } else {
        errorMessage = `请求失败: ${err.toString()}`
      }
      
      setError(errorMessage)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchSymbols()
  }, [interval])

  const INTERVALS = [
    { value: '1m', label: '1分钟' },
    { value: '5m', label: '5分钟' },
    { value: '15m', label: '15分钟' },
    { value: '1h', label: '1小时' },
    { value: '4h', label: '4小时' },
    { value: '1d', label: '1天' },
  ]

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-semibold">交易对列表</h2>
        <div className="flex items-center space-x-4">
          <select
            value={interval}
            onChange={(e) => setInterval(e.target.value)}
            className="px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {INTERVALS.map((int) => (
              <option key={int.value} value={int.value}>
                {int.label}
              </option>
            ))}
          </select>
          <button
            onClick={fetchSymbols}
            disabled={loading}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50"
          >
            {loading ? '加载中...' : '刷新'}
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-500/20 text-red-400 border border-red-500/50 rounded-lg">
          {error}
        </div>
      )}

      {loading && !symbols.length ? (
        <div className="text-center py-8 text-gray-400">加载中...</div>
      ) : symbols.length === 0 ? (
        <div className="text-center py-8 text-gray-400">暂无数据</div>
      ) : (
        <div className="bg-gray-700/50 rounded-lg p-4">
          <div className="mb-4 text-sm text-gray-400">
            共 {symbols.length} 个交易对
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
            {symbols.map((symbol) => (
              <div
                key={symbol}
                className="px-3 py-2 bg-gray-600/50 rounded-lg text-center hover:bg-gray-600 transition-colors"
              >
                {symbol}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

