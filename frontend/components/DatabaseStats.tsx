'use client'

import { useState, useEffect } from 'react'
import { API_URLS } from '../lib/api-config'
import { safeFetch, getErrorMessage } from '../lib/error-handler'

const API_BASE_URL = API_URLS.data

interface IntervalStats {
  table_count: number
  total_rows: number
  sampled_tables: number
  latest_date?: string | null
  tables: Array<{
    table_name: string
    row_count: number
  }>
}

interface DatabaseStats {
  total_tables: number
  total_rows: number
  kline_tables: number
  kline_rows: number
  by_interval: { [interval: string]: IntervalStats }
  other_tables: {
    count: number
    total_rows: number
    tables: Array<{
      table_name: string
      row_count: number
    }>
  }
  database_name: string
  host: string
}

export default function DatabaseStats() {
  const [stats, setStats] = useState<DatabaseStats | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expandedIntervals, setExpandedIntervals] = useState<Set<string>>(new Set())

  const fetchStats = async () => {
    setLoading(true)
    setError(null)

    try {
      const response = await safeFetch(`${API_BASE_URL}/api/database-stats`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '获取统计信息失败')
      }

      const data = await response.json()
      setStats(data)
    } catch (err: any) {
      console.error('获取数据库统计信息失败:', err)
      setError(getErrorMessage(err, API_BASE_URL))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchStats()
  }, [])

  const toggleInterval = (interval: string) => {
    const newExpanded = new Set(expandedIntervals)
    if (newExpanded.has(interval)) {
      newExpanded.delete(interval)
    } else {
      newExpanded.add(interval)
    }
    setExpandedIntervals(newExpanded)
  }

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return `${(num / 1000000).toFixed(2)}M`
    } else if (num >= 1000) {
      return `${(num / 1000).toFixed(2)}K`
    }
    return num.toLocaleString()
  }

  const INTERVAL_LABELS: { [key: string]: string } = {
    '1m': '1分钟',
    '3m': '3分钟',
    '5m': '5分钟',
    '15m': '15分钟',
    '30m': '30分钟',
    '1h': '1小时',
    '2h': '2小时',
    '4h': '4小时',
    '6h': '6小时',
    '8h': '8小时',
    '12h': '12小时',
    '1d': '1天',
    '3d': '3天',
    '1w': '1周',
    '1M': '1月',
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-gray-400">正在加载数据库统计信息...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-4 bg-red-900/50 border border-red-700 rounded-lg">
        <div className="text-red-300 font-semibold mb-2">获取统计信息失败</div>
        <div className="text-red-400 text-sm whitespace-pre-line">{error}</div>
        <button
          onClick={fetchStats}
          className="mt-4 px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700"
        >
          重试
        </button>
      </div>
    )
  }

  if (!stats) {
    return null
  }

  // 按表数量排序 interval
  const sortedIntervals = Object.entries(stats.by_interval).sort(
    (a, b) => b[1].table_count - a[1].table_count
  )

  return (
    <div className="space-y-6">
      {/* 总体统计 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-blue-900/30 border border-blue-700 rounded-lg p-4">
          <div className="text-blue-300 text-sm mb-1">总表数</div>
          <div className="text-2xl font-bold text-white">{stats.total_tables.toLocaleString()}</div>
        </div>
        <div className="bg-green-900/30 border border-green-700 rounded-lg p-4">
          <div className="text-green-300 text-sm mb-1">总行数</div>
          <div className="text-2xl font-bold text-white">{formatNumber(stats.total_rows)}</div>
        </div>
        <div className="bg-purple-900/30 border border-purple-700 rounded-lg p-4">
          <div className="text-purple-300 text-sm mb-1">K线表数</div>
          <div className="text-2xl font-bold text-white">{stats.kline_tables.toLocaleString()}</div>
        </div>
        <div className="bg-orange-900/30 border border-orange-700 rounded-lg p-4">
          <div className="text-orange-300 text-sm mb-1">K线数据行数</div>
          <div className="text-2xl font-bold text-white">{formatNumber(stats.kline_rows)}</div>
        </div>
      </div>

      {/* 数据库信息 */}
      <div className="bg-gray-700/50 rounded-lg p-4">
        <div className="text-gray-300 text-sm">
          <div>数据库: <span className="text-white font-semibold">{stats.database_name}</span></div>
          <div>主机: <span className="text-white font-semibold">{stats.host}</span></div>
        </div>
      </div>

      {/* 按 Interval 统计 */}
      <div className="space-y-3">
        <h2 className="text-xl font-semibold text-white">按时间间隔统计</h2>
        {sortedIntervals.map(([interval, intervalStats]) => (
          <div
            key={interval}
            className="bg-gray-700/50 border border-gray-600 rounded-lg overflow-hidden"
          >
            <button
              onClick={() => toggleInterval(interval)}
              className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-700/70 transition-colors"
            >
              <div className="flex items-center space-x-4">
                <span className="font-semibold text-white">
                  {INTERVAL_LABELS[interval] || interval}
                </span>
                <span className="text-gray-400 text-sm">({interval})</span>
              </div>
              <div className="flex items-center space-x-6 text-sm">
                <span className="text-gray-300">
                  表数: <span className="text-white font-semibold">{intervalStats.table_count}</span>
                </span>
                <span className="text-gray-300">
                  行数: <span className="text-white font-semibold">{formatNumber(intervalStats.total_rows)}</span>
                </span>
                {intervalStats.latest_date && (
                  <span className="text-gray-300">
                    最新数据: <span className="text-white font-semibold">{intervalStats.latest_date}</span>
                  </span>
                )}
                <svg
                  className={`w-5 h-5 text-gray-400 transition-transform ${
                    expandedIntervals.has(interval) ? 'transform rotate-180' : ''
                  }`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </div>
            </button>
            
            {expandedIntervals.has(interval) && (
              <div className="px-4 py-3 bg-gray-800/50 border-t border-gray-600">
                <div className="text-sm text-gray-400 mb-2">
                  {intervalStats.sampled_tables < intervalStats.table_count && (
                    <span className="text-yellow-400">
                      ⚠️ 显示前 {intervalStats.sampled_tables} 个表的详细信息
                      （共 {intervalStats.table_count} 个表）
                    </span>
                  )}
                </div>
                <div className="max-h-64 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-600">
                        <th className="text-left py-2 text-gray-300">表名</th>
                        <th className="text-right py-2 text-gray-300">行数</th>
                      </tr>
                    </thead>
                    <tbody>
                      {intervalStats.tables.map((table) => (
                        <tr key={table.table_name} className="border-b border-gray-700/50">
                          <td className="py-1 text-gray-300 font-mono text-xs">{table.table_name}</td>
                          <td className="py-1 text-right text-gray-400">
                            {table.row_count.toLocaleString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* 其他表 */}
      {stats.other_tables.count > 0 && (
        <div className="space-y-3">
          <h2 className="text-xl font-semibold text-white">其他表</h2>
          <div className="bg-gray-700/50 border border-gray-600 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <span className="text-gray-300">
                表数: <span className="text-white font-semibold">{stats.other_tables.count}</span>
              </span>
              <span className="text-gray-300">
                总行数: <span className="text-white font-semibold">{formatNumber(stats.other_tables.total_rows)}</span>
              </span>
            </div>
            {stats.other_tables.tables.length > 0 && (
              <div className="max-h-48 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-600">
                      <th className="text-left py-2 text-gray-300">表名</th>
                      <th className="text-right py-2 text-gray-300">行数</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stats.other_tables.tables.map((table) => (
                      <tr key={table.table_name} className="border-b border-gray-700/50">
                        <td className="py-1 text-gray-300 font-mono text-xs">{table.table_name}</td>
                        <td className="py-1 text-right text-gray-400">
                          {table.row_count.toLocaleString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {/* 刷新按钮 */}
      <div className="flex justify-end">
        <button
          onClick={fetchStats}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
        >
          刷新统计
        </button>
      </div>
    </div>
  )
}
