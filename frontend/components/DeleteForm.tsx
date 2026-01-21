'use client'

import { useState } from 'react'

const INTERVALS = [
  { value: '1m', label: '1分钟' },
  { value: '3m', label: '3分钟' },
  { value: '5m', label: '5分钟' },
  { value: '15m', label: '15分钟' },
  { value: '30m', label: '30分钟' },
  { value: '1h', label: '1小时' },
  { value: '2h', label: '2小时' },
  { value: '4h', label: '4小时' },
  { value: '6h', label: '6小时' },
  { value: '8h', label: '8小时' },
  { value: '12h', label: '12小时' },
  { value: '1d', label: '1天' },
  { value: '3d', label: '3天' },
  { value: '1w', label: '1周' },
  { value: '1M', label: '1月' },
]

import { API_URLS } from '../lib/api-config'
const API_BASE_URL = API_URLS.data

export default function DeleteForm() {
  const [deleteFormData, setDeleteFormData] = useState({
    interval: '1d',
    symbol: '',
    startTime: '',
    endTime: '',
  })
  const [deleteLoading, setDeleteLoading] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [deleteMessage, setDeleteMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  const handleDelete = async () => {
    if (!deleteFormData.symbol) {
      setDeleteMessage({
        type: 'error',
        text: '请输入交易对符号'
      })
      return
    }

    setDeleteLoading(true)
    setDeleteMessage(null)

    try {
      const payload: any = {
        symbol: deleteFormData.symbol.toUpperCase(),
        interval: deleteFormData.interval,
      }

      // 如果提供了时间范围，添加到payload
      if (deleteFormData.startTime) {
        payload.start_time = deleteFormData.startTime
      }
      if (deleteFormData.endTime) {
        payload.end_time = deleteFormData.endTime
      }

      const response = await fetch(`${API_BASE_URL}/api/kline-data`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        let errorDetail = '删除失败'
        try {
          const errorData = await response.json()
          errorDetail = errorData.detail || errorData.message || `HTTP ${response.status}`
        } catch {
          errorDetail = `HTTP ${response.status}: ${response.statusText}`
        }
        throw new Error(errorDetail)
      }

      const data = await response.json()
      setDeleteMessage({
        type: 'success',
        text: data.message || `成功删除 ${data.deleted_count === -1 ? '整个表' : `${data.deleted_count} 条记录`}`,
      })
      setShowDeleteConfirm(false)
      
      // 清空删除表单
      setDeleteFormData({
        interval: '1d',
        symbol: '',
        startTime: '',
        endTime: '',
      })
    } catch (error: any) {
      console.error('删除错误:', error)
      let errorMessage = '请求失败'
      
      if (error.message) {
        errorMessage = error.message
      } else if (error.name === 'TypeError' && error.message.includes('fetch')) {
        errorMessage = `无法连接到后端服务器 (${API_BASE_URL})。请确保后端服务已启动。`
      } else {
        errorMessage = `请求失败: ${error.toString()}`
      }
      
      setDeleteMessage({
        type: 'error',
        text: errorMessage,
      })
    } finally {
      setDeleteLoading(false)
    }
  }

  const handleDeleteClick = () => {
    if (!deleteFormData.symbol) {
      setDeleteMessage({
        type: 'error',
        text: '请输入交易对符号'
      })
      return
    }
    setShowDeleteConfirm(true)
  }

  const getDeleteConfirmMessage = () => {
    const { symbol, interval, startTime, endTime } = deleteFormData
    if (!startTime && !endTime) {
      return `确定要删除交易对 ${symbol} 在 ${interval} 间隔下的所有数据吗？此操作不可恢复！`
    }
    const timeRange = startTime && endTime 
      ? `${startTime} 至 ${endTime}`
      : startTime 
      ? `从 ${startTime} 开始的所有数据`
      : `到 ${endTime} 为止的所有数据`
    return `确定要删除交易对 ${symbol} 在 ${interval} 间隔下 ${timeRange} 的数据吗？此操作不可恢复！`
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-6 text-red-400">删除K线数据</h2>
      
      {deleteMessage && !showDeleteConfirm && (
        <div
          className={`mb-4 p-4 rounded-lg ${
            deleteMessage.type === 'success'
              ? 'bg-green-500/20 text-green-400 border border-green-500/50'
              : 'bg-red-500/20 text-red-400 border border-red-500/50'
          }`}
        >
          {deleteMessage.text}
        </div>
      )}

      {/* 删除确认对话框 */}
      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg p-6 max-w-md w-full mx-4 border border-red-500/50">
            <h3 className="text-xl font-bold text-red-400 mb-4">⚠️ 确认删除</h3>
            <p className="text-gray-300 mb-6">{getDeleteConfirmMessage()}</p>
            
            {deleteMessage && (
              <div
                className={`mb-4 p-3 rounded-lg ${
                  deleteMessage.type === 'success'
                    ? 'bg-green-500/20 text-green-400 border border-green-500/50'
                    : 'bg-red-500/20 text-red-400 border border-red-500/50'
                }`}
              >
                {deleteMessage.text}
              </div>
            )}
            
            <div className="flex space-x-4">
              <button
                onClick={handleDelete}
                disabled={deleteLoading}
                className={`flex-1 py-2 px-4 rounded-lg font-medium transition-colors ${
                  deleteLoading
                    ? 'bg-gray-600 cursor-not-allowed'
                    : 'bg-red-600 hover:bg-red-700 text-white'
                }`}
              >
                {deleteLoading ? '删除中...' : '确认删除'}
              </button>
              <button
                onClick={() => {
                  setShowDeleteConfirm(false)
                  setDeleteMessage(null)
                }}
                disabled={deleteLoading}
                className="flex-1 py-2 px-4 rounded-lg font-medium bg-gray-700 hover:bg-gray-600 text-white transition-colors"
              >
                取消
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="bg-gray-800/50 p-6 rounded-lg border border-red-500/30">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
          {/* K线间隔 */}
          <div>
            <label className="block text-sm font-medium mb-2">K线间隔 *</label>
            <select
              value={deleteFormData.interval}
              onChange={(e) => setDeleteFormData({ ...deleteFormData, interval: e.target.value })}
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500"
              required
            >
              {INTERVALS.map((interval) => (
                <option key={interval.value} value={interval.value}>
                  {interval.label}
                </option>
              ))}
            </select>
          </div>

          {/* 交易对符号 */}
          <div>
            <label className="block text-sm font-medium mb-2">
              交易对符号 *
            </label>
            <input
              type="text"
              value={deleteFormData.symbol}
              onChange={(e) => setDeleteFormData({ ...deleteFormData, symbol: e.target.value.toUpperCase() })}
              placeholder="例如: BTCUSDT"
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500"
              required
            />
          </div>

          {/* 开始时间 */}
          <div>
            <label className="block text-sm font-medium mb-2">
              开始时间（留空则删除全部）
            </label>
            <input
              type="text"
              value={deleteFormData.startTime}
              onChange={(e) => setDeleteFormData({ ...deleteFormData, startTime: e.target.value })}
              placeholder="例如: 2025-01-01 或 2025-01-01 00:00:00"
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500"
            />
            <p className="text-xs text-gray-400 mt-1">留空则删除该交易对的所有数据</p>
          </div>

          {/* 结束时间 */}
          <div>
            <label className="block text-sm font-medium mb-2">
              结束时间（留空则删除全部）
            </label>
            <input
              type="text"
              value={deleteFormData.endTime}
              onChange={(e) => setDeleteFormData({ ...deleteFormData, endTime: e.target.value })}
              placeholder="例如: 2025-12-31 或 2025-12-31 23:59:59"
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500"
            />
            <p className="text-xs text-gray-400 mt-1">留空则删除该交易对的所有数据</p>
          </div>
        </div>

        {/* 删除按钮 */}
        <button
          type="button"
          onClick={handleDeleteClick}
          disabled={deleteLoading || !deleteFormData.symbol}
          className={`w-full py-3 px-6 rounded-lg font-medium transition-colors ${
            deleteLoading || !deleteFormData.symbol
              ? 'bg-gray-600 cursor-not-allowed'
              : 'bg-gradient-to-r from-red-600 to-red-700 hover:from-red-700 hover:to-red-800'
          }`}
        >
          {deleteLoading ? '删除中...' : '删除数据'}
        </button>
      </div>
    </div>
  )
}
