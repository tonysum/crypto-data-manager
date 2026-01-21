'use client'

import { useState } from 'react'
import { API_URLS } from '../lib/api-config'

const API_BASE_URL = API_URLS.data

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

interface KlineData {
  trade_date: string
  open_time?: number
  open: number
  high: number
  low: number
  close: number
  volume: number
  close_time?: number
  quote_volume?: number
  trade_count?: number
  active_buy_volume?: number
  active_buy_quote_volume?: number
  reserved_field?: string
  diff?: number
  pct_chg?: number
}

export default function EditDataForm() {
  const [formData, setFormData] = useState({
    interval: '1d',
    symbol: '',
    tradeDate: '',
  })
  const [currentData, setCurrentData] = useState<KlineData | null>(null)
  const [editData, setEditData] = useState<Partial<KlineData>>({})
  const [loading, setLoading] = useState(false)
  const [updating, setUpdating] = useState(false)
  const [showUpdateConfirm, setShowUpdateConfirm] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  const handleQuery = async () => {
    if (!formData.symbol || !formData.tradeDate) {
      setMessage({
        type: 'error',
        text: '请输入交易对符号和交易日期'
      })
      return
    }

    setLoading(true)
    setMessage(null)
    setCurrentData(null)
    setEditData({})

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/kline/${formData.interval}/${formData.symbol}?start_date=${formData.tradeDate}&end_date=${formData.tradeDate}&limit=1`,
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      )

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '查询失败')
      }

      const data = await response.json()
      
      if (data.data && data.data.length > 0) {
        const record = data.data[0]
        setCurrentData(record as KlineData)
        setEditData({}) // 重置编辑数据
        setMessage({
          type: 'success',
          text: '查询成功'
        })
      } else {
        setMessage({
          type: 'error',
          text: `未找到 ${formData.symbol} 在 ${formData.tradeDate} 的数据`
        })
      }
    } catch (err: any) {
      setMessage({
        type: 'error',
        text: err.message || '查询失败'
      })
    } finally {
      setLoading(false)
    }
  }

  const handleUpdateClick = () => {
    if (!formData.symbol || !formData.tradeDate) {
      setMessage({
        type: 'error',
        text: '请先查询数据'
      })
      return
    }

    if (!currentData) {
      setMessage({
        type: 'error',
        text: '请先查询数据'
      })
      return
    }

    // 检查是否有任何字段被修改
    const hasChanges = Object.keys(editData).length > 0 && 
      Object.keys(editData).some(key => {
        const editValue = editData[key as keyof KlineData]
        const currentValue = currentData[key as keyof KlineData]
        return editValue !== currentValue && editValue !== undefined && editValue !== null
      })

    if (!hasChanges) {
      setMessage({
        type: 'error',
        text: '请至少修改一个字段'
      })
      return
    }

    setShowUpdateConfirm(true)
  }

  const handleUpdate = async () => {
    setUpdating(true)
    setMessage(null)

    try {
      const payload: any = {
        symbol: formData.symbol,
        interval: formData.interval,
        trade_date: formData.tradeDate,
      }

      // 只添加有值的字段
      if (editData.open !== undefined && editData.open !== null) payload.open = editData.open
      if (editData.high !== undefined && editData.high !== null) payload.high = editData.high
      if (editData.low !== undefined && editData.low !== null) payload.low = editData.low
      if (editData.close !== undefined && editData.close !== null) payload.close = editData.close
      if (editData.volume !== undefined && editData.volume !== null) payload.volume = editData.volume
      if (editData.quote_volume !== undefined && editData.quote_volume !== null) payload.quote_volume = editData.quote_volume
      if (editData.trade_count !== undefined && editData.trade_count !== null) payload.trade_count = editData.trade_count
      if (editData.active_buy_volume !== undefined && editData.active_buy_volume !== null) payload.active_buy_volume = editData.active_buy_volume
      if (editData.active_buy_quote_volume !== undefined && editData.active_buy_quote_volume !== null) payload.active_buy_quote_volume = editData.active_buy_quote_volume

      const response = await fetch(`${API_BASE_URL}/api/kline-data`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '更新失败')
      }

      const result = await response.json()
      
      // 更新当前数据
      if (result.data) {
        setCurrentData(result.data as KlineData)
        setEditData({}) // 清空编辑数据
      }
      
      setMessage({
        type: 'success',
        text: result.message || '更新成功'
      })
      setShowUpdateConfirm(false)
    } catch (err: any) {
      setMessage({
        type: 'error',
        text: err.message || '更新失败'
      })
    } finally {
      setUpdating(false)
    }
  }

  const getUpdateConfirmMessage = () => {
    if (!currentData) return ''
    
    const modifiedFields: string[] = []
    const changes: string[] = []
    
    Object.keys(editData).forEach(key => {
      const field = key as keyof KlineData
      const editValue = editData[field]
      const currentValue = currentData[field]
      
      if (editValue !== undefined && editValue !== null && editValue !== currentValue) {
        modifiedFields.push(field)
        changes.push(`${field}: ${currentValue} → ${editValue}`)
      }
    })
    
    return `确定要更新 ${formData.symbol} (${formData.interval}) 在 ${formData.tradeDate} 的数据吗？\n\n修改的字段：\n${changes.join('\n')}`
  }

  const handleFieldChange = (field: keyof KlineData, value: string) => {
    const numValue = value === '' ? undefined : parseFloat(value)
    setEditData(prev => ({
      ...prev,
      [field]: numValue
    }))
  }

  const getFieldValue = (field: keyof KlineData): string => {
    if (editData[field] !== undefined && editData[field] !== null) {
      return editData[field]?.toString() || ''
    }
    if (currentData && currentData[field] !== undefined && currentData[field] !== null) {
      return currentData[field]?.toString() || ''
    }
    return ''
  }

  const isFieldModified = (field: keyof KlineData): boolean => {
    if (!currentData) return false
    const editValue = editData[field]
    const currentValue = currentData[field]
    return editValue !== undefined && editValue !== null && editValue !== currentValue
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-6 text-yellow-400">修改K线数据</h2>

      {message && !showUpdateConfirm && (
        <div
          className={`mb-4 p-4 rounded-lg ${
            message.type === 'success'
              ? 'bg-green-500/20 text-green-400 border border-green-500/50'
              : 'bg-red-500/20 text-red-400 border border-red-500/50'
          }`}
        >
          {message.text}
        </div>
      )}

      {/* 更新确认对话框 */}
      {showUpdateConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg p-6 max-w-md w-full mx-4 border border-yellow-500/50">
            <h3 className="text-xl font-bold text-yellow-400 mb-4">⚠️ 确认更新</h3>
            <div className="text-gray-300 mb-6 whitespace-pre-line">
              {getUpdateConfirmMessage()}
            </div>

            {message && (
              <div
                className={`mb-4 p-3 rounded-lg ${
                  message.type === 'success'
                    ? 'bg-green-500/20 text-green-400 border border-green-500/50'
                    : 'bg-red-500/20 text-red-400 border border-red-500/50'
                }`}
              >
                {message.text}
              </div>
            )}

            <div className="flex space-x-4">
              <button
                onClick={handleUpdate}
                disabled={updating}
                className={`flex-1 py-2 px-4 rounded-lg font-medium transition-colors ${
                  updating
                    ? 'bg-gray-600 cursor-not-allowed'
                    : 'bg-yellow-600 hover:bg-yellow-700 text-white'
                }`}
              >
                {updating ? '更新中...' : '确认更新'}
              </button>
              <button
                onClick={() => {
                  setShowUpdateConfirm(false)
                  setMessage(null)
                }}
                disabled={updating}
                className="flex-1 py-2 px-4 rounded-lg font-medium bg-gray-700 hover:bg-gray-600 text-white transition-colors"
              >
                取消
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 查询表单 */}
      <div className="bg-gray-800/50 p-6 rounded-lg border border-yellow-500/30 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
          {/* K线间隔 */}
          <div>
            <label className="block text-sm font-medium mb-2">K线间隔 *</label>
            <select
              value={formData.interval}
              onChange={(e) => setFormData({ ...formData, interval: e.target.value })}
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
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
            <label className="block text-sm font-medium mb-2">交易对符号 *</label>
            <input
              type="text"
              value={formData.symbol}
              onChange={(e) => setFormData({ ...formData, symbol: e.target.value.toUpperCase() })}
              placeholder="例如: BTCUSDT"
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              required
            />
          </div>

          {/* 交易日期 */}
          <div>
            <label className="block text-sm font-medium mb-2">交易日期 *</label>
            <input
              type="text"
              value={formData.tradeDate}
              onChange={(e) => setFormData({ ...formData, tradeDate: e.target.value })}
              placeholder="例如: 2025-01-01 或 2025-01-01 00:00:00"
              className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              required
            />
            <p className="text-xs text-gray-400 mt-1">日线格式: YYYY-MM-DD，小时线格式: YYYY-MM-DD HH:MM:SS</p>
          </div>
        </div>

        {/* 查询按钮 */}
        <button
          type="button"
          onClick={handleQuery}
          disabled={loading || !formData.symbol || !formData.tradeDate}
          className={`w-full py-3 px-6 rounded-lg font-medium transition-colors ${
            loading || !formData.symbol || !formData.tradeDate
              ? 'bg-gray-600 cursor-not-allowed'
              : 'bg-gradient-to-r from-yellow-600 to-yellow-700 hover:from-yellow-700 hover:to-yellow-800'
          }`}
        >
          {loading ? '查询中...' : '查询数据'}
        </button>
      </div>

      {/* 数据编辑表单 */}
      {currentData && (
        <div className="bg-gray-800/50 p-6 rounded-lg border border-yellow-500/30">
          <h3 className="text-xl font-semibold mb-4 text-yellow-400">编辑数据</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            {/* 开盘价 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                开盘价 (open) {isFieldModified('open') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('open')}
                onChange={(e) => handleFieldChange('open', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 最高价 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                最高价 (high) {isFieldModified('high') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('high')}
                onChange={(e) => handleFieldChange('high', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 最低价 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                最低价 (low) {isFieldModified('low') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('low')}
                onChange={(e) => handleFieldChange('low', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 收盘价 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                收盘价 (close) {isFieldModified('close') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('close')}
                onChange={(e) => handleFieldChange('close', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 成交量 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                成交量 (volume) {isFieldModified('volume') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('volume')}
                onChange={(e) => handleFieldChange('volume', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 成交额 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                成交额 (quote_volume) {isFieldModified('quote_volume') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('quote_volume')}
                onChange={(e) => handleFieldChange('quote_volume', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 成交笔数 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                成交笔数 (trade_count) {isFieldModified('trade_count') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="1"
                value={getFieldValue('trade_count')}
                onChange={(e) => handleFieldChange('trade_count', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 主动买入成交量 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                主动买入成交量 (active_buy_volume) {isFieldModified('active_buy_volume') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('active_buy_volume')}
                onChange={(e) => handleFieldChange('active_buy_volume', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>

            {/* 主动买入成交额 */}
            <div>
              <label className="block text-sm font-medium mb-2">
                主动买入成交额 (active_buy_quote_volume) {isFieldModified('active_buy_quote_volume') && <span className="text-yellow-400">* 已修改</span>}
              </label>
              <input
                type="number"
                step="any"
                value={getFieldValue('active_buy_quote_volume')}
                onChange={(e) => handleFieldChange('active_buy_quote_volume', e.target.value)}
                className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-yellow-500"
              />
            </div>
          </div>

          {/* 只读字段显示 */}
          <div className="mb-6 p-4 bg-gray-700/50 rounded-lg">
            <h4 className="text-sm font-medium mb-2 text-gray-400">其他字段（自动计算）</h4>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="text-gray-400">日期:</span>
                <span className="ml-2 text-white">{currentData.trade_date}</span>
              </div>
              {currentData.diff !== undefined && (
                <div>
                  <span className="text-gray-400">涨跌额 (diff):</span>
                  <span className="ml-2 text-white">{currentData.diff?.toLocaleString()}</span>
                </div>
              )}
              {currentData.pct_chg !== undefined && (
                <div>
                  <span className="text-gray-400">涨跌幅 (pct_chg):</span>
                  <span className="ml-2 text-white">{(currentData.pct_chg! * 100).toFixed(2)}%</span>
                </div>
              )}
            </div>
          </div>

          {/* 更新按钮 */}
          <button
            type="button"
            onClick={handleUpdateClick}
            disabled={updating || !currentData}
            className={`w-full py-3 px-6 rounded-lg font-medium transition-colors ${
              updating || !currentData
                ? 'bg-gray-600 cursor-not-allowed'
                : 'bg-gradient-to-r from-yellow-600 to-yellow-700 hover:from-yellow-700 hover:to-yellow-800'
            }`}
          >
            更新数据
          </button>
        </div>
      )}
    </div>
  )
}
