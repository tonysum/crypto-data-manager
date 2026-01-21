'use client'

import { useEffect, useRef, useState, useCallback } from 'react'
import { createChart, ColorType, IChartApi, ISeriesApi } from 'lightweight-charts'

import { API_URLS } from '../lib/api-config'
const API_BASE_URL = API_URLS.data

interface KlineData {
  trade_date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  pct_chg?: number
}

export default function KlineChart() {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)

  const [formData, setFormData] = useState({
    interval: '1d',
    symbol: 'BTCUSDT',
    startDate: '',
    endDate: '',
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [dataCount, setDataCount] = useState(0)
  const [autoLoaded, setAutoLoaded] = useState(false)

  // 初始化图表
  useEffect(() => {
    if (!chartContainerRef.current) return

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#1e1e1e' },
        textColor: '#d1d5db',
      },
      grid: {
        vertLines: { color: '#2a2a2a' },
        horzLines: { color: '#2a2a2a' },
      },
      width: chartContainerRef.current.clientWidth,
      height: 500,
      timeScale: {
        timeVisible: true,
        secondsVisible: true,
        borderColor: '#485563',
      },
      rightPriceScale: {
        borderColor: '#485563',
      },
      localization: {
        timeFormatter: (businessDayOrTimestamp: number) => {
          const date = new Date(businessDayOrTimestamp * 1000)
          const year = date.getFullYear()
          const month = String(date.getMonth() + 1).padStart(2, '0')
          const day = String(date.getDate()).padStart(2, '0')
          const hours = String(date.getHours()).padStart(2, '0')
          const minutes = String(date.getMinutes()).padStart(2, '0')
          const seconds = String(date.getSeconds()).padStart(2, '0')
          return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
        },
      },
    })

    chartRef.current = chart

    // 创建K线系列
    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    })
    candlestickSeriesRef.current = candlestickSeries

    // 创建成交量系列
    const volumeSeries = chart.addHistogramSeries({
      color: '#26a69a',
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '',
    })
    volumeSeriesRef.current = volumeSeries
    
    // 设置成交量价格轴的边距
    chart.priceScale('').applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    })

    // 响应式调整
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
    }
  }, [])


  // 解析时间字符串为Unix时间戳（秒）
  const parseTime = (timeStr: string): number => {
    try {
      // 如果是日期格式 YYYY-MM-DD
      if (timeStr.length === 10) {
        const date = new Date(timeStr + 'T00:00:00Z')
        return Math.floor(date.getTime() / 1000)
      }
      // 如果是完整时间格式 YYYY-MM-DD HH:MM:SS
      const date = new Date(timeStr)
      return Math.floor(date.getTime() / 1000)
    } catch (e) {
      console.error('时间解析错误:', timeStr, e)
      return 0
    }
  }

  // 转换数据格式
  const convertToChartData = (data: KlineData[]) => {
    const candlestickData = data
      .map((item) => {
        const time = parseTime(item.trade_date)
        if (time === 0) return null
        
        return {
          time: time as any,
          open: parseFloat(item.open.toString()),
          high: parseFloat(item.high.toString()),
          low: parseFloat(item.low.toString()),
          close: parseFloat(item.close.toString()),
        }
      })
      .filter((item): item is NonNullable<typeof item> => item !== null)

    const volumeData = data
      .map((item) => {
        const time = parseTime(item.trade_date)
        if (time === 0) return null
        
        return {
          time: time as any,
          value: parseFloat(item.volume.toString()),
          color: item.pct_chg && item.pct_chg >= 0 ? '#26a69a' : '#ef5350',
        }
      })
      .filter((item): item is NonNullable<typeof item> => item !== null)

    return { candlestickData, volumeData }
  }

  // 获取K线数据（内部函数，支持传入日期参数）
  const fetchKlineDataInternal = useCallback(async (startDate?: string, endDate?: string) => {
    const start = startDate || formData.startDate
    const end = endDate || formData.endDate
    
    if (!formData.symbol) {
      setError('请输入交易对符号')
      return
    }

    setLoading(true)
    setError(null)

    try {
      let url = `${API_BASE_URL}/api/kline/${formData.interval}/${formData.symbol}`
      const params = new URLSearchParams()
      if (start) params.append('start_date', start)
      if (end) params.append('end_date', end)
      if (params.toString()) url += '?' + params.toString()

      const response = await fetch(url)

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

      const result = await response.json()
      const klineData: KlineData[] = result.data || []

      if (klineData.length === 0) {
        setError('没有找到数据')
        setDataCount(0)
        return
      }

      setDataCount(klineData.length)

      // 转换数据格式（时间已在convertToChartData中转换）
      const { candlestickData, volumeData } = convertToChartData(klineData)

      // 更新图表数据
      if (candlestickSeriesRef.current && candlestickData.length > 0) {
        candlestickSeriesRef.current.setData(candlestickData as any)
      }
      if (volumeSeriesRef.current && volumeData.length > 0) {
        volumeSeriesRef.current.setData(volumeData as any)
      }

      // 调整图表以适应数据
      if (chartRef.current && candlestickData.length > 0) {
        chartRef.current.timeScale().fitContent()
      }
    } catch (err: any) {
      console.error('获取K线数据错误:', err)
      console.error('API地址:', `${API_BASE_URL}/api/kline/${formData.interval}/${formData.symbol}`)
      
      let errorMessage = '请求失败'

      if (err.message) {
        errorMessage = err.message
      } else if (err.name === 'TypeError' && err.message && err.message.includes('fetch')) {
        errorMessage = `无法连接到后端服务器 (${API_BASE_URL})。请确保后端服务已启动。错误详情: ${err.message}`
      } else if (err instanceof TypeError && err.message === 'Failed to fetch') {
        errorMessage = `网络请求失败。请检查：\n1. 后端服务是否运行在 ${API_BASE_URL}\n2. 浏览器控制台是否有CORS错误\n3. 网络连接是否正常`
      } else {
        errorMessage = `请求失败: ${err.toString()}`
      }

      setError(errorMessage)
    } finally {
      setLoading(false)
    }
  }, [formData.interval, formData.symbol, formData.startDate, formData.endDate])

  // 获取K线数据（公开方法，供按钮调用）
  const fetchKlineData = async () => {
    await fetchKlineDataInternal()
  }

  // 自动加载数据：获取日期范围并加载图表
  useEffect(() => {
    const autoLoadData = async () => {
      if (autoLoaded) return // 避免重复加载
      
      try {
        // 1. 获取该交易对的数据日期列表
        const datesResponse = await fetch(`${API_BASE_URL}/api/dates/${formData.interval}/${formData.symbol}`)
        
        if (!datesResponse.ok) {
          console.warn('无法获取日期列表，跳过自动加载')
          return
        }
        
        const datesData = await datesResponse.json()
        const dates = datesData.dates || []
        
        if (dates.length === 0) {
          console.warn('没有找到数据，跳过自动加载')
          return
        }
        
        // 2. 自动填充开始和结束日期
        const sortedDates = dates.sort()
        const startDate = sortedDates[0]
        const endDate = sortedDates[sortedDates.length - 1]
        
        setFormData(prev => ({
          ...prev,
          startDate: startDate.substring(0, 10), // 只取日期部分 YYYY-MM-DD
          endDate: endDate.substring(0, 10),
        }))
        
        // 3. 自动加载图表数据
        setAutoLoaded(true)
        
        // 延迟一下再加载，确保状态已更新，并等待日期状态更新
        setTimeout(async () => {
          try {
            await fetchKlineDataInternal(startDate.substring(0, 10), endDate.substring(0, 10))
          } catch (err) {
            console.error('自动加载图表数据失败:', err)
            setError(`自动加载失败: ${err instanceof Error ? err.message : String(err)}`)
          }
        }, 300)
      } catch (err) {
        console.error('自动加载失败:', err)
        // 静默失败，不影响用户手动操作
      }
    }
    
    // 只在组件首次挂载时执行一次
    if (!autoLoaded && formData.symbol) {
      autoLoadData()
    }
  }, [formData.symbol, formData.interval, autoLoaded, fetchKlineDataInternal])

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
      <h2 className="text-2xl font-semibold mb-6">K线图表</h2>

      {/* 查询表单 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div>
          <label className="block text-sm font-medium mb-2">K线间隔</label>
          <select
            value={formData.interval}
            onChange={(e) => setFormData({ ...formData, interval: e.target.value })}
            className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {INTERVALS.map((int) => (
              <option key={int.value} value={int.value}>
                {int.label}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-2">交易对符号 *</label>
          <input
            type="text"
            value={formData.symbol}
            onChange={(e) => setFormData({ ...formData, symbol: e.target.value.toUpperCase() })}
            placeholder="例如: BTCUSDT"
            className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-2">开始日期</label>
          <input
            type="date"
            value={formData.startDate}
            onChange={(e) => setFormData({ ...formData, startDate: e.target.value })}
            className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-2">结束日期</label>
          <input
            type="date"
            value={formData.endDate}
            onChange={(e) => setFormData({ ...formData, endDate: e.target.value })}
            className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      <div className="flex items-center space-x-4 mb-6">
        <button
          onClick={fetchKlineData}
          disabled={loading || !formData.symbol}
          className="px-6 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50"
        >
          {loading ? '加载中...' : '加载图表'}
        </button>
        {dataCount > 0 && (
          <span className="text-sm text-gray-400">已加载 {dataCount} 条K线数据</span>
        )}
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-500/20 text-red-400 border border-red-500/50 rounded-lg">
          {error}
        </div>
      )}

      {/* 图表容器 */}
      <div className="bg-gray-800 rounded-lg p-4">
        <div ref={chartContainerRef} className="w-full" style={{ minHeight: '500px' }} />
      </div>

      {/* 图表说明 */}
      <div className="mt-4 text-sm text-gray-400">
        <p>💡 提示：</p>
        <ul className="list-disc list-inside ml-4 space-y-1">
          <li>绿色K线表示上涨，红色K线表示下跌</li>
          <li>可以使用鼠标滚轮缩放图表</li>
          <li>可以拖拽图表查看不同时间段的数据</li>
          <li>图表会自动适应窗口大小</li>
        </ul>
      </div>
    </div>
  )
}

