'use client'

import { useState, useEffect } from 'react'
import { API_URLS } from '../lib/api-config'

const API_BASE_URL = API_URLS.data

interface OtherDataTask {
  task_id: string
  status: 'running' | 'completed' | 'failed'
  message: string
  progress?: {
    download_trader: boolean
    download_funding: boolean
    download_basis: boolean
    download_premium: boolean
    run_once: boolean
    symbols_processed?: number
    total_symbols?: number
    trader_success?: number
    funding_success?: number
    basis_success?: number
    premium_success?: number
  }
  start_time?: string
  end_time?: string
}

export default function OtherDataDownload() {
  const [formData, setFormData] = useState({
    downloadTrader: true,
    downloadFunding: true,
    downloadBasis: true,
    downloadPremium: true,
    runOnce: true,
  })
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)
  const [currentTask, setCurrentTask] = useState<OtherDataTask | null>(null)
  const [polling, setPolling] = useState(false)

  // 轮询任务状态
  useEffect(() => {
    if (!polling || !currentTask?.task_id) return

    const interval = setInterval(async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/api/other-data/status/${currentTask.task_id}`)
        if (response.ok) {
          const data = await response.json()
          setCurrentTask(data)
          
          if (data.status === 'completed' || data.status === 'failed') {
            setPolling(false)
            setLoading(false)
            setMessage({
              type: data.status === 'completed' ? 'success' : 'error',
              text: data.message
            })
          }
        }
      } catch (error) {
        console.error('获取任务状态失败:', error)
      }
    }, 2000) // 每2秒轮询一次

    return () => clearInterval(interval)
  }, [polling, currentTask?.task_id])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setMessage(null)
    setCurrentTask(null)

    try {
      const payload = {
        download_trader: formData.downloadTrader,
        download_funding: formData.downloadFunding,
        download_basis: formData.downloadBasis,
        download_premium: formData.downloadPremium,
        run_once: formData.runOnce,
      }

      const response = await fetch(`${API_BASE_URL}/api/other-data/start`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '启动下载失败')
      }

      const data = await response.json()
      setCurrentTask(data)
      setPolling(true)
      setMessage({
        type: 'success',
        text: '下载任务已启动'
      })
    } catch (error: any) {
      setLoading(false)
      setMessage({
        type: 'error',
        text: error.message || '启动下载失败'
      })
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold mb-2">其他数据下载</h2>
        <p className="text-gray-400 text-sm">
          下载交易者数据、资金费率、基差数据和Premium Index数据
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* 数据选择 */}
        <div className="bg-gray-700/50 rounded-lg p-4">
          <h3 className="text-lg font-semibold mb-4">选择要下载的数据类型</h3>
          <div className="space-y-3">
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.downloadTrader}
                onChange={(e) => setFormData({ ...formData, downloadTrader: e.target.checked })}
                className="w-5 h-5 rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">交易者数据</div>
                <div className="text-sm text-gray-400">顶级交易者的账户和持仓比例数据</div>
              </div>
            </label>
            
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.downloadFunding}
                onChange={(e) => setFormData({ ...formData, downloadFunding: e.target.checked })}
                className="w-5 h-5 rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">资金费率</div>
                <div className="text-sm text-gray-400">资金费率数据</div>
              </div>
            </label>
            
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.downloadBasis}
                onChange={(e) => setFormData({ ...formData, downloadBasis: e.target.checked })}
                className="w-5 h-5 rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">基差数据</div>
                <div className="text-sm text-gray-400">期货与现货价格差数据</div>
              </div>
            </label>
            
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.downloadPremium}
                onChange={(e) => setFormData({ ...formData, downloadPremium: e.target.checked })}
                className="w-5 h-5 rounded border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">Premium Index</div>
                <div className="text-sm text-gray-400">Premium Index K线数据</div>
              </div>
            </label>
          </div>
        </div>

        {/* 运行模式 */}
        <div className="bg-gray-700/50 rounded-lg p-4">
          <h3 className="text-lg font-semibold mb-4">运行模式</h3>
          <div className="space-y-3">
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="radio"
                name="runMode"
                checked={formData.runOnce}
                onChange={() => setFormData({ ...formData, runOnce: true })}
                className="w-5 h-5 border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">运行一次</div>
                <div className="text-sm text-gray-400">下载完成后自动停止</div>
              </div>
            </label>
            
            <label className="flex items-center space-x-3 cursor-pointer">
              <input
                type="radio"
                name="runMode"
                checked={!formData.runOnce}
                onChange={() => setFormData({ ...formData, runOnce: false })}
                className="w-5 h-5 border-gray-600 bg-gray-700 text-blue-600 focus:ring-blue-500"
              />
              <div>
                <div className="font-medium">持续运行（守护进程）</div>
                <div className="text-sm text-gray-400">每小时自动更新数据，持续运行</div>
              </div>
            </label>
          </div>
        </div>

        {/* 提交按钮 */}
        <div className="flex items-center space-x-4">
          <button
            type="submit"
            disabled={loading || (!formData.downloadTrader && !formData.downloadFunding && !formData.downloadBasis && !formData.downloadPremium)}
            className="px-6 py-3 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            {loading ? '启动中...' : '开始下载'}
          </button>
          
          {(!formData.downloadTrader && !formData.downloadFunding && !formData.downloadBasis && !formData.downloadPremium) && (
            <span className="text-sm text-yellow-400">请至少选择一种数据类型</span>
          )}
        </div>
      </form>

      {/* 消息提示 */}
      {message && (
        <div className={`p-4 rounded-lg ${
          message.type === 'success' 
            ? 'bg-green-500/20 text-green-400 border border-green-500/50' 
            : 'bg-red-500/20 text-red-400 border border-red-500/50'
        }`}>
          {message.text}
        </div>
      )}

      {/* 任务状态 */}
      {currentTask && (
        <div className="bg-gray-700/50 rounded-lg p-4">
          <h3 className="text-lg font-semibold mb-4">任务状态</h3>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-gray-400">任务ID:</span>
              <span className="font-mono text-sm">{currentTask.task_id}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-gray-400">状态:</span>
              <span className={`font-semibold ${
                currentTask.status === 'completed' ? 'text-green-400' :
                currentTask.status === 'failed' ? 'text-red-400' :
                'text-blue-400'
              }`}>
                {currentTask.status === 'completed' ? '已完成' :
                 currentTask.status === 'failed' ? '失败' :
                 '运行中'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-gray-400">消息:</span>
              <span className="text-sm">{currentTask.message}</span>
            </div>
            {currentTask.progress && (
              <div className="mt-4 pt-4 border-t border-gray-600">
                <div className="text-sm text-gray-400 mb-2">下载配置:</div>
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div>交易者数据: {currentTask.progress.download_trader ? '✅' : '❌'}</div>
                  <div>资金费率: {currentTask.progress.download_funding ? '✅' : '❌'}</div>
                  <div>基差数据: {currentTask.progress.download_basis ? '✅' : '❌'}</div>
                  <div>Premium Index: {currentTask.progress.download_premium ? '✅' : '❌'}</div>
                  <div>运行模式: {currentTask.progress.run_once ? '运行一次' : '持续运行'}</div>
                </div>
                {currentTask.progress.total_symbols && (
                  <div className="mt-2 text-sm text-gray-400">
                    进度: {currentTask.progress.symbols_processed || 0} / {currentTask.progress.total_symbols} 个交易对
                  </div>
                )}
              </div>
            )}
            {currentTask.start_time && (
              <div className="flex items-center justify-between mt-2 text-sm text-gray-400">
                <span>开始时间:</span>
                <span>{new Date(currentTask.start_time).toLocaleString('zh-CN')}</span>
              </div>
            )}
            {currentTask.end_time && (
              <div className="flex items-center justify-between text-sm text-gray-400">
                <span>结束时间:</span>
                <span>{new Date(currentTask.end_time).toLocaleString('zh-CN')}</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
