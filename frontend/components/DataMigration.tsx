'use client'

import { useState, useEffect } from 'react'
import { API_URLS } from '../lib/api-config'

const API_BASE_URL = API_URLS.data

interface MigrationTask {
  task_id: string
  status: 'running' | 'completed' | 'failed'
  message: string
  progress?: any
  start_time?: string
  end_time?: string
}

export default function DataMigration() {
  const [formData, setFormData] = useState({
    targetHost: '',
    targetPort: '5432',
    targetDb: 'crypto_data',
    targetUser: 'postgres',
    targetPassword: '',
    method: 'dump',
    tableFilter: '',
    skipExisting: false,
    compareOnly: false,
  })
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)
  const [currentTask, setCurrentTask] = useState<MigrationTask | null>(null)
  const [polling, setPolling] = useState(false)
  const [showPassword, setShowPassword] = useState(false)

  // 轮询任务状态
  useEffect(() => {
    if (!polling || !currentTask?.task_id) return

    const interval = setInterval(async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/api/migration/status/${currentTask.task_id}`)
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
      const payload: any = {
        target_host: formData.targetHost,
        target_port: parseInt(formData.targetPort),
        target_db: formData.targetDb,
        target_user: formData.targetUser,
        target_password: formData.targetPassword,
        method: formData.method,
        skip_existing: formData.skipExisting,
        compare_only: formData.compareOnly,
      }

      if (formData.tableFilter) {
        payload.table_filter = formData.tableFilter
      }

      const response = await fetch(`${API_BASE_URL}/api/migration/start`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || '启动迁移失败')
      }

      const data = await response.json()
      setCurrentTask(data)
      setPolling(true)
      setMessage({
        type: 'success',
        text: data.message || '迁移任务已启动'
      })
    } catch (error: any) {
      setLoading(false)
      setMessage({
        type: 'error',
        text: error.message || '启动迁移失败'
      })
    }
  }

  const renderProgress = () => {
    if (!currentTask?.progress) return null

    const progress = currentTask.progress

    if (formData.compareOnly) {
      // 对比模式
      return (
        <div className="mt-4 p-4 bg-blue-900/30 rounded-lg border border-blue-700">
          <h3 className="font-semibold mb-2 text-white">对比结果</h3>
          <div className="space-y-1 text-sm text-gray-300">
            <p>源数据库表数量: {progress.source_count}</p>
            <p>目标数据库表数量: {progress.target_count}</p>
            <p>共同表数量: {progress.common_count}</p>
            {progress.only_in_source && progress.only_in_source.length > 0 && (
              <p className="text-orange-400">
                仅在源数据库中的表: {progress.only_in_source.length} 个
              </p>
            )}
            {progress.only_in_target && progress.only_in_target.length > 0 && (
              <p className="text-blue-400">
                仅在目标数据库中的表: {progress.only_in_target.length} 个
              </p>
            )}
          </div>
        </div>
      )
    }

    if (progress.total !== undefined) {
      // Python 方法进度
      return (
        <div className="mt-4 p-4 bg-blue-900/30 rounded-lg border border-blue-700">
          <h3 className="font-semibold mb-2 text-white">迁移进度</h3>
          <div className="space-y-1 text-sm text-gray-300">
            <p>总表数: {progress.total}</p>
            <p className="text-green-400">✓ 成功: {progress.success}</p>
            <p className="text-red-400">✗ 失败: {progress.failed}</p>
            <p>总迁移行数: {progress.total_rows?.toLocaleString() || 0}</p>
          </div>
        </div>
      )
    }

    return null
  }

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-2xl font-bold mb-6 text-white">数据迁移</h1>

      <form onSubmit={handleSubmit} className="bg-gray-800/50 backdrop-blur-sm p-6 rounded-lg shadow-xl space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">目标数据库主机</label>
            <input
              type="text"
              value={formData.targetHost}
              onChange={(e) => setFormData({ ...formData, targetHost: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              placeholder="8.216.33.6"
            />
          </div>

          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">端口</label>
            <input
              type="number"
              value={formData.targetPort}
              onChange={(e) => setFormData({ ...formData, targetPort: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">数据库名</label>
            <input
              type="text"
              value={formData.targetDb}
              onChange={(e) => setFormData({ ...formData, targetDb: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">用户名</label>
            <input
              type="text"
              value={formData.targetUser}
              onChange={(e) => setFormData({ ...formData, targetUser: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            />
          </div>

          <div className="col-span-2">
            <label className="block text-sm font-medium mb-1 text-gray-300">密码</label>
            <div className="relative">
              <input
                type={showPassword ? "text" : "password"}
                value={formData.targetPassword}
                onChange={(e) => setFormData({ ...formData, targetPassword: e.target.value })}
                className="w-full px-3 py-2 pr-10 bg-gray-700 border border-gray-600 rounded-md text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-300 focus:outline-none"
                aria-label={showPassword ? "隐藏密码" : "显示密码"}
              >
                {showPassword ? (
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
                  </svg>
                ) : (
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l3.59 3.59m0 0A9.953 9.953 0 0112 5c4.478 0 8.268 2.943 9.543 7a10.025 10.025 0 01-4.132 5.411m0 0L21 21" />
                  </svg>
                )}
              </button>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">迁移方法</label>
            <select
              value={formData.method}
              onChange={(e) => setFormData({ ...formData, method: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="dump">pg_dump（推荐，速度快）</option>
              <option value="python">Python 脚本（支持增量迁移）</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium mb-1 text-gray-300">表过滤（可选）</label>
            <input
              type="text"
              value={formData.tableFilter}
              onChange={(e) => setFormData({ ...formData, tableFilter: e.target.value })}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="如: K1d 表示只迁移K1d开头的表"
            />
          </div>

          <div className="col-span-2 space-y-2">
            <label className="flex items-center text-gray-300 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.skipExisting}
                onChange={(e) => setFormData({ ...formData, skipExisting: e.target.checked })}
                className="mr-2 w-4 h-4 text-blue-600 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
              />
              <span className="text-sm">跳过已存在的数据（增量迁移）</span>
            </label>

            <label className="flex items-center text-gray-300 cursor-pointer">
              <input
                type="checkbox"
                checked={formData.compareOnly}
                onChange={(e) => setFormData({ ...formData, compareOnly: e.target.checked })}
                className="mr-2 w-4 h-4 text-blue-600 bg-gray-700 border-gray-600 rounded focus:ring-blue-500"
              />
              <span className="text-sm">只对比数据库，不执行迁移</span>
            </label>
          </div>
        </div>

        {message && (
          <div className={`p-3 rounded-md ${
            message.type === 'success' 
              ? 'bg-green-900/50 text-green-300 border border-green-700' 
              : 'bg-red-900/50 text-red-300 border border-red-700'
          }`}>
            {message.text}
          </div>
        )}

        {currentTask && (
          <div className="p-4 bg-gray-700/50 rounded-lg border border-gray-600">
            <div className="flex items-center justify-between mb-2">
              <span className="font-semibold text-white">任务状态: {currentTask.status}</span>
              {currentTask.status === 'running' && (
                <span className="text-sm text-blue-400 animate-pulse">运行中...</span>
              )}
            </div>
            <p className="text-sm text-gray-300">{currentTask.message}</p>
            {renderProgress()}
          </div>
        )}

        <button
          type="submit"
          disabled={loading || polling}
          className="w-full py-3 px-4 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed font-medium transition-colors"
        >
          {loading || polling ? '处理中...' : '开始迁移'}
        </button>
      </form>
    </div>
  )
}
