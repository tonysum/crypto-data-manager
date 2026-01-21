'use client'

import { useState, useEffect } from 'react'
import { API_URLS } from '../lib/api-config'

const API_BASE_URL = API_URLS.data

interface IPInfo {
  client_ip: string | null
  real_ip: string | null
  ip_service: string | null
  headers: {
    'X-Forwarded-For': string | null
    'X-Real-IP': string | null
    'CF-Connecting-IP': string | null
  }
}

export default function IPInfo() {
  const [ipInfo, setIpInfo] = useState<IPInfo | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchIPInfo = async () => {
    setLoading(true)
    setError(null)
    
    try {
      const response = await fetch(`${API_BASE_URL}/api/ip-info`)
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: '获取IP信息失败' }))
        throw new Error(errorData.detail || '获取IP信息失败')
      }
      
      const data = await response.json()
      setIpInfo(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : '获取IP信息失败')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchIPInfo()
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold mb-2">IP地址信息</h2>
          <p className="text-gray-400">查看当前IP地址信息</p>
        </div>
        <button
          onClick={fetchIPInfo}
          disabled={loading}
          className={`px-4 py-2 rounded-lg font-medium transition-colors ${
            loading
              ? 'bg-gray-600 cursor-not-allowed'
              : 'bg-blue-600 hover:bg-blue-700'
          }`}
        >
          {loading ? '刷新中...' : '🔄 刷新'}
        </button>
      </div>

      {error && (
        <div className="p-4 bg-red-500/20 text-red-400 border border-red-500/50 rounded-lg">
          {error}
        </div>
      )}

      {ipInfo && (
        <div className="grid md:grid-cols-2 gap-6">
          {/* 客户端IP（可能是VPN IP） */}
          <div className="p-6 bg-gray-800/50 rounded-lg border border-gray-700">
            <div className="flex items-center mb-4">
              <span className="text-3xl mr-3">🌐</span>
              <div>
                <h3 className="text-lg font-semibold">客户端IP</h3>
                <p className="text-sm text-gray-400">从请求头获取（可能是VPN IP）</p>
              </div>
            </div>
            <div className="mt-4">
              {ipInfo.client_ip ? (
                <div className="bg-gray-900/50 p-4 rounded-lg">
                  <code className="text-green-400 text-lg font-mono break-all">
                    {ipInfo.client_ip}
                  </code>
                </div>
              ) : (
                <p className="text-gray-500">无法获取</p>
              )}
            </div>
          </div>

          {/* 真实IP */}
          <div className="p-6 bg-gray-800/50 rounded-lg border border-gray-700">
            <div className="flex items-center mb-4">
              <span className="text-3xl mr-3">📍</span>
              <div>
                <h3 className="text-lg font-semibold">真实IP</h3>
                <p className="text-sm text-gray-400">通过外部API获取</p>
              </div>
            </div>
            <div className="mt-4">
              {ipInfo.real_ip ? (
                <div className="bg-gray-900/50 p-4 rounded-lg">
                  <code className="text-blue-400 text-lg font-mono break-all">
                    {ipInfo.real_ip}
                  </code>
                </div>
              ) : (
                <p className="text-gray-500">无法获取</p>
              )}
            </div>
            {ipInfo.ip_service && (
              <p className="text-xs text-gray-500 mt-2">
                数据来源: {ipInfo.ip_service}
              </p>
            )}
          </div>
        </div>
      )}

      {/* 请求头信息（调试用） */}
      {ipInfo && (ipInfo.headers['X-Forwarded-For'] || ipInfo.headers['X-Real-IP'] || ipInfo.headers['CF-Connecting-IP']) && (
        <div className="p-4 bg-blue-500/10 border border-blue-500/30 rounded-lg">
          <h4 className="text-sm font-semibold text-blue-400 mb-2">请求头信息（调试）</h4>
          <div className="text-xs text-gray-400 space-y-1 font-mono">
            {ipInfo.headers['X-Forwarded-For'] && (
              <p>X-Forwarded-For: {ipInfo.headers['X-Forwarded-For']}</p>
            )}
            {ipInfo.headers['X-Real-IP'] && (
              <p>X-Real-IP: {ipInfo.headers['X-Real-IP']}</p>
            )}
            {ipInfo.headers['CF-Connecting-IP'] && (
              <p>CF-Connecting-IP: {ipInfo.headers['CF-Connecting-IP']}</p>
            )}
          </div>
        </div>
      )}

      {/* 使用说明 */}
      <div className="p-4 bg-gray-800/50 border border-gray-700 rounded-lg">
        <h4 className="text-sm font-semibold text-gray-300 mb-2">💡 说明</h4>
        <ul className="text-xs text-gray-400 space-y-1">
          <li>• <strong>客户端IP</strong>: 从HTTP请求头获取的IP地址，如果使用VPN，这里显示的是VPN的IP</li>
          <li>• <strong>真实IP</strong>: 通过外部IP查询服务获取的真实公网IP地址</li>
          <li>• 如果两个IP相同，说明没有使用VPN或代理</li>
          <li>• 如果两个IP不同，说明可能使用了VPN或代理服务</li>
        </ul>
      </div>
    </div>
  )
}
